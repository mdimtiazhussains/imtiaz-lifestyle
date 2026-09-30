/**
 * Imtiaz Lifestyle - Production Reactive Engine & Sync Hub
 * Real-time synchronization across mobile and web with 15-minute morning agenda,
 * 24-hour daily planning, and offline-first IndexedDB resilience.
 */

(function(window) {
  'use strict';

  // ---------------------------------------------------------------------------
  // Global Application State
  // ---------------------------------------------------------------------------
  const state = {
    clientId: 'device_' + Math.random().toString(36).substr(2, 9),
    currentDate: getTodayDateString(),
    activeView: 'dual', // 'dual', 'agenda', 'diary', 'morning', 'simulator'
    searchQuery: '',
    diaryEntries: [],
    agendaTasks: [],
    morningAgenda: null,
    metrics: { total_tasks: 0, completed_tasks: 0, deferred_tasks: 0, pending_tasks: 0, completion_rate: 0, diary_count: 0, morning_completed: false },
    activeDevicesCount: 1,
    wsConnected: false,
    syncState: 'cloud-saved', // 'cloud-saved', 'syncing', 'offline'
    pendingOutboxCount: 0,
    
    // Morning Timer (15 minutes = 900 seconds)
    morningTimer: {
      totalSeconds: 15 * 60,
      remainingSeconds: 15 * 60,
      timerInterval: null,
      isRunning: false
    }
  };

  let ws = null;
  let reconnectTimeout = null;
  let reconnectAttempts = 0;
  let heartbeatInterval = null;

  function getTodayDateString() {
    const d = new Date();
    const year = d.getFullYear();
    const month = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  }

  function formatDisplayDate(dateStr) {
    if (!dateStr) return '';
    const [y, m, d] = dateStr.split('-').map(Number);
    const dateObj = new Date(y, m - 1, d);
    const options = { weekday: 'short', month: 'short', day: 'numeric', year: 'numeric' };
    return dateObj.toLocaleDateString(undefined, options);
  }

  // ---------------------------------------------------------------------------
  // Google Minimalist Sync Status Badge Controller
  // ---------------------------------------------------------------------------
  function setSyncStatus(status, text) {
    state.syncState = status;
    const badge = document.getElementById('syncStatusBadge');
    if (!badge) return;

    badge.className = `g-sync-status ${status}`;
    let icon = '';
    if (status === 'cloud-saved') {
      icon = '<span style="color:#137333;">☁️✓</span>';
    } else if (status === 'syncing') {
      icon = '<span class="spin-icon">🔄</span>';
    } else if (status === 'offline') {
      icon = '<span>📴</span>';
    } else {
      icon = '<span class="pulse-dot"></span>';
    }
    badge.innerHTML = `${icon} <span>${text}</span>`;
  }

  async function updateOutboxCounter() {
    if (window.ImtiazStorage) {
      try {
        const pending = await window.ImtiazStorage.getPendingOutbox();
        state.pendingOutboxCount = pending.length;
        if (state.pendingOutboxCount > 0 && !state.wsConnected) {
          setSyncStatus('offline', `Offline (${state.pendingOutboxCount} pending)`);
        }
      } catch (e) {
        console.warn('Error reading outbox:', e);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Real-time WebSocket Client with Auto-Retry & Backoff
  // ---------------------------------------------------------------------------
  function initWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const host = window.location.host;
    const wsUrl = `${protocol}//${host}/ws`;

    setSyncStatus('syncing', 'Syncing...');

    try {
      ws = new WebSocket(wsUrl);

      ws.onopen = async () => {
        state.wsConnected = true;
        reconnectAttempts = 0;
        setSyncStatus('cloud-saved', 'Saved to Cloud');

        if (reconnectTimeout) {
          clearTimeout(reconnectTimeout);
          reconnectTimeout = null;
        }

        // Start heartbeat ping every 25 seconds
        if (heartbeatInterval) clearInterval(heartbeatInterval);
        heartbeatInterval = setInterval(() => {
          if (ws && ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ action: 'PING', client_id: state.clientId }));
          }
        }, 25000);

        // Reconcile pending offline outbox queue first
        await flushOfflineOutbox();

        // Fetch fresh server state
        sendWsAction('GET_DAY_DATA', { date: state.currentDate });
      };

      ws.onclose = () => {
        state.wsConnected = false;
        if (heartbeatInterval) clearInterval(heartbeatInterval);
        updateOutboxCounter();
        scheduleReconnect();
      };

      ws.onerror = (err) => {
        console.warn('[WS] Network note:', err);
        ws.close();
      };

      ws.onmessage = (event) => {
        try {
          const msg = JSON.parse(event.data);
          handleWebSocketMessage(msg);
        } catch (e) {
          console.error('[WS] Failed to parse message:', e);
        }
      };
    } catch (err) {
      console.warn('[WS] Initialization failed, running in offline mode:', err);
      state.wsConnected = false;
      scheduleReconnect();
    }
  }

  function scheduleReconnect() {
    if (reconnectTimeout) return;
    // Exponential backoff: 2s, 4s, 8s, up to 16s
    reconnectAttempts++;
    const delay = Math.min(16000, 1000 * Math.pow(2, Math.min(reconnectAttempts, 4)));
    setSyncStatus('offline', `Offline (Retry in ${Math.round(delay/1000)}s)`);

    reconnectTimeout = setTimeout(() => {
      reconnectTimeout = null;
      initWebSocket();
    }, delay);
  }

  function sendWsAction(action, data = {}) {
    const payload = {
      action: action,
      client_id: state.clientId,
      date: state.currentDate,
      ...data
    };
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(payload));
      return true;
    }
    return false;
  }

  async function handleWebSocketMessage(msg) {
    if (msg.type === 'CONNECTION_ESTABLISHED') {
      state.activeDevicesCount = msg.active_devices || 1;
      setSyncStatus('cloud-saved', `Saved to Cloud (${state.activeDevicesCount} dev)`);
    } else if (msg.type === 'DEVICE_PRESENCE_CHANGE') {
      state.activeDevicesCount = msg.active_devices || 1;
      setSyncStatus('cloud-saved', `Saved to Cloud (${state.activeDevicesCount} dev)`);
    } else if (msg.type === 'DAY_DATA' || msg.type === 'DAY_DATA_UPDATED') {
      const data = msg.data;
      if (data && data.date === state.currentDate) {
        state.diaryEntries = data.diary_entries || [];
        state.agendaTasks = data.agenda_tasks || [];
        state.morningAgenda = data.morning_agenda || null;
        state.metrics = data.metrics || state.metrics;

        // Cache locally into IndexedDB for instant offline access
        if (window.ImtiazStorage) {
          window.ImtiazStorage.saveLocalTasks(state.agendaTasks);
          window.ImtiazStorage.saveLocalDiaries(state.diaryEntries);
          if (state.morningAgenda) {
            window.ImtiazStorage.saveLocalMorningPlan(state.morningAgenda);
          }
        }

        renderAllViews();
        setSyncStatus('cloud-saved', 'Saved to Cloud');
      }
    } else if (msg.type === 'BATCH_SYNC_ACK') {
      console.log(`[Sync] Batch synchronized: ${msg.processed_count} items`);
      if (window.ImtiazStorage) {
        await window.ImtiazStorage.clearOutboxAll();
      }
      setSyncStatus('cloud-saved', 'All changes saved');
    }
  }

  // ---------------------------------------------------------------------------
  // Offline Outbox Flusher
  // ---------------------------------------------------------------------------
  async function flushOfflineOutbox() {
    if (!window.ImtiazStorage) return;
    try {
      const pendingItems = await window.ImtiazStorage.getPendingOutbox();
      if (!pendingItems || pendingItems.length === 0) return;

      setSyncStatus('syncing', `Syncing ${pendingItems.length} changes...`);

      if (ws && ws.readyState === WebSocket.OPEN) {
        sendWsAction('BATCH_SYNC', { items: pendingItems });
      } else {
        // Fallback to REST batch endpoint
        const res = await fetch('/api/v1/sync/batch', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-Client-ID': state.clientId },
          body: JSON.stringify({ items: pendingItems, date: state.currentDate })
        });
        if (res.ok) {
          await window.ImtiazStorage.clearOutboxAll();
          setSyncStatus('cloud-saved', 'Saved to Cloud');
        }
      }
    } catch (e) {
      console.warn('Failed to flush outbox:', e);
    }
  }

  async function loadInitialData(dateStr) {
    // 1. Try local IndexedDB first for instant 0ms startup
    if (window.ImtiazStorage) {
      try {
        const localTasks = await window.ImtiazStorage.getLocalTasksByDate(dateStr);
        const localDiaries = await window.ImtiazStorage.getLocalDiariesByDate(dateStr);
        const localMorning = await window.ImtiazStorage.getLocalMorningPlan(dateStr);
        if (localTasks.length > 0 || localDiaries.length > 0 || localMorning) {
          state.agendaTasks = localTasks;
          state.diaryEntries = localDiaries;
          state.morningAgenda = localMorning;
          renderAllViews();
        }
      } catch (e) {
        console.warn('IndexedDB initial read:', e);
      }
    }

    // 2. Fetch fresh state from REST API or WebSocket
    try {
      const res = await fetch(`/api/v1/agenda?date=${dateStr}`);
      if (res.ok) {
        const agendaRes = await res.json();
        state.agendaTasks = agendaRes.tasks || [];
      }
      const resD = await fetch(`/api/v1/diary?date=${dateStr}`);
      if (resD.ok) {
        const diaryRes = await resD.json();
        state.diaryEntries = diaryRes.entries || [];
      }
      const resM = await fetch(`/api/v1/morning?date=${dateStr}`);
      if (resM.ok) {
        const morningRes = await resM.json();
        state.morningAgenda = morningRes.morning_agenda || null;
      }
      renderAllViews();
    } catch (err) {
      console.warn('REST initial fetch failed, relying on offline cache:', err);
    }
  }

  // ---------------------------------------------------------------------------
  // View Routing & Navigation
  // ---------------------------------------------------------------------------
  function setView(viewName) {
    state.activeView = viewName;

    // Desktop Tabs
    document.querySelectorAll('.g-tab-btn').forEach(btn => {
      btn.classList.toggle('active', btn.dataset.view === viewName);
    });

    // Mobile Bottom Nav
    document.querySelectorAll('.bottom-nav-item').forEach(btn => {
      btn.classList.toggle('active', btn.dataset.view === viewName);
    });

    // Container toggles
    const views = ['Dual', 'Diary', 'Agenda', 'Morning', 'Simulator'];
    views.forEach(v => {
      const el = document.getElementById(`view${v}`);
      if (el) el.classList.toggle('active', viewName.toLowerCase() === v.toLowerCase());
    });

    renderAllViews();
  }

  function changeDate(daysOffset) {
    const [y, m, d] = state.currentDate.split('-').map(Number);
    const dateObj = new Date(y, m - 1, d);
    dateObj.setDate(dateObj.getDate() + daysOffset);
    const newY = dateObj.getFullYear();
    const newM = String(dateObj.getMonth() + 1).padStart(2, '0');
    const newD = String(dateObj.getDate()).padStart(2, '0');
    setDate(`${newY}-${newM}-${newD}`);
  }

  function setDate(dateStr) {
    state.currentDate = dateStr;
    updateDateDisplay();
    if (!sendWsAction('GET_DAY_DATA', { date: state.currentDate })) {
      loadInitialData(dateStr);
    }
  }

  function updateDateDisplay() {
    const dateText = document.getElementById('currentDateText');
    const hiddenInput = document.getElementById('datePickerInput');
    if (dateText) dateText.textContent = formatDisplayDate(state.currentDate);
    if (hiddenInput) hiddenInput.value = state.currentDate;
  }

  // ---------------------------------------------------------------------------
  // 15-Minute Morning Agenda Component
  // ---------------------------------------------------------------------------
  function initMorningTimer() {
    updateTimerDisplay();
    const startBtn = document.getElementById('startTimerBtn');
    const resetBtn = document.getElementById('resetTimerBtn');
    if (startBtn) startBtn.addEventListener('click', toggleMorningTimer);
    if (resetBtn) resetBtn.addEventListener('click', resetMorningTimer);
  }

  function toggleMorningTimer() {
    const t = state.morningTimer;
    const startBtn = document.getElementById('startTimerBtn');
    if (t.isRunning) {
      clearInterval(t.timerInterval);
      t.isRunning = false;
      if (startBtn) startBtn.innerHTML = '▶ Start';
    } else {
      t.isRunning = true;
      if (startBtn) startBtn.innerHTML = '⏸ Pause';
      t.timerInterval = setInterval(() => {
        if (t.remainingSeconds > 0) {
          t.remainingSeconds--;
          updateTimerDisplay();
        } else {
          clearInterval(t.timerInterval);
          t.isRunning = false;
          if (startBtn) startBtn.innerHTML = '▶ Restart';
          alert('☀️ 15-Minute Morning Agenda finished! You are clear, aligned, and ready to win the day.');
        }
      }, 1000);
    }
  }

  function resetMorningTimer() {
    const t = state.morningTimer;
    clearInterval(t.timerInterval);
    t.isRunning = false;
    t.remainingSeconds = t.totalSeconds;
    const startBtn = document.getElementById('startTimerBtn');
    if (startBtn) startBtn.innerHTML = '▶ Start';
    updateTimerDisplay();
  }

  function updateTimerDisplay() {
    const timerEl = document.getElementById('morningTimerDisplay');
    if (!timerEl) return;
    const mins = Math.floor(state.morningTimer.remainingSeconds / 60);
    const secs = state.morningTimer.remainingSeconds % 60;
    timerEl.textContent = `${String(mins).padStart(2, '0')}:${String(secs).padStart(2, '0')}`;
  }

  function renderMorningAgendaForm() {
    const m = state.morningAgenda || {};

    const g1 = document.getElementById('morningG1');
    const g2 = document.getElementById('morningG2');
    const g3 = document.getElementById('morningG3');
    const f1 = document.getElementById('morningF1');
    const f2 = document.getElementById('morningF2');
    const f3 = document.getElementById('morningF3');
    const aff = document.getElementById('morningAffirmation');
    const notes = document.getElementById('morningNotes');

    const gratitudes = Array.isArray(m.gratitudes) ? m.gratitudes : [m.gratitude_1, m.gratitude_2, m.gratitude_3];
    const objectives = Array.isArray(m.daily_objectives) ? m.daily_objectives : [m.focus_goal_1, m.focus_goal_2, m.focus_goal_3];

    if (g1) g1.value = gratitudes[0] || '';
    if (g2) g2.value = gratitudes[1] || '';
    if (g3) g3.value = gratitudes[2] || '';
    if (f1) f1.value = objectives[0] || '';
    if (f2) f2.value = objectives[1] || '';
    if (f3) f3.value = objectives[2] || '';
    if (aff) aff.value = m.affirmation || 'I approach each hour with intentionality, clarity, and calm focus.';
    if (notes) notes.value = m.notes || '';

    renderWaterTracker(m.hydration_completed || m.water_glasses || 0);

    const energyPills = document.querySelectorAll('.energy-pill');
    energyPills.forEach(pill => {
      pill.classList.toggle('active', pill.dataset.energy === (m.energy_level || 'High'));
    });

    const statusChip = document.getElementById('morningStatusChip');
    if (statusChip) {
      if (m.completed) {
        statusChip.innerHTML = '✓ Completed & Synced';
        statusChip.style.background = 'var(--g-green-surface)';
        statusChip.style.color = '#137333';
      } else {
        statusChip.innerHTML = '☀️ 15-Min Kickstart';
        statusChip.style.background = 'var(--g-yellow-surface)';
        statusChip.style.color = '#b06000';
      }
    }
  }

  function renderWaterTracker(count) {
    const container = document.getElementById('waterTrackerContainer');
    if (!container) return;
    container.innerHTML = '';
    for (let i = 1; i <= 8; i++) {
      const drop = document.createElement('div');
      drop.className = `water-drop ${i <= count ? 'filled' : ''}`;
      drop.innerHTML = '💧';
      drop.title = `Glass ${i} of 8`;
      drop.addEventListener('click', () => {
        saveWaterCount(i === count ? i - 1 : i);
      });
      container.appendChild(drop);
    }
  }

  function saveWaterCount(newCount) {
    if (!state.morningAgenda) {
      state.morningAgenda = { date: state.currentDate };
    }
    state.morningAgenda.hydration_completed = newCount;
    state.morningAgenda.water_glasses = newCount;
    renderWaterTracker(newCount);
    saveMorningAgendaData(false);
  }

  async function saveMorningAgendaData(pushToGoals = false) {
    const g1 = document.getElementById('morningG1')?.value || '';
    const g2 = document.getElementById('morningG2')?.value || '';
    const g3 = document.getElementById('morningG3')?.value || '';
    const f1 = document.getElementById('morningF1')?.value || '';
    const f2 = document.getElementById('morningF2')?.value || '';
    const f3 = document.getElementById('morningF3')?.value || '';
    const aff = document.getElementById('morningAffirmation')?.value || '';
    const notes = document.getElementById('morningNotes')?.value || '';

    const planData = {
      id: `morning_${state.currentDate}`,
      date: state.currentDate,
      completed: 1,
      completed_at: new Date().toISOString(),
      daily_objectives: [f1, f2, f3].filter(Boolean),
      gratitudes: [g1, g2, g3].filter(Boolean),
      gratitude_1: g1,
      gratitude_2: g2,
      gratitude_3: g3,
      focus_goal_1: f1,
      focus_goal_2: f2,
      focus_goal_3: f3,
      affirmation: aff,
      hydration_target: 8,
      hydration_completed: state.morningAgenda?.hydration_completed || 4,
      water_glasses: state.morningAgenda?.hydration_completed || 4,
      mindset_score: 5,
      focus_score: 9,
      energy_level: document.querySelector('.energy-pill.active')?.dataset.energy || 'High',
      notes: notes,
      version: (state.morningAgenda?.version || 1) + 1,
      updated_at: new Date().toISOString()
    };

    // Optimistic UI Update
    state.morningAgenda = planData;
    renderMorningAgendaForm();

    // Persist locally in IndexedDB
    if (window.ImtiazStorage) {
      await window.ImtiazStorage.saveLocalMorningPlan(planData);
    }

    // Try WebSocket broadcast, else queue for offline sync
    const sent = sendWsAction('SAVE_MORNING_AGENDA', {
      morning: planData,
      push_goals_to_agenda: pushToGoals
    });

    if (!sent && window.ImtiazStorage) {
      await window.ImtiazStorage.queueOutboxAction('morning_plan', 'UPSERT', planData);
      setSyncStatus('offline', 'Offline (Saved locally)');
      updateOutboxCounter();
    }

    if (pushToGoals) {
      // Create rich reflection diary entry automatically
      const diaryContent = `### ☀️ 15-Minute Morning Routine Reflection\n` +
        `**Date:** ${formatDisplayDate(state.currentDate)}\n\n` +
        `#### 🙏 Morning Gratitudes\n` +
        `1. ${g1 || 'Peace of mind & gratitude'}\n` +
        `2. ${g2 || 'Energy and vital discipline'}\n` +
        `3. ${g3 || 'Continuous self-mastery'}\n\n` +
        `#### 🎯 Top 3 Non-Negotiables\n` +
        `- [ ] **Priority 1:** ${f1 || 'Deep Focus Morning Block'}\n` +
        `- [ ] **Priority 2:** ${f2 || 'Core Execution Deliverable'}\n` +
        `- [ ] **Priority 3:** ${f3 || 'Physical Movement & Wind-down'}\n\n` +
        `> "${aff}"`;

      const diaryEntry = {
        id: `diary_morning_${state.currentDate}`,
        date: state.currentDate,
        time: new Date().toTimeString().substring(0, 5),
        title: 'Morning Kickstart Reflection & Daily Blueprint',
        content: diaryContent,
        mood: 'Energized',
        energy: 5,
        tags: ['Morning Agenda', 'Intentions', 'Blueprint'],
        version: 1,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      };

      saveDiaryDirect(diaryEntry);
    }

    showToast(pushToGoals ? '☀️ Morning Agenda saved & pushed to 24-Hour Schedule!' : 'Saved successfully!');
  }

  // ---------------------------------------------------------------------------
  // 24-Hour Agenda Engine (pending, completed, deferred)
  // ---------------------------------------------------------------------------
  function renderAgendaTimeline() {
    const container = document.getElementById('timeline24hContainer');
    const dualContainer = document.getElementById('dualTimelineContainer');
    if (!container && !dualContainer) return;

    const q = state.searchQuery.toLowerCase();
    const tasks = state.agendaTasks.filter(t => {
      if (!q) return true;
      return (t.title && t.title.toLowerCase().includes(q)) ||
             (t.description && t.description.toLowerCase().includes(q)) ||
             (t.category && t.category.toLowerCase().includes(q)) ||
             (t.status && t.status.toLowerCase().includes(q));
    });

    const hoursHtml = build24HourRows(tasks);

    if (container) container.innerHTML = hoursHtml;
    if (dualContainer) dualContainer.innerHTML = hoursHtml;

    updateNowIndicator();
    updateProgressMetrics();
    checkDailyReviewBanner();
  }

  function build24HourRows(tasks) {
    let html = '';
    for (let h = 0; h < 24; h++) {
      const hourStr = String(h).padStart(2, '0') + ':00';
      const slotTasks = tasks.filter(t => {
        const slot = t.time_slot || t.time_start || '09:00';
        return parseInt(slot.split(':')[0], 10) === h;
      });

      let periodTag = '';
      if (h === 0) periodTag = '<span style="font-size:10px; color:#80868b; margin-left:4px;">(Midnight)</span>';
      else if (h === 5) periodTag = '<span style="font-size:10px; color:#1a73e8; margin-left:4px;">(Dawn)</span>';
      else if (h === 12) periodTag = '<span style="font-size:10px; color:#f9ab00; margin-left:4px;">(Noon)</span>';
      else if (h === 21) periodTag = '<span style="font-size:10px; color:#8430ce; margin-left:4px;">(Night)</span>';

      html += `
        <div class="hour-row" data-hour="${h}">
          <div class="hour-label">
            ${hourStr}
            ${periodTag}
          </div>
          <div class="hour-track">
            <button class="quick-add-btn" onclick="window.imtiazApp.openQuickAddTaskModal('${hourStr}')">+ Add Slot</button>
            ${slotTasks.map(t => renderTaskItem(t)).join('')}
          </div>
        </div>
      `;
    }
    return html;
  }

  function renderTaskItem(task) {
    const status = task.status || (task.completed ? 'completed' : 'pending');
    const isCompleted = status === 'completed';
    const isDeferred = status === 'deferred';

    let statusPillHtml = '';
    if (status === 'completed') {
      statusPillHtml = `<span class="status-pill status-completed" onclick="window.imtiazApp.cycleTaskStatus('${task.id}')" title="Click to cycle status">✓ Done</span>`;
    } else if (status === 'deferred') {
      statusPillHtml = `<span class="status-pill status-deferred" onclick="window.imtiazApp.cycleTaskStatus('${task.id}')" title="Click to cycle status">↪️ Deferred</span>`;
    } else {
      statusPillHtml = `<span class="status-pill status-pending" onclick="window.imtiazApp.cycleTaskStatus('${task.id}')" title="Click to cycle status">⚪ Pending</span>`;
    }

    const catClass = `cat-${(task.category || 'work').toLowerCase()}`;

    return `
      <div class="task-item ${status}" data-task-id="${task.id}">
        <div class="task-left">
          <div class="g-checkbox ${isCompleted ? 'checked' : ''}" onclick="window.imtiazApp.toggleTask('${task.id}')">
            ${isCompleted ? '✓' : ''}
          </div>
          <span class="task-time-pill">${task.time_start || task.time_slot || 'All-day'}${task.time_end ? ' - ' + task.time_end : ''}</span>
          <div style="display:flex; flex-direction:column; min-width:0;">
            <span class="task-text">${escapeHtml(task.title)}</span>
            ${task.description ? `<span style="font-size:11.5px; color:var(--g-text-secondary);">${escapeHtml(task.description)}</span>` : ''}
          </div>
        </div>
        <div style="display:flex; align-items:center; gap:6px;">
          ${statusPillHtml}
          <span class="tag-badge ${catClass}">${escapeHtml(task.category || 'Work')}</span>
          ${task.priority === 'High' ? '<span style="color:var(--g-red); font-size:11px; font-weight:600;">HIGH</span>' : ''}
          <div class="task-actions">
            <button class="g-icon-btn" style="width:26px; height:26px; font-size:12px;" onclick="window.imtiazApp.editTask('${task.id}')" title="Edit">✏️</button>
            <button class="g-icon-btn" style="width:26px; height:26px; font-size:12px;" onclick="window.imtiazApp.deleteTask('${task.id}')" title="Delete">🗑️</button>
          </div>
        </div>
      </div>
    `;
  }

  function updateNowIndicator() {
    const today = getTodayDateString();
    if (state.currentDate !== today) return;

    const now = new Date();
    const h = now.getHours();
    const m = now.getMinutes();
    const row = document.querySelector(`.hour-row[data-hour="${h}"]`);
    if (!row) return;

    document.querySelectorAll('.now-line').forEach(el => el.remove());

    const line = document.createElement('div');
    line.className = 'now-line';
    const offsetPercent = (m / 60) * 100;
    line.style.top = `${offsetPercent}%`;
    line.title = `Current Time: ${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
    row.appendChild(line);
  }

  function updateProgressMetrics() {
    const total = state.agendaTasks.length;
    const completed = state.agendaTasks.filter(t => t.status === 'completed' || t.completed === 1).length;
    const deferred = state.agendaTasks.filter(t => t.status === 'deferred').length;
    const rate = total > 0 ? Math.round((completed / total) * 100) : 0;

    document.querySelectorAll('.progress-fill').forEach(f => f.style.width = `${rate}%`);
    document.querySelectorAll('.progress-label').forEach(l => {
      let extra = deferred > 0 ? ` (${deferred} deferred)` : '';
      l.textContent = `${completed} of ${total} tasks (${rate}%)${extra}`;
    });
  }

  function checkDailyReviewBanner() {
    const hour = new Date().getHours();
    const banner = document.getElementById('eveningReviewBanner');
    if (banner) {
      banner.style.display = (hour >= 18 && state.currentDate === getTodayDateString()) ? 'flex' : 'none';
    }
  }

  async function cycleTaskStatus(taskId) {
    const task = state.agendaTasks.find(t => t.id === taskId);
    if (!task) return;

    const statuses = ['pending', 'completed', 'deferred'];
    const currentStatus = task.status || (task.completed ? 'completed' : 'pending');
    const nextIdx = (statuses.indexOf(currentStatus) + 1) % statuses.length;
    const nextStatus = statuses[nextIdx];

    task.status = nextStatus;
    task.completed = nextStatus === 'completed' ? 1 : 0;
    task.updated_at = new Date().toISOString();

    // Optimistic UI update
    renderAgendaTimeline();

    // Local IndexedDB update
    if (window.ImtiazStorage) {
      await window.ImtiazStorage.saveLocalTasks([task]);
    }

    const sent = sendWsAction('UPDATE_TASK_STATUS', { id: taskId, status: nextStatus, completed: task.completed });
    if (!sent && window.ImtiazStorage) {
      await window.ImtiazStorage.queueOutboxAction('agenda_task', 'UPSERT', task);
      setSyncStatus('offline', 'Offline (Saved locally)');
      updateOutboxCounter();
    }
  }

  async function toggleTask(taskId) {
    const task = state.agendaTasks.find(t => t.id === taskId);
    if (!task) return;

    const newStatus = (task.status === 'completed' || task.completed === 1) ? 'pending' : 'completed';
    task.status = newStatus;
    task.completed = newStatus === 'completed' ? 1 : 0;
    task.updated_at = new Date().toISOString();

    renderAgendaTimeline();

    if (window.ImtiazStorage) {
      await window.ImtiazStorage.saveLocalTasks([task]);
    }

    const sent = sendWsAction('UPDATE_TASK_STATUS', { id: taskId, status: newStatus, completed: task.completed });
    if (!sent && window.ImtiazStorage) {
      await window.ImtiazStorage.queueOutboxAction('agenda_task', 'UPSERT', task);
      setSyncStatus('offline', 'Offline (Saved locally)');
      updateOutboxCounter();
    }
  }

  async function saveTaskDirect(task) {
    const existingIdx = state.agendaTasks.findIndex(t => t.id === task.id);
    if (existingIdx >= 0) {
      state.agendaTasks[existingIdx] = task;
    } else {
      state.agendaTasks.push(task);
    }
    renderAgendaTimeline();

    if (window.ImtiazStorage) {
      await window.ImtiazStorage.saveLocalTasks([task]);
    }

    const sent = sendWsAction('SAVE_TASK', { task: task });
    if (!sent && window.ImtiazStorage) {
      await window.ImtiazStorage.queueOutboxAction('agenda_task', 'UPSERT', task);
      setSyncStatus('offline', 'Offline (Saved locally)');
      updateOutboxCounter();
    }
  }

  async function deleteTask(taskId) {
    if (confirm('Delete this task?')) {
      state.agendaTasks = state.agendaTasks.filter(t => t.id !== taskId);
      renderAgendaTimeline();

      const sent = sendWsAction('DELETE_TASK', { id: taskId });
      if (!sent && window.ImtiazStorage) {
        await window.ImtiazStorage.queueOutboxAction('agenda_task', 'DELETE', { id: taskId });
        setSyncStatus('offline', 'Offline (Saved locally)');
        updateOutboxCounter();
      }
      showToast('Task deleted');
    }
  }

  // ---------------------------------------------------------------------------
  // Dual Diary Controller with Rich Markdown Support
  // ---------------------------------------------------------------------------
  function renderDiaryList() {
    const container = document.getElementById('diaryListContainer');
    const dualContainer = document.getElementById('dualDiaryListContainer');
    if (!container && !dualContainer) return;

    const q = state.searchQuery.toLowerCase();
    const entries = state.diaryEntries.filter(d => {
      if (!q) return true;
      return (d.title && d.title.toLowerCase().includes(q)) ||
             (d.content && d.content.toLowerCase().includes(q)) ||
             (d.mood && d.mood.toLowerCase().includes(q));
    });

    let html = '';
    if (entries.length === 0) {
      html = `
        <div class="g-card" style="text-align:center; padding:36px 20px; color:var(--g-text-secondary);">
          <div style="font-size:32px; margin-bottom:10px;">📖</div>
          <div style="font-size:15px; font-weight:500; color:var(--g-text-primary);">No reflections recorded yet</div>
          <div style="font-size:12.5px; margin-top:4px; max-width:320px; margin-left:auto; margin-right:auto;">
            Record your daily insights, gratitude, and lessons in Imtiaz lifestyle.
          </div>
          <button class="g-btn-primary" style="margin-top:16px;" onclick="window.imtiazApp.openDiaryModal()">+ Write Entry</button>
        </div>
      `;
    } else {
      html = entries.map(d => renderDiaryCard(d)).join('');
    }

    if (container) container.innerHTML = html;
    if (dualContainer) dualContainer.innerHTML = html;
  }

  function renderDiaryCard(entry) {
    const moodClass = `mood-${(entry.mood || 'calm').toLowerCase()}`;
    const tags = Array.isArray(entry.tags) ? entry.tags : [];
    const renderedContent = renderMarkdownToHtml(entry.content || '');

    return `
      <div class="g-card" data-diary-id="${entry.id}">
        <div class="g-card-header">
          <div>
            <div class="g-card-meta">
              <span>🕒 ${entry.time || '12:00'}</span>
              <span class="mood-badge ${moodClass}">${escapeHtml(entry.mood || 'Reflective')}</span>
              ${entry.energy ? `<span style="color:#f9ab00;">${'★'.repeat(entry.energy)}</span>` : ''}
            </div>
            <h3 class="g-card-title" style="margin-top:4px;">${escapeHtml(entry.title)}</h3>
          </div>
          <div style="display:flex; gap:4px;">
            <button class="g-icon-btn" onclick="window.imtiazApp.editDiary('${entry.id}')" title="Edit">✏️</button>
            <button class="g-icon-btn" onclick="window.imtiazApp.deleteDiary('${entry.id}')" title="Delete">🗑️</button>
          </div>
        </div>
        <div class="g-card-content">${renderedContent}</div>
        ${tags.length > 0 ? `
          <div style="display:flex; gap:6px; flex-wrap:wrap; margin-top:12px;">
            ${tags.map(t => `<span class="tag-badge">#${escapeHtml(t)}</span>`).join('')}
          </div>
        ` : ''}
      </div>
    `;
  }

  // Client-side minimalist Markdown parser
  function renderMarkdownToHtml(md) {
    if (!md) return '';
    let html = escapeHtml(md);

    // Headers
    html = html.replace(/^### (.*$)/gim, '<h3 style="font-size:14px; font-weight:600; margin:8px 0 4px 0;">$1</h3>');
    html = html.replace(/^## (.*$)/gim, '<h2 style="font-size:15px; font-weight:600; margin:10px 0 4px 0;">$1</h2>');
    html = html.replace(/^# (.*$)/gim, '<h1 style="font-size:16px; font-weight:700; margin:12px 0 6px 0;">$1</h1>');

    // Blockquotes
    html = html.replace(/^\> (.*$)/gim, '<blockquote style="border-left:3px solid var(--g-blue); padding-left:10px; margin:6px 0; color:var(--g-text-secondary); font-style:italic;">$1</blockquote>');

    // Checkboxes
    html = html.replace(/^- \[x\] (.*$)/gim, '<div style="display:flex; align-items:center; gap:6px; color:#137333;"><span>☑</span> <span style="text-decoration:line-through;">$1</span></div>');
    html = html.replace(/^- \[ \] (.*$)/gim, '<div style="display:flex; align-items:center; gap:6px;"><span>☐</span> <span>$1</span></div>');

    // Bullet lists
    html = html.replace(/^- (.*$)/gim, '<li style="margin-left:18px;">$1</li>');

    // Bold & Italic
    html = html.replace(/\*\*(.*?)\*\*/gim, '<strong>$1</strong>');
    html = html.replace(/\*(.*?)\*/gim, '<em>$1</em>');

    // Inline Code
    html = html.replace(/`(.*?)`/gim, '<code style="background:#f1f3f4; padding:2px 5px; border-radius:4px; font-size:12px;">$1</code>');

    // Line breaks
    html = html.replace(/\n/gim, '<br>');

    return html;
  }

  async function saveDiaryDirect(entry) {
    const existingIdx = state.diaryEntries.findIndex(d => d.id === entry.id);
    if (existingIdx >= 0) {
      state.diaryEntries[existingIdx] = entry;
    } else {
      state.diaryEntries.unshift(entry);
    }
    renderDiaryList();

    if (window.ImtiazStorage) {
      await window.ImtiazStorage.saveLocalDiaries([entry]);
    }

    const sent = sendWsAction('SAVE_DIARY', { entry: entry });
    if (!sent && window.ImtiazStorage) {
      await window.ImtiazStorage.queueOutboxAction('diary_entry', 'UPSERT', entry);
      setSyncStatus('offline', 'Offline (Saved locally)');
      updateOutboxCounter();
    }
  }

  async function deleteDiary(diaryId) {
    if (confirm('Delete this diary reflection?')) {
      state.diaryEntries = state.diaryEntries.filter(d => d.id !== diaryId);
      renderDiaryList();

      const sent = sendWsAction('DELETE_DIARY', { id: diaryId });
      if (!sent && window.ImtiazStorage) {
        await window.ImtiazStorage.queueOutboxAction('diary_entry', 'DELETE', { id: diaryId });
        setSyncStatus('offline', 'Offline (Saved locally)');
        updateOutboxCounter();
      }
      showToast('Diary reflection deleted');
    }
  }

  // ---------------------------------------------------------------------------
  // Modals & User Actions
  // ---------------------------------------------------------------------------
  function openTaskModal(taskId = null, defaultSlot = '09:00') {
    closeFabSpeedDial();
    const modal = document.getElementById('taskModal');
    const modalTitle = document.getElementById('taskModalTitle');
    const idInput = document.getElementById('taskInputId');
    const titleInput = document.getElementById('taskInputTitle');
    const descInput = document.getElementById('taskInputDesc');
    const slotInput = document.getElementById('taskInputSlot');
    const endInput = document.getElementById('taskInputEnd');
    const catSelect = document.getElementById('taskInputCat');
    const statusSelect = document.getElementById('taskInputStatus');
    const prioritySelect = document.getElementById('taskInputPriority');

    if (taskId) {
      const t = state.agendaTasks.find(item => item.id === taskId);
      if (t) {
        modalTitle.textContent = 'Edit 24h Task';
        idInput.value = t.id;
        titleInput.value = t.title || '';
        descInput.value = t.description || '';
        slotInput.value = t.time_start || t.time_slot || defaultSlot;
        endInput.value = t.time_end || '';
        catSelect.value = t.category || 'Work';
        if (statusSelect) statusSelect.value = t.status || (t.completed ? 'completed' : 'pending');
        prioritySelect.value = t.priority || 'Medium';
      }
    } else {
      modalTitle.textContent = 'Schedule 24h Task';
      idInput.value = '';
      titleInput.value = '';
      descInput.value = '';
      slotInput.value = defaultSlot;
      endInput.value = '';
      catSelect.value = 'Work';
      if (statusSelect) statusSelect.value = 'pending';
      prioritySelect.value = 'Medium';
    }

    modal.classList.add('active');
    titleInput.focus();
  }

  function setTaskTimeFromChip(timeStr) {
    const slotInput = document.getElementById('taskInputSlot');
    if (slotInput) slotInput.value = timeStr;
  }

  async function saveTaskFromModal() {
    const id = document.getElementById('taskInputId').value;
    const title = document.getElementById('taskInputTitle').value.trim();
    const desc = document.getElementById('taskInputDesc').value.trim();
    const slot = document.getElementById('taskInputSlot').value || '09:00';
    const end = document.getElementById('taskInputEnd').value || '';
    const cat = document.getElementById('taskInputCat').value || 'Work';
    const status = document.getElementById('taskInputStatus')?.value || 'pending';
    const prio = document.getElementById('taskInputPriority').value || 'Medium';

    if (!title) {
      alert('Please enter a task title');
      return;
    }

    const taskObj = {
      id: id || `task_${Date.now()}`,
      date: state.currentDate,
      time_slot: slot,
      time_start: slot,
      time_end: end,
      title: title,
      description: desc,
      category: cat,
      category_tags: [cat],
      status: status,
      completed: status === 'completed' ? 1 : 0,
      priority: prio,
      order_index: parseInt(slot.split(':')[0], 10),
      version: 1,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    };

    closeModal('taskModal');
    await saveTaskDirect(taskObj);
    showToast('Task saved & synced in real-time');
  }

  function openDiaryModal(diaryId = null) {
    closeFabSpeedDial();
    const modal = document.getElementById('diaryModal');
    const modalTitle = document.getElementById('diaryModalTitle');
    const idInput = document.getElementById('diaryInputId');
    const titleInput = document.getElementById('diaryInputTitle');
    const contentInput = document.getElementById('diaryInputContent');
    const timeInput = document.getElementById('diaryInputTime');
    const moodSelect = document.getElementById('diaryInputMood');
    const tagsInput = document.getElementById('diaryInputTags');

    // Switch to edit tab
    switchDiaryTab('edit');

    if (diaryId) {
      const d = state.diaryEntries.find(item => item.id === diaryId);
      if (d) {
        modalTitle.textContent = 'Edit Diary Reflection';
        idInput.value = d.id;
        titleInput.value = d.title || '';
        contentInput.value = d.content || '';
        timeInput.value = d.time || new Date().toTimeString().substring(0, 5);
        moodSelect.value = d.mood || 'Calm';
        tagsInput.value = Array.isArray(d.tags) ? d.tags.join(', ') : '';
      }
    } else {
      modalTitle.textContent = 'New Diary Reflection';
      idInput.value = '';
      titleInput.value = '';
      contentInput.value = '';
      timeInput.value = new Date().toTimeString().substring(0, 5);
      moodSelect.value = 'Focused';
      tagsInput.value = 'Lifestyle, Growth, Mindset';
    }

    modal.classList.add('active');
    titleInput.focus();
  }

  function switchDiaryTab(mode) {
    const editTab = document.getElementById('diaryTabEdit');
    const previewTab = document.getElementById('diaryTabPreview');
    const contentInput = document.getElementById('diaryInputContent');
    const previewBox = document.getElementById('diaryPreviewBox');

    if (mode === 'preview') {
      editTab?.classList.remove('active');
      previewTab?.classList.add('active');
      if (contentInput) contentInput.style.display = 'none';
      if (previewBox) {
        previewBox.style.display = 'block';
        previewBox.innerHTML = renderMarkdownToHtml(contentInput?.value || '*No content to preview*');
      }
    } else {
      previewTab?.classList.remove('active');
      editTab?.classList.add('active');
      if (contentInput) contentInput.style.display = 'block';
      if (previewBox) previewBox.style.display = 'none';
    }
  }

  async function saveDiaryFromModal() {
    const id = document.getElementById('diaryInputId').value;
    const title = document.getElementById('diaryInputTitle').value.trim();
    const content = document.getElementById('diaryInputContent').value.trim();
    const time = document.getElementById('diaryInputTime').value || '12:00';
    const mood = document.getElementById('diaryInputMood').value || 'Calm';
    const tagsStr = document.getElementById('diaryInputTags').value;
    const tags = tagsStr ? tagsStr.split(',').map(s => s.trim()).filter(Boolean) : [];

    if (!title && !content) {
      alert('Please provide a title or reflection content');
      return;
    }

    const entryObj = {
      id: id || `diary_${Date.now()}`,
      date: state.currentDate,
      time: time,
      title: title || 'Reflection',
      content: content,
      mood: mood,
      energy: 4,
      tags: tags,
      version: 1,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    };

    closeModal('diaryModal');
    await saveDiaryDirect(entryObj);
    showToast('Reflection saved & synchronized');
  }

  // ---------------------------------------------------------------------------
  // Floating Action Button (FAB) Speed Dial
  // ---------------------------------------------------------------------------
  function toggleFabSpeedDial() {
    const menu = document.getElementById('fabSpeedDial');
    if (menu) menu.classList.toggle('active');
  }

  function closeFabSpeedDial() {
    const menu = document.getElementById('fabSpeedDial');
    if (menu) menu.classList.remove('active');
  }

  function triggerEveningReview() {
    setView('diary');
    openDiaryModal();
    const titleInput = document.getElementById('diaryInputTitle');
    const contentInput = document.getElementById('diaryInputContent');
    const moodSelect = document.getElementById('diaryInputMood');
    if (titleInput) titleInput.value = 'Evening Daily Review & Wind-down';
    if (moodSelect) moodSelect.value = 'Reflective';
    if (contentInput) {
      contentInput.value = `### 🌙 Evening Daily Review\n` +
        `**Date:** ${formatDisplayDate(state.currentDate)}\n\n` +
        `#### 1. What were today's major wins?\n- \n\n` +
        `#### 2. What challenged me and what did I learn?\n- \n\n` +
        `#### 3. Gratitude before sleep:\n- Peaceful mind\n- Health & focus\n\n` +
        `> "Tomorrow is won tonight."`;
    }
  }

  function closeModal(modalId) {
    const el = document.getElementById(modalId);
    if (el) el.classList.remove('active');
  }

  function resetSampleDay() {
    if (confirm('Reset today to the standard Imtiaz Lifestyle 24-hour template and morning routine?')) {
      sendWsAction('RESET_SAMPLE_DATA');
      showToast('Template reset successfully');
    }
  }

  // ---------------------------------------------------------------------------
  // Cloud & LAN Network Modals
  // ---------------------------------------------------------------------------
  async function openNetworkModal() {
    const modal = document.getElementById('networkModal');
    const container = document.getElementById('networkUrlsContainer');
    if (!modal || !container) return;

    try {
      const res = await fetch('/api/network');
      const data = await res.json();
      let html = '<div style="display:flex; flex-direction:column; gap:8px;">';
      data.urls.forEach(url => {
        html += `
          <div style="display:flex; align-items:center; justify-content:space-between; background:var(--g-surface); padding:8px 12px; border-radius:var(--radius-sm); border:1px solid var(--g-border);">
            <code style="font-size:13px; color:var(--g-blue); font-weight:600;">${url}</code>
            <a href="${url}" target="_blank" class="g-btn-outline" style="padding:4px 10px; font-size:11px; text-decoration:none;">Open ↗</a>
          </div>
        `;
      });
      html += `</div>
        <div style="margin-top:14px; padding:10px; background:#f8f9fa; border-radius:8px; border:1px solid #dadce0;">
          <div style="font-weight:600; font-size:12px; margin-bottom:4px;">☁️ Cloud Provider Status:</div>
          <div style="font-size:12px; color:var(--g-text-secondary);">Active: <b>${data.cloud_provider || 'Local Resilient SQLite'}</b></div>
          <button class="g-btn-outline" style="margin-top:8px; font-size:11px; padding:4px 10px;" onclick="window.imtiazApp.openCloudConfigModal()">Configure Cloudflare / Supabase →</button>
        </div>
      `;
      container.innerHTML = html;
      modal.classList.add('active');
    } catch (e) {
      console.error(e);
    }
  }

  async function openCloudConfigModal() {
    closeModal('networkModal');
    const modal = document.getElementById('cloudModal');
    if (!modal) return;
    try {
      const res = await fetch('/api/v1/cloud/config');
      const cfg = await res.json();
      const select = document.getElementById('cloudProviderSelect');
      if (select) select.value = cfg.active_provider || 'local';
      modal.classList.add('active');
    } catch (e) {
      console.error(e);
    }
  }

  async function testCloudConnection() {
    const provider = document.getElementById('cloudProviderSelect')?.value || 'local';
    const statusBox = document.getElementById('cloudTestResult');
    if (statusBox) statusBox.innerHTML = '<span class="spin-icon">🔄</span> Testing connection...';

    try {
      const res = await fetch('/api/v1/cloud/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'test', provider: provider })
      });
      const data = await res.json();
      if (statusBox) {
        if (data.success) {
          statusBox.innerHTML = `<span style="color:#137333;">✓ ${data.message}</span>`;
        } else {
          statusBox.innerHTML = `<span style="color:var(--g-red);">✕ ${data.message}</span>`;
        }
      }
    } catch (e) {
      if (statusBox) statusBox.innerHTML = `<span style="color:var(--g-red);">Error: ${e.message}</span>`;
    }
  }

  function showToast(text) {
    const toast = document.getElementById('gToast');
    if (!toast) return;
    toast.textContent = text;
    toast.style.display = 'block';
    toast.style.opacity = '1';
    clearTimeout(toast._timeout);
    toast._timeout = setTimeout(() => {
      toast.style.opacity = '0';
      setTimeout(() => toast.style.display = 'none', 250);
    }, 2400);
  }

  function escapeHtml(str) {
    if (!str) return '';
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }

  function renderAllViews() {
    renderAgendaTimeline();
    renderDiaryList();
    renderMorningAgendaForm();
  }

  // ---------------------------------------------------------------------------
  // Global App Initialization
  // ---------------------------------------------------------------------------
  async function initApp() {
    updateDateDisplay();
    initMorningTimer();

    // Initialize offline IndexedDB storage & load local cached state
    if (window.ImtiazStorage) {
      await window.ImtiazStorage.openDB();
    }
    await loadInitialData(state.currentDate);

    // Connect WebSocket
    initWebSocket();

    // Listeners: Desktop Tabs & Mobile Bottom Nav
    document.querySelectorAll('.g-tab-btn').forEach(btn => {
      btn.addEventListener('click', () => setView(btn.dataset.view));
    });
    document.querySelectorAll('.bottom-nav-item').forEach(btn => {
      btn.addEventListener('click', () => setView(btn.dataset.view));
    });

    // Date Picker Input
    const picker = document.getElementById('datePickerInput');
    if (picker) {
      picker.addEventListener('change', (e) => {
        if (e.target.value) setDate(e.target.value);
      });
    }

    // Search Filter
    const search = document.getElementById('globalSearchInput');
    if (search) {
      search.addEventListener('input', (e) => {
        state.searchQuery = e.target.value.trim();
        renderAgendaTimeline();
        renderDiaryList();
      });
    }

    // Modal background dismiss & ESC key
    document.querySelectorAll('.modal-overlay').forEach(overlay => {
      overlay.addEventListener('click', (e) => {
        if (e.target === overlay) overlay.classList.remove('active');
      });
    });

    window.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') {
        document.querySelectorAll('.modal-overlay').forEach(o => o.classList.remove('active'));
        closeFabSpeedDial();
      }
    });

    // Keep live "now" indicator updated every minute
    setInterval(updateNowIndicator, 60000);
    setInterval(checkDailyReviewBanner, 60000);

    // Online / Offline window events
    window.addEventListener('online', () => {
      setSyncStatus('syncing', 'Reconnected, syncing...');
      initWebSocket();
    });

    window.addEventListener('offline', () => {
      setSyncStatus('offline', 'Offline Mode');
      updateOutboxCounter();
    });
  }

  // Public Interface for Inline HTML Handlers
  window.imtiazApp = {
    state,
    setView,
    changeDate,
    setDate,
    openQuickAddTaskModal: (slot) => openTaskModal(null, slot),
    openTaskModal,
    setTaskTimeFromChip,
    saveTaskFromModal,
    toggleTask,
    cycleTaskStatus,
    deleteTask,
    editTask: (id) => openTaskModal(id),
    openDiaryModal,
    switchDiaryTab,
    saveDiaryFromModal,
    editDiary: (id) => openDiaryModal(id),
    deleteDiary,
    saveMorningAgenda: () => saveMorningAgendaData(false),
    completeMorningRoutine: () => saveMorningAgendaData(true),
    toggleFabSpeedDial,
    triggerEveningReview,
    closeModal,
    resetSampleDay,
    openNetworkModal,
    openCloudConfigModal,
    testCloudConnection
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initApp);
  } else {
    initApp();
  }
})(window);
