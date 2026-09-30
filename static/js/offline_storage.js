/**
 * Imtiaz Lifestyle - Client Offline-First IndexedDB Storage Engine
 * Manages local persistence for:
 * 1. agenda_tasks
 * 2. diary_entries
 * 3. morning_plans
 * 4. sync_outbox (offline queue with auto-reconciliation)
 */

(function(window) {
  'use strict';

  const DB_NAME = 'ImtiazLifestyleDB';
  const DB_VERSION = 2;

  let dbPromise = null;

  function openDB() {
    if (dbPromise) return dbPromise;

    dbPromise = new Promise((resolve, reject) => {
      if (!window.indexedDB) {
        console.warn('[IndexedDB] Not supported, falling back to in-memory/localStorage');
        resolve(null);
        return;
      }

      const request = indexedDB.open(DB_NAME, DB_VERSION);

      request.onupgradeneeded = (event) => {
        const db = event.target.result;

        if (!db.objectStoreNames.contains('agenda_tasks')) {
          const taskStore = db.createObjectStore('agenda_tasks', { keyPath: 'id' });
          taskStore.createIndex('date', 'date', { unique: false });
          taskStore.createIndex('updated_at', 'updated_at', { unique: false });
        }

        if (!db.objectStoreNames.contains('diary_entries')) {
          const diaryStore = db.createObjectStore('diary_entries', { keyPath: 'id' });
          diaryStore.createIndex('date', 'date', { unique: false });
          diaryStore.createIndex('updated_at', 'updated_at', { unique: false });
        }

        if (!db.objectStoreNames.contains('morning_plans')) {
          const morningStore = db.createObjectStore('morning_plans', { keyPath: 'date' });
          morningStore.createIndex('updated_at', 'updated_at', { unique: false });
        }

        if (!db.objectStoreNames.contains('sync_outbox')) {
          db.createObjectStore('sync_outbox', { keyPath: 'queue_id', autoIncrement: true });
        }
      };

      request.onsuccess = (event) => {
        resolve(event.target.result);
      };

      request.onerror = (event) => {
        console.error('[IndexedDB] Error opening database:', event.target.error);
        reject(event.target.error);
      };
    });

    return dbPromise;
  }

  // ---------------------------------------------------------------------------
  // Outbox Offline Queue Management
  // ---------------------------------------------------------------------------
  async function queueOutboxAction(type, action, payload) {
    const db = await openDB();
    if (!db) return;

    return new Promise((resolve, reject) => {
      const tx = db.transaction('sync_outbox', 'readwrite');
      const store = tx.objectStore('sync_outbox');
      const item = {
        type: type, // 'agenda_task', 'diary_entry', 'morning_plan'
        action: action, // 'UPSERT', 'DELETE'
        payload: payload,
        timestamp: new Date().toISOString()
      };
      const req = store.add(item);
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }

  async function getPendingOutbox() {
    const db = await openDB();
    if (!db) return [];

    return new Promise((resolve, reject) => {
      const tx = db.transaction('sync_outbox', 'readonly');
      const store = tx.objectStore('sync_outbox');
      const req = store.getAll();
      req.onsuccess = () => resolve(req.result || []);
      req.onerror = () => reject(req.error);
    });
  }

  async function clearOutboxItems(queueIds) {
    const db = await openDB();
    if (!db || !queueIds || queueIds.length === 0) return;

    return new Promise((resolve, reject) => {
      const tx = db.transaction('sync_outbox', 'readwrite');
      const store = tx.objectStore('sync_outbox');
      queueIds.forEach(id => store.delete(id));
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  async function clearOutboxAll() {
    const db = await openDB();
    if (!db) return;

    return new Promise((resolve, reject) => {
      const tx = db.transaction('sync_outbox', 'readwrite');
      const store = tx.objectStore('sync_outbox');
      const req = store.clear();
      req.onsuccess = () => resolve();
      req.onerror = () => reject(req.error);
    });
  }

  // ---------------------------------------------------------------------------
  // Local Entity Storage (Fast Offline Reads/Writes)
  // ---------------------------------------------------------------------------
  async function saveLocalTasks(tasks) {
    const db = await openDB();
    if (!db || !tasks) return;
    return new Promise((resolve, reject) => {
      const tx = db.transaction('agenda_tasks', 'readwrite');
      const store = tx.objectStore('agenda_tasks');
      tasks.forEach(t => store.put(t));
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  async function getLocalTasksByDate(dateStr) {
    const db = await openDB();
    if (!db) return [];
    return new Promise((resolve, reject) => {
      const tx = db.transaction('agenda_tasks', 'readonly');
      const store = tx.objectStore('agenda_tasks');
      const index = store.index('date');
      const req = index.getAll(IDBKeyRange.only(dateStr));
      req.onsuccess = () => resolve(req.result || []);
      req.onerror = () => reject(req.error);
    });
  }

  async function saveLocalDiaries(entries) {
    const db = await openDB();
    if (!db || !entries) return;
    return new Promise((resolve, reject) => {
      const tx = db.transaction('diary_entries', 'readwrite');
      const store = tx.objectStore('diary_entries');
      entries.forEach(e => store.put(e));
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  async function getLocalDiariesByDate(dateStr) {
    const db = await openDB();
    if (!db) return [];
    return new Promise((resolve, reject) => {
      const tx = db.transaction('diary_entries', 'readonly');
      const store = tx.objectStore('diary_entries');
      const index = store.index('date');
      const req = index.getAll(IDBKeyRange.only(dateStr));
      req.onsuccess = () => resolve(req.result || []);
      req.onerror = () => reject(req.error);
    });
  }

  async function saveLocalMorningPlan(plan) {
    const db = await openDB();
    if (!db || !plan) return;
    return new Promise((resolve, reject) => {
      const tx = db.transaction('morning_plans', 'readwrite');
      const store = tx.objectStore('morning_plans');
      const req = store.put(plan);
      req.onsuccess = () => resolve();
      req.onerror = () => reject(req.error);
    });
  }

  async function getLocalMorningPlan(dateStr) {
    const db = await openDB();
    if (!db) return null;
    return new Promise((resolve, reject) => {
      const tx = db.transaction('morning_plans', 'readonly');
      const store = tx.objectStore('morning_plans');
      const req = store.get(dateStr);
      req.onsuccess = () => resolve(req.result || null);
      req.onerror = () => reject(req.error);
    });
  }

  // Export to global scope
  window.ImtiazStorage = {
    openDB,
    queueOutboxAction,
    getPendingOutbox,
    clearOutboxItems,
    clearOutboxAll,
    saveLocalTasks,
    getLocalTasksByDate,
    saveLocalDiaries,
    getLocalDiariesByDate,
    saveLocalMorningPlan,
    getLocalMorningPlan
  };

})(window);
