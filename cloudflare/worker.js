/**
 * Cloudflare Worker for Imtiaz Lifestyle
 * Implements RESTful API and WebSocket Durable Object / Hibernation with Cloudflare D1
 */

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;

    // CORS Headers
    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, PUT, PATCH, DELETE, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Client-ID',
    };

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders, status: 204 });
    }

    try {
      // 1. GET /api/v1/agenda?date=YYYY-MM-DD
      if (path === '/api/v1/agenda' && request.method === 'GET') {
        const date = url.searchParams.get('date') || new Date().toISOString().substring(0, 10);
        const { results } = await env.DB.prepare(
          'SELECT * FROM agenda_tasks WHERE date = ? ORDER BY time_slot ASC, order_index ASC'
        ).bind(date).all();

        return Response.json({ date, tasks: results || [] }, { headers: corsHeaders });
      }

      // 2. POST /api/v1/agenda
      if (path === '/api/v1/agenda' && request.method === 'POST') {
        const body = await request.json();
        const now = new Date().toISOString();
        const id = body.id || `task_${Date.now()}`;
        const date = body.date || now.substring(0, 10);
        const status = body.status || 'pending';
        const completed = status === 'completed' ? 1 : 0;

        await env.DB.prepare(`
          INSERT OR REPLACE INTO agenda_tasks (
            id, date, time_slot, time_start, time_end, title, description, category, category_tags,
            status, priority, completed, order_index, version, created_at, updated_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `).bind(
          id, date, body.time_slot || '09:00', body.time_start || '09:00', body.time_end || '',
          body.title || 'Untitled', body.description || '', body.category || 'Work',
          JSON.stringify(body.category_tags || [body.category || 'Work']),
          status, body.priority || 'Medium', completed, body.order_index || 0,
          1, now, now
        ).run();

        return Response.json({ status: 'success', id }, { headers: corsHeaders, status: 201 });
      }

      // 3. GET /api/v1/diary
      if (path === '/api/v1/diary' && request.method === 'GET') {
        const date = url.searchParams.get('date');
        let query = 'SELECT * FROM diary_entries';
        let params = [];
        if (date) {
          query += ' WHERE date = ? ORDER BY time ASC';
          params.push(date);
        } else {
          query += ' ORDER BY date DESC, time ASC LIMIT 50';
        }
        const { results } = await env.DB.prepare(query).bind(...params).all();
        return Response.json({ entries: results || [] }, { headers: corsHeaders });
      }

      // 4. GET /api/v1/morning
      if (path === '/api/v1/morning' && request.method === 'GET') {
        const date = url.searchParams.get('date') || new Date().toISOString().substring(0, 10);
        const { results } = await env.DB.prepare(
          'SELECT * FROM morning_agendas WHERE date = ?'
        ).bind(date).all();
        return Response.json({ date, morning_agenda: results?.[0] || null }, { headers: corsHeaders });
      }

      return new Response('Not Found', { status: 404, headers: corsHeaders });
    } catch (e) {
      return Response.json({ error: e.message }, { status: 500, headers: corsHeaders });
    }
  }
};
