# Imtiaz Lifestyle — Production Deployment & Architecture Guide

> **Resilient Backend & Real-Time Synchronization Engine for Mobile & Web**  
> Google Minimalist Material 3 Interface with Offline-First IndexedDB and Cloud Persistence.

---

## 🏛️ Architecture Overview

The enhanced **Imtiaz Lifestyle** architecture is designed for zero-latency user interaction, resilient offline operation, and bi-directional real-time cloud synchronization.

```
                      ┌────────────────────────────────────────┐
                      │    Mobile Client (PWA / Responsive)    │
                      │  • Optimistic UI (0ms latency)         │
                      │  • IndexedDB Storage Engine            │
                      │  • Offline Outbox Queue                │
                      └──────────────────┬─────────────────────┘
                                         │  WebSocket / REST
                                         ▼
┌────────────────────────────────────────────────────────────────────────┐
│                   Imtiaz Lifestyle Backend Gateway                     │
│  • Tornado Asynchronous Event Broker                                   │
│  • Strict JSON Schema Validation (models.py)                           │
│  • Last-Write-Wins (LWW) Conflict Resolution (updated_at + version)    │
│  • RESTful V1 API: /api/v1/agenda, /api/v1/diary, /api/v1/morning      │
│  • Offline Batch Outbox Reconciliation: /api/v1/sync/batch             │
└──────────────┬─────────────────────────┬───────────────────────────────┘
               │                         │
               ▼                         ▼
┌─────────────────────────────┐   ┌──────────────────────────────────────┐
│  Local SQLite WAL Database  │   │     Cloud Persistence Adapters       │
│  • Write-Ahead Logging (WAL)│   │  • Cloudflare Workers D1             │
│  • Sub-ms Indexed Queries   │   │  • Supabase PostgREST (PostgreSQL)   │
│  • Sync Audit Log           │   │  • Zero-Configuration Local Fallback │
└─────────────────────────────┘   └──────────────────────────────────────┘
```

---

## 🚀 1. Local & LAN Execution

### Prerequisites
- Python 3.10+
- Tornado (`pip install tornado`)
- WebSockets (`pip install websockets`)

### Running the Server
```bash
cd /root/imtiaz_lifestyle

# Run migrations (automatically verifies schema & indices)
python3 migrate.py

# Launch the server
python3 server.py
```

### Accessing Locally & On Mobile
- **Local Web**: `http://localhost:8080`
- **Local Network (LAN)**: Open `http://<your-lan-ip>:8080` on any iPhone, Android phone, or iPad connected to the same Wi-Fi.
- **Offline Mode**: If Wi-Fi disconnects, the app switches to **Offline Mode**, storing all tasks and reflections in IndexedDB. When connection returns, changes are automatically pushed to the cloud.

---

## 🌐 2. Exposing the Server Globally (Public Internet Access)

### Option A: Cloudflare Tunnel (Recommended — Free & Secure)
1. Install `cloudflared`:
   ```bash
   curl -L --output cloudflared.deb https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm64.deb
   dpkg -i cloudflared.deb
   ```
2. Start quick tunnel:
   ```bash
   cloudflared tunnel --url http://localhost:8080
   ```
   Cloudflare provides a public HTTPS URL (e.g. `https://random-subdomain.trycloudflare.com`) with WebSocket support enabled by default.

### Option B: Ngrok
```bash
ngrok http 8080
```

---

## ☁️ 3. Cloudflare Workers D1 Deployment (Serverless)

The repository includes pre-built Cloudflare Worker configuration in `cloudflare/`:

1. **Install Wrangler**:
   ```bash
   npm install -g wrangler
   ```
2. **Authenticate with Cloudflare**:
   ```bash
   wrangler login
   ```
3. **Create the D1 Database**:
   ```bash
   wrangler d1 create imtiaz-lifestyle-db
   ```
   Copy the `database_id` into `cloudflare/wrangler.toml`.
4. **Execute Schema Migration**:
   ```bash
   wrangler d1 execute imtiaz-lifestyle-db --file=cloudflare/schema.sql
   ```
5. **Deploy**:
   ```bash
   cd cloudflare
   wrangler deploy
   ```

---

## ⚡ 4. Supabase Integration (Managed PostgreSQL)

1. Open your [Supabase Dashboard](https://app.supabase.com) and go to the **SQL Editor**.
2. Run the migration script in `supabase/schema.sql`.
3. Set environment variables:
   ```bash
   export CLOUD_PROVIDER="supabase"
   export SUPABASE_URL="https://your-project.supabase.co"
   export SUPABASE_ANON_KEY="your-anon-or-service-role-key"
   ```
4. Restart `server.py`. The application will sync records with Supabase automatically.

---

## 🧪 5. Automated Verification & Testing

Run the included automated test suite to verify REST CRUD, validation, and real-time WebSocket sync:

```bash
cd /root/imtiaz_lifestyle
python3 test_resilient_sync.py
```

Expected output:
```
==================================================
▶ Test 1: RESTful API CRUD & Schema Validation
==================================================
  ✓ Schema validator rejected empty title with HTTP 400
  ✓ Schema validator rejected invalid date with HTTP 400
  ✓ Created 24h task via REST API
  ✓ Updated task status to 'deferred'
  ✓ Updated task status to 'completed'
  ✓ Created Markdown Diary Entry
  ✓ Upserted Morning 15m Plan & pushed goals to 24h schedule
  ✓ Retrieved Cloud Storage Provider Status

==================================================
▶ Test 2: Multi-Device WebSocket Real-time Sync
==================================================
  ✓ Connected simulated Mobile and Web clients simultaneously
  ✓ Web client received instantaneous broadcast from Mobile: SAVE_TASK
  ✓ Mobile client received status update (completed) from Web client!
  ✓ Real-time multi-device sync test completed successfully

==================================================
🎉 ALL TESTS PASSED! Backend is resilient & verified.
==================================================
```
