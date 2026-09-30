# Imtiaz lifestyle — Dual Diary & Agenda Application

> **Real-Time Synchronized Dual Diary & 24-Hour Agenda Application for Mobile and Web**  
> Designed with Google's minimalist design aesthetic.

---

## ✨ Features Overview

### 1. Dual Diary & Agenda Engine
- **Diary Reflections**: Chronicle personal mindset, lessons, thoughts, and moods with rich tagging and energy ratings.
- **24-Hour Daily Agenda**: Comprehensive 00:00 to 23:59 day & night timeblocking, real-time "NOW" indicator line, category chips, priority flags, and dynamic completion tracking.
- **Unified Dual View**: Side-by-side split layout on desktop and fluid responsive flow on mobile.

### 2. 15-Minute Morning Planning Routine
- **Guided 15:00 Countdown Timer**: Keep morning planning focused and distraction-free.
- **Gratitude Triad**: Three mindful gratitude reflections.
- **Top 3 Daily Non-Negotiables**: Identify the essential deliverables that make today a victory.
- **Vitality Check**: Interactive 8-glass water hydration counter and energy state selector.
- **1-Click Push**: Automatically schedules the Top 3 priorities into today's 24-hour agenda slots and saves a morning diary reflection.

### 3. Real-Time Synchronization Across Mobile & Web
- **WebSocket Engine**: High-performance Tornado WebSocket broker. Any task check, new diary entry, or morning routine update broadcasts across all connected mobile devices and web browsers in milliseconds.
- **Persistent Storage**: Robust SQLite database (`imtiaz_lifestyle.db`).
- **Offline-First & PWA**: Installable to Android & iOS home screens via `manifest.json` and service worker caching.

### 4. Google Minimalist Aesthetic
- Clean Google Sans / Roboto typography.
- Authentic Google color palette (`#1a73e8`, `#34A853`, `#FBBC05`, `#EA4335`).
- Google pill search bar with real-time filtering.
- Google Material 3 cards, pill chips, and Floating Action Button (FAB).
- 100% visual parity between mobile screens and desktop displays.

---

## 🚀 Running the Application

The server runs on Python 3 with Tornado:

```bash
cd /root/imtiaz_lifestyle
python3 server.py
```

### Access URLs:
- **Local Web Browser**: [http://localhost:8080](http://localhost:8080)
- **Mobile LAN Access**: `http://<your-lan-ip>:8080` (displayed in the app under the sync badge)
- **Multi-Device Live Simulator**: Switch to the **📱 Multi-Device** tab to watch live 2-way real-time sync between a simulated Google Pixel and Web dashboard simultaneously.
