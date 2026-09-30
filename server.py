#!/usr/bin/env python3
"""
Imtiaz Lifestyle - Production-Ready Server & API Gateway
Provides:
1. RESTful APIs for Agenda, Diary, Morning 15m, and Batch Sync
2. Real-time WebSocket event broker with multi-device presence
3. Cloudflare D1 / Supabase cloud sync endpoints
4. Static file serving & PWA manifest support
"""

import os
import sys
import json
import socket
import datetime
import logging
from typing import Optional, Dict, Any

import tornado.ioloop
import tornado.web
import tornado.websocket

import database
from models import MorningPlanModel, AgendaTaskModel, DiaryEntryModel
from sync_engine import RealtimeWebSocketHandler, SyncHub
import cloud_adapters

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ImtiazServer")

PORT = int(os.environ.get("PORT", 8080))
STATIC_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")


def get_network_ips():
    ips = ["127.0.0.1", "localhost"]
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0.1)
        s.connect(("8.8.8.8", 80))
        lan_ip = s.getsockname()[0]
        s.close()
        if lan_ip not in ips:
            ips.append(lan_ip)
    except Exception:
        pass
    return ips


def seed_demo_data(conn, today_str):
    cursor = conn.cursor()
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

    cursor.execute("""
    INSERT OR REPLACE INTO morning_agendas (
        id, date, completed, completed_at, daily_objectives, gratitudes,
        gratitude_1, gratitude_2, gratitude_3, focus_goal_1, focus_goal_2, focus_goal_3,
        affirmation, hydration_target, hydration_completed, water_glasses,
        mindset_score, focus_score, energy_level, notes, version, created_at, updated_at
    ) VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
    )
    """, (
        f"morning_{today_str}",
        today_str,
        1,
        f"{today_str}T06:15:00Z",
        json.dumps([
            "Architect resilient multi-device sync engine",
            "Maintain 24-hour mindful task execution and hydration",
            "Deep 45-minute evening physical conditioning & reflection"
        ]),
        json.dumps([
            "Peaceful morning silence and fresh energy",
            "Clear vision for personal and professional growth",
            "Continuous discipline and vitality"
        ]),
        "Peaceful morning silence and fresh energy",
        "Clear vision for personal and professional growth",
        "Continuous discipline and vitality",
        "Architect resilient multi-device sync engine",
        "Maintain 24-hour mindful task execution and hydration",
        "Deep 45-minute evening physical conditioning & reflection",
        "I approach each hour with intentionality, clarity, and calm focus.",
        8,
        5,
        5,
        5,
        9,
        "High",
        "15-minute kickstart completed with high clarity. Day is structured for peak flow.",
        1,
        now_iso,
        now_iso
    ))

    sample_diaries = [
        (
            f"diary_{today_str}_1",
            today_str,
            "06:30",
            "Dawn Awakening & Intentionality",
            "Woke up before dawn. The 15-minute morning agenda aligned my mind immediately.\n\n### Morning Insights\n- The quiet hours of early morning are where the day is won.\n- Prioritizing clarity over busyness.\n- Hydration target: 8 glasses daily.",
            "Energized",
            5,
            json.dumps(["Morning Routine", "Mindset", "Clarity"]),
            json.dumps(["Fresh morning start", "Quiet focus"]),
            1,
            now_iso,
            now_iso
        ),
        (
            f"diary_{today_str}_2",
            today_str,
            "13:00",
            "Midday Momentum & Focus",
            "Productivity is high. The 24-hour timeblocking method removes all decision fatigue.\n\n> \"Discipline is freedom.\"\n\nEating clean, staying hydrated, and keeping distractions at zero.",
            "Focused",
            4,
            json.dumps(["Productivity", "Nutrition", "Focus"]),
            json.dumps(["Healthy nutritious meal", "Sustained drive"]),
            1,
            now_iso,
            now_iso
        )
    ]

    for d in sample_diaries:
        cursor.execute("""
        INSERT OR REPLACE INTO diary_entries (
            id, date, time, title, content, mood, energy, tags, gratitude_notes, version, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, d)

    sample_tasks = [
        (f"task_{today_str}_0500", today_str, "05:00", "05:00", "05:30", "Early Rise & Cold Hydration", "500ml lemon water, breathwork & quiet stretching", "Health", json.dumps(["Health"]), "completed", "High", 1, 0, 1, now_iso, now_iso),
        (f"task_{today_str}_0530", today_str, "05:30", "05:30", "06:00", "Physical Training & Movement", "Cardio conditioning and core strength session", "Health", json.dumps(["Health", "Movement"]), "completed", "High", 1, 1, 1, now_iso, now_iso),
        (f"task_{today_str}_0600", today_str, "06:00", "06:00", "06:15", "15-Minute Morning Kickstart", "Gratitude, top 3 priorities, daily affirmation", "Routine", json.dumps(["Routine", "Mindset"]), "completed", "High", 1, 2, 1, now_iso, now_iso),
        (f"task_{today_str}_0630", today_str, "06:30", "06:30", "07:30", "High-Priority Deep Work Block 1", "Core architectural deliverables & strategic analysis", "Work", json.dumps(["Work", "Deep Focus"]), "completed", "High", 1, 3, 1, now_iso, now_iso),
        (f"task_{today_str}_0800", today_str, "08:00", "08:00", "08:30", "Nutritious Breakfast & Fuel", "High protein breakfast and green tea", "Health", json.dumps(["Health", "Fuel"]), "completed", "Medium", 1, 4, 1, now_iso, now_iso),
        (f"task_{today_str}_0900", today_str, "09:00", "09:00", "11:30", "Deep Focus: Real-Time Sync Engine", "Building resilient multi-client synchronization", "Work", json.dumps(["Work", "Core"]), "completed", "High", 1, 5, 1, now_iso, now_iso),
        (f"task_{today_str}_1200", today_str, "12:00", "12:00", "13:00", "Clean Lunch & 15-Min Walk", "Sunshine exposure, disconnect from digital screens", "Personal", json.dumps(["Personal", "Wellness"]), "pending", "Medium", 0, 6, 1, now_iso, now_iso),
        (f"task_{today_str}_1400", today_str, "14:00", "14:00", "16:00", "Product Review & Communication", "Cross-device verification on mobile & web", "Work", json.dumps(["Work"]), "pending", "High", 0, 7, 1, now_iso, now_iso),
        (f"task_{today_str}_1700", today_str, "17:00", "17:00", "18:00", "Lifestyle Reading & Skill Growth", "30 pages of leadership and systems thinking", "Personal", json.dumps(["Personal", "Learning"]), "deferred", "Medium", 0, 8, 1, now_iso, now_iso),
        (f"task_{today_str}_1900", today_str, "19:00", "19:00", "20:00", "Family Dinner & Conversation", "Nourishing dinner, social connection", "Personal", json.dumps(["Personal"]), "pending", "High", 0, 9, 1, now_iso, now_iso),
        (f"task_{today_str}_2100", today_str, "21:00", "21:00", "21:30", "Evening Dual Diary Reflection", "Review today's achievements and lessons in Imtiaz lifestyle", "Routine", json.dumps(["Routine", "Reflection"]), "pending", "Medium", 0, 10, 1, now_iso, now_iso),
        (f"task_{today_str}_2200", today_str, "22:00", "22:00", "22:30", "Digital Sunset & Wind-Down", "Dim warm lights, zero blue light, prepare for restorative sleep", "Health", json.dumps(["Health", "Rest"]), "pending", "High", 0, 11, 1, now_iso, now_iso)
    ]

    for t in sample_tasks:
        cursor.execute("""
        INSERT OR REPLACE INTO agenda_tasks (
            id, date, time_slot, time_start, time_end, title, description, category, category_tags,
            status, priority, completed, order_index, version, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, t)

    conn.commit()


# ---------------------------------------------------------------------------
# Base API Request Handler with CORS & Error Formatting
# ---------------------------------------------------------------------------
class BaseApiHandler(tornado.web.RequestHandler):
    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with, content-type, authorization")
        self.set_header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")

    def options(self, *args, **kwargs):
        self.set_status(204)
        self.finish()

    def get_json_body(self) -> Dict[str, Any]:
        try:
            return json.loads(self.request.body.decode("utf-8")) if self.request.body else {}
        except Exception as e:
            raise tornado.web.HTTPError(400, reason=f"Invalid JSON: {str(e)}")

    def write_json(self, data: Any, status: int = 200):
        self.set_status(status)
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps(data, indent=2))


# ---------------------------------------------------------------------------
# RESTful API: Agenda Tasks
# ---------------------------------------------------------------------------
class ApiAgendaHandler(BaseApiHandler):
    def get(self, task_id: Optional[str] = None):
        if task_id:
            conn = database.get_db()
            cur = conn.cursor()
            cur.execute("SELECT * FROM agenda_tasks WHERE id = ?", (task_id,))
            row = cur.fetchone()
            conn.close()
            if not row:
                self.write_json({"error": "Task not found"}, 404)
                return
            t = dict(row)
            try:
                t["category_tags"] = json.loads(t.get("category_tags", "[]"))
            except Exception:
                t["category_tags"] = [t.get("category", "Work")]
            self.write_json(t)
        else:
            date_str = self.get_argument("date", datetime.date.today().strftime("%Y-%m-%d"))
            conn = database.get_db()
            cur = conn.cursor()
            cur.execute("SELECT * FROM agenda_tasks WHERE date = ? ORDER BY time_slot ASC, order_index ASC", (date_str,))
            tasks = [dict(r) for r in cur.fetchall()]
            conn.close()
            for t in tasks:
                try:
                    t["category_tags"] = json.loads(t.get("category_tags", "[]"))
                except Exception:
                    t["category_tags"] = [t.get("category", "Work")]
            self.write_json({"date": date_str, "tasks": tasks, "count": len(tasks)})

    def post(self, task_id: Optional[str] = None):
        body = self.get_json_body()
        valid, task, err = AgendaTaskModel.validate_and_sanitize(body)
        if not valid or not task:
            self.write_json({"error": err}, 400)
            return

        client_id = self.request.headers.get("X-Client-ID", "rest_client")
        saved_task, applied = database.upsert_agenda_task(task, client_id=client_id)

        # Broadcast via WebSocket
        SyncHub.broadcast({
            "type": "DAY_DATA_UPDATED",
            "action": "SAVE_TASK",
            "date": task["date"],
            "data": database.get_day_snapshot(task["date"]),
            "item": saved_task,
            "sender_id": client_id,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
        })
        self.write_json({"status": "success", "task": saved_task, "applied": applied}, 201)

    def put(self, task_id: Optional[str] = None):
        if not task_id:
            self.write_json({"error": "Task ID required"}, 400)
            return
        body = self.get_json_body()
        body["id"] = task_id
        valid, task, err = AgendaTaskModel.validate_and_sanitize(body)
        if not valid or not task:
            self.write_json({"error": err}, 400)
            return

        client_id = self.request.headers.get("X-Client-ID", "rest_client")
        saved_task, applied = database.upsert_agenda_task(task, client_id=client_id)

        SyncHub.broadcast({
            "type": "DAY_DATA_UPDATED",
            "action": "SAVE_TASK",
            "date": task["date"],
            "data": database.get_day_snapshot(task["date"]),
            "item": saved_task,
            "sender_id": client_id,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
        })
        self.write_json({"status": "success", "task": saved_task, "applied": applied})

    def patch(self, task_id: Optional[str] = None):
        """Update task status (pending, completed, deferred)."""
        if not task_id:
            self.write_json({"error": "Task ID required"}, 400)
            return
        body = self.get_json_body()
        status = body.get("status")
        if status not in ("pending", "completed", "deferred"):
            self.write_json({"error": f"Invalid status: {status}. Must be pending, completed, or deferred"}, 400)
            return

        conn = database.get_db()
        cur = conn.cursor()
        cur.execute("SELECT * FROM agenda_tasks WHERE id = ?", (task_id,))
        row = cur.fetchone()
        conn.close()

        if not row:
            self.write_json({"error": "Task not found"}, 404)
            return

        task_data = dict(row)
        task_data["status"] = status
        task_data["completed"] = 1 if status == "completed" else 0
        task_data["updated_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

        valid, task, _ = AgendaTaskModel.validate_and_sanitize(task_data)
        client_id = self.request.headers.get("X-Client-ID", "rest_client")
        saved_task, applied = database.upsert_agenda_task(task, client_id=client_id)

        SyncHub.broadcast({
            "type": "DAY_DATA_UPDATED",
            "action": "UPDATE_TASK_STATUS",
            "date": task["date"],
            "data": database.get_day_snapshot(task["date"]),
            "item": saved_task,
            "sender_id": client_id,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
        })
        self.write_json({"status": "success", "task": saved_task})

    def delete(self, task_id: Optional[str] = None):
        if not task_id:
            self.write_json({"error": "Task ID required"}, 400)
            return
        client_id = self.request.headers.get("X-Client-ID", "rest_client")
        deleted = database.delete_agenda_task(task_id, client_id=client_id)
        if deleted:
            date_str = self.get_argument("date", datetime.date.today().strftime("%Y-%m-%d"))
            SyncHub.broadcast({
                "type": "DAY_DATA_UPDATED",
                "action": "DELETE_TASK",
                "date": date_str,
                "deleted_id": task_id,
                "data": database.get_day_snapshot(date_str),
                "sender_id": client_id,
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
            })
            self.write_json({"status": "success", "deleted_id": task_id})
        else:
            self.write_json({"error": "Task not found"}, 404)


class ApiAgendaStatusHandler(BaseApiHandler):
    def patch(self, task_id: str):
        self.update_status(task_id)

    def put(self, task_id: str):
        self.update_status(task_id)

    def update_status(self, task_id: str):
        body = self.get_json_body()
        status = body.get("status")
        if status not in ("pending", "completed", "deferred"):
            self.write_json({"error": f"Invalid status: {status}. Must be pending, completed, or deferred"}, 400)
            return

        conn = database.get_db()
        cur = conn.cursor()
        cur.execute("SELECT * FROM agenda_tasks WHERE id = ?", (task_id,))
        row = cur.fetchone()
        conn.close()

        if not row:
            self.write_json({"error": "Task not found"}, 404)
            return

        task_data = dict(row)
        task_data["status"] = status
        task_data["completed"] = 1 if status == "completed" else 0
        task_data["updated_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

        valid, task, _ = AgendaTaskModel.validate_and_sanitize(task_data)
        client_id = self.request.headers.get("X-Client-ID", "rest_client")
        saved_task, applied = database.upsert_agenda_task(task, client_id=client_id)

        SyncHub.broadcast({
            "type": "DAY_DATA_UPDATED",
            "action": "UPDATE_TASK_STATUS",
            "date": task["date"],
            "data": database.get_day_snapshot(task["date"]),
            "item": saved_task,
            "sender_id": client_id,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
        })
        self.write_json({"status": "success", "task": saved_task})


# ---------------------------------------------------------------------------
# RESTful API: Diary Entries
# ---------------------------------------------------------------------------
class ApiDiaryHandler(BaseApiHandler):
    def get(self, entry_id: Optional[str] = None):
        if entry_id:
            conn = database.get_db()
            cur = conn.cursor()
            cur.execute("SELECT * FROM diary_entries WHERE id = ?", (entry_id,))
            row = cur.fetchone()
            conn.close()
            if not row:
                self.write_json({"error": "Diary entry not found"}, 404)
                return
            d = dict(row)
            try:
                d["tags"] = json.loads(d.get("tags", "[]"))
            except Exception:
                d["tags"] = []
            try:
                d["gratitude_notes"] = json.loads(d.get("gratitude_notes", "[]"))
            except Exception:
                d["gratitude_notes"] = []
            self.write_json(d)
        else:
            date_str = self.get_argument("date", None)
            conn = database.get_db()
            cur = conn.cursor()
            if date_str:
                cur.execute("SELECT * FROM diary_entries WHERE date = ? ORDER BY time ASC, created_at ASC", (date_str,))
            else:
                cur.execute("SELECT * FROM diary_entries ORDER BY date DESC, time ASC LIMIT 50")
            entries = [dict(r) for r in cur.fetchall()]
            conn.close()
            for d in entries:
                try:
                    d["tags"] = json.loads(d.get("tags", "[]"))
                except Exception:
                    d["tags"] = []
                try:
                    d["gratitude_notes"] = json.loads(d.get("gratitude_notes", "[]"))
                except Exception:
                    d["gratitude_notes"] = []
            self.write_json({"entries": entries, "count": len(entries)})

    def post(self, entry_id: Optional[str] = None):
        body = self.get_json_body()
        valid, entry, err = DiaryEntryModel.validate_and_sanitize(body)
        if not valid or not entry:
            self.write_json({"error": err}, 400)
            return

        client_id = self.request.headers.get("X-Client-ID", "rest_client")
        saved_entry, applied = database.upsert_diary_entry(entry, client_id=client_id)

        SyncHub.broadcast({
            "type": "DAY_DATA_UPDATED",
            "action": "SAVE_DIARY",
            "date": entry["date"],
            "data": database.get_day_snapshot(entry["date"]),
            "item": saved_entry,
            "sender_id": client_id,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
        })
        self.write_json({"status": "success", "entry": saved_entry, "applied": applied}, 201)

    def put(self, entry_id: Optional[str] = None):
        if not entry_id:
            self.write_json({"error": "Entry ID required"}, 400)
            return
        body = self.get_json_body()
        body["id"] = entry_id
        valid, entry, err = DiaryEntryModel.validate_and_sanitize(body)
        if not valid or not entry:
            self.write_json({"error": err}, 400)
            return

        client_id = self.request.headers.get("X-Client-ID", "rest_client")
        saved_entry, applied = database.upsert_diary_entry(entry, client_id=client_id)

        SyncHub.broadcast({
            "type": "DAY_DATA_UPDATED",
            "action": "SAVE_DIARY",
            "date": entry["date"],
            "data": database.get_day_snapshot(entry["date"]),
            "item": saved_entry,
            "sender_id": client_id,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
        })
        self.write_json({"status": "success", "entry": saved_entry, "applied": applied})

    def delete(self, entry_id: Optional[str] = None):
        if not entry_id:
            self.write_json({"error": "Entry ID required"}, 400)
            return
        client_id = self.request.headers.get("X-Client-ID", "rest_client")
        deleted = database.delete_diary_entry(entry_id, client_id=client_id)
        if deleted:
            date_str = self.get_argument("date", datetime.date.today().strftime("%Y-%m-%d"))
            SyncHub.broadcast({
                "type": "DAY_DATA_UPDATED",
                "action": "DELETE_DIARY",
                "date": date_str,
                "deleted_id": entry_id,
                "data": database.get_day_snapshot(date_str),
                "sender_id": client_id,
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
            })
            self.write_json({"status": "success", "deleted_id": entry_id})
        else:
            self.write_json({"error": "Entry not found"}, 404)


# ---------------------------------------------------------------------------
# RESTful API: Morning 15-Minute Planning
# ---------------------------------------------------------------------------
class ApiMorningHandler(BaseApiHandler):
    def get(self, date_str: Optional[str] = None):
        target_date = date_str or self.get_argument("date", datetime.date.today().strftime("%Y-%m-%d"))
        snapshot = database.get_day_snapshot(target_date)
        self.write_json({"date": target_date, "morning_agenda": snapshot["morning_agenda"]})

    def put(self, date_str: Optional[str] = None):
        target_date = date_str or self.get_argument("date", datetime.date.today().strftime("%Y-%m-%d"))
        body = self.get_json_body()
        body["date"] = target_date
        valid, morning, err = MorningPlanModel.validate_and_sanitize(body)
        if not valid or not morning:
            self.write_json({"error": err}, 400)
            return

        client_id = self.request.headers.get("X-Client-ID", "rest_client")
        saved_plan, applied = database.upsert_morning_plan(morning, client_id=client_id)

        # Auto-push top objectives into agenda tasks if requested
        if body.get("push_goals_to_agenda"):
            objectives = morning.get("daily_objectives", [])
            time_slots = [("09:00", "10:30"), ("11:00", "12:30"), ("14:00", "15:30")]
            for idx, obj in enumerate(objectives[:3]):
                if obj:
                    start_t, end_t = time_slots[idx]
                    t_id = f"task_m_{target_date}_{idx+1}"
                    t_data = {
                        "id": t_id,
                        "date": target_date,
                        "time_start": start_t,
                        "time_slot": start_t,
                        "time_end": end_t,
                        "title": f"🎯 Priority #{idx+1}: {obj}",
                        "description": "Auto-scheduled from Imtiaz 15-Minute Morning Kickstart",
                        "category": "Work",
                        "category_tags": ["Work", "High Priority"],
                        "status": "pending",
                        "priority": "High",
                        "order_index": idx + 10,
                    }
                    v, t, _ = AgendaTaskModel.validate_and_sanitize(t_data)
                    if v and t:
                        database.upsert_agenda_task(t, client_id=client_id)

        updated_day = database.get_day_snapshot(target_date)
        SyncHub.broadcast({
            "type": "DAY_DATA_UPDATED",
            "action": "SAVE_MORNING_AGENDA",
            "date": target_date,
            "data": updated_day,
            "item": saved_plan,
            "sender_id": client_id,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
        })
        self.write_json({"status": "success", "morning_agenda": saved_plan, "applied": applied})


# ---------------------------------------------------------------------------
# RESTful API: Batch Sync (Offline Outbox Reconciliation)
# ---------------------------------------------------------------------------
class ApiBatchSyncHandler(BaseApiHandler):
    def post(self):
        body = self.get_json_body()
        client_id = body.get("client_id", self.request.headers.get("X-Client-ID", "rest_sync_client"))
        items = body.get("items", [])
        date_str = body.get("date", datetime.date.today().strftime("%Y-%m-%d"))

        processed = 0
        for item in items:
            entity_type = item.get("type")
            action = item.get("action")
            payload = item.get("payload", {})

            if entity_type == "agenda_task":
                if action == "DELETE":
                    database.delete_agenda_task(payload.get("id"), client_id=client_id)
                else:
                    v, t, _ = AgendaTaskModel.validate_and_sanitize(payload)
                    if v and t:
                        database.upsert_agenda_task(t, client_id=client_id)
            elif entity_type == "diary_entry":
                if action == "DELETE":
                    database.delete_diary_entry(payload.get("id"), client_id=client_id)
                else:
                    v, d, _ = DiaryEntryModel.validate_and_sanitize(payload)
                    if v and d:
                        database.upsert_diary_entry(d, client_id=client_id)
            elif entity_type == "morning_plan":
                v, m, _ = MorningPlanModel.validate_and_sanitize(payload)
                if v and m:
                    database.upsert_morning_plan(m, client_id=client_id)
            processed += 1

        updated_day = database.get_day_snapshot(date_str)
        SyncHub.broadcast({
            "type": "DAY_DATA_UPDATED",
            "action": "BATCH_SYNC",
            "date": date_str,
            "data": updated_day,
            "sender_id": client_id,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
        })

        self.write_json({
            "status": "success",
            "processed_items": processed,
            "day_data": updated_day,
            "synced_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
        })


# ---------------------------------------------------------------------------
# RESTful API: Cloud Storage Configuration & Health
# ---------------------------------------------------------------------------
class ApiCloudConfigHandler(BaseApiHandler):
    def get(self):
        cfg = cloud_adapters.load_cloud_config()
        # Redact sensitive token details
        redacted = dict(cfg)
        if redacted.get("cloudflare_d1", {}).get("api_token"):
            redacted["cloudflare_d1"]["api_token"] = "••••••••" + redacted["cloudflare_d1"]["api_token"][-4:]
        if redacted.get("supabase", {}).get("anon_key"):
            redacted["supabase"]["anon_key"] = "••••••••" + redacted["supabase"]["anon_key"][-4:]
        self.write_json(redacted)

    def post(self):
        body = self.get_json_body()
        action = body.get("action", "save")

        if action == "test":
            provider = body.get("provider", "local")
            if provider == "cloudflare_d1":
                cf = body.get("cloudflare_d1", {})
                adapter = cloud_adapters.CloudflareD1Adapter(
                    cf.get("account_id", ""),
                    cf.get("database_id", ""),
                    cf.get("api_token", "")
                )
                success, msg = adapter.test_connection()
                self.write_json({"provider": provider, "success": success, "message": msg})
            elif provider == "supabase":
                sb = body.get("supabase", {})
                adapter = cloud_adapters.SupabaseAdapter(
                    sb.get("url", ""),
                    sb.get("anon_key", "")
                )
                success, msg = adapter.test_connection()
                self.write_json({"provider": provider, "success": success, "message": msg})
            else:
                self.write_json({"provider": "local", "success": True, "message": "Local SQLite WAL storage is fully active and healthy."})
        else:
            cfg = cloud_adapters.load_cloud_config()
            for key in ["active_provider", "cloudflare_d1", "supabase", "auto_sync_enabled"]:
                if key in body:
                    cfg[key] = body[key]
            cloud_adapters.save_cloud_config(cfg)
            self.write_json({"status": "saved", "config": cfg})


# ---------------------------------------------------------------------------
# Backward Compatibility & Utility Handlers
# ---------------------------------------------------------------------------
class ApiLegacyDataHandler(BaseApiHandler):
    def get(self):
        date_str = self.get_argument("date", datetime.date.today().strftime("%Y-%m-%d"))
        snapshot = database.get_day_snapshot(date_str)
        self.write_json(snapshot)


class ApiNetworkHandler(BaseApiHandler):
    def get(self):
        self.write_json({
            "port": PORT,
            "ips": get_network_ips(),
            "urls": [f"http://{ip}:{PORT}" for ip in get_network_ips()],
            "active_clients": len(SyncHub.clients),
            "cloud_provider": cloud_adapters.load_cloud_config().get("active_provider", "local")
        })


class ApiExportHandler(BaseApiHandler):
    def get(self):
        conn = database.get_db()
        cur = conn.cursor()
        cur.execute("SELECT * FROM diary_entries ORDER BY date DESC, time ASC")
        diaries = [dict(r) for r in cur.fetchall()]
        cur.execute("SELECT * FROM agenda_tasks ORDER BY date DESC, time_slot ASC")
        tasks = [dict(r) for r in cur.fetchall()]
        cur.execute("SELECT * FROM morning_agendas ORDER BY date DESC")
        mornings = [dict(r) for r in cur.fetchall()]
        conn.close()

        export_data = {
            "application": "Imtiaz lifestyle",
            "version": "2.0.0-resilient",
            "exported_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "diaries": diaries,
            "agenda_tasks": tasks,
            "morning_agendas": mornings
        }
        self.set_header("Content-Type", "application/json")
        self.set_header("Content-Disposition", 'attachment; filename="imtiaz_lifestyle_production_backup.json"')
        self.write(json.dumps(export_data, indent=2))


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.render("index.html")

    def head(self):
        self.set_status(200)
        self.finish()


def make_app():
    return tornado.web.Application(
        [
            (r"/", MainHandler),
            (r"/ws", RealtimeWebSocketHandler),
            # Legacy endpoints
            (r"/api/data", ApiLegacyDataHandler),
            (r"/api/network", ApiNetworkHandler),
            (r"/api/export", ApiExportHandler),
            # RESTful V1 API Endpoints
            (r"/api/v1/agenda/([a-zA-Z0-9_\-]+)/status", ApiAgendaStatusHandler),
            (r"/api/v1/agenda(?:/([a-zA-Z0-9_\-]+))?", ApiAgendaHandler),
            (r"/api/v1/diary(?:/([a-zA-Z0-9_\-]+))?", ApiDiaryHandler),
            (r"/api/v1/morning(?:/([0-9]{4}-[0-9]{2}-[0-9]{2}))?", ApiMorningHandler),
            (r"/api/v1/sync/batch", ApiBatchSyncHandler),
            (r"/api/v1/cloud/config", ApiCloudConfigHandler),
            # Static files & PWA
            (r"/(.*)", tornado.web.StaticFileHandler, {"path": STATIC_PATH, "default_filename": "index.html"}),
        ],
        template_path=STATIC_PATH,
        static_path=STATIC_PATH,
        debug=True,
    )


def main():
    database.run_migrations()

    # Check if today has seed data
    today_str = datetime.date.today().strftime("%Y-%m-%d")
    conn = database.get_db()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) as cnt FROM diary_entries WHERE date = ?", (today_str,))
    if cur.fetchone()["cnt"] == 0:
        seed_demo_data(conn, today_str)
    conn.close()

    app = make_app()
    app.listen(PORT, address="0.0.0.0")
    ips = get_network_ips()

    logger.info("==================================================")
    logger.info(f"✨ Imtiaz Lifestyle Resilient Engine Started on Port {PORT}")
    logger.info(f"📱 Web & Mobile Local:  http://localhost:{PORT}")
    for ip in ips:
        if ip not in ["127.0.0.1", "localhost"]:
            logger.info(f"📲 LAN Mobile Sync:    http://{ip}:{PORT}")
    logger.info("==================================================")
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()
