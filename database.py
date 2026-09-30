"""
Imtiaz Lifestyle - Production-Ready Resilient Database & Persistence Layer
Features:
- SQLite WAL (Write-Ahead Logging) mode with concurrent reader/writer handling
- Automated schema migrations with non-destructive column additions and index tuning
- Last-Write-Wins (LWW) conflict resolution with versioning & ISO 8601 timestamps
- Incremental delta synchronization query support (get_changes_since)
"""

import os
import json
import sqlite3
import datetime
import logging
from typing import Dict, Any, List, Optional, Tuple

logger = logging.getLogger("ImtiazDatabase")

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "imtiaz_lifestyle.db")


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    return conn


def run_migrations():
    """Applies schema migrations safely, preserving all existing user data."""
    conn = get_db()
    cursor = conn.cursor()

    # 1. Diary entries table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS diary_entries (
        id TEXT PRIMARY KEY,
        date TEXT NOT NULL,
        time TEXT NOT NULL,
        title TEXT NOT NULL,
        content TEXT NOT NULL,
        mood TEXT DEFAULT 'Calm',
        energy INTEGER DEFAULT 3,
        tags TEXT DEFAULT '[]',
        gratitude_notes TEXT DEFAULT '[]',
        version INTEGER DEFAULT 1,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """)

    # 2. Agenda tasks table (supporting pending, completed, deferred statuses)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS agenda_tasks (
        id TEXT PRIMARY KEY,
        date TEXT NOT NULL,
        time_slot TEXT NOT NULL,
        time_start TEXT DEFAULT '09:00',
        time_end TEXT DEFAULT '',
        title TEXT NOT NULL,
        description TEXT DEFAULT '',
        category TEXT DEFAULT 'Work',
        category_tags TEXT DEFAULT '["Work"]',
        status TEXT DEFAULT 'pending',
        priority TEXT DEFAULT 'Medium',
        completed INTEGER DEFAULT 0,
        order_index INTEGER DEFAULT 0,
        version INTEGER DEFAULT 1,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """)

    # 3. Morning agendas table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS morning_agendas (
        id TEXT PRIMARY KEY,
        date TEXT UNIQUE NOT NULL,
        completed INTEGER DEFAULT 0,
        completed_at TEXT DEFAULT '',
        daily_objectives TEXT DEFAULT '[]',
        gratitudes TEXT DEFAULT '[]',
        gratitude_1 TEXT DEFAULT '',
        gratitude_2 TEXT DEFAULT '',
        gratitude_3 TEXT DEFAULT '',
        focus_goal_1 TEXT DEFAULT '',
        focus_goal_2 TEXT DEFAULT '',
        focus_goal_3 TEXT DEFAULT '',
        affirmation TEXT DEFAULT '',
        hydration_target INTEGER DEFAULT 8,
        hydration_completed INTEGER DEFAULT 0,
        water_glasses INTEGER DEFAULT 0,
        mindset_score INTEGER DEFAULT 5,
        focus_score INTEGER DEFAULT 8,
        energy_level TEXT DEFAULT 'High',
        notes TEXT DEFAULT '',
        version INTEGER DEFAULT 1,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """)

    # 4. Outbox and Sync audit table (for server-side offline tracking / cloud sync logs)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sync_audit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        entity_type TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        action TEXT NOT NULL,
        client_id TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        payload TEXT NOT NULL
    )
    """)

    # Migration: Add new columns if migrating from earlier schema
    tables_columns = {
        "agenda_tasks": [
            ("time_start", "TEXT DEFAULT '09:00'"),
            ("category_tags", "TEXT DEFAULT '[\"Work\"]'"),
            ("status", "TEXT DEFAULT 'pending'"),
            ("version", "INTEGER DEFAULT 1"),
        ],
        "diary_entries": [
            ("gratitude_notes", "TEXT DEFAULT '[]'"),
            ("version", "INTEGER DEFAULT 1"),
        ],
        "morning_agendas": [
            ("daily_objectives", "TEXT DEFAULT '[]'"),
            ("gratitudes", "TEXT DEFAULT '[]'"),
            ("hydration_target", "INTEGER DEFAULT 8"),
            ("hydration_completed", "INTEGER DEFAULT 0"),
            ("focus_score", "INTEGER DEFAULT 8"),
            ("version", "INTEGER DEFAULT 1"),
        ],
    }

    for table, cols in tables_columns.items():
        cursor.execute(f"PRAGMA table_info({table})")
        existing = {row["name"] for row in cursor.fetchall()}
        for col_name, col_def in cols:
            if col_name not in existing:
                try:
                    cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_def}")
                    logger.info(f"Migrated {table}: added column {col_name}")
                except Exception as e:
                    logger.warning(f"Column add note: {e}")

    # Create indices for sub-millisecond query performance
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_agenda_date_time ON agenda_tasks (date, time_slot);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_agenda_updated_at ON agenda_tasks (updated_at);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_agenda_status ON agenda_tasks (status);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_diary_date ON diary_entries (date);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_diary_updated_at ON diary_entries (updated_at);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_morning_date ON morning_agendas (date);")

    conn.commit()
    conn.close()
    logger.info("Database migrations completed successfully.")


# ---------------------------------------------------------------------------
# Last-Write-Wins (LWW) Resolution Helper
# ---------------------------------------------------------------------------
def should_apply_update(existing_updated_at: Optional[str], incoming_updated_at: str) -> bool:
    """True if incoming update is newer or if no existing record exists."""
    if not existing_updated_at:
        return True
    try:
        # ISO string comparison works lexicographically, but parse to be safe
        t_exist = datetime.datetime.fromisoformat(existing_updated_at.replace("Z", "+00:00"))
        t_incom = datetime.datetime.fromisoformat(incoming_updated_at.replace("Z", "+00:00"))
        return t_incom >= t_exist
    except Exception:
        return incoming_updated_at >= existing_updated_at


# ---------------------------------------------------------------------------
# CRUD & Sync Operations
# ---------------------------------------------------------------------------
def upsert_agenda_task(task: Dict[str, Any], client_id: str = "local") -> Tuple[Dict[str, Any], bool]:
    """Upserts task with LWW conflict resolution."""
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM agenda_tasks WHERE id = ?", (task["id"],))
    existing = cursor.fetchone()

    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
    incoming_updated = task.get("updated_at") or now_iso

    if existing:
        if not should_apply_update(existing["updated_at"], incoming_updated):
            conn.close()
            # Existing is newer; return existing record
            return dict(existing), False
        new_version = int(existing["version"] or 1) + 1
    else:
        new_version = int(task.get("version", 1))

    status = task.get("status", "pending")
    completed = 1 if status == "completed" else 0
    cat_tags_json = json.dumps(task.get("category_tags", [task.get("category", "Work")]))

    cursor.execute("""
    INSERT OR REPLACE INTO agenda_tasks (
        id, date, time_slot, time_start, time_end, title, description, category, category_tags,
        status, priority, completed, order_index, version, created_at, updated_at
    ) VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
        COALESCE((SELECT created_at FROM agenda_tasks WHERE id = ?), ?),
        ?
    )
    """, (
        task["id"],
        task["date"],
        task.get("time_slot", task.get("time_start", "09:00")),
        task.get("time_start", task.get("time_slot", "09:00")),
        task.get("time_end", ""),
        task["title"],
        task.get("description", ""),
        task.get("category", "Work"),
        cat_tags_json,
        status,
        task.get("priority", "Medium"),
        completed,
        task.get("order_index", 0),
        new_version,
        task["id"],
        task.get("created_at") or now_iso,
        incoming_updated
    ))

    # Audit log entry
    cursor.execute(
        "INSERT INTO sync_audit_log (entity_type, entity_id, action, client_id, timestamp, payload) VALUES (?, ?, ?, ?, ?, ?)",
        ("agenda_task", task["id"], "UPSERT", client_id, now_iso, json.dumps(task))
    )

    conn.commit()

    cursor.execute("SELECT * FROM agenda_tasks WHERE id = ?", (task["id"],))
    row = dict(cursor.fetchone())
    try:
        row["category_tags"] = json.loads(row["category_tags"])
    except Exception:
        row["category_tags"] = [row["category"]]

    conn.close()
    return row, True


def delete_agenda_task(task_id: str, client_id: str = "local") -> bool:
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM agenda_tasks WHERE id = ?", (task_id,))
    deleted = cursor.rowcount > 0
    if deleted:
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        cursor.execute(
            "INSERT INTO sync_audit_log (entity_type, entity_id, action, client_id, timestamp, payload) VALUES (?, ?, ?, ?, ?, ?)",
            ("agenda_task", task_id, "DELETE", client_id, now_iso, json.dumps({"id": task_id}))
        )
    conn.commit()
    conn.close()
    return deleted


def upsert_diary_entry(entry: Dict[str, Any], client_id: str = "local") -> Tuple[Dict[str, Any], bool]:
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM diary_entries WHERE id = ?", (entry["id"],))
    existing = cursor.fetchone()

    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
    incoming_updated = entry.get("updated_at") or now_iso

    if existing:
        if not should_apply_update(existing["updated_at"], incoming_updated):
            conn.close()
            return dict(existing), False
        new_version = int(existing["version"] or 1) + 1
    else:
        new_version = int(entry.get("version", 1))

    tags_json = json.dumps(entry.get("tags", []))
    gratitude_json = json.dumps(entry.get("gratitude_notes", []))

    cursor.execute("""
    INSERT OR REPLACE INTO diary_entries (
        id, date, time, title, content, mood, energy, tags, gratitude_notes,
        version, created_at, updated_at
    ) VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
        COALESCE((SELECT created_at FROM diary_entries WHERE id = ?), ?),
        ?
    )
    """, (
        entry["id"],
        entry["date"],
        entry.get("time", datetime.datetime.now().strftime("%H:%M")),
        entry.get("title", "Reflection"),
        entry.get("content", ""),
        entry.get("mood", "Calm"),
        int(entry.get("energy", 3)),
        tags_json,
        gratitude_json,
        new_version,
        entry["id"],
        entry.get("created_at") or now_iso,
        incoming_updated
    ))

    cursor.execute(
        "INSERT INTO sync_audit_log (entity_type, entity_id, action, client_id, timestamp, payload) VALUES (?, ?, ?, ?, ?, ?)",
        ("diary_entry", entry["id"], "UPSERT", client_id, now_iso, json.dumps(entry))
    )

    conn.commit()
    cursor.execute("SELECT * FROM diary_entries WHERE id = ?", (entry["id"],))
    row = dict(cursor.fetchone())
    try:
        row["tags"] = json.loads(row["tags"])
    except Exception:
        row["tags"] = []
    try:
        row["gratitude_notes"] = json.loads(row["gratitude_notes"])
    except Exception:
        row["gratitude_notes"] = []

    conn.close()
    return row, True


def delete_diary_entry(entry_id: str, client_id: str = "local") -> bool:
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM diary_entries WHERE id = ?", (entry_id,))
    deleted = cursor.rowcount > 0
    if deleted:
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        cursor.execute(
            "INSERT INTO sync_audit_log (entity_type, entity_id, action, client_id, timestamp, payload) VALUES (?, ?, ?, ?, ?, ?)",
            ("diary_entry", entry_id, "DELETE", client_id, now_iso, json.dumps({"id": entry_id}))
        )
    conn.commit()
    conn.close()
    return deleted


def upsert_morning_plan(plan: Dict[str, Any], client_id: str = "local") -> Tuple[Dict[str, Any], bool]:
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM morning_agendas WHERE date = ?", (plan["date"],))
    existing = cursor.fetchone()

    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
    incoming_updated = plan.get("updated_at") or now_iso

    if existing:
        if not should_apply_update(existing["updated_at"], incoming_updated):
            conn.close()
            return dict(existing), False
        new_version = int(existing["version"] or 1) + 1
    else:
        new_version = int(plan.get("version", 1))

    daily_obj_json = json.dumps(plan.get("daily_objectives", []))
    gratitudes_json = json.dumps(plan.get("gratitudes", []))

    cursor.execute("""
    INSERT OR REPLACE INTO morning_agendas (
        id, date, completed, completed_at, daily_objectives, gratitudes,
        gratitude_1, gratitude_2, gratitude_3, focus_goal_1, focus_goal_2, focus_goal_3,
        affirmation, hydration_target, hydration_completed, water_glasses,
        mindset_score, focus_score, energy_level, notes, version, created_at, updated_at
    ) VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
        COALESCE((SELECT created_at FROM morning_agendas WHERE date = ?), ?),
        ?
    )
    """, (
        plan["id"],
        plan["date"],
        1 if plan.get("completed") else 0,
        plan.get("completed_at", ""),
        daily_obj_json,
        gratitudes_json,
        plan.get("gratitude_1", ""),
        plan.get("gratitude_2", ""),
        plan.get("gratitude_3", ""),
        plan.get("focus_goal_1", ""),
        plan.get("focus_goal_2", ""),
        plan.get("focus_goal_3", ""),
        plan.get("affirmation", ""),
        int(plan.get("hydration_target", 8)),
        int(plan.get("hydration_completed", 0)),
        int(plan.get("water_glasses", plan.get("hydration_completed", 0))),
        int(plan.get("mindset_score", 5)),
        int(plan.get("focus_score", 8)),
        plan.get("energy_level", "High"),
        plan.get("notes", ""),
        new_version,
        plan["date"],
        plan.get("created_at") or now_iso,
        incoming_updated
    ))

    cursor.execute(
        "INSERT INTO sync_audit_log (entity_type, entity_id, action, client_id, timestamp, payload) VALUES (?, ?, ?, ?, ?, ?)",
        ("morning_plan", plan["id"], "UPSERT", client_id, now_iso, json.dumps(plan))
    )

    conn.commit()
    cursor.execute("SELECT * FROM morning_agendas WHERE date = ?", (plan["date"],))
    row = dict(cursor.fetchone())
    try:
        row["daily_objectives"] = json.loads(row["daily_objectives"])
    except Exception:
        row["daily_objectives"] = []
    try:
        row["gratitudes"] = json.loads(row["gratitudes"])
    except Exception:
        row["gratitudes"] = []

    conn.close()
    return row, True


def get_day_snapshot(date_str: str) -> Dict[str, Any]:
    """Fetches full state snapshot for a given date."""
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM diary_entries WHERE date = ? ORDER BY time ASC, created_at ASC", (date_str,))
    diaries = [dict(r) for r in cursor.fetchall()]
    for d in diaries:
        try:
            d["tags"] = json.loads(d["tags"])
        except Exception:
            d["tags"] = []
        try:
            d["gratitude_notes"] = json.loads(d.get("gratitude_notes", "[]"))
        except Exception:
            d["gratitude_notes"] = []

    cursor.execute("SELECT * FROM agenda_tasks WHERE date = ? ORDER BY time_slot ASC, order_index ASC", (date_str,))
    tasks = [dict(r) for r in cursor.fetchall()]
    for t in tasks:
        try:
            t["category_tags"] = json.loads(t.get("category_tags", "[]"))
        except Exception:
            t["category_tags"] = [t.get("category", "Work")]

    cursor.execute("SELECT * FROM morning_agendas WHERE date = ?", (date_str,))
    m_row = cursor.fetchone()
    if m_row:
        morning = dict(m_row)
        try:
            morning["daily_objectives"] = json.loads(morning.get("daily_objectives", "[]"))
        except Exception:
            morning["daily_objectives"] = []
        try:
            morning["gratitudes"] = json.loads(morning.get("gratitudes", "[]"))
        except Exception:
            morning["gratitudes"] = []
    else:
        morning = {
            "id": f"morning_{date_str}",
            "date": date_str,
            "completed": 0,
            "completed_at": "",
            "daily_objectives": [],
            "gratitudes": [],
            "gratitude_1": "",
            "gratitude_2": "",
            "gratitude_3": "",
            "focus_goal_1": "",
            "focus_goal_2": "",
            "focus_goal_3": "",
            "affirmation": "I approach each hour with intentionality, clarity, and calm focus.",
            "hydration_target": 8,
            "hydration_completed": 0,
            "water_glasses": 0,
            "mindset_score": 5,
            "focus_score": 8,
            "energy_level": "High",
            "notes": "",
            "version": 1,
            "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
        }

    total_tasks = len(tasks)
    completed_tasks = sum(1 for t in tasks if t.get("status") == "completed" or t.get("completed") == 1)
    deferred_tasks = sum(1 for t in tasks if t.get("status") == "deferred")
    pending_tasks = sum(1 for t in tasks if t.get("status") == "pending")

    metrics = {
        "total_tasks": total_tasks,
        "completed_tasks": completed_tasks,
        "deferred_tasks": deferred_tasks,
        "pending_tasks": pending_tasks,
        "completion_rate": int((completed_tasks / total_tasks * 100) if total_tasks > 0 else 0),
        "diary_count": len(diaries),
        "morning_completed": bool(morning.get("completed", 0))
    }

    conn.close()
    return {
        "date": date_str,
        "diary_entries": diaries,
        "agenda_tasks": tasks,
        "morning_agenda": morning,
        "metrics": metrics
    }


def get_changes_since(since_iso: str) -> Dict[str, Any]:
    """Delta synchronization helper: returns all entities modified after since_iso."""
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM agenda_tasks WHERE updated_at > ? ORDER BY updated_at ASC", (since_iso,))
    tasks = [dict(r) for r in cursor.fetchall()]

    cursor.execute("SELECT * FROM diary_entries WHERE updated_at > ? ORDER BY updated_at ASC", (since_iso,))
    diaries = [dict(r) for r in cursor.fetchall()]

    cursor.execute("SELECT * FROM morning_agendas WHERE updated_at > ? ORDER BY updated_at ASC", (since_iso,))
    mornings = [dict(r) for r in cursor.fetchall()]

    conn.close()
    return {
        "since": since_iso,
        "tasks": tasks,
        "diaries": diaries,
        "morning_agendas": mornings
    }
