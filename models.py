"""
Imtiaz Lifestyle - Data Models & Strict Schema Validation
Provides typed schemas, validation, and serialization for:
1. Morning 15-Minute Planning
2. 24-Hour Agenda Tasks (with pending, completed, deferred statuses)
3. Diary Entries (with markdown support, mood, gratitude, energy)
"""

import re
import datetime
from typing import Dict, Any, Tuple, List, Optional

DATE_REGEX = re.compile(r"^\d{4}-\d{2}-\d{2}$")
TIME_REGEX = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")

VALID_STATUSES = {"pending", "completed", "deferred"}
VALID_PRIORITIES = {"high", "medium", "low"}
VALID_ENERGY_LEVELS = {"Calm", "High", "Peak Flow", "Low", "Balanced"}
VALID_MOODS = {"Energized", "Focused", "Calm", "Reflective", "Tired", "Grateful", "Inspired"}


def get_current_iso_time() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def validate_date(date_str: str) -> bool:
    if not isinstance(date_str, str) or not DATE_REGEX.match(date_str):
        return False
    try:
        y, m, d = map(int, date_str.split("-"))
        datetime.date(y, m, d)
        return True
    except ValueError:
        return False


def validate_time(time_str: str) -> bool:
    if not isinstance(time_str, str):
        return False
    return bool(TIME_REGEX.match(time_str))


# ---------------------------------------------------------------------------
# 1. Morning 15-Minute Planning Model & Validator
# ---------------------------------------------------------------------------
class MorningPlanModel:
    @staticmethod
    def validate_and_sanitize(data: Dict[str, Any]) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
        if not isinstance(data, dict):
            return False, None, "Payload must be a JSON object"

        date_str = data.get("date")
        if not date_str or not validate_date(date_str):
            return False, None, f"Invalid date format: {date_str}. Expected YYYY-MM-DD"

        plan_id = str(data.get("id") or f"morning_{date_str}")
        completed = bool(data.get("completed", False))
        completed_at = str(data.get("completed_at", ""))
        if completed and not completed_at:
            completed_at = get_current_iso_time()

        # Daily objectives (array of strings)
        raw_objectives = data.get("daily_objectives")
        if raw_objectives is None:
            # Fallback to focus_goal_1, focus_goal_2, focus_goal_3 for backward compatibility
            raw_objectives = [
                data.get("focus_goal_1", ""),
                data.get("focus_goal_2", ""),
                data.get("focus_goal_3", ""),
            ]
        elif not isinstance(raw_objectives, list):
            return False, None, "daily_objectives must be a list of strings"
        daily_objectives = [str(obj).strip() for obj in raw_objectives if str(obj).strip()]

        # Gratitudes (array of strings)
        raw_gratitudes = data.get("gratitudes")
        if raw_gratitudes is None:
            raw_gratitudes = [
                data.get("gratitude_1", ""),
                data.get("gratitude_2", ""),
                data.get("gratitude_3", ""),
            ]
        elif not isinstance(raw_gratitudes, list):
            return False, None, "gratitudes must be a list of strings"
        gratitudes = [str(g).strip() for g in raw_gratitudes if str(g).strip()]

        # Mindset & Focus scores (1-10)
        try:
            mindset_score = int(data.get("mindset_score", 5))
            mindset_score = max(1, min(10, mindset_score))
        except (ValueError, TypeError):
            mindset_score = 5

        try:
            focus_score = int(data.get("focus_score", 8))
            focus_score = max(1, min(10, focus_score))
        except (ValueError, TypeError):
            focus_score = 8

        # Energy level
        energy_level = str(data.get("energy_level", "High"))
        if energy_level not in VALID_ENERGY_LEVELS:
            energy_level = "High"

        affirmation = str(data.get("affirmation", "I approach each hour with intentionality, clarity, and calm focus.")).strip()

        try:
            hydration_target = int(data.get("hydration_target", 8))
            hydration_completed = int(data.get("water_glasses", data.get("hydration_completed", 0)))
        except (ValueError, TypeError):
            hydration_target = 8
            hydration_completed = 0

        notes = str(data.get("notes", "")).strip()
        version = int(data.get("version", 1))
        now_iso = get_current_iso_time()
        created_at = str(data.get("created_at") or now_iso)
        updated_at = str(data.get("updated_at") or now_iso)

        sanitized = {
            "id": plan_id,
            "date": date_str,
            "completed": 1 if completed else 0,
            "completed_at": completed_at,
            "daily_objectives": daily_objectives,
            "gratitudes": gratitudes,
            "mindset_score": mindset_score,
            "focus_score": focus_score,
            "energy_level": energy_level,
            "affirmation": affirmation,
            "hydration_target": hydration_target,
            "hydration_completed": hydration_completed,
            "notes": notes,
            "version": version,
            "created_at": created_at,
            "updated_at": updated_at,
            # Legacy fields for backward compatibility
            "gratitude_1": gratitudes[0] if len(gratitudes) > 0 else "",
            "gratitude_2": gratitudes[1] if len(gratitudes) > 1 else "",
            "gratitude_3": gratitudes[2] if len(gratitudes) > 2 else "",
            "focus_goal_1": daily_objectives[0] if len(daily_objectives) > 0 else "",
            "focus_goal_2": daily_objectives[1] if len(daily_objectives) > 1 else "",
            "focus_goal_3": daily_objectives[2] if len(daily_objectives) > 2 else "",
            "water_glasses": hydration_completed,
        }
        return True, sanitized, None


# ---------------------------------------------------------------------------
# 2. 24-Hour Agenda Task Model & Validator
# ---------------------------------------------------------------------------
class AgendaTaskModel:
    @staticmethod
    def validate_and_sanitize(data: Dict[str, Any]) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
        if not isinstance(data, dict):
            return False, None, "Payload must be a JSON object"

        title = str(data.get("title", "")).strip()
        if not title:
            return False, None, "Task title is required and cannot be empty"

        date_str = data.get("date")
        if not date_str or not validate_date(date_str):
            return False, None, f"Invalid date: {date_str}. Expected YYYY-MM-DD"

        task_id = str(data.get("id") or f"task_{int(datetime.datetime.now().timestamp() * 1000)}")

        # Time slot
        time_slot = str(data.get("time_slot", data.get("time_start", "09:00"))).strip()
        if not validate_time(time_slot):
            time_slot = "09:00"

        time_end = str(data.get("time_end", "")).strip()
        if time_end and not validate_time(time_end):
            time_end = ""

        # Status: pending | completed | deferred
        status = str(data.get("status", "")).lower().strip()
        if not status:
            completed_flag = bool(data.get("completed", False))
            status = "completed" if completed_flag else "pending"
        elif status not in VALID_STATUSES:
            return False, None, f"Invalid status: {status}. Must be one of: {', '.join(VALID_STATUSES)}"

        # Priority: high | medium | low
        priority = str(data.get("priority", "medium")).lower().strip()
        if priority not in VALID_PRIORITIES:
            priority = "medium"

        # Category tags (list of strings)
        raw_cats = data.get("category_tags")
        if raw_cats is None:
            cat_single = str(data.get("category", "Work")).strip()
            category_tags = [cat_single] if cat_single else ["Work"]
        elif isinstance(raw_cats, list):
            category_tags = [str(c).strip() for c in raw_cats if str(c).strip()]
        else:
            category_tags = ["Work"]

        description = str(data.get("description", "")).strip()

        try:
            order_index = int(data.get("order_index", 0))
        except (ValueError, TypeError):
            order_index = 0

        version = int(data.get("version", 1))
        now_iso = get_current_iso_time()
        created_at = str(data.get("created_at") or now_iso)
        updated_at = str(data.get("updated_at") or now_iso)

        sanitized = {
            "id": task_id,
            "date": date_str,
            "time_slot": time_slot,
            "time_start": time_slot,
            "time_end": time_end,
            "title": title,
            "description": description,
            "category": category_tags[0] if category_tags else "Work",
            "category_tags": category_tags,
            "status": status,
            "completed": 1 if status == "completed" else 0,
            "priority": priority.capitalize(),
            "order_index": order_index,
            "version": version,
            "created_at": created_at,
            "updated_at": updated_at,
        }
        return True, sanitized, None


# ---------------------------------------------------------------------------
# 3. Diary Entry Model & Validator (Markdown & Gratitude Support)
# ---------------------------------------------------------------------------
class DiaryEntryModel:
    @staticmethod
    def validate_and_sanitize(data: Dict[str, Any]) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
        if not isinstance(data, dict):
            return False, None, "Payload must be a JSON object"

        title = str(data.get("title", "")).strip()
        content = str(data.get("content", "")).strip()

        if not title and not content:
            return False, None, "Diary entry must have at least a title or reflection content"

        date_str = data.get("date")
        if not date_str or not validate_date(date_str):
            return False, None, f"Invalid date: {date_str}. Expected YYYY-MM-DD"

        entry_id = str(data.get("id") or f"diary_{int(datetime.datetime.now().timestamp() * 1000)}")

        time_val = str(data.get("time", "")).strip()
        if not validate_time(time_val):
            time_val = datetime.datetime.now().strftime("%H:%M")

        mood = str(data.get("mood", "Calm")).strip()
        if mood not in VALID_MOODS:
            mood = "Calm"

        try:
            energy = int(data.get("energy", 3))
            energy = max(1, min(5, energy))
        except (ValueError, TypeError):
            energy = 3

        # Tags (list of strings)
        raw_tags = data.get("tags")
        if isinstance(raw_tags, list):
            tags = [str(t).strip() for t in raw_tags if str(t).strip()]
        elif isinstance(raw_tags, str):
            tags = [s.strip() for s in raw_tags.split(",") if s.strip()]
        else:
            tags = []

        # Gratitude notes embedded in reflection
        raw_gratitude = data.get("gratitude_notes")
        if isinstance(raw_gratitude, list):
            gratitude_notes = [str(g).strip() for g in raw_gratitude if str(g).strip()]
        elif isinstance(raw_gratitude, str) and raw_gratitude.strip():
            gratitude_notes = [raw_gratitude.strip()]
        else:
            gratitude_notes = []

        version = int(data.get("version", 1))
        now_iso = get_current_iso_time()
        created_at = str(data.get("created_at") or now_iso)
        updated_at = str(data.get("updated_at") or now_iso)

        sanitized = {
            "id": entry_id,
            "date": date_str,
            "time": time_val,
            "title": title or "Reflection",
            "content": content,
            "mood": mood,
            "energy": energy,
            "tags": tags,
            "gratitude_notes": gratitude_notes,
            "version": version,
            "created_at": created_at,
            "updated_at": updated_at,
        }
        return True, sanitized, None
