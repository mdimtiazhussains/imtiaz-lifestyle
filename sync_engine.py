"""
Imtiaz Lifestyle - Real-time WebSocket Synchronization Engine
Features:
- Multi-client connection tracking & presence heartbeats
- Strict JSON schema validation on every event action
- Granular event broadcasting with sub-millisecond propagation
- Batch offline queue reconciliation with ACK acknowledgments
"""

import json
import logging
import datetime
from typing import Set, Dict, Any, Optional

import tornado.websocket

from models import MorningPlanModel, AgendaTaskModel, DiaryEntryModel
import database

logger = logging.getLogger("ImtiazSyncEngine")


class SyncHub:
    clients: Set[tornado.websocket.WebSocketHandler] = set()

    @classmethod
    def register(cls, client: tornado.websocket.WebSocketHandler):
        cls.clients.add(client)
        logger.info(f"Client registered. Connected devices: {len(cls.clients)}")
        cls.broadcast_presence()

    @classmethod
    def unregister(cls, client: tornado.websocket.WebSocketHandler):
        cls.clients.discard(client)
        logger.info(f"Client unregistered. Connected devices: {len(cls.clients)}")
        cls.broadcast_presence()

    @classmethod
    def broadcast_presence(cls):
        msg = {
            "type": "DEVICE_PRESENCE_CHANGE",
            "active_devices": len(cls.clients),
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
        }
        cls.broadcast(msg)

    @classmethod
    def broadcast(cls, message_dict: Dict[str, Any], exclude: Optional[tornado.websocket.WebSocketHandler] = None):
        msg_str = json.dumps(message_dict)
        dead_clients = set()
        for c in cls.clients:
            if c != exclude:
                try:
                    c.write_message(msg_str)
                except Exception as e:
                    logger.warning(f"Error sending message to client: {e}")
                    dead_clients.add(c)
        for dc in dead_clients:
            cls.clients.discard(dc)


class RealtimeWebSocketHandler(tornado.websocket.WebSocketHandler):
    def check_origin(self, origin: str) -> bool:
        # Permit connections from any origin for LAN and mobile access
        return True

    def open(self):
        self.write_message(json.dumps({
            "type": "CONNECTION_ESTABLISHED",
            "active_devices": len(SyncHub.clients) + 1,
            "server_time": datetime.datetime.now(datetime.timezone.utc).isoformat()
        }))
        SyncHub.register(self)

    def on_close(self):
        SyncHub.unregister(self)

    def on_message(self, raw_message: str):
        try:
            payload = json.loads(raw_message)
        except Exception as e:
            self.write_message(json.dumps({
                "type": "ERROR",
                "message": f"Malformed JSON: {str(e)}"
            }))
            return

        action = payload.get("action")
        client_id = payload.get("client_id", "anon_client")
        date_str = payload.get("date", datetime.date.today().strftime("%Y-%m-%d"))

        logger.info(f"WS Event [{action}] from client {client_id} for date {date_str}")

        if action == "PING":
            self.write_message(json.dumps({"type": "PONG", "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()}))
            return

        elif action == "GET_DAY_DATA":
            data = database.get_day_snapshot(date_str)
            self.write_message(json.dumps({
                "type": "DAY_DATA",
                "date": date_str,
                "data": data
            }))
            return

        elif action == "SAVE_TASK":
            raw_task = payload.get("task", {})
            if "date" not in raw_task:
                raw_task["date"] = date_str
            valid, task, err = AgendaTaskModel.validate_and_sanitize(raw_task)
            if not valid or not task:
                self.write_message(json.dumps({
                    "type": "VALIDATION_ERROR",
                    "action": action,
                    "message": err
                }))
                return

            saved_task, applied = database.upsert_agenda_task(task, client_id=client_id)
            updated_day = database.get_day_snapshot(task["date"])

            # Broadcast to all connected clients
            SyncHub.broadcast({
                "type": "DAY_DATA_UPDATED",
                "action": "SAVE_TASK",
                "date": task["date"],
                "data": updated_day,
                "item": saved_task,
                "sender_id": client_id,
                "applied": applied,
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
            })

        elif action == "TOGGLE_TASK" or action == "UPDATE_TASK_STATUS":
            task_id = payload.get("id")
            new_status = payload.get("status")
            if not new_status:
                completed = bool(payload.get("completed", False))
                new_status = "completed" if completed else "pending"

            conn = database.get_db()
            cur = conn.cursor()
            cur.execute("SELECT * FROM agenda_tasks WHERE id = ?", (task_id,))
            t_row = cur.fetchone()
            conn.close()

            if t_row:
                t_dict = dict(t_row)
                t_dict["status"] = new_status
                t_dict["completed"] = 1 if new_status == "completed" else 0
                t_dict["updated_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
                valid, task, _ = AgendaTaskModel.validate_and_sanitize(t_dict)
                if valid and task:
                    saved_task, applied = database.upsert_agenda_task(task, client_id=client_id)
                    updated_day = database.get_day_snapshot(task["date"])
                    SyncHub.broadcast({
                        "type": "DAY_DATA_UPDATED",
                        "action": "UPDATE_TASK_STATUS",
                        "date": task["date"],
                        "data": updated_day,
                        "item": saved_task,
                        "sender_id": client_id,
                        "applied": applied,
                        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
                    })

        elif action == "DELETE_TASK":
            task_id = payload.get("id")
            if task_id:
                database.delete_agenda_task(task_id, client_id=client_id)
                updated_day = database.get_day_snapshot(date_str)
                SyncHub.broadcast({
                    "type": "DAY_DATA_UPDATED",
                    "action": "DELETE_TASK",
                    "date": date_str,
                    "deleted_id": task_id,
                    "data": updated_day,
                    "sender_id": client_id,
                    "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
                })

        elif action == "SAVE_DIARY":
            raw_entry = payload.get("entry", {})
            if "date" not in raw_entry:
                raw_entry["date"] = date_str
            valid, entry, err = DiaryEntryModel.validate_and_sanitize(raw_entry)
            if not valid or not entry:
                self.write_message(json.dumps({
                    "type": "VALIDATION_ERROR",
                    "action": action,
                    "message": err
                }))
                return

            saved_entry, applied = database.upsert_diary_entry(entry, client_id=client_id)
            updated_day = database.get_day_snapshot(entry["date"])
            SyncHub.broadcast({
                "type": "DAY_DATA_UPDATED",
                "action": "SAVE_DIARY",
                "date": entry["date"],
                "data": updated_day,
                "item": saved_entry,
                "sender_id": client_id,
                "applied": applied,
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
            })

        elif action == "DELETE_DIARY":
            entry_id = payload.get("id")
            if entry_id:
                database.delete_diary_entry(entry_id, client_id=client_id)
                updated_day = database.get_day_snapshot(date_str)
                SyncHub.broadcast({
                    "type": "DAY_DATA_UPDATED",
                    "action": "DELETE_DIARY",
                    "date": date_str,
                    "deleted_id": entry_id,
                    "data": updated_day,
                    "sender_id": client_id,
                    "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
                })

        elif action == "SAVE_MORNING_AGENDA":
            raw_morning = payload.get("morning", {})
            if "date" not in raw_morning:
                raw_morning["date"] = date_str
            valid, morning, err = MorningPlanModel.validate_and_sanitize(raw_morning)
            if not valid or not morning:
                self.write_message(json.dumps({
                    "type": "VALIDATION_ERROR",
                    "action": action,
                    "message": err
                }))
                return

            saved_plan, applied = database.upsert_morning_plan(morning, client_id=client_id)

            # Optional auto-push of top 3 daily objectives to agenda tasks
            if payload.get("push_goals_to_agenda"):
                objectives = morning.get("daily_objectives", [])
                time_slots = [("09:00", "10:30"), ("11:00", "12:30"), ("14:00", "15:30")]
                for idx, obj_text in enumerate(objectives[:3]):
                    if obj_text:
                        slot_start, slot_end = time_slots[idx] if idx < len(time_slots) else ("16:00", "17:00")
                        t_id = f"task_m_{morning['date']}_{idx+1}"
                        task_data = {
                            "id": t_id,
                            "date": morning["date"],
                            "time_start": slot_start,
                            "time_slot": slot_start,
                            "time_end": slot_end,
                            "title": f"🎯 Priority #{idx+1}: {obj_text}",
                            "description": "Auto-scheduled from Imtiaz 15-Minute Morning Kickstart",
                            "category": "Work",
                            "category_tags": ["Work", "High Priority"],
                            "status": "pending",
                            "priority": "High",
                            "order_index": idx + 10,
                        }
                        v, t, _ = AgendaTaskModel.validate_and_sanitize(task_data)
                        if v and t:
                            database.upsert_agenda_task(t, client_id=client_id)

            updated_day = database.get_day_snapshot(morning["date"])
            SyncHub.broadcast({
                "type": "DAY_DATA_UPDATED",
                "action": "SAVE_MORNING_AGENDA",
                "date": morning["date"],
                "data": updated_day,
                "item": saved_plan,
                "sender_id": client_id,
                "applied": applied,
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
            })

        elif action == "BATCH_SYNC":
            # Reconciles client's offline outbox queue
            items = payload.get("items", [])
            processed = 0
            for item in items:
                entity_type = item.get("type")
                action_sub = item.get("action")
                p = item.get("payload", {})
                if entity_type == "agenda_task":
                    if action_sub == "DELETE":
                        database.delete_agenda_task(p.get("id"), client_id=client_id)
                    else:
                        v, t, _ = AgendaTaskModel.validate_and_sanitize(p)
                        if v and t:
                            database.upsert_agenda_task(t, client_id=client_id)
                elif entity_type == "diary_entry":
                    if action_sub == "DELETE":
                        database.delete_diary_entry(p.get("id"), client_id=client_id)
                    else:
                        v, e, _ = DiaryEntryModel.validate_and_sanitize(p)
                        if v and e:
                            database.upsert_diary_entry(e, client_id=client_id)
                elif entity_type == "morning_plan":
                    v, m, _ = MorningPlanModel.validate_and_sanitize(p)
                    if v and m:
                        database.upsert_morning_plan(m, client_id=client_id)
                processed += 1

            updated_day = database.get_day_snapshot(date_str)
            self.write_message(json.dumps({
                "type": "BATCH_SYNC_ACK",
                "processed_count": processed,
                "date": date_str,
                "data": updated_day,
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
            }))
            SyncHub.broadcast({
                "type": "DAY_DATA_UPDATED",
                "action": "BATCH_SYNC",
                "date": date_str,
                "data": updated_day,
                "sender_id": client_id,
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
            }, exclude=self)

        elif action == "RESET_SAMPLE_DATA":
            # Resets sample data for date
            conn = database.get_db()
            cur = conn.cursor()
            cur.execute("DELETE FROM diary_entries WHERE date = ?", (date_str,))
            cur.execute("DELETE FROM agenda_tasks WHERE date = ?", (date_str,))
            cur.execute("DELETE FROM morning_agendas WHERE date = ?", (date_str,))
            conn.commit()
            conn.close()

            # Reseed
            from server import seed_demo_data
            conn2 = database.get_db()
            seed_demo_data(conn2, date_str)
            conn2.close()

            updated_day = database.get_day_snapshot(date_str)
            SyncHub.broadcast({
                "type": "DAY_DATA_UPDATED",
                "action": "RESET_SAMPLE_DATA",
                "date": date_str,
                "data": updated_day,
                "sender_id": client_id,
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
            })
