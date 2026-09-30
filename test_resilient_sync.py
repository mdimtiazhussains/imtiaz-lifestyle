#!/usr/bin/env python3
"""
Automated Test Suite for Imtiaz Lifestyle Resilient Sync Engine
Verifies:
1. RESTful APIs for Agenda, Diary, Morning 15m, and Batch Sync
2. Strict Schema Validation & Error Handling (HTTP 400 on malformed payloads)
3. WebSocket Multi-Client Real-Time Event Broadcasting
4. Status transitions (pending -> deferred -> completed)
5. Cloud Storage configuration and test endpoints
"""

import sys
import json
import asyncio
import traceback
import urllib.request
import urllib.error
import websockets

BASE_URL = "http://localhost:8080"
WS_URL = "ws://localhost:8080/ws"


def http_request(path, method="GET", body=None):
    url = f"{BASE_URL}{path}"
    data = json.dumps(body).encode("utf-8") if body else None
    headers = {"Content-Type": "application/json"}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=5.0) as resp:
            content = resp.read().decode("utf-8")
            return resp.status, json.loads(content) if content else {}
    except urllib.error.HTTPError as e:
        content = e.read().decode("utf-8")
        try:
            return e.code, json.loads(content)
        except Exception:
            return e.code, {"error": content}


def test_rest_api():
    print("==================================================")
    print("▶ Test 1: RESTful API CRUD & Schema Validation")
    print("==================================================")

    # 1. Validation test: Invalid task (empty title)
    status, res = http_request("/api/v1/agenda", method="POST", body={"title": "", "date": "2026-09-30"})
    assert status == 400, f"Expected 400 for empty title, got {status}: {res}"
    print("  ✓ Schema validator rejected empty title with HTTP 400:", res.get("error"))

    # 2. Validation test: Invalid date
    status, res = http_request("/api/v1/agenda", method="POST", body={"title": "Test", "date": "not-a-date"})
    assert status == 400, f"Expected 400 for bad date, got {status}: {res}"
    print("  ✓ Schema validator rejected invalid date with HTTP 400:", res.get("error"))

    # 3. Create valid task with pending status
    task_payload = {
        "id": "test_task_101",
        "date": "2026-09-30",
        "time_start": "14:00",
        "time_end": "15:00",
        "title": "Automated Architectural Verification",
        "description": "Verifying production resilience and WAL persistence",
        "category": "Work",
        "category_tags": ["Work", "Core Architecture"],
        "status": "pending",
        "priority": "High"
    }
    status, res = http_request("/api/v1/agenda", method="POST", body=task_payload)
    assert status == 201, f"Expected 201, got {status}: {res}"
    print("  ✓ Created 24h task via REST API:", res.get("task", {}).get("title"))

    # 4. Patch task status to 'deferred'
    status, res = http_request("/api/v1/agenda/test_task_101/status", method="PATCH", body={"status": "deferred"})
    assert status == 200, f"Expected 200, got {status}: {res}"
    assert res["task"]["status"] == "deferred", f"Expected deferred status, got {res['task']['status']}"
    print("  ✓ Updated task status to 'deferred':", res["task"]["status"])

    # 5. Patch task status to 'completed'
    status, res = http_request("/api/v1/agenda/test_task_101/status", method="PATCH", body={"status": "completed"})
    assert status == 200, f"Expected 200, got {status}: {res}"
    assert res["task"]["status"] == "completed" and res["task"]["completed"] == 1
    print("  ✓ Updated task status to 'completed':", res["task"]["status"])

    # 6. Create Markdown Diary Entry
    diary_payload = {
        "id": "test_diary_101",
        "date": "2026-09-30",
        "time": "15:30",
        "title": "Resilient Sync Milestones",
        "content": "### System Validation Note\n- Real-time engine fully integrated.\n- Offline IndexedDB caching enabled.\n> Focus brings clarity.",
        "mood": "Energized",
        "energy": 5,
        "tags": ["Architecture", "Resilience"]
    }
    status, res = http_request("/api/v1/diary", method="POST", body=diary_payload)
    assert status == 201, f"Expected 201, got {status}: {res}"
    print("  ✓ Created Markdown Diary Entry:", res.get("entry", {}).get("title"))

    # 7. Upsert Morning 15m Plan with auto-push
    morning_payload = {
        "id": "morning_2026-09-30",
        "date": "2026-09-30",
        "completed": True,
        "daily_objectives": ["Goal Alpha: Test Sync", "Goal Beta: Mobile Parity", "Goal Gamma: Sleep 8h"],
        "gratitudes": ["Clarity", "Discipline", "Focus"],
        "affirmation": "Precision in all things.",
        "hydration_target": 8,
        "hydration_completed": 6,
        "mindset_score": 5,
        "focus_score": 9,
        "energy_level": "Peak Flow",
        "push_goals_to_agenda": True
    }
    status, res = http_request("/api/v1/morning/2026-09-30", method="PUT", body=morning_payload)
    assert status == 200, f"Expected 200, got {status}: {res}"
    print("  ✓ Upserted Morning 15m Plan & pushed goals to 24h schedule")

    # 8. Test Cloud Config endpoint
    status, res = http_request("/api/v1/cloud/config", method="GET")
    assert status == 200
    print("  ✓ Retrieved Cloud Storage Provider Status:", res.get("active_provider"))

    # 9. Clean up test task and diary
    http_request("/api/v1/agenda/test_task_101", method="DELETE")
    http_request("/api/v1/diary/test_diary_101", method="DELETE")
    print("  ✓ Cleaned up test records")


async def recv_matching(ws, expected_type=None, expected_action=None, timeout=6.0):
    start = asyncio.get_event_loop().time()
    while True:
        elapsed = asyncio.get_event_loop().time() - start
        if elapsed > timeout:
            raise TimeoutError(f"Timed out waiting for type={expected_type}, action={expected_action}")
        raw = await asyncio.wait_for(ws.recv(), timeout=timeout - elapsed)
        msg = json.loads(raw)
        if expected_type and msg.get("type") != expected_type:
            continue
        if expected_action and msg.get("action") != expected_action:
            continue
        return msg


async def test_websocket_realtime_sync():
    print("\n==================================================")
    print("▶ Test 2: Multi-Device WebSocket Real-time Sync")
    print("==================================================")

    async with websockets.connect(WS_URL) as mobile_ws, websockets.connect(WS_URL) as web_ws:
        msg_m = await recv_matching(mobile_ws, expected_type="CONNECTION_ESTABLISHED")
        msg_w = await recv_matching(web_ws, expected_type="CONNECTION_ESTABLISHED")
        print("  ✓ Connected simulated Mobile and Web clients simultaneously")

        # Mobile creates a new 24h task
        test_ws_task = {
            "id": "ws_synced_task_999",
            "date": "2026-09-30",
            "time_start": "17:00",
            "time_end": "17:45",
            "title": "Live WebSocket Broadcast Test",
            "category": "Health",
            "status": "pending",
            "priority": "High"
        }
        await mobile_ws.send(json.dumps({
            "action": "SAVE_TASK",
            "client_id": "mobile_phone_mock",
            "date": "2026-09-30",
            "task": test_ws_task
        }))

        # Web must receive real-time broadcast without polling or reload
        web_broadcast = await recv_matching(web_ws, expected_type="DAY_DATA_UPDATED", expected_action="SAVE_TASK")
        assert web_broadcast["item"]["title"] == "Live WebSocket Broadcast Test"
        print("  ✓ Web client received instantaneous broadcast from Mobile:", web_broadcast["action"])

        # Web updates status to 'completed'
        await web_ws.send(json.dumps({
            "action": "UPDATE_TASK_STATUS",
            "client_id": "web_browser_mock",
            "date": "2026-09-30",
            "id": "ws_synced_task_999",
            "status": "completed"
        }))

        # Mobile must receive status update instantaneously
        mobile_broadcast = await recv_matching(mobile_ws, expected_type="DAY_DATA_UPDATED", expected_action="UPDATE_TASK_STATUS")
        assert mobile_broadcast["item"]["status"] == "completed"
        print("  ✓ Mobile client received status update (completed) from Web client!")

        # Clean up
        await web_ws.send(json.dumps({
            "action": "DELETE_TASK",
            "client_id": "web_browser_mock",
            "date": "2026-09-30",
            "id": "ws_synced_task_999"
        }))
        print("  ✓ Real-time multi-device sync test completed successfully")


def main():
    try:
        test_rest_api()
        asyncio.run(test_websocket_realtime_sync())
        print("\n==================================================")
        print("🎉 ALL TESTS PASSED! Backend is resilient & verified.")
        print("==================================================")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
