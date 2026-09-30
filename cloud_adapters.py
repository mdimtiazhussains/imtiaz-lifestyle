"""
Imtiaz Lifestyle - Cloud Storage Adapters & Multi-Target Sync Layer
Supports:
1. Cloudflare Workers D1 / KV REST integration
2. Supabase PostgREST synchronization
3. Local Resilient SQLite WAL fallback
"""

import os
import json
import urllib.request
import urllib.error
import logging
from typing import Dict, Any, Tuple, Optional, List

logger = logging.getLogger("ImtiazCloudAdapters")

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cloud_config.json")


def load_cloud_config() -> Dict[str, Any]:
    default_config = {
        "active_provider": os.environ.get("CLOUD_PROVIDER", "local"),  # 'local', 'cloudflare_d1', 'supabase'
        "cloudflare_d1": {
            "account_id": os.environ.get("CF_ACCOUNT_ID", ""),
            "database_id": os.environ.get("CF_DATABASE_ID", ""),
            "api_token": os.environ.get("CF_API_TOKEN", ""),
        },
        "supabase": {
            "url": os.environ.get("SUPABASE_URL", ""),
            "anon_key": os.environ.get("SUPABASE_ANON_KEY", ""),
        },
        "auto_sync_enabled": True,
        "last_sync_timestamp": None,
        "last_sync_status": "Idle",
    }
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                saved = json.load(f)
                default_config.update(saved)
        except Exception as e:
            logger.warning(f"Failed to read cloud_config.json: {e}")
    return default_config


def save_cloud_config(config: Dict[str, Any]) -> None:
    try:
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to write cloud_config.json: {e}")


# ---------------------------------------------------------------------------
# Cloudflare D1 Storage Adapter
# ---------------------------------------------------------------------------
class CloudflareD1Adapter:
    def __init__(self, account_id: str, database_id: str, api_token: str):
        self.account_id = account_id
        self.database_id = database_id
        self.api_token = api_token
        self.base_url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/database/{database_id}/query"

    def is_configured(self) -> bool:
        return bool(self.account_id and self.database_id and self.api_token)

    def execute_sql(self, sql: str, params: Optional[List[Any]] = None) -> Tuple[bool, Any, Optional[str]]:
        if not self.is_configured():
            return False, None, "Cloudflare D1 credentials not fully configured"

        payload = {"sql": sql, "params": params or []}
        data_bytes = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self.base_url,
            data=data_bytes,
            headers={
                "Authorization": f"Bearer {self.api_token}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=8.0) as resp:
                res_body = json.loads(resp.read().decode("utf-8"))
                if res_body.get("success"):
                    return True, res_body.get("result", []), None
                else:
                    errors = res_body.get("errors", [])
                    return False, None, str(errors)
        except urllib.error.HTTPError as e:
            err_msg = e.read().decode("utf-8")
            return False, None, f"HTTP {e.code}: {err_msg}"
        except Exception as e:
            return False, None, str(e)

    def test_connection(self) -> Tuple[bool, str]:
        if not self.is_configured():
            return False, "Cloudflare D1 is missing Account ID, Database ID, or API Token."
        success, result, err = self.execute_sql("SELECT 1 as ping;")
        if success:
            return True, "Cloudflare D1 connection verified! Database is reachable."
        return False, f"Cloudflare D1 connection failed: {err}"


# ---------------------------------------------------------------------------
# Supabase PostgREST Adapter
# ---------------------------------------------------------------------------
class SupabaseAdapter:
    def __init__(self, url: str, anon_key: str):
        self.url = url.rstrip("/") if url else ""
        self.anon_key = anon_key

    def is_configured(self) -> bool:
        return bool(self.url and self.anon_key)

    def upsert_records(self, table: str, records: List[Dict[str, Any]]) -> Tuple[bool, Optional[str]]:
        if not self.is_configured():
            return False, "Supabase credentials not fully configured"
        if not records:
            return True, None

        endpoint = f"{self.url}/rest/v1/{table}"
        data_bytes = json.dumps(records).encode("utf-8")
        req = urllib.request.Request(
            endpoint,
            data=data_bytes,
            headers={
                "apikey": self.anon_key,
                "Authorization": f"Bearer {self.anon_key}",
                "Content-Type": "application/json",
                "Prefer": "resolution=merge-duplicates",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=8.0) as resp:
                if resp.status in (200, 201, 204):
                    return True, None
                return False, f"Unexpected response status: {resp.status}"
        except urllib.error.HTTPError as e:
            err_msg = e.read().decode("utf-8")
            return False, f"HTTP {e.code}: {err_msg}"
        except Exception as e:
            return False, str(e)

    def test_connection(self) -> Tuple[bool, str]:
        if not self.is_configured():
            return False, "Supabase URL and Anon/Service Key are required."
        endpoint = f"{self.url}/rest/v1/agenda_tasks?select=id&limit=1"
        req = urllib.request.Request(
            endpoint,
            headers={
                "apikey": self.anon_key,
                "Authorization": f"Bearer {self.anon_key}",
            },
            method="GET",
        )
        try:
            with urllib.request.urlopen(req, timeout=8.0) as resp:
                if resp.status in (200, 204):
                    return True, "Supabase REST connection verified! Endpoint reachable."
                return False, f"HTTP {resp.status} received from Supabase"
        except urllib.error.HTTPError as e:
            # 404 or 400 might mean table doesn't exist yet, but endpoint is active
            if e.code == 404:
                return True, "Supabase endpoint reachable (run migration schema to create tables)."
            err_msg = e.read().decode("utf-8")
            return False, f"HTTP {e.code}: {err_msg}"
        except Exception as e:
            return False, f"Connection failed: {str(e)}"
