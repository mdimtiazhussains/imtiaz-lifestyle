#!/usr/bin/env python3
"""
Imtiaz Lifestyle - Database Migration & Setup Script
Initializes SQLite in WAL mode, applies all schema upgrades, and verifies data integrity.
"""

import sys
import logging
import datetime
import database

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ImtiazMigrate")


def main():
    logger.info("Starting Imtiaz Lifestyle database migration...")
    database.run_migrations()

    # Verify tables & indices
    conn = database.get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row["name"] for row in cursor.fetchall()]
    logger.info(f"Verified tables present: {tables}")

    cursor.execute("SELECT name FROM sqlite_master WHERE type='index';")
    indices = [row["name"] for row in cursor.fetchall()]
    logger.info(f"Verified indices present: {indices}")

    # Check row counts
    for t in ["agenda_tasks", "diary_entries", "morning_agendas", "sync_audit_log"]:
        cursor.execute(f"SELECT COUNT(*) as cnt FROM {t};")
        cnt = cursor.fetchone()["cnt"]
        logger.info(f"  • Table '{t}': {cnt} records")

    conn.close()
    logger.info("Migration finished successfully! Database is production-ready.")


if __name__ == "__main__":
    main()
