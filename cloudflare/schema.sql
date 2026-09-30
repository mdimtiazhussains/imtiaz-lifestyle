-- =============================================================================
-- Cloudflare D1 Migration Schema for Imtiaz Lifestyle
-- Run with: npx wrangler d1 execute imtiaz-lifestyle-db --file=./schema.sql
-- =============================================================================

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
);

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
);

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
);

CREATE INDEX IF NOT EXISTS idx_agenda_date ON agenda_tasks (date, time_slot);
CREATE INDEX IF NOT EXISTS idx_agenda_status ON agenda_tasks (status);
CREATE INDEX IF NOT EXISTS idx_diary_date ON diary_entries (date);
CREATE INDEX IF NOT EXISTS idx_morning_date ON morning_agendas (date);
