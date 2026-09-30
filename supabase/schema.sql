-- =============================================================================
-- Supabase PostgreSQL Schema for Imtiaz Lifestyle
-- Execute in Supabase SQL Editor: https://app.supabase.com/project/_/sql
-- =============================================================================

CREATE TABLE IF NOT EXISTS diary_entries (
    id TEXT PRIMARY KEY,
    date TEXT NOT NULL,
    time TEXT NOT NULL,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    mood TEXT DEFAULT 'Calm',
    energy INTEGER DEFAULT 3,
    tags JSONB DEFAULT '[]'::jsonb,
    gratitude_notes JSONB DEFAULT '[]'::jsonb,
    version INTEGER DEFAULT 1,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
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
    category_tags JSONB DEFAULT '["Work"]'::jsonb,
    status TEXT DEFAULT 'pending', -- pending, completed, deferred
    priority TEXT DEFAULT 'Medium',
    completed INTEGER DEFAULT 0,
    order_index INTEGER DEFAULT 0,
    version INTEGER DEFAULT 1,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS morning_agendas (
    id TEXT PRIMARY KEY,
    date TEXT UNIQUE NOT NULL,
    completed INTEGER DEFAULT 0,
    completed_at TEXT DEFAULT '',
    daily_objectives JSONB DEFAULT '[]'::jsonb,
    gratitudes JSONB DEFAULT '[]'::jsonb,
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
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Enable Row Level Security (RLS)
ALTER TABLE diary_entries ENABLE ROW LEVEL SECURITY;
ALTER TABLE agenda_tasks ENABLE ROW LEVEL SECURITY;
ALTER TABLE morning_agendas ENABLE ROW LEVEL SECURITY;

-- Allow anon read/write for demo / personal single-tenant use (customize with auth.uid() if multi-user)
CREATE POLICY "Allow public all access on diary_entries" ON diary_entries FOR ALL USING (true);
CREATE POLICY "Allow public all access on agenda_tasks" ON agenda_tasks FOR ALL USING (true);
CREATE POLICY "Allow public all access on morning_agendas" ON morning_agendas FOR ALL USING (true);
