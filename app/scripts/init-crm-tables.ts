/* eslint-disable no-console */
import { sqlite } from "../src/lib/db/client";

sqlite.exec(`
  CREATE TABLE IF NOT EXISTS dpe_estimates (
    copro_id INTEGER PRIMARY KEY REFERENCES copros(id) ON DELETE CASCADE,
    classe_reelle TEXT,
    classe_simulee TEXT,
    classe_finale TEXT,
    ge_score_moyen REAL,
    conso_moyenne REAL,
    nb_dpe_individuels INTEGER,
    rayon_recherche INTEGER,
    computed_at INTEGER DEFAULT (unixepoch())
  );

  CREATE TABLE IF NOT EXISTS prospects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    copro_id INTEGER REFERENCES copros(id) ON DELETE CASCADE,
    custom_label TEXT,
    custom_address TEXT,
    custom_lat REAL,
    custom_lon REAL,
    stage TEXT NOT NULL DEFAULT 'lead',
    priority INTEGER NOT NULL DEFAULT 2,
    estimated_value REAL,
    expected_close_date INTEGER,
    next_action_at INTEGER,
    next_action_label TEXT,
    assigned_to TEXT,
    tags TEXT,
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    updated_at INTEGER NOT NULL DEFAULT (unixepoch())
  );
  CREATE INDEX IF NOT EXISTS idx_prospects_stage ON prospects(stage);
  CREATE INDEX IF NOT EXISTS idx_prospects_copro ON prospects(copro_id);
  CREATE INDEX IF NOT EXISTS idx_prospects_next_action ON prospects(next_action_at);

  CREATE TABLE IF NOT EXISTS contacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    prospect_id INTEGER NOT NULL REFERENCES prospects(id) ON DELETE CASCADE,
    role TEXT,
    full_name TEXT,
    email TEXT,
    phone TEXT,
    company TEXT,
    notes TEXT,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
  );
  CREATE INDEX IF NOT EXISTS idx_contacts_prospect ON contacts(prospect_id);

  CREATE TABLE IF NOT EXISTS notes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    prospect_id INTEGER NOT NULL REFERENCES prospects(id) ON DELETE CASCADE,
    body TEXT NOT NULL,
    author TEXT,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
  );
  CREATE INDEX IF NOT EXISTS idx_notes_prospect ON notes(prospect_id);

  CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    prospect_id INTEGER REFERENCES prospects(id) ON DELETE CASCADE,
    title TEXT NOT NULL,
    kind TEXT,
    due_at INTEGER,
    done_at INTEGER,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
  );
  CREATE INDEX IF NOT EXISTS idx_tasks_prospect ON tasks(prospect_id);
  CREATE INDEX IF NOT EXISTS idx_tasks_due ON tasks(due_at);

  CREATE TABLE IF NOT EXISTS activities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    prospect_id INTEGER NOT NULL REFERENCES prospects(id) ON DELETE CASCADE,
    type TEXT NOT NULL,
    payload TEXT,
    author TEXT,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
  );
  CREATE INDEX IF NOT EXISTS idx_activities_prospect ON activities(prospect_id);
`);

console.log("[OK] CRM tables ready.");
sqlite.close();
