import { db } from "./client";

/**
 * Crée la table users si elle n'existe pas. Idempotent.
 * Stocke : email (unique), password_hash bcrypt, role (admin/member),
 * name, created_at, last_login_at.
 */
let ensured = false;

export async function ensureUsersTable(): Promise<void> {
  if (ensured) return;
  await db.exec(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      email TEXT NOT NULL UNIQUE,
      password_hash TEXT NOT NULL,
      role TEXT NOT NULL DEFAULT 'member',
      name TEXT,
      active INTEGER NOT NULL DEFAULT 1,
      must_change_password INTEGER NOT NULL DEFAULT 0,
      created_at INTEGER NOT NULL DEFAULT (unixepoch()),
      created_by INTEGER,
      last_login_at INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
  `);
  ensured = true;
}
