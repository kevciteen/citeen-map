import { db } from "./client";

/**
 * Tables collaboration :
 *  - comments : commentaires polymorphes (entity_type + entity_id), avec
 *    author_id (FK users), threading par parent_id, body en Markdown léger
 *    + format @mention `@[Nom](user:42)` parsé côté serveur.
 *  - notifications : 1 ligne par user notifié quand il est tagué/un commentaire
 *    le concerne. Marquage lu/non-lu.
 */
let ensured = false;

export async function ensureCommentsAndNotificationsTables(): Promise<void> {
  if (ensured) return;
  await db.exec(`
    CREATE TABLE IF NOT EXISTS comments (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      entity_type TEXT NOT NULL,        -- 'prospect' | 'copro' | 'maison' | 'syndic'
      entity_id TEXT NOT NULL,           -- TEXT pour accepter id num OU slug (syndic)
      author_id INTEGER NOT NULL,
      body TEXT NOT NULL,
      parent_id INTEGER,
      created_at INTEGER NOT NULL DEFAULT (unixepoch()),
      updated_at INTEGER NOT NULL DEFAULT (unixepoch())
    );
    CREATE INDEX IF NOT EXISTS idx_comments_entity ON comments(entity_type, entity_id);
    CREATE INDEX IF NOT EXISTS idx_comments_author ON comments(author_id);

    CREATE TABLE IF NOT EXISTS notifications (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      kind TEXT NOT NULL,                -- 'mention' | 'reply' | 'system'
      title TEXT NOT NULL,
      body TEXT,
      link TEXT,                          -- /prospects/123 par ex
      from_user_id INTEGER,
      from_user_name TEXT,
      entity_type TEXT,
      entity_id TEXT,
      comment_id INTEGER,
      read_at INTEGER,
      created_at INTEGER NOT NULL DEFAULT (unixepoch())
    );
    CREATE INDEX IF NOT EXISTS idx_notifications_user ON notifications(user_id, read_at);
    CREATE INDEX IF NOT EXISTS idx_notifications_user_recent ON notifications(user_id, created_at DESC);
  `);
  ensured = true;
}
