import { db } from "./client";

/**
 * Migration idempotente : crée la table `documents` liée aux prospects.
 *
 * Pour la phase B, on stocke uniquement une URL externe (Google Drive,
 * Dropbox, OneDrive...) avec un nom et un type. Pas d'upload réel : ça
 * demanderait un service de stockage (Vercel Blob, S3) qu'on intégrera
 * dans une phase ultérieure si besoin.
 */
let ensured = false;

export async function ensureDocuments(): Promise<void> {
  if (ensured) return;
  await db.exec(`
    CREATE TABLE IF NOT EXISTS documents (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      prospect_id INTEGER NOT NULL REFERENCES prospects(id) ON DELETE CASCADE,
      name TEXT NOT NULL,
      url TEXT NOT NULL,
      kind TEXT,
      description TEXT,
      created_at INTEGER NOT NULL DEFAULT (unixepoch()),
      created_by TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_documents_prospect ON documents(prospect_id);
  `);
  ensured = true;
}
