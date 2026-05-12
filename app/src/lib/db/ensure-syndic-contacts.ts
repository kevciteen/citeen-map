import { db } from "./client";

/**
 * Crée la table syndic_contacts si elle n'existe pas encore.
 * Idempotent — appelé au début de chaque route API qui touche les syndics.
 * Marqué une fois en mémoire pour éviter le coût répété.
 *
 * Cette table stocke les enrichissements user (email, tel, notes…) qui ne
 * viennent ni du Registre national ni de l'API Sirene.
 */
let ensured = false;

export async function ensureSyndicContactsTable(): Promise<void> {
  if (ensured) return;
  await db.exec(`
    CREATE TABLE IF NOT EXISTS syndic_contacts (
      slug TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT,
      phone TEXT,
      contact_person TEXT,
      website TEXT,
      address_override TEXT,
      notes TEXT,
      sirene_json TEXT,
      created_at INTEGER NOT NULL DEFAULT (unixepoch()),
      updated_at INTEGER NOT NULL DEFAULT (unixepoch())
    );
  `);
  ensured = true;
}

export function slugifySyndic(name: string): string {
  return name
    .trim()
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80);
}
