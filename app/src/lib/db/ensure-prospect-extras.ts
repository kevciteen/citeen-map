import { db } from "./client";

/**
 * Migration idempotente : enrichit la table prospects avec les colonnes
 * Phase C (commercial) :
 *  - assigned_user_id (FK users.id, remplace progressivement assigned_to text)
 *  - won_reason / lost_reason : raison de gain ou perte (multiselect compressé en TEXT)
 *
 * SQLite ne supporte pas ADD COLUMN IF NOT EXISTS directement, on attrape
 * l'erreur "duplicate column" silencieusement.
 */
let ensured = false;

async function tryAddColumn(sql: string): Promise<void> {
  try {
    await db.exec(sql);
  } catch (e) {
    const msg = (e as Error).message ?? "";
    if (!/duplicate column|already exists/i.test(msg)) throw e;
  }
}

export async function ensureProspectExtras(): Promise<void> {
  if (ensured) return;
  await tryAddColumn("ALTER TABLE prospects ADD COLUMN assigned_user_id INTEGER");
  await tryAddColumn("ALTER TABLE prospects ADD COLUMN won_reason TEXT");
  await tryAddColumn("ALTER TABLE prospects ADD COLUMN lost_reason TEXT");
  await db.exec(
    "CREATE INDEX IF NOT EXISTS idx_prospects_assigned_user ON prospects(assigned_user_id)",
  );
  ensured = true;
}
