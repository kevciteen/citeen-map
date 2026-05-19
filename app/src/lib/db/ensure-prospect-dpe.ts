import { db } from "./client";

/**
 * Migration idempotente : ajoute la colonne `numero_dpe` sur la table
 * prospects. Utile pour les prospects de type maison / appartement —
 * on peut alors lier le prospect à sa fiche DPE détaillée
 * /maisons/[numero_dpe] ou /appartements/[numero_dpe] sans avoir à
 * relancer une recherche ADEME.
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

export async function ensureProspectDpe(): Promise<void> {
  if (ensured) return;
  await tryAddColumn("ALTER TABLE prospects ADD COLUMN numero_dpe TEXT");
  await db.exec(
    "CREATE INDEX IF NOT EXISTS idx_prospects_numero_dpe ON prospects(numero_dpe)",
  );
  ensured = true;
}
