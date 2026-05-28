import { db } from "./client";

/**
 * Ajoute idempotent les colonnes de provenance des coordonnées copros :
 *   - coords_source : 'registre' | 'ban' | 'cadastre' | 'none'
 *   - coords_score : confiance BAN (0..1) ou 1 pour registre/cadastre
 *   - coords_updated_at : timestamp dernière màj
 *
 * Permet d'afficher la qualité du géocodage côté UI et de prioriser le
 * backfill (re-géocoder les low-score en priorité).
 */
let ensured = false;

export async function ensureCoprosCoords(): Promise<void> {
  if (ensured) return;
  const cols = await db.all<{ name: string }>(`PRAGMA table_info(copros)`);
  const colSet = new Set(cols.map((c) => c.name));
  const additions: Array<[string, string]> = [
    ["coords_source", "TEXT"],
    ["coords_score", "REAL"],
    ["coords_updated_at", "INTEGER"],
  ];
  for (const [name, type] of additions) {
    if (!colSet.has(name)) {
      await db.exec(`ALTER TABLE copros ADD COLUMN ${name} ${type}`);
    }
  }
  // Backfill : toute copro avec lat/lon non-NULL et source non renseignée
  // vient du registre (import initial).
  await db.run(
    `UPDATE copros
       SET coords_source = 'registre', coords_score = 1.0
     WHERE coords_source IS NULL AND lat IS NOT NULL AND lon IS NOT NULL`,
  );
  ensured = true;
}
