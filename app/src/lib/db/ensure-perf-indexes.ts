import { db } from "./client";

/**
 * Index de performance ajoutés après audit perf (juin 2026).
 *
 * Toutes les requêtes du cockpit filtrent ou groupent sur dpe_estimates.classe_finale
 * (passoires F/G, topSyndics, priority...). Sans index, chaque scan = full table.
 *
 * Idem pour copros.nb_lots qui sert au préfiltre de /api/copros/priority.
 *
 * Idempotent (CREATE INDEX IF NOT EXISTS) et mémoïsé module-scope.
 */
let ensured = false;

export async function ensurePerfIndexes(): Promise<void> {
  if (ensured) return;
  await db.exec(`
    CREATE INDEX IF NOT EXISTS idx_dpe_estimates_classe
      ON dpe_estimates(classe_finale);
    CREATE INDEX IF NOT EXISTS idx_copros_nb_lots
      ON copros(nb_lots);
    CREATE INDEX IF NOT EXISTS idx_prospects_copro_stage
      ON prospects(copro_id, stage);
  `);
  ensured = true;
}
