/**
 * Compteurs globaux mémoïsés (totalCopros, totalSyndics, totalTertiaires…)
 *
 * Ces COUNT(*) / COUNT(DISTINCT) sont des **full table scans** sur des tables
 * de 100k+ lignes — ils prennent 500ms à 2s chacun. Avant cette mémo, chaque
 * navigation vers /copros ou /syndics les recalculait en SSR, bloquant
 * l'affichage de la page = ressenti "le clic ne répond pas".
 *
 * unstable_cache fournit un cache mémoire process-wide + Vercel data cache,
 * indépendant de `force-dynamic` (qui ne désactive QUE le rendering cache,
 * pas le data cache). TTL = 5 min → numéros à jour quand on importe vraiment.
 */
import { unstable_cache } from "next/cache";
import { db } from "@/lib/db/client";

export const getCoproSyndicCounts = unstable_cache(
  async () => {
    const [coprosRow, syndicsRow] = await Promise.all([
      db.get<{ c: number }>("SELECT COUNT(*) AS c FROM copros"),
      db.get<{ c: number }>(
        `SELECT COUNT(DISTINCT TRIM(syndic)) AS c
         FROM copros
         WHERE syndic IS NOT NULL AND LOWER(syndic) != 'non connu'`,
      ),
    ]);
    return {
      totalCopros: coprosRow?.c ?? 0,
      totalSyndics: syndicsRow?.c ?? 0,
    };
  },
  ["global-counts-copros-syndics"],
  { revalidate: 300, tags: ["global-counts"] },
);

/**
 * Agrégats tertiaires (COUNT + GROUP BY full-scan sur ~80k lignes). Recalculés
 * à chaque montage de la carte tertiaire avant mémoïsation → blocage de
 * l'affichage. Même traitement que les COUNT copro : parallélisé + caché 5 min.
 */
export const getTertiaireStats = unstable_cache(
  async () => {
    const [total, withCoords, parisCount, byDept, bySource] = await Promise.all([
      db.get<{ n: number }>("SELECT COUNT(*) AS n FROM tertiary_buildings"),
      db.get<{ n: number }>(
        "SELECT COUNT(*) AS n FROM tertiary_buildings WHERE lat IS NOT NULL AND lon IS NOT NULL",
      ),
      db.get<{ n: number }>(
        `SELECT COUNT(*) AS n FROM tertiary_buildings
         WHERE lat BETWEEN 48.80 AND 48.95 AND lon BETWEEN 2.20 AND 2.50`,
      ),
      db.all<{ departement: string | null; n: number }>(
        `SELECT departement, COUNT(*) AS n FROM tertiary_buildings
         GROUP BY departement ORDER BY n DESC LIMIT 10`,
      ),
      db.all<{ source: string | null; n: number }>(
        "SELECT source, COUNT(*) AS n FROM tertiary_buildings GROUP BY source",
      ),
    ]);
    return {
      totalBuildings: total?.n ?? 0,
      withCoords: withCoords?.n ?? 0,
      parisBboxCount: parisCount?.n ?? 0,
      byDept,
      bySource,
    };
  },
  ["global-counts-tertiaire"],
  { revalidate: 300, tags: ["global-counts"] },
);
