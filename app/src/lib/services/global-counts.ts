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
