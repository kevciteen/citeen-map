/**
 * Backfill des coordonnées copros importées sans lat/lon.
 *
 * Stratégie cascade :
 *   1. BAN forward sur (adresse + code_postal + commune) — résolution la plus
 *      fiable, score ≥ 0.6 exigé (résidentiel = filtre strict)
 *   2. Cadastre via (code_insee, section, numero_parcelle) — fallback légal
 *      qui retourne le centroïde de la parcelle (±20-50 m)
 *   3. Sinon : laisse NULL, marque coords_source = 'none'
 */
import { db } from "@/lib/db/client";
import { geocodeAddress } from "./ban";
import { getParcelByRef } from "./cadastre";
import { syncDirectoryCopro } from "./directory-sync";

export type BackfillBatchResult = {
  scanned: number;
  byBan: number;
  byCadastre: number;
  unresolved: number;
  errors: number;
};

const BAN_MIN_SCORE = 0.6;

type CoproRow = {
  id: number;
  adresse: string | null;
  code_postal: string | null;
  commune: string | null;
  code_insee_commune: string | null;
  section: string | null;
  numero_parcelle: string | null;
};

export async function backfillCoprosCoordsBatch(
  limit = 100,
): Promise<BackfillBatchResult> {
  const rows = await db.all<CoproRow>(
    `SELECT id, adresse, code_postal, commune, code_insee_commune, section, numero_parcelle
     FROM copros
     WHERE (lat IS NULL OR lon IS NULL)
       AND (coords_source IS NULL OR coords_source != 'none')
     LIMIT ?`,
    [limit],
  );

  let byBan = 0;
  let byCadastre = 0;
  let unresolved = 0;
  let errors = 0;

  for (const row of rows) {
    try {
      const banResult = await tryBan(row);
      if (banResult) {
        await db.run(
          `UPDATE copros SET
             lat = ?, lon = ?,
             coords_source = 'ban', coords_score = ?, coords_updated_at = unixepoch()
           WHERE id = ?`,
          [banResult.lat, banResult.lon, banResult.score, row.id],
        );
        await syncDirectoryCopro(row.id).catch(() => {});
        byBan++;
        continue;
      }
      const cadResult = await tryCadastre(row);
      if (cadResult) {
        await db.run(
          `UPDATE copros SET
             lat = ?, lon = ?,
             coords_source = 'cadastre', coords_score = 1.0, coords_updated_at = unixepoch()
           WHERE id = ?`,
          [cadResult.lat, cadResult.lon, row.id],
        );
        await syncDirectoryCopro(row.id).catch(() => {});
        byCadastre++;
        continue;
      }
      // Marque comme tenté pour ne pas réessayer indéfiniment
      await db.run(
        `UPDATE copros SET
           coords_source = 'none', coords_updated_at = unixepoch()
         WHERE id = ?`,
        [row.id],
      );
      await syncDirectoryCopro(row.id).catch(() => {});
      unresolved++;
    } catch {
      errors++;
    }
  }

  return { scanned: rows.length, byBan, byCadastre, unresolved, errors };
}

async function tryBan(
  row: CoproRow,
): Promise<{ lat: number; lon: number; score: number } | null> {
  const adr = row.adresse?.trim();
  if (!adr) return null;
  const cp = row.code_postal?.trim();
  const commune = row.commune?.trim();
  const query = [adr, cp, commune].filter(Boolean).join(" ");
  if (query.length < 6) return null;
  const results = await geocodeAddress(query, {
    limit: 1,
    postcode: cp || undefined,
  });
  const best = results[0];
  if (!best || best.score < BAN_MIN_SCORE) return null;
  return { lat: best.lat, lon: best.lon, score: best.score };
}

async function tryCadastre(
  row: CoproRow,
): Promise<{ lat: number; lon: number } | null> {
  if (!row.code_insee_commune || !row.section || !row.numero_parcelle) {
    return null;
  }
  const parcelle = await getParcelByRef(
    row.code_insee_commune,
    row.section,
    row.numero_parcelle,
  );
  if (!parcelle) return null;
  return { lat: parcelle.centroidLat, lon: parcelle.centroidLon };
}
