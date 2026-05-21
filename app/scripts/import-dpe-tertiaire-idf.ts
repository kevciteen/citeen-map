/* eslint-disable no-console */
/**
 * Import du dataset ADEME `dpe-tertiaire` pour l'Île-de-France
 * → tables tertiary_buildings + tertiary_dpe (libsql / Turso ou SQLite local).
 *
 * Le dataset contient ~ 30-40k DPE tertiaires par département IDF, mais
 * inclut aussi du résidentiel collectif → filtré via isReallyTertiary().
 *
 * Idempotent : recharger remplace les DPE existants par leur version la plus
 * récente (UPSERT par source + external_id).
 *
 * Usage :
 *   npx tsx scripts/import-dpe-tertiaire-idf.ts          # tout l'IDF (8 dépts)
 *   npx tsx scripts/import-dpe-tertiaire-idf.ts 75 92    # filtre départements
 *   npx tsx scripts/import-dpe-tertiaire-idf.ts --reset 75   # purge + ré-importe
 */
import { db } from "../src/lib/db/client";
import { ensureTertiary } from "../src/lib/db/ensure-tertiary";
import {
  fetchDpeTertiaireByDepartement,
  extractLatLon,
  isReallyTertiary,
  normalizeSector,
  type DpeTertiaireRecord,
} from "../src/lib/services/dpe-tertiaire";

const ALL_IDF = ["75", "77", "78", "91", "92", "93", "94", "95"];
const PAGE_SIZE = 1000;

function num(v: unknown): number | null {
  if (v === null || v === undefined || v === "" || v === "\\N") return null;
  const n = Number(v);
  return Number.isFinite(n) && n !== 0 ? n : null;
}

function str(v: unknown): string | null {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  if (!s || s === "\\N" || s === "null") return null;
  return s;
}

function dpeClass(v: unknown): string | null {
  const s = str(v);
  if (!s) return null;
  const up = s.toUpperCase();
  return "ABCDEFG".includes(up[0]) ? up[0] : null;
}

function parseDateMs(v: unknown): number | null {
  const s = str(v);
  if (!s) return null;
  const t = Date.parse(s);
  return Number.isFinite(t) ? Math.floor(t / 1000) : null;
}

function buildAdresse(r: DpeTertiaireRecord): string | null {
  return str(r.geo_adresse) ?? str(r.nom_rue);
}

async function importBatch(records: DpeTertiaireRecord[], departement: string): Promise<{ imported: number; skipped: number }> {
  let imported = 0;
  let skipped = 0;

  for (const r of records) {
    const numero = str(r.numero_dpe);
    if (!numero) { skipped++; continue; }
    if (!isReallyTertiary(r)) { skipped++; continue; }
    const ll = extractLatLon(r);
    if (!ll) { skipped++; continue; }

    const adresse = buildAdresse(r);
    const codePostal = str(r.code_postal);
    const commune = str(r.commune);
    const codeInsee = str(r.code_insee_commune_actualise) ?? str(r.code_insee_commune);
    // Déduit le département du code postal réel (pas du paramètre d'import,
    // qui peut grouper plusieurs départements via le wildcard `code_postal:75*`).
    const realDepartement = codePostal ? codePostal.substring(0, 2) : departement;
    const secteur = normalizeSector(r.secteur_activite);
    const typeUsage = str(r.secteur_activite);
    const surface = num(r.surface_utile) ?? num(r.surface_habitable) ?? num(r.shon) ?? num(r.surface_thermique_lot);
    const anneeConstruction = num(r.annee_construction);

    // Upsert bâtiment (source = dpe-tertiaire, external_id = numero_dpe)
    const existing = await db.get<{ id: number }>(
      `SELECT id FROM tertiary_buildings WHERE source = 'dpe-tertiaire' AND external_id = ?`,
      [numero],
    );
    let buildingId: number;
    if (existing?.id) {
      buildingId = existing.id;
      await db.run(
        `UPDATE tertiary_buildings SET
           label = ?, adresse = ?, code_postal = ?, commune = ?, code_insee_commune = ?,
           departement = ?, lat = ?, lon = ?, secteur = ?, type_usage = ?,
           surface_m2 = ?, annee_construction = ?, imported_at = unixepoch()
         WHERE id = ?`,
        [
          adresse ?? `DPE ${numero}`,
          adresse,
          codePostal,
          commune,
          codeInsee,
          realDepartement,
          ll.lat,
          ll.lon,
          secteur,
          typeUsage,
          surface,
          anneeConstruction,
          buildingId,
        ],
      );
    } else {
      const res = await db.run(
        `INSERT INTO tertiary_buildings (
           source, external_id, label, adresse, code_postal, commune,
           code_insee_commune, departement, lat, lon, secteur, type_usage,
           surface_m2, annee_construction
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          "dpe-tertiaire",
          numero,
          adresse ?? `DPE ${numero}`,
          adresse,
          codePostal,
          commune,
          codeInsee,
          realDepartement,
          ll.lat,
          ll.lon,
          secteur,
          typeUsage,
          surface,
          anneeConstruction,
        ],
      );
      buildingId = res.lastInsertRowid;
    }

    // Upsert DPE
    await db.run(
      `INSERT INTO tertiary_dpe (
         building_id, numero_dpe, etiquette_dpe, etiquette_ges,
         conso_energie_primaire, emissions_ges, surface_utile,
         type_usage_dpe, date_etablissement
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(building_id) DO UPDATE SET
         numero_dpe = excluded.numero_dpe,
         etiquette_dpe = excluded.etiquette_dpe,
         etiquette_ges = excluded.etiquette_ges,
         conso_energie_primaire = excluded.conso_energie_primaire,
         emissions_ges = excluded.emissions_ges,
         surface_utile = excluded.surface_utile,
         type_usage_dpe = excluded.type_usage_dpe,
         date_etablissement = excluded.date_etablissement,
         cached_at = unixepoch()`,
      [
        buildingId,
        numero,
        dpeClass(r.classe_consommation_energie),
        dpeClass(r.classe_estimation_ges),
        num(r.consommation_energie),
        num(r.estimation_ges),
        num(r.surface_utile),
        typeUsage,
        parseDateMs(r.date_etablissement_dpe) ?? parseDateMs(r.date_reception_dpe),
      ],
    );

    imported++;
  }
  return { imported, skipped };
}

async function importDepartement(departement: string): Promise<void> {
  console.log(`\n[${departement}] début import…`);
  let after: string | null = null;
  let totalFetched = 0;
  let totalImported = 0;
  let totalSkipped = 0;
  let page = 0;

  while (true) {
    page++;
    const { records, nextAfter } = await fetchDpeTertiaireByDepartement({
      departement, size: PAGE_SIZE, after: after ?? undefined,
    });
    totalFetched += records.length;

    if (records.length === 0) {
      console.log(`[${departement}] page ${page} : 0 records → fin`);
      break;
    }

    const { imported, skipped } = await importBatch(records, departement);
    totalImported += imported;
    totalSkipped += skipped;
    console.log(
      `[${departement}] page ${page} : ${records.length} fetched, ${imported} imp, ${skipped} skip (cumulé ${totalImported}/${totalFetched})`,
    );

    if (!nextAfter || nextAfter === after) break;
    after = nextAfter;
  }

  console.log(`[${departement}] ✓ ${totalImported} importés / ${totalSkipped} filtrés (sur ${totalFetched} récupérés)`);
}

async function main() {
  await ensureTertiary();

  const args = process.argv.slice(2);
  const reset = args.includes("--reset");
  const depts = args.filter((a) => /^\d{2,3}$/.test(a));
  const target = depts.length ? depts : ALL_IDF;

  if (reset && target.length > 0) {
    console.log(`[RESET] Suppression des bâtiments existants pour ${target.join(", ")}…`);
    for (const dept of target) {
      const res = await db.run(
        `DELETE FROM tertiary_buildings WHERE source = 'dpe-tertiaire' AND departement = ?`,
        [dept],
      );
      console.log(`  [${dept}] supprimé ${res.changes} bâtiments`);
    }
  }

  console.log(`\nImport DPE tertiaire pour ${target.length} département(s) : ${target.join(", ")}`);
  const start = Date.now();
  for (const dept of target) {
    try {
      await importDepartement(dept);
    } catch (err) {
      console.error(`[${dept}] ERREUR :`, (err as Error).message);
    }
  }

  const summary = await db.all<{ secteur: string | null; n: number }>(
    `SELECT secteur, COUNT(*) AS n FROM tertiary_buildings GROUP BY secteur ORDER BY n DESC`,
  );
  console.log("\n=== Résumé par secteur ===");
  for (const s of summary) console.log(`  ${(s.secteur ?? "(null)").padEnd(28)} : ${s.n}`);

  const byDept = await db.all<{ departement: string | null; n: number }>(
    `SELECT departement, COUNT(*) AS n FROM tertiary_buildings GROUP BY departement ORDER BY departement`,
  );
  console.log("\n=== Résumé par département ===");
  for (const d of byDept) console.log(`  ${(d.departement ?? "??").padEnd(4)} : ${d.n}`);

  const total = await db.get<{ n: number }>(`SELECT COUNT(*) AS n FROM tertiary_buildings`);
  console.log(`\nTOTAL : ${total?.n ?? 0} bâtiments tertiaires en base`);
  console.log(`Durée : ${Math.round((Date.now() - start) / 1000)}s`);
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error("Fatal :", err);
    process.exit(1);
  });
