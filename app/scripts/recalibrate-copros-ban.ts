/* eslint-disable no-console */
/**
 * Recalibrage massif des copros via BAN reverse-geocode.
 *
 * Pour chaque copro avec lat/lon dans la base :
 *  1. On envoie (id, lat, lon) à l'endpoint bulk BAN /reverse/csv/
 *  2. La BAN renvoie l'adresse canonique (housenumber, street, postcode, city)
 *     + ses propres lat/lon "snappées" sur le bâtiment
 *  3. Si score ≥ 0.8 → on remplace lat, lon, adresse, et code_postal si absent.
 *  4. On garde une sauvegarde dans original_lat / original_lon / original_adresse
 *     + on stocke ban_score, ban_label, ban_housenumber, ban_street pour audit.
 *
 * Le cache dpe_estimates est purgé à la fin (les positions changent → l'algo
 * doit recalculer le matching strict).
 */
import { parse } from "csv-parse/sync";
import { sqlite } from "../src/lib/db/client";

const BATCH_SIZE = 25_000;
const BAN_URL = "https://api-adresse.data.gouv.fr/reverse/csv/";
/* La BAN bulk reverse ne renvoie PAS un score numérique mais :
   - result_status : "ok" / "not-found" / "skipped"
   - result_type   : "housenumber" / "street" / "locality" / "municipality"
   - result_distance : distance en mètres entre la coord envoyée et l'adresse trouvée
   Critère de confiance : status=ok ET type=housenumber ET distance ≤ MAX_DIST. */
const HIGH_CONFIDENCE_TYPES = new Set(["housenumber", "street"]);
const MAX_DISTANCE_M = 80;

type BanResultRow = Record<string, string>;

function ensureBackupColumns() {
  const cols = sqlite
    .prepare("SELECT name FROM pragma_table_info('copros')")
    .all() as { name: string }[];
  const names = new Set(cols.map((c) => c.name));
  const toAdd: [string, string][] = [
    ["original_lat", "REAL"],
    ["original_lon", "REAL"],
    ["original_adresse", "TEXT"],
    ["ban_score", "REAL"],
    ["ban_label", "TEXT"],
    ["ban_housenumber", "TEXT"],
    ["ban_street", "TEXT"],
    ["ban_postcode", "TEXT"],
    ["ban_city", "TEXT"],
    ["ban_resolved_at", "INTEGER"],
  ];
  for (const [col, type] of toAdd) {
    if (!names.has(col)) {
      sqlite.exec(`ALTER TABLE copros ADD COLUMN ${col} ${type}`);
      console.log(`[migrate] added column ${col}`);
    }
  }
}

function backupOriginals() {
  const info = sqlite
    .prepare(
      "UPDATE copros SET original_lat = lat, original_lon = lon, original_adresse = adresse WHERE original_lat IS NULL AND lat IS NOT NULL",
    )
    .run();
  console.log(`[backup] saved originals on ${info.changes} rows`);
}

async function reverseBatchBan(
  chunk: Array<{ id: number; lat: number; lon: number }>,
): Promise<BanResultRow[]> {
  const lines = ["id,latitude,longitude"];
  for (const r of chunk) lines.push(`${r.id},${r.lat},${r.lon}`);
  const csv = lines.join("\n");

  const fd = new FormData();
  fd.append("data", new Blob([csv], { type: "text/csv" }), "data.csv");

  const res = await fetch(BAN_URL, { method: "POST", body: fd });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`BAN HTTP ${res.status}: ${body.slice(0, 300)}`);
  }
  const text = await res.text();
  const records: BanResultRow[] = parse(text, {
    columns: true,
    skip_empty_lines: true,
    relax_quotes: true,
    relax_column_count: true,
  });
  return records;
}

async function main() {
  console.log("=== RECALIBRAGE COPROS via BAN reverse bulk ===");
  ensureBackupColumns();
  backupOriginals();

  const total = (
    sqlite
      .prepare("SELECT COUNT(*) AS c FROM copros WHERE lat IS NOT NULL AND lon IS NOT NULL")
      .get() as { c: number }
  ).c;
  console.log(`[start] ${total} copros à recaler`);

  const rows = sqlite
    .prepare(
      "SELECT id, lat, lon FROM copros WHERE lat IS NOT NULL AND lon IS NOT NULL ORDER BY id",
    )
    .all() as { id: number; lat: number; lon: number }[];

  /* La BAN bulk reverse ne fournit pas de lat/lon recalées — on garde celles du
     registre (déjà à <30m de la bonne entrée d'immeuble en général) mais on
     remplace adresse / code_postal / commune par les valeurs canoniques BAN. */
  const update = sqlite.prepare(`
    UPDATE copros SET
      adresse = ?,
      code_postal = ?,
      commune = ?,
      ban_score = ?, ban_label = ?, ban_housenumber = ?, ban_street = ?,
      ban_postcode = ?, ban_city = ?, ban_resolved_at = unixepoch()
    WHERE id = ?
  `);
  const audit = sqlite.prepare(`
    UPDATE copros SET
      ban_score = ?, ban_label = ?, ban_housenumber = ?, ban_street = ?,
      ban_postcode = ?, ban_city = ?, ban_resolved_at = unixepoch()
    WHERE id = ?
  `);

  let processed = 0;
  let highConfidence = 0;
  let lowConfidence = 0;
  let noResult = 0;
  let errors = 0;

  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const chunk = rows.slice(i, i + BATCH_SIZE);
    console.log(
      `[batch ${Math.floor(i / BATCH_SIZE) + 1}] sending ${chunk.length} rows to BAN…`,
    );
    let results: BanResultRow[];
    try {
      results = await reverseBatchBan(chunk);
    } catch (err) {
      console.error(`  ERROR: ${(err as Error).message}`);
      errors += chunk.length;
      continue;
    }

    const byId = new Map<number, BanResultRow>();
    for (const r of results) {
      const id = Number(r.id);
      if (Number.isFinite(id)) byId.set(id, r);
    }

    const trx = sqlite.transaction(() => {
      for (const src of chunk) {
        const r = byId.get(src.id);
        if (!r) {
          noResult++;
          continue;
        }
        const status = (r.result_status || "").trim();
        const type = (r.result_type || "").trim();
        const distance = Number(r.result_distance);
        const label = r.result_label || null;
        const housenumber = r.result_housenumber || null;
        const street = r.result_street || null;
        const postcode = r.result_postcode || null;
        const city = r.result_city || null;

        // Score synthétique 0–1 pour la colonne ban_score
        // (basé sur type + distance, faute de score natif fourni par /reverse/csv/)
        let pseudoScore: number | null = null;
        if (status === "ok" && Number.isFinite(distance)) {
          if (type === "housenumber") pseudoScore = Math.max(0, 1 - distance / 200);
          else if (type === "street") pseudoScore = Math.max(0, 0.7 - distance / 300);
          else pseudoScore = Math.max(0, 0.4 - distance / 500);
        }

        const isHighConfidence =
          status === "ok" &&
          HIGH_CONFIDENCE_TYPES.has(type) &&
          Number.isFinite(distance) &&
          distance <= MAX_DISTANCE_M &&
          street;

        if (isHighConfidence) {
          const canonicalAddress = housenumber ? `${housenumber} ${street}` : street;
          update.run(
            canonicalAddress,
            postcode,
            city,
            pseudoScore,
            label,
            housenumber,
            street,
            postcode,
            city,
            src.id,
          );
          highConfidence++;
        } else {
          audit.run(
            pseudoScore,
            label,
            housenumber,
            street,
            postcode,
            city,
            src.id,
          );
          lowConfidence++;
        }
      }
    });
    trx();

    processed += chunk.length;
    console.log(
      `  done. cumulé: processed=${processed}, high=${highConfidence}, low=${lowConfidence}, noResult=${noResult}, errors=${errors}`,
    );
  }

  // Purge le cache DPE (positions/adresses ont changé, le matching strict doit recalculer)
  const cleared = sqlite.prepare("DELETE FROM dpe_estimates").run().changes;

  console.log(`\n=== TERMINÉ ===`);
  console.log(`Total processed       : ${processed}`);
  console.log(`Recalés (type≤housenumber/street, dist ≤ ${MAX_DISTANCE_M}m) : ${highConfidence}`);
  console.log(`Confidence faible     : ${lowConfidence}`);
  console.log(`Pas de résultat BAN   : ${noResult}`);
  console.log(`Erreurs réseau        : ${errors}`);
  console.log(`Cache DPE purgé       : ${cleared} entrées`);
  sqlite.close();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
