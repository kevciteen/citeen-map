console.log("=== IMPORT SCRIPT START ===");
console.log("[DEBUG] cwd =", process.cwd());


import fs from "node:fs";
import path from "node:path";
import { parse } from "csv-parse";
import pg from "pg";
import "dotenv/config";

const { Client } = pg;

const DATA_DIR = path.join(process.cwd(), "data", "copro");
const FILES = [
  "registredescopro75.csv",
  "registredescopro77.csv",
  "registredescopro78.csv",
  "registredescopro91.csv",
  "registredescopro92.csv",
  "registredescopro93.csv",
  "registredescopro94.csv",
  "registredescopro95.csv",
];

function toNumberFR(v) {
  if (v === null || v === undefined) return NaN;
  const s = String(v).trim().replace(/\s+/g, "").replace(",", ".");
  const n = Number(s);
  return Number.isFinite(n) ? n : NaN;
}

function pick(row, keys) {
  for (const k of keys) {
    const val = row[k];
    if (val !== undefined && val !== null && String(val).trim() !== "") return val;
  }
  return null;
}

async function importFile(client, filename) {
  const filepath = path.join(DATA_DIR, filename);
  const dep = filename.match(/\d{2}/)?.[0] ?? null;

  if (!fs.existsSync(filepath)) {
    console.warn(`[SKIP] introuvable: ${filepath}`);
    return;
  }

  console.log(`\n[IMPORT] ${filename}`);

  const parser = fs
    .createReadStream(filepath)
    .pipe(parse({ columns: true, delimiter: ";", skip_empty_lines: true }));

  const BATCH_SIZE = 1000;
  let batch = [];
  let read = 0;
  let inserted = 0;

  async function flush() {
    if (batch.length === 0) return;

    const values = [];
    const params = [];
    let i = 1;

    for (const r of batch) {
      params.push(
        `($${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++},$${i++},ST_SetSRID(ST_MakePoint($${i++},$${i++}),4326),$${i++},$${i++},$${i++},$${i++},$${i++})`
      );

      values.push(
        r.numero_immatriculation,
        r.nom_copro,
        r.adresse,
        r.code_postal,
        r.commune,
        r.syndic,
        r.lat,
        r.lon,
        r.lon,
        r.lat,
        r.code_insee_commune_1,
        r.section_1,
        r.numero_parcelle_1,
        r.reference_cadastrale_1,
        r.departement
      );
    }

    const sql = `
      INSERT INTO copros (
        numero_immatriculation, nom_copro, adresse, code_postal, commune, syndic,
        lat, lon, geom,
        code_insee_commune_1, section_1, numero_parcelle_1, reference_cadastrale_1,
        departement
      ) VALUES ${params.join(",")}
    `;

    await client.query(sql, values);
    inserted += batch.length;
    batch = [];
    process.stdout.write(`  -> insérés: ${inserted}\r`);
  }

  for await (const row of parser) {
    read++;

    const lat = toNumberFR(pick(row, ["lat", "Lat", "LAT"]));
    const lon = toNumberFR(pick(row, ["long", "Long", "LONG", "lon", "Lon"]));
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;

    batch.push({
      numero_immatriculation: pick(row, ["numero_d_immatriculation"]),
      nom_copro: pick(row, ["nom_d_usage_de_la_copropriete", "nom_d_usage_copropriete"]),
      adresse: pick(row, ["adresse_de_reference", "numero_et_voie_adresse_de_reference"]),
      code_postal: pick(row, ["code_postal_adresse_de_reference", "code_postal"]),
      commune: pick(row, ["commune_adresse_de_reference", "nom_officiel_commune", "commune"]),
      syndic: pick(row, ["raison_sociale_du_representant_legal", "syndic"]),
      lat,
      lon,
      code_insee_commune_1: pick(row, ["code_insee_commune_1"]),
      section_1: pick(row, ["section_1"]),
      numero_parcelle_1: pick(row, ["numero_parcelle_1"]),
      reference_cadastrale_1: pick(row, ["reference_cadastrale_1"]),
      departement: dep,
    });

    if (batch.length >= BATCH_SIZE) await flush();
  }

  await flush();
  console.log(`\n[DONE] ${filename} lues: ${read} | insérées: ${inserted}`);
}

async function main() {
  const client = new Client({ connectionString: process.env.DATABASE_URL });
  await client.connect();

  // sécurité : on repart propre à chaque import
  await client.query("TRUNCATE copros RESTART IDENTITY;");

  for (const f of FILES) await importFile(client, f);

  await client.end();
  console.log("\nImport terminé.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
