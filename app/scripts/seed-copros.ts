/* eslint-disable no-console */
import { createReadStream } from "node:fs";
import { join, resolve } from "node:path";
import { parse } from "csv-parse";
import { sqlite } from "../src/lib/db/client";

const CSV_DIR = resolve(process.cwd(), "..", "backend", "data", "copro");
const ALL_FILES = [
  "registredescopro75.csv",
  "registredescopro77.csv",
  "registredescopro78.csv",
  "registredescopro91.csv",
  "registredescopro92.csv",
  "registredescopro93.csv",
  "registredescopro94.csv",
  "registredescopro95.csv",
];
const ONLY = (process.env.DEPT || "").split(",").map((s) => s.trim()).filter(Boolean);
const FILES = ONLY.length
  ? ALL_FILES.filter((f) => ONLY.some((d) => f.includes(d)))
  : ALL_FILES;

function toNumberFR(v: unknown): number | null {
  if (v === null || v === undefined) return null;
  const s = String(v).trim().replace(/\s+/g, "").replace(",", ".");
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function pick<T = string>(row: Record<string, unknown>, keys: string[]): T | null {
  for (const k of keys) {
    const val = row[k];
    if (val !== undefined && val !== null && String(val).trim() !== "") {
      return val as T;
    }
  }
  return null;
}

function toInt(v: unknown): number | null {
  if (v === null || v === undefined) return null;
  const n = parseInt(String(v).replace(/\D/g, ""), 10);
  return Number.isFinite(n) ? n : null;
}

async function importFile(filename: string): Promise<{ read: number; inserted: number }> {
  const filepath = join(CSV_DIR, filename);
  const dep = filename.match(/\d{2}/)?.[0] ?? null;

  console.log(`\n[IMPORT] ${filename}`);

  const parser = createReadStream(filepath).pipe(
    parse({ columns: true, delimiter: ";", skip_empty_lines: true, relax_quotes: true, relax_column_count: true }),
  );

  const insert = sqlite.prepare(`
    INSERT OR IGNORE INTO copros (
      numero_immatriculation, nom_copro, adresse, code_postal, commune, departement,
      syndic, nb_lots, nb_lots_habitation, periode_construction,
      lat, lon,
      code_insee_commune, section, numero_parcelle, reference_cadastrale
    ) VALUES (
      @numero_immatriculation, @nom_copro, @adresse, @code_postal, @commune, @departement,
      @syndic, @nb_lots, @nb_lots_habitation, @periode_construction,
      @lat, @lon,
      @code_insee_commune, @section, @numero_parcelle, @reference_cadastrale
    )
  `);

  const txInsert = sqlite.transaction((rows: any[]) => {
    for (const row of rows) insert.run(row);
  });

  let read = 0;
  let inserted = 0;
  let batch: any[] = [];

  for await (const row of parser as AsyncIterable<Record<string, unknown>>) {
    read++;
    const lat = toNumberFR(pick(row, ["lat", "Lat", "LAT"]));
    const lon = toNumberFR(pick(row, ["long", "Long", "LONG", "lon", "Lon"]));
    if (lat == null || lon == null) continue;

    batch.push({
      numero_immatriculation: pick(row, ["numero_d_immatriculation"]),
      nom_copro: pick(row, ["nom_d_usage_de_la_copropriete", "nom_d_usage_copropriete"]),
      adresse: pick(row, ["adresse_de_reference", "numero_et_voie_adresse_de_reference"]),
      code_postal: pick(row, ["code_postal_adresse_de_reference", "code_postal"]),
      commune: pick(row, ["commune_adresse_de_reference", "nom_officiel_commune", "commune"]),
      departement: pick(row, ["code_officiel_departement"]) ?? dep,
      syndic: pick(row, ["raison_sociale_du_representant_legal", "syndic"]),
      nb_lots: toInt(pick(row, ["nombre_total_de_lots"])),
      nb_lots_habitation: toInt(pick(row, ["nombre_de_lots_a_usage_d_habitation"])),
      periode_construction: pick(row, ["periode_de_construction"]),
      lat,
      lon,
      code_insee_commune: pick(row, ["code_insee_commune_1"]),
      section: pick(row, ["section_1"]),
      numero_parcelle: pick(row, ["numero_parcelle_1"]),
      reference_cadastrale: pick(row, ["reference_cadastrale_1"]),
    });

    if (batch.length >= 1000) {
      txInsert(batch);
      inserted += batch.length;
      batch = [];
      process.stdout.write(`  read=${read}  inserted=${inserted}\r`);
    }
  }
  if (batch.length) {
    txInsert(batch);
    inserted += batch.length;
  }
  console.log(`\n[DONE] ${filename} — lues: ${read} | insérées: ${inserted}`);
  return { read, inserted };
}

async function main() {
  console.log("=== SEED COPROS START ===");
  console.log("DB:", resolve(process.cwd(), process.env.DATABASE_URL ?? "./data/citeen.db"));
  console.log("CSV dir:", CSV_DIR);

  // Apply schema (drizzle-kit migrations would do this; we keep it simple)
  sqlite.exec(`
    CREATE TABLE IF NOT EXISTS copros (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      numero_immatriculation TEXT NOT NULL UNIQUE,
      nom_copro TEXT,
      adresse TEXT,
      code_postal TEXT,
      commune TEXT,
      departement TEXT,
      syndic TEXT,
      nb_lots INTEGER,
      nb_lots_habitation INTEGER,
      periode_construction TEXT,
      lat REAL,
      lon REAL,
      code_insee_commune TEXT,
      section TEXT,
      numero_parcelle TEXT,
      reference_cadastrale TEXT,
      imported_at INTEGER DEFAULT (unixepoch())
    );
    CREATE INDEX IF NOT EXISTS idx_copros_bbox ON copros(lat, lon);
    CREATE INDEX IF NOT EXISTS idx_copros_cp ON copros(code_postal);
    CREATE INDEX IF NOT EXISTS idx_copros_commune ON copros(commune);
    CREATE INDEX IF NOT EXISTS idx_copros_dept ON copros(departement);
    CREATE INDEX IF NOT EXISTS idx_copros_syndic ON copros(syndic);
  `);

  let totalRead = 0;
  let totalInserted = 0;
  for (const f of FILES) {
    const { read, inserted } = await importFile(f);
    totalRead += read;
    totalInserted += inserted;
  }

  const count = sqlite.prepare("SELECT COUNT(*) AS c FROM copros").get() as { c: number };
  console.log(`\n=== IMPORT TERMINÉ — lues: ${totalRead} | insérées: ${totalInserted} | total en base: ${count.c} ===`);
  sqlite.close();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});

// Use db to avoid unused-import warning when tree-shaken
