/* eslint-disable no-console */
/**
 * Migration idempotente — tables tertiaire (Décret Tertiaire, BDNB, DPE tertiaire).
 *
 * Crée :
 *   - tertiary_buildings
 *   - tertiary_dpe
 *   - tertiary_occupants
 * Et ajoute la FK `tertiary_building_id` à `prospects` si absente.
 *
 * Idempotent : peut être rejoué sans danger.
 */
import { sqlite } from "../src/lib/db/client";

sqlite.exec(`
  CREATE TABLE IF NOT EXISTS tertiary_buildings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    external_id TEXT,
    label TEXT,
    adresse TEXT,
    code_postal TEXT,
    commune TEXT,
    code_insee_commune TEXT,
    departement TEXT,
    lat REAL,
    lon REAL,
    section TEXT,
    numero_parcelle TEXT,
    reference_cadastrale TEXT,
    secteur TEXT,
    type_usage TEXT,
    surface_m2 REAL,
    annee_construction INTEGER,
    imported_at INTEGER DEFAULT (unixepoch())
  );
  CREATE INDEX IF NOT EXISTS idx_tertiary_bbox ON tertiary_buildings(lat, lon);
  CREATE INDEX IF NOT EXISTS idx_tertiary_cp ON tertiary_buildings(code_postal);
  CREATE INDEX IF NOT EXISTS idx_tertiary_commune ON tertiary_buildings(commune);
  CREATE INDEX IF NOT EXISTS idx_tertiary_dept ON tertiary_buildings(departement);
  CREATE INDEX IF NOT EXISTS idx_tertiary_secteur ON tertiary_buildings(secteur);
  CREATE INDEX IF NOT EXISTS idx_tertiary_source ON tertiary_buildings(source, external_id);

  CREATE TABLE IF NOT EXISTS tertiary_dpe (
    building_id INTEGER PRIMARY KEY REFERENCES tertiary_buildings(id) ON DELETE CASCADE,
    numero_dpe TEXT,
    etiquette_dpe TEXT,
    etiquette_ges TEXT,
    conso_energie_primaire REAL,
    conso_energie_finale REAL,
    emissions_ges REAL,
    surface_utile REAL,
    type_usage_dpe TEXT,
    date_etablissement INTEGER,
    date_modification INTEGER,
    cached_at INTEGER DEFAULT (unixepoch())
  );

  CREATE TABLE IF NOT EXISTS tertiary_occupants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    building_id INTEGER NOT NULL REFERENCES tertiary_buildings(id) ON DELETE CASCADE,
    siret TEXT,
    siren TEXT,
    denomination TEXT,
    naf_code TEXT,
    naf_label TEXT,
    tranche_effectif TEXT,
    adresse_enregistree TEXT,
    est_siege INTEGER,
    est_actif INTEGER,
    cached_at INTEGER DEFAULT (unixepoch())
  );
  CREATE INDEX IF NOT EXISTS idx_occupants_building ON tertiary_occupants(building_id);
  CREATE INDEX IF NOT EXISTS idx_occupants_siret ON tertiary_occupants(siret);
`);

// Ajout idempotent de la colonne FK sur prospects
const prospectsCols = sqlite
  .prepare("PRAGMA table_info(prospects)")
  .all() as Array<{ name: string }>;
const hasTertiaryFk = prospectsCols.some((c) => c.name === "tertiary_building_id");
if (!hasTertiaryFk) {
  sqlite.exec(
    `ALTER TABLE prospects ADD COLUMN tertiary_building_id INTEGER REFERENCES tertiary_buildings(id) ON DELETE CASCADE;`,
  );
  console.log("[ALTER] prospects.tertiary_building_id ajoutée");
} else {
  console.log("[SKIP] prospects.tertiary_building_id déjà présente");
}
sqlite.exec(
  `CREATE INDEX IF NOT EXISTS idx_prospects_tertiary ON prospects(tertiary_building_id);`,
);

console.log("[OK] Tables tertiaire prêtes.");
sqlite.close();
