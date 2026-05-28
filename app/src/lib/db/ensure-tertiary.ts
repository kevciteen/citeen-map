import { db } from "./client";

/**
 * Migration idempotente — tables tertiaire (Décret Tertiaire, BDNB,
 * DPE tertiaire ADEME, occupants SIRENE).
 *
 * Appelée au début de chaque route /api/tertiaire/* qui touche la DB,
 * pour garantir la présence des tables côté Turso/libsql (pas seulement
 * SQLite local).
 *
 * Ajoute aussi la FK `prospects.tertiary_building_id` si absente.
 */
let ensured = false;

export async function ensureTertiary(): Promise<void> {
  if (ensured) return;

  await db.exec(`
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
      dirigeants_json TEXT,
      cached_at INTEGER DEFAULT (unixepoch())
    );
    CREATE INDEX IF NOT EXISTS idx_occupants_building ON tertiary_occupants(building_id);
    CREATE INDEX IF NOT EXISTS idx_occupants_siret ON tertiary_occupants(siret);
  `);

  // Ajout idempotent de la FK sur prospects (libsql ne supporte pas
  // IF NOT EXISTS sur ALTER TABLE — on vérifie via PRAGMA).
  const cols = await db.all<{ name: string }>(`PRAGMA table_info(prospects)`);
  const hasFk = cols.some((c) => c.name === "tertiary_building_id");
  if (!hasFk) {
    await db.exec(
      `ALTER TABLE prospects ADD COLUMN tertiary_building_id INTEGER REFERENCES tertiary_buildings(id) ON DELETE CASCADE`,
    );
  }
  await db.exec(
    `CREATE INDEX IF NOT EXISTS idx_prospects_tertiary ON prospects(tertiary_building_id)`,
  );

  // ALTER idempotent : colonnes ajoutées après le déploiement initial
  const occCols = await db.all<{ name: string }>(`PRAGMA table_info(tertiary_occupants)`);
  const colSet = new Set(occCols.map((c) => c.name));
  const additions: Array<[string, string]> = [
    ["dirigeants_json", "TEXT"],
    ["phone", "TEXT"],
    ["website", "TEXT"],
    ["email", "TEXT"],
    ["hours", "TEXT"],
    ["contact_source", "TEXT"],
    ["contact_fetched_at", "INTEGER"],
  ];
  for (const [name, type] of additions) {
    if (!colSet.has(name)) {
      await db.exec(`ALTER TABLE tertiary_occupants ADD COLUMN ${name} ${type}`);
    }
  }

  // Index unique partiel pour upsert refresh (préserve les contacts enrichis)
  await db.exec(
    `CREATE UNIQUE INDEX IF NOT EXISTS ux_tert_occ_building_siret
       ON tertiary_occupants(building_id, siret) WHERE siret IS NOT NULL`,
  );

  // Cache persistant des lookups contacts OSM/Google (TTL géré via expires_at)
  await db.exec(`
    CREATE TABLE IF NOT EXISTS contact_cache (
      cache_key TEXT PRIMARY KEY,
      payload_json TEXT NOT NULL,
      source TEXT NOT NULL,
      expires_at INTEGER NOT NULL,
      created_at INTEGER NOT NULL DEFAULT (unixepoch())
    );
    CREATE INDEX IF NOT EXISTS idx_contact_cache_expires ON contact_cache(expires_at);
  `);

  // Compteur d'usage Google Places (un compteur par jour UTC)
  await db.exec(`
    CREATE TABLE IF NOT EXISTS google_places_usage (
      day TEXT PRIMARY KEY,
      find_count INTEGER NOT NULL DEFAULT 0,
      details_count INTEGER NOT NULL DEFAULT 0,
      updated_at INTEGER NOT NULL DEFAULT (unixepoch())
    );
  `);

  ensured = true;
}
