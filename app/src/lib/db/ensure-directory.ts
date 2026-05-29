/**
 * Table `directory` — annuaire unifié des entités prospectables.
 *
 * Vision unique des sociétés / copros / syndics / adresses libres avec
 * leurs coordonnées (lat/lon) et canaux de contact (phone/email/website).
 * Permet une recherche cross-entité ("où trouver le syndic XYZ + ses copros
 * géolocalisées + les sociétés tertiaires du même quartier").
 *
 * À ne pas confondre avec la table existante `contacts` (CRM) qui stocke les
 * personnes physiques liées à un prospect (gardien, président conseil syndical…).
 *
 * Alimentée par /api/directory/sync depuis les tables canoniques (copros,
 * tertiary_occupants, syndic_contacts, prospects) qui restent la source de
 * vérité côté écriture. Pour l'instant : read-only mirror, basculable en
 * double-write plus tard.
 */
import { db } from "./client";

let ensured = false;

export type DirectoryEntityType =
  | "occupant"
  | "copro"
  | "syndic"
  | "prospect_custom";

export async function ensureDirectory(): Promise<void> {
  if (ensured) return;
  await db.exec(`
    CREATE TABLE IF NOT EXISTS directory (
      id INTEGER PRIMARY KEY AUTOINCREMENT,

      -- Polymorphic FK : (entity_type, entity_ref) identifie de manière
      -- unique la ligne source. entity_ref = id (TEXT cast) pour
      -- occupant/copro/prospect, slug pour syndic.
      entity_type TEXT NOT NULL,
      entity_ref TEXT NOT NULL,

      -- Affichage
      display_name TEXT NOT NULL,
      display_subtitle TEXT,

      -- Localisation
      address TEXT,
      postcode TEXT,
      city TEXT,
      departement TEXT,
      lat REAL,
      lon REAL,
      coords_source TEXT,
      coords_score REAL,

      -- Canaux (flat — manuel prioritaire, fallback auto)
      phone TEXT,
      phone_source TEXT,
      email TEXT,
      email_source TEXT,
      website TEXT,
      website_source TEXT,
      hours TEXT,

      -- Liens contextuels
      parent_copro_id INTEGER,
      parent_building_id INTEGER,

      -- Audit
      enriched_at INTEGER,
      synced_at INTEGER NOT NULL DEFAULT (unixepoch()),
      created_at INTEGER NOT NULL DEFAULT (unixepoch()),
      updated_at INTEGER NOT NULL DEFAULT (unixepoch())
    );
    CREATE UNIQUE INDEX IF NOT EXISTS ux_directory_entity ON directory(entity_type, entity_ref);
    -- Indexes pour les filtres avancés annuaire (DPE class, lots, secteur)
    -- Les colonnes elles-mêmes sont ajoutées en ALTER plus bas (idempotent).
    CREATE INDEX IF NOT EXISTS idx_directory_bbox ON directory(lat, lon);
    CREATE INDEX IF NOT EXISTS idx_directory_type ON directory(entity_type);
    CREATE INDEX IF NOT EXISTS idx_directory_parent_copro ON directory(parent_copro_id);
    CREATE INDEX IF NOT EXISTS idx_directory_parent_building ON directory(parent_building_id);
    CREATE INDEX IF NOT EXISTS idx_directory_postcode ON directory(postcode);
    CREATE INDEX IF NOT EXISTS idx_directory_synced ON directory(synced_at);

    -- Indexes composites pour filtres fréquents annuaire
    CREATE INDEX IF NOT EXISTS idx_directory_type_postcode ON directory(entity_type, postcode);
    CREATE INDEX IF NOT EXISTS idx_directory_type_dept ON directory(entity_type, departement);
  `);

  // ALTER idempotent : colonnes ajoutées après le déploiement initial
  // pour les filtres avancés (DPE, nb_lots, secteur tertiaire)
  const cols = await db.all<{ name: string }>(`PRAGMA table_info(directory)`);
  const colSet = new Set(cols.map((c) => c.name));
  const additions: Array<[string, string]> = [
    ["dpe_class", "TEXT"],
    ["nb_lots", "INTEGER"],
    ["secteur", "TEXT"],
  ];
  for (const [name, type] of additions) {
    if (!colSet.has(name)) {
      await db.exec(`ALTER TABLE directory ADD COLUMN ${name} ${type}`);
    }
  }
  await db.exec(`
    CREATE INDEX IF NOT EXISTS idx_directory_dpe ON directory(dpe_class);
    CREATE INDEX IF NOT EXISTS idx_directory_secteur ON directory(secteur);
  `);

  // FTS5 : recherche full-text sub-10ms sur display_name + address +
  // city + postcode. Évite les LIKE '%x%' qui font full-scan sur 100k+ rows.
  // Synchronisée via triggers AFTER INSERT/UPDATE/DELETE sur directory.
  await db.exec(`
    CREATE VIRTUAL TABLE IF NOT EXISTS directory_fts USING fts5(
      display_name,
      address,
      city,
      postcode,
      content='directory',
      content_rowid='id',
      tokenize='unicode61 remove_diacritics 2'
    );
  `);
  await db.exec(`
    CREATE TRIGGER IF NOT EXISTS directory_fts_ai AFTER INSERT ON directory BEGIN
      INSERT INTO directory_fts(rowid, display_name, address, city, postcode)
      VALUES (new.id,
              COALESCE(new.display_name, ''),
              COALESCE(new.address, ''),
              COALESCE(new.city, ''),
              COALESCE(new.postcode, ''));
    END;
    CREATE TRIGGER IF NOT EXISTS directory_fts_ad AFTER DELETE ON directory BEGIN
      INSERT INTO directory_fts(directory_fts, rowid, display_name, address, city, postcode)
      VALUES('delete', old.id,
             COALESCE(old.display_name, ''),
             COALESCE(old.address, ''),
             COALESCE(old.city, ''),
             COALESCE(old.postcode, ''));
    END;
    CREATE TRIGGER IF NOT EXISTS directory_fts_au AFTER UPDATE ON directory BEGIN
      INSERT INTO directory_fts(directory_fts, rowid, display_name, address, city, postcode)
      VALUES('delete', old.id,
             COALESCE(old.display_name, ''),
             COALESCE(old.address, ''),
             COALESCE(old.city, ''),
             COALESCE(old.postcode, ''));
      INSERT INTO directory_fts(rowid, display_name, address, city, postcode)
      VALUES (new.id,
              COALESCE(new.display_name, ''),
              COALESCE(new.address, ''),
              COALESCE(new.city, ''),
              COALESCE(new.postcode, ''));
    END;
  `);

  ensured = true;
}

/**
 * Reconstruit l'index FTS depuis le contenu actuel de directory.
 * À appeler après une grosse sync si on suspecte une désync FTS↔directory.
 */
export async function rebuildDirectoryFts(): Promise<void> {
  await ensureDirectory();
  await db.exec(`INSERT INTO directory_fts(directory_fts) VALUES('rebuild')`);
}
