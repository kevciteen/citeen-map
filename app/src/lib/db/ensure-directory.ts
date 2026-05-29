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
    CREATE INDEX IF NOT EXISTS idx_directory_bbox ON directory(lat, lon);
    CREATE INDEX IF NOT EXISTS idx_directory_type ON directory(entity_type);
    CREATE INDEX IF NOT EXISTS idx_directory_parent_copro ON directory(parent_copro_id);
    CREATE INDEX IF NOT EXISTS idx_directory_parent_building ON directory(parent_building_id);
    CREATE INDEX IF NOT EXISTS idx_directory_postcode ON directory(postcode);
    CREATE INDEX IF NOT EXISTS idx_directory_synced ON directory(synced_at);
  `);
  ensured = true;
}
