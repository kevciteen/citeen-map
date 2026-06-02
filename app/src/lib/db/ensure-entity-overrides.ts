/**
 * Tables d'OVERLAY éditable au-dessus des données ADEME / Sirene / registres.
 *
 * Architecture : les modules consomment des sources publiques (ADEME pour les
 * DPE, Sirene pour les entreprises, etc.) qu'on ne peut pas modifier. Pour
 * permettre à l'utilisateur de corriger / enrichir / commenter ces données
 * sans perdre la source, on stocke ses modifs dans 3 tables d'overlay :
 *
 *   - entity_overrides : surcharge de champs spécifiques (nom, tel, mail, ...)
 *   - entity_notes     : notes libres datées par entité
 *   - entity_tags      : tags personnalisés (filtrables)
 *
 * `entity_type` + `entity_ref` adressent n'importe quelle entité de façon
 * polymorphe (cf. table directory) :
 *   - 'copro' + id_db
 *   - 'tertiary_building' + id_db
 *   - 'occupant' + id_db
 *   - 'dpe' + numero_dpe
 *   - 'address' + label normalisé (pour les adresses sans entité DB)
 *   - 'maison' + numero_dpe
 *   - 'appartement' + numero_dpe
 *   - 'syndic' + slug
 */
import { db } from "./client";

let ensured = false;

export type OverrideEntityType =
  | "copro"
  | "tertiary_building"
  | "occupant"
  | "dpe"
  | "address"
  | "maison"
  | "appartement"
  | "syndic";

export async function ensureEntityOverrides(): Promise<void> {
  if (ensured) return;
  await db.exec(`
    CREATE TABLE IF NOT EXISTS entity_overrides (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      entity_type TEXT NOT NULL,
      entity_ref TEXT NOT NULL,
      field_name TEXT NOT NULL,
      value TEXT,
      author_id INTEGER,
      created_at INTEGER NOT NULL DEFAULT (unixepoch()),
      updated_at INTEGER NOT NULL DEFAULT (unixepoch())
    );
    CREATE UNIQUE INDEX IF NOT EXISTS ux_entity_overrides
      ON entity_overrides(entity_type, entity_ref, field_name);
    CREATE INDEX IF NOT EXISTS idx_entity_overrides_entity
      ON entity_overrides(entity_type, entity_ref);

    CREATE TABLE IF NOT EXISTS entity_notes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      entity_type TEXT NOT NULL,
      entity_ref TEXT NOT NULL,
      body TEXT NOT NULL,
      author_id INTEGER,
      created_at INTEGER NOT NULL DEFAULT (unixepoch())
    );
    CREATE INDEX IF NOT EXISTS idx_entity_notes_entity
      ON entity_notes(entity_type, entity_ref);
    CREATE INDEX IF NOT EXISTS idx_entity_notes_created
      ON entity_notes(created_at);

    CREATE TABLE IF NOT EXISTS entity_tags (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      entity_type TEXT NOT NULL,
      entity_ref TEXT NOT NULL,
      tag TEXT NOT NULL,
      author_id INTEGER,
      created_at INTEGER NOT NULL DEFAULT (unixepoch())
    );
    CREATE UNIQUE INDEX IF NOT EXISTS ux_entity_tags
      ON entity_tags(entity_type, entity_ref, tag);
    CREATE INDEX IF NOT EXISTS idx_entity_tags_entity
      ON entity_tags(entity_type, entity_ref);
    CREATE INDEX IF NOT EXISTS idx_entity_tags_value
      ON entity_tags(tag);
  `);
  ensured = true;
}
