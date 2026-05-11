/* eslint-disable no-console */
/**
 * Ajoute une virtual table FTS5 (full-text search) sur les copros pour
 * accélérer drastiquement la recherche multi-mots/multi-champs.
 *
 * LIKE '%term%' sur 134k rows = full scan (~500ms).
 * FTS5 MATCH = index inversé = quelques ms même sur 1M rows.
 */
import { sqlite } from "../src/lib/db/client";

const t0 = Date.now();
console.log("[FTS5] Setup virtual table…");

sqlite.exec(`
  DROP TABLE IF EXISTS copros_fts;
  CREATE VIRTUAL TABLE copros_fts USING fts5(
    nom_copro,
    adresse,
    syndic,
    commune,
    code_postal,
    numero_immatriculation,
    content='copros',
    content_rowid='id',
    tokenize = "unicode61 remove_diacritics 2"
  );
`);

console.log("[FTS5] Populating index from 134k copros (transaction)…");
const insert = sqlite.prepare(`
  INSERT INTO copros_fts (rowid, nom_copro, adresse, syndic, commune, code_postal, numero_immatriculation)
  SELECT id, nom_copro, adresse, syndic, commune, code_postal, numero_immatriculation FROM copros
`);
sqlite.transaction(() => {
  insert.run();
})();

const count = (
  sqlite.prepare("SELECT COUNT(*) AS c FROM copros_fts").get() as { c: number }
).c;
console.log(`[FTS5] Indexed ${count} rows in ${Date.now() - t0}ms`);

// Triggers pour maintenir l'index synchronisé sur INSERT / UPDATE / DELETE
console.log("[FTS5] Setup triggers…");
sqlite.exec(`
  DROP TRIGGER IF EXISTS copros_ai;
  DROP TRIGGER IF EXISTS copros_au;
  DROP TRIGGER IF EXISTS copros_ad;
  CREATE TRIGGER copros_ai AFTER INSERT ON copros BEGIN
    INSERT INTO copros_fts(rowid, nom_copro, adresse, syndic, commune, code_postal, numero_immatriculation)
    VALUES (new.id, new.nom_copro, new.adresse, new.syndic, new.commune, new.code_postal, new.numero_immatriculation);
  END;
  CREATE TRIGGER copros_au AFTER UPDATE ON copros BEGIN
    INSERT INTO copros_fts(copros_fts, rowid, nom_copro, adresse, syndic, commune, code_postal, numero_immatriculation)
    VALUES('delete', old.id, old.nom_copro, old.adresse, old.syndic, old.commune, old.code_postal, old.numero_immatriculation);
    INSERT INTO copros_fts(rowid, nom_copro, adresse, syndic, commune, code_postal, numero_immatriculation)
    VALUES (new.id, new.nom_copro, new.adresse, new.syndic, new.commune, new.code_postal, new.numero_immatriculation);
  END;
  CREATE TRIGGER copros_ad AFTER DELETE ON copros BEGIN
    INSERT INTO copros_fts(copros_fts, rowid, nom_copro, adresse, syndic, commune, code_postal, numero_immatriculation)
    VALUES('delete', old.id, old.nom_copro, old.adresse, old.syndic, old.commune, old.code_postal, old.numero_immatriculation);
  END;
`);

// Index supplémentaires sur les champs filtrés sans LIKE
console.log("[FTS5] Adding supporting indexes…");
sqlite.exec(`
  CREATE INDEX IF NOT EXISTS idx_copros_periode ON copros(periode_construction);
  CREATE INDEX IF NOT EXISTS idx_copros_lots_hab ON copros(nb_lots_habitation);
  CREATE INDEX IF NOT EXISTS idx_dpe_classe_finale ON dpe_estimates(classe_finale);
  CREATE INDEX IF NOT EXISTS idx_dpe_quality ON dpe_estimates(quality_level);
  CREATE INDEX IF NOT EXISTS idx_dpe_conso ON dpe_estimates(conso_moyenne);
  CREATE INDEX IF NOT EXISTS idx_prospects_copro ON prospects(copro_id);
`);

// Quick test
const test = sqlite
  .prepare(
    `SELECT COUNT(*) AS c FROM copros_fts WHERE copros_fts MATCH 'sabimo'`,
  )
  .get() as { c: number };
console.log(`[FTS5] Quick test "sabimo" → ${test.c} hits`);

console.log(`[FTS5] DONE in ${Date.now() - t0}ms`);
sqlite.close();
