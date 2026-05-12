/* eslint-disable no-console */
/**
 * Finalisation FTS5 sur Turso : populate par chunks + triggers + ré-active FK.
 * À lancer si migrate-to-turso.ts a planté sur le populate FTS.
 */
import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { createClient } from "@libsql/client";

const envLocal = resolve(process.cwd(), ".env.local");
if (existsSync(envLocal)) {
  for (const line of readFileSync(envLocal, "utf8").split("\n")) {
    const m = line.match(/^([A-Z_][A-Z0-9_]*)=(.*)$/);
    if (m && !process.env[m[1]]) process.env[m[1]] = m[2].replace(/^["']|["']$/g, "");
  }
}

const remote = createClient({
  url: process.env.TURSO_DATABASE_URL!,
  authToken: process.env.TURSO_AUTH_TOKEN!,
  intMode: "number",
});

async function main() {
  const t0 = Date.now();

  // Compter ce qui est déjà dans copros_fts
  const ftsCountRow = (await remote.execute({
    sql: "SELECT COUNT(*) AS c FROM copros_fts",
  })).rows[0];
  const ftsCount = Number(ftsCountRow?.c ?? 0);
  const coprosCount = Number(
    (await remote.execute({ sql: "SELECT COUNT(*) AS c FROM copros" })).rows[0]?.c ?? 0,
  );

  console.log(`[FTS] État actuel : copros_fts=${ftsCount} / copros=${coprosCount}`);

  if (ftsCount > 0 && ftsCount < coprosCount) {
    console.log(`[FTS] Truncate partiel…`);
    await remote.execute({ sql: "DELETE FROM copros_fts" });
  }

  if (ftsCount < coprosCount) {
    const CHUNK = 5000;
    let offset = 0;
    while (offset < coprosCount) {
      const t = Date.now();
      await remote.execute({
        sql: `INSERT INTO copros_fts (rowid, nom_copro, adresse, syndic, commune, code_postal, numero_immatriculation)
              SELECT id, nom_copro, adresse, syndic, commune, code_postal, numero_immatriculation
              FROM copros
              ORDER BY id
              LIMIT ? OFFSET ?`,
        args: [CHUNK, offset],
      });
      offset += CHUNK;
      console.log(`  ${Math.min(offset, coprosCount).toLocaleString("fr-FR")}/${coprosCount.toLocaleString("fr-FR")} (+${((Date.now() - t) / 1000).toFixed(1)}s)`);
    }
    const final = Number(
      (await remote.execute({ sql: "SELECT COUNT(*) AS c FROM copros_fts" })).rows[0]?.c ?? 0,
    );
    console.log(`\n✓ copros_fts populé : ${final.toLocaleString("fr-FR")} rows`);
  } else {
    console.log(`✓ copros_fts déjà à jour, skip populate`);
  }

  // Triggers FTS pour synchro auto sur INSERT/UPDATE/DELETE de copros
  console.log(`\n[FTS] Création des triggers de synchro…`);
  const triggers = [
    `CREATE TRIGGER IF NOT EXISTS copros_ai AFTER INSERT ON copros BEGIN
       INSERT INTO copros_fts(rowid, nom_copro, adresse, syndic, commune, code_postal, numero_immatriculation)
       VALUES (new.id, new.nom_copro, new.adresse, new.syndic, new.commune, new.code_postal, new.numero_immatriculation);
     END`,
    `CREATE TRIGGER IF NOT EXISTS copros_au AFTER UPDATE ON copros BEGIN
       INSERT INTO copros_fts(copros_fts, rowid, nom_copro, adresse, syndic, commune, code_postal, numero_immatriculation)
       VALUES('delete', old.id, old.nom_copro, old.adresse, old.syndic, old.commune, old.code_postal, old.numero_immatriculation);
       INSERT INTO copros_fts(rowid, nom_copro, adresse, syndic, commune, code_postal, numero_immatriculation)
       VALUES (new.id, new.nom_copro, new.adresse, new.syndic, new.commune, new.code_postal, new.numero_immatriculation);
     END`,
    `CREATE TRIGGER IF NOT EXISTS copros_ad AFTER DELETE ON copros BEGIN
       INSERT INTO copros_fts(copros_fts, rowid, nom_copro, adresse, syndic, commune, code_postal, numero_immatriculation)
       VALUES('delete', old.id, old.nom_copro, old.adresse, old.syndic, old.commune, old.code_postal, old.numero_immatriculation);
     END`,
  ];
  for (const sql of triggers) {
    await remote.execute({ sql });
    console.log(`  ✓ trigger`);
  }

  await remote.execute({ sql: "PRAGMA foreign_keys = ON" });
  console.log(`\n[OK] Réactivation FK · durée totale ${((Date.now() - t0) / 1000).toFixed(1)}s`);

  // Sanity
  console.log(`\nVérifications :`);
  for (const sql of [
    "SELECT COUNT(*) AS c FROM copros",
    "SELECT COUNT(*) AS c FROM dpe_estimates",
    "SELECT COUNT(*) AS c FROM copros_fts",
    "SELECT COUNT(*) AS c FROM copros_fts WHERE copros_fts MATCH 'sabimo'",
  ]) {
    const r = await remote.execute({ sql });
    console.log(`  ${sql} → ${r.rows[0]?.c}`);
  }
}

main().catch((e) => {
  console.error("[FATAL]", e);
  process.exit(1);
});
