/* eslint-disable no-console */
/**
 * Migration directe SQLite local → Turso (libsql cloud).
 *
 * STRATÉGIE BULK LOAD optimisée :
 *   1. Drop tout sur Turso
 *   2. Créer SEULEMENT les tables (pas d'indexes, pas de triggers, pas de FTS)
 *   3. Bulk-insert toutes les rows (rapide car aucune contrainte/index)
 *   4. Créer les indexes en fin (1 fois, sur DB déjà chargée)
 *   5. Créer la virtual table FTS5 + populate via INSERT…SELECT
 *   6. Créer les triggers FTS
 */
import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import Database from "better-sqlite3";
import { createClient } from "@libsql/client";

// Charge .env.local manuellement
const envLocal = resolve(process.cwd(), ".env.local");
if (existsSync(envLocal)) {
  for (const line of readFileSync(envLocal, "utf8").split("\n")) {
    const m = line.match(/^([A-Z_][A-Z0-9_]*)=(.*)$/);
    if (m && !process.env[m[1]]) process.env[m[1]] = m[2].replace(/^["']|["']$/g, "");
  }
}

const tursoUrl = process.env.TURSO_DATABASE_URL;
const tursoToken = process.env.TURSO_AUTH_TOKEN;
if (!tursoUrl?.startsWith("libsql://") || !tursoToken) {
  console.error("[ERROR] TURSO_DATABASE_URL ou TURSO_AUTH_TOKEN manquant dans .env.local");
  process.exit(1);
}

const localPath = resolve(process.cwd(), "data", "citeen.db");
if (!existsSync(localPath)) {
  console.error(`[ERROR] DB locale introuvable : ${localPath}`);
  process.exit(1);
}

console.log(`[INFO] Local : ${localPath}`);
console.log(`[INFO] Turso : ${tursoUrl}\n`);

const local = new Database(localPath, { readonly: true });
const remote = createClient({ url: tursoUrl, authToken: tursoToken, intMode: "number" });

type SqliteMaster = {
  type: string;
  name: string;
  tbl_name: string;
  sql: string | null;
};

function isFtsShadow(name: string): boolean {
  return /_fts_(data|idx|docsize|config|content)$/.test(name);
}
function isFtsVirtual(sql: string | null): boolean {
  return !!sql && /virtual\s+table/i.test(sql) && /fts5/i.test(sql);
}
function isFtsTrigger(name: string): boolean {
  return /^copros_a[iud]$/.test(name); // copros_ai/au/ad pour FTS sync
}

const TABLE_ORDER = [
  "copros",
  "dpe_estimates",
  "prospects",
  "contacts",
  "notes",
  "tasks",
  "activities",
];

async function exec(sql: string): Promise<void> {
  await remote.execute({ sql });
}

async function main() {
  const t0 = Date.now();

  /* 1. Récupérer le schéma local */
  const schema = (
    local
      .prepare(
        `SELECT type, name, tbl_name, sql FROM sqlite_master
         WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%'
         ORDER BY type, name`,
      )
      .all() as SqliteMaster[]
  ).filter((s) => !isFtsShadow(s.name));

  const tables = schema.filter((s) => s.type === "table" && !isFtsVirtual(s.sql));
  const indexes = schema.filter((s) => s.type === "index");
  const triggers = schema.filter((s) => s.type === "trigger" && !isFtsTrigger(s.name));
  const ftsTriggers = schema.filter((s) => s.type === "trigger" && isFtsTrigger(s.name));
  const ftsVirtuals = schema.filter((s) => s.type === "table" && isFtsVirtual(s.sql));

  console.log(
    `[SCHEMA] ${tables.length} tables, ${indexes.length} indexes, ${triggers.length} triggers métier, ${ftsTriggers.length} triggers FTS, ${ftsVirtuals.length} FTS virtual`,
  );

  /* 2. Wipe Turso */
  console.log(`\n[REMOTE] Drop existant…`);
  const remoteSchema = await remote.execute({
    sql: `SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'
          ORDER BY CASE type WHEN 'trigger' THEN 1 WHEN 'index' THEN 2 WHEN 'table' THEN 3 END`,
  });
  for (const row of remoteSchema.rows) {
    const type = String(row.type);
    const name = String(row.name);
    if (isFtsShadow(name)) continue;
    try {
      await exec(`DROP ${type} IF EXISTS "${name}"`);
    } catch {
      /* ignore */
    }
  }

  /* 3. Créer SEULEMENT les tables (pas d'index ni FTS) */
  console.log(`[REMOTE] Création des ${tables.length} tables…`);
  for (const t of tables) {
    if (!t.sql) continue;
    await exec(t.sql);
    console.log(`  ✓ table ${t.name}`);
  }

  /* 4. Désactiver les contraintes le temps de l'import */
  await exec("PRAGMA foreign_keys = OFF");

  /* 5. Bulk-insert table par table dans l'ordre des FK */
  const tableNames = new Set(tables.map((t) => t.name));
  const orderedTables = [
    ...TABLE_ORDER.filter((t) => tableNames.has(t)),
    ...[...tableNames].filter((t) => !TABLE_ORDER.includes(t)),
  ];

  let grandTotal = 0;
  for (const table of orderedTables) {
    const count = (local.prepare(`SELECT COUNT(*) AS c FROM "${table}"`).get() as { c: number }).c;
    if (count === 0) {
      console.log(`\n[${table}] vide — skip`);
      continue;
    }

    const cols = (
      local.prepare(`PRAGMA table_info("${table}")`).all() as { name: string }[]
    ).map((c) => c.name);
    const colList = cols.map((c) => `"${c}"`).join(",");

    // Sans indexes, chaque INSERT est très rapide. On peut utiliser des
    // batches modérés pour éviter le timeout HTTP (300s par défaut undici).
    const ROWS_PER_STATEMENT = Math.max(
      1,
      Math.min(300, Math.floor(20000 / Math.max(cols.length, 1))),
    );
    const STATEMENTS_PER_BATCH = 8;

    console.log(
      `\n[${table}] ${count.toLocaleString("fr-FR")} rows · ${cols.length} cols · ${ROWS_PER_STATEMENT}×${STATEMENTS_PER_BATCH} = ${ROWS_PER_STATEMENT * STATEMENTS_PER_BATCH} rows/batch`,
    );

    const iter = local.prepare(`SELECT ${colList} FROM "${table}"`).iterate();
    let pushed = 0;
    let rowBuffer: unknown[][] = [];
    let statementBuffer: Array<{ sql: string; args: unknown[] }> = [];

    const flushRowsToStatement = () => {
      if (rowBuffer.length === 0) return;
      const valuesSql = rowBuffer
        .map(() => `(${cols.map(() => "?").join(",")})`)
        .join(",");
      const flatArgs: unknown[] = [];
      for (const r of rowBuffer) flatArgs.push(...r);
      statementBuffer.push({
        sql: `INSERT INTO "${table}" (${colList}) VALUES ${valuesSql}`,
        args: flatArgs,
      });
      rowBuffer = [];
    };

    const flushBatch = async (retries = 3): Promise<void> => {
      if (statementBuffer.length === 0) return;
      const rowsInBatch = statementBuffer.reduce(
        (s, st) => s + st.args.length / cols.length,
        0,
      );
      const toSend = statementBuffer;
      statementBuffer = [];
      try {
        await remote.batch(toSend as never);
        pushed += rowsInBatch;
        process.stdout.write(
          `  ${pushed.toLocaleString("fr-FR")}/${count.toLocaleString("fr-FR")}\r`,
        );
      } catch (err) {
        if (retries > 0) {
          console.warn(
            `\n  ⚠ batch erreur (${(err as Error).message.slice(0, 80)}), retry…`,
          );
          await new Promise((r) => setTimeout(r, 2000));
          statementBuffer = toSend;
          return flushBatch(retries - 1);
        }
        throw err;
      }
    };

    for (const r of iter) {
      rowBuffer.push(cols.map((c) => (r as Record<string, unknown>)[c]));
      if (rowBuffer.length >= ROWS_PER_STATEMENT) {
        flushRowsToStatement();
        if (statementBuffer.length >= STATEMENTS_PER_BATCH) await flushBatch();
      }
    }
    flushRowsToStatement();
    await flushBatch();

    console.log(`\n  ✓ ${pushed.toLocaleString("fr-FR")} rows ${table}`);
    grandTotal += pushed;
  }

  /* 6. Créer les indexes (une fois la data en place) */
  console.log(`\n[REMOTE] Création des ${indexes.length} indexes…`);
  for (const idx of indexes) {
    if (!idx.sql) continue;
    await exec(idx.sql);
    console.log(`  ✓ index ${idx.name}`);
  }

  /* 7. Créer les triggers métier (non-FTS) */
  for (const tr of triggers) {
    if (!tr.sql) continue;
    await exec(tr.sql);
    console.log(`  ✓ trigger ${tr.name}`);
  }

  /* 8. FTS5 : créer la virtual table + populate + triggers */
  for (const fts of ftsVirtuals) {
    if (!fts.sql) continue;
    console.log(`\n[FTS] Créer ${fts.name}…`);
    await exec(fts.sql);

    if (fts.name === "copros_fts") {
      console.log(`[FTS] Populate copros_fts depuis copros…`);
      await exec(
        `INSERT INTO copros_fts (rowid, nom_copro, adresse, syndic, commune, code_postal, numero_immatriculation)
         SELECT id, nom_copro, adresse, syndic, commune, code_postal, numero_immatriculation FROM copros`,
      );
    }
  }

  for (const tr of ftsTriggers) {
    if (!tr.sql) continue;
    await exec(tr.sql);
    console.log(`  ✓ trigger FTS ${tr.name}`);
  }

  /* 9. Réactiver les FK */
  await exec("PRAGMA foreign_keys = ON");

  /* 10. Vérifications finales */
  const dt = ((Date.now() - t0) / 1000).toFixed(1);
  console.log(`\n=== MIGRATION TERMINÉE en ${dt}s ===`);
  console.log(`Total rows migrées : ${grandTotal.toLocaleString("fr-FR")}\n`);

  for (const sql of [
    "SELECT COUNT(*) AS c FROM copros",
    "SELECT COUNT(*) AS c FROM dpe_estimates",
    "SELECT COUNT(*) AS c FROM prospects",
    "SELECT COUNT(*) AS c FROM copros_fts",
  ]) {
    try {
      const r = await remote.execute({ sql });
      console.log(`  ${sql} → ${r.rows[0]?.c}`);
    } catch (e) {
      console.log(`  ${sql} → ${(e as Error).message}`);
    }
  }

  local.close();
}

main().catch((e) => {
  console.error("\n[FATAL]", e);
  process.exit(1);
});
