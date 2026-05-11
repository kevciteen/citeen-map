/**
 * Client base de données unifié — Turso/libsql en prod, fichier SQLite en dev.
 *
 * Exports :
 *   - `db.get/all/run/exec/batch` — API ASYNC, utilisée par le code applicatif
 *     (routes Next.js + Server Components). Marche partout (Vercel + local).
 *   - `sqlite` — better-sqlite3 SYNCHRONE, utilisé UNIQUEMENT par les scripts
 *     locaux (seed, recalibrage, FTS). Proxy paresseux — n'ouvre rien tant que
 *     non accédé. Sur Vercel, jamais touché par les scripts (qui ne tournent pas).
 */
import Database from "better-sqlite3";
import { createClient, type Client, type InValue } from "@libsql/client";
import { existsSync, mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";

const tursoUrl = process.env.TURSO_DATABASE_URL;
const localPath = resolve(
  process.cwd(),
  (process.env.DATABASE_URL ?? "./data/citeen.db").replace(/^file:/, ""),
);
const url = tursoUrl?.startsWith("libsql://")
  ? tursoUrl
  : `file:${localPath}`;

if (url.startsWith("file:")) {
  const dir = dirname(localPath);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
}

declare global {
  // eslint-disable-next-line no-var
  var __libsql: Client | undefined;
  // eslint-disable-next-line no-var
  var __sqlite: Database.Database | undefined;
}

/* -------------------- LIBSQL ASYNC (app + Vercel) -------------------- */
const client: Client =
  global.__libsql ??
  createClient({
    url,
    authToken: process.env.TURSO_AUTH_TOKEN,
    intMode: "number",
  });
if (process.env.NODE_ENV !== "production") global.__libsql = client;

export type Row = Record<string, unknown>;

export const db = {
  async get<T = Row>(sql: string, args: InValue[] = []): Promise<T | undefined> {
    const res = await client.execute({ sql, args });
    return res.rows[0] as T | undefined;
  },
  async all<T = Row>(sql: string, args: InValue[] = []): Promise<T[]> {
    const res = await client.execute({ sql, args });
    return res.rows as unknown as T[];
  },
  async run(
    sql: string,
    args: InValue[] = [],
  ): Promise<{ changes: number; lastInsertRowid: number }> {
    const res = await client.execute({ sql, args });
    return {
      changes: res.rowsAffected,
      lastInsertRowid: Number(res.lastInsertRowid ?? 0),
    };
  },
  async exec(sql: string): Promise<void> {
    await client.executeMultiple(sql);
  },
  async batch(
    statements: Array<{ sql: string; args?: InValue[] }>,
  ): Promise<void> {
    await client.batch(statements);
  },
  client,
};

/* -------------------- BETTER-SQLITE3 SYNC (scripts only) -------------------- */
function openSqlite(): Database.Database {
  if (global.__sqlite) return global.__sqlite;
  const inst = new Database(localPath);
  inst.pragma("journal_mode = WAL");
  inst.pragma("synchronous = NORMAL");
  inst.pragma("temp_store = MEMORY");
  inst.pragma("foreign_keys = ON");
  if (process.env.NODE_ENV !== "production") global.__sqlite = inst;
  return inst;
}

// Lazy proxy — n'ouvre better-sqlite3 que quand un script y accède.
// Côté Vercel, les scripts ne tournent jamais, donc better-sqlite3 n'est
// jamais chargé en runtime.
export const sqlite: Database.Database = new Proxy({} as Database.Database, {
  get(_, prop) {
    const inst = openSqlite() as unknown as Record<string | symbol, unknown>;
    const val = inst[prop];
    return typeof val === "function" ? (val as (...a: unknown[]) => unknown).bind(inst) : val;
  },
});

export * from "./schema";
