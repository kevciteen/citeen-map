/**
 * Client base de données — version transitoire.
 *
 * - LOCAL (dev actuel) : better-sqlite3 SYNCHRONE → `sqlite.prepare(...).get()`
 * - PROD (Vercel cible) : @libsql/client ASYNC → `await db.execute(...)`
 *
 * Les deux clients sont exportés. Le code applicatif utilise actuellement
 * `sqlite` (sync). Le refactor vers `db` (async) est documenté dans DEPLOY.md
 * et reste à faire pour activer le déploiement Vercel + Turso.
 */
import Database from "better-sqlite3";
import { drizzle } from "drizzle-orm/better-sqlite3";
import { createClient, type Client } from "@libsql/client";
import { existsSync, mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import * as schema from "./schema";

const dbPath = resolve(
  process.cwd(),
  process.env.DATABASE_URL?.replace(/^file:/, "") ?? "./data/citeen.db",
);
const dbDir = dirname(dbPath);
if (!existsSync(dbDir)) mkdirSync(dbDir, { recursive: true });

declare global {
  // eslint-disable-next-line no-var
  var __sqlite: Database.Database | undefined;
  // eslint-disable-next-line no-var
  var __libsql: Client | undefined;
}

/* ----------------------- Client synchrone (local) ----------------------- */
const sqliteClient = global.__sqlite ?? new Database(dbPath);
if (process.env.NODE_ENV !== "production") global.__sqlite = sqliteClient;

sqliteClient.pragma("journal_mode = WAL");
sqliteClient.pragma("synchronous = NORMAL");
sqliteClient.pragma("temp_store = MEMORY");
sqliteClient.pragma("foreign_keys = ON");

export const sqlite = sqliteClient;
export const drizzleDb = drizzle(sqliteClient, { schema });
export const db = drizzleDb;
export * from "./schema";

/* --------------------- Client libsql (Turso, async) --------------------- */
/* Activé uniquement si TURSO_DATABASE_URL est défini. À utiliser après le
   refactor async (voir DEPLOY.md). En attendant, exporté pour les futurs
   call sites async — pas utilisé dans le code actuel. */
let libsqlClient: Client | null = null;
if (process.env.TURSO_DATABASE_URL) {
  libsqlClient =
    global.__libsql ??
    createClient({
      url: process.env.TURSO_DATABASE_URL,
      authToken: process.env.TURSO_AUTH_TOKEN,
      intMode: "number",
    });
  if (process.env.NODE_ENV !== "production") global.__libsql = libsqlClient;
}
export const libsql = libsqlClient;
