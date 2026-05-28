/**
 * Cache 2 niveaux (L1 mémoire process + L2 table SQLite) pour les lookups
 * contacts OSM/Google. Le cache mémoire évite le roundtrip DB sur hot data
 * dans la même requête ; le cache DB survit aux redeploys Vercel et
 * économise le quota Google Places.
 */
import { db } from "@/lib/db/client";

type MemEntry = { exp: number; payload: unknown; source: string };
const mem = new Map<string, MemEntry>();

const POSITIVE_TTL_MS = 7 * 24 * 3600 * 1000; // 7j sur résultat trouvé
const NEGATIVE_TTL_MS = 60 * 60 * 1000;       //  1h sur "aucun résultat"

export type CacheHit<T> = { payload: T; source: string };

export async function getCachedContact<T>(key: string): Promise<CacheHit<T> | null> {
  const now = Date.now();
  const hit = mem.get(key);
  if (hit) {
    if (now < hit.exp) return { payload: hit.payload as T, source: hit.source };
    mem.delete(key);
  }
  const row = await db.get<{ payload_json: string; source: string; expires_at: number }>(
    `SELECT payload_json, source, expires_at FROM contact_cache WHERE cache_key = ?`,
    [key],
  );
  if (!row) return null;
  const expMs = row.expires_at * 1000;
  if (expMs < now) {
    // Expiré : purge paresseuse
    void db.run(`DELETE FROM contact_cache WHERE cache_key = ?`, [key]).catch(() => {});
    return null;
  }
  let payload: T;
  try {
    payload = JSON.parse(row.payload_json) as T;
  } catch {
    return null;
  }
  mem.set(key, { exp: expMs, payload, source: row.source });
  return { payload, source: row.source };
}

export async function setCachedContact(
  key: string,
  payload: unknown,
  source: string,
  ttlMs?: number,
): Promise<void> {
  const isNegative = payload === null;
  const effectiveTtl = ttlMs ?? (isNegative ? NEGATIVE_TTL_MS : POSITIVE_TTL_MS);
  const expMs = Date.now() + effectiveTtl;
  const expSec = Math.floor(expMs / 1000);
  mem.set(key, { exp: expMs, payload, source });
  await db.run(
    `INSERT INTO contact_cache (cache_key, payload_json, source, expires_at, created_at)
     VALUES (?, ?, ?, ?, unixepoch())
     ON CONFLICT(cache_key) DO UPDATE SET
       payload_json = excluded.payload_json,
       source = excluded.source,
       expires_at = excluded.expires_at,
       created_at = unixepoch()`,
    [key, JSON.stringify(payload), source, expSec],
  );
}
