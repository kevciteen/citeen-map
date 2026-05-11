// In-memory TTL cache used by external API services.
type Entry<T> = { exp: number; data: T };
const store = new Map<string, Entry<unknown>>();
const DEFAULT_TTL = 10 * 60 * 1000;

export function cacheGet<T>(key: string): T | null {
  const v = store.get(key) as Entry<T> | undefined;
  if (!v) return null;
  if (Date.now() > v.exp) {
    store.delete(key);
    return null;
  }
  return v.data;
}

export function cacheSet<T>(key: string, data: T, ttlMs = DEFAULT_TTL): void {
  store.set(key, { exp: Date.now() + ttlMs, data });
}
