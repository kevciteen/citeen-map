/**
 * Rate limiting via Upstash Redis (HTTP-based, fonctionne en edge).
 *
 * NO-OP si UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN ne sont pas
 * configurés → utile en dev local, et on évite de bloquer la prod si Upstash
 * tombe (graceful degradation).
 *
 * 3 limiters distincts selon le risque :
 *   - rateLimitHeavy : ADEME/BAN/Sirene/Sirene-export → 30 req/min/user
 *   - rateLimitSearch : recherches DB → 60 req/min/user
 *   - rateLimitLogin : login brute-force → 5 req/min/IP
 */
import { Ratelimit } from "@upstash/ratelimit";
import { Redis } from "@upstash/redis";
import { NextResponse } from "next/server";

let redis: Redis | null = null;
function getRedis(): Redis | null {
  if (redis) return redis;
  const url = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) return null;
  redis = new Redis({ url, token });
  return redis;
}

function buildLimiter(perMinute: number, prefix: string): Ratelimit | null {
  const r = getRedis();
  if (!r) return null;
  return new Ratelimit({
    redis: r,
    limiter: Ratelimit.slidingWindow(perMinute, "60 s"),
    prefix,
    analytics: true,
  });
}

const heavy = () => buildLimiter(30, "rl:heavy");
const search = () => buildLimiter(60, "rl:search");
const loginFn = () => buildLimiter(5, "rl:login");

export type RateLimitKind = "heavy" | "search" | "login";

/**
 * Check rate-limit pour une clé donnée (user_id, IP, etc.).
 * Retourne null si OK, sinon une NextResponse 429.
 */
export async function rateLimit(
  kind: RateLimitKind,
  key: string,
): Promise<NextResponse | null> {
  const limiter =
    kind === "heavy" ? heavy() : kind === "search" ? search() : loginFn();
  if (!limiter) return null; // pas de Upstash configuré → no-op
  const r = await limiter.limit(key);
  if (r.success) return null;
  return NextResponse.json(
    {
      error: "Trop de requêtes — patiente quelques secondes",
      retryAfter: Math.ceil((r.reset - Date.now()) / 1000),
    },
    {
      status: 429,
      headers: {
        "retry-after": String(Math.ceil((r.reset - Date.now()) / 1000)),
        "x-ratelimit-limit": String(r.limit),
        "x-ratelimit-remaining": String(r.remaining),
      },
    },
  );
}
