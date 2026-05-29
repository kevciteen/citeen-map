/**
 * POST /api/contacts/scrape
 *
 * Scrape Pages Jaunes (B2B) ou Pages Blanches (B2C) pour une adresse donnée.
 *
 * Body : { source: "pj" | "pb", address?, cp?, city?, name? }
 *
 * Rate-limit DUR (heavy). Cache 7j en DB. Configurable via env var
 * SCRAPINGBEE_API_KEY pour bypass Cloudflare et IP datacenter Vercel.
 *
 * À utiliser avec parcimonie côté UI : 1 clic utilisateur = 1 requête.
 * Pas de batch ni de loop côté serveur.
 */
import { NextRequest, NextResponse } from "next/server";
import { ensureAuth } from "@/lib/auth/guards";
import { rateLimit } from "@/lib/rate-limit";
import { scrapeContacts, type ScrapeSource } from "@/lib/services/contact-scraper";

export const runtime = "nodejs";
export const maxDuration = 30;

export async function POST(req: NextRequest) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const limited = await rateLimit("heavy", `user:${guard.id}`);
  if (limited) return limited;

  const body = (await req.json().catch(() => ({}))) as {
    source?: string;
    address?: string;
    cp?: string;
    city?: string;
    name?: string;
  };

  if (body.source !== "pj" && body.source !== "pb") {
    return NextResponse.json(
      { error: "source doit être 'pj' (Pages Jaunes) ou 'pb' (Pages Blanches)" },
      { status: 400 },
    );
  }
  if (!body.address && !body.name && !body.cp && !body.city) {
    return NextResponse.json(
      { error: "Précise au moins un critère (address, name, cp, city)" },
      { status: 400 },
    );
  }

  const result = await scrapeContacts({
    source: body.source as ScrapeSource,
    address: body.address ?? null,
    cp: body.cp ?? null,
    city: body.city ?? null,
    name: body.name ?? null,
  });

  return NextResponse.json(result);
}
