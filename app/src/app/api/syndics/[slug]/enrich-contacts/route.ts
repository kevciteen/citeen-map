/**
 * POST /api/syndics/[slug]/enrich-contacts
 *
 * Auto-enrichit un syndic : Sirene (siège) → BAN (lat/lon) → OSM/Google
 * (téléphone/site/email/horaires). Persiste dans les colonnes auto_* de
 * `syndic_contacts` sans écraser le travail manuel (colonnes phone/email/
 * website/contact_person/notes restent intactes).
 *
 * Body : `{ name?: string }` — nom canonique requis la première fois (à
 * chercher dans la table copros si non fourni).
 */
import { NextRequest, NextResponse } from "next/server";
import { ensureAuth } from "@/lib/auth/guards";
import { rateLimit } from "@/lib/rate-limit";
import { db } from "@/lib/db/client";
import { slugifySyndic } from "@/lib/db/ensure-syndic-contacts";
import { enrichSyndicAuto } from "@/lib/services/syndic-enrich";

export const runtime = "nodejs";
export const maxDuration = 30;

export async function POST(
  req: NextRequest,
  { params }: { params: Promise<{ slug: string }> },
) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const limited = await rateLimit("heavy", `user:${guard.id}`);
  if (limited) return limited;

  const { slug } = await params;
  const body = (await req.json().catch(() => ({}))) as { name?: string };

  // Résoud le nom canonique : body > syndic_contacts.name > copros.syndic (premier match)
  let name = body.name?.trim();
  if (!name) {
    const stored = await db.get<{ name: string | null }>(
      `SELECT name FROM syndic_contacts WHERE slug = ?`,
      [slug],
    );
    name = stored?.name?.trim() ?? undefined;
  }
  if (!name) {
    // Fallback : on cherche dans copros — n'importe quel syndic dont le slug match.
    // Comme le slug est dérivé du nom, on récupère via un LIKE inverse sur tous
    // les noms distincts. C'est un fallback rare (premier appel sans body.name).
    const allSyndics = await db.all<{ syndic: string }>(
      `SELECT DISTINCT TRIM(syndic) AS syndic FROM copros WHERE syndic IS NOT NULL AND syndic != ''`,
    );
    const match = allSyndics.find((s) => slugifySyndic(s.syndic) === slug);
    name = match?.syndic;
  }
  if (!name) {
    return NextResponse.json(
      { error: "Nom syndic introuvable. Fournir { name } dans le body." },
      { status: 400 },
    );
  }

  const result = await enrichSyndicAuto(slug, name);
  return NextResponse.json(result);
}
