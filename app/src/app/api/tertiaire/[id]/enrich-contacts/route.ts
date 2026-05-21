/**
 * POST /api/tertiaire/[id]/enrich-contacts
 *
 * Enrichit chaque occupant d'un bâtiment tertiaire avec ses coordonnées
 * (téléphone, site web, email, horaires) via :
 *   1. OpenStreetMap Overpass (gratuit, sans clé)
 *   2. Google Places en fallback (clé GOOGLE_MAPS_API_KEY, free tier 1000/mois)
 *
 * Persiste les résultats dans tertiary_occupants (cache long).
 *
 * Body optionnel : `{ onlyEmployers?: boolean }` — limite l'enrichissement aux
 * sociétés avec salariés (économise du quota Google).
 */
import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import { ensureTertiary } from "@/lib/db/ensure-tertiary";
import { ensureAuth } from "@/lib/auth/guards";
import { rateLimit } from "@/lib/rate-limit";
import { findContactInfo } from "@/lib/services/coords";

export const runtime = "nodejs";
export const maxDuration = 60;

type Row = {
  id: number;
  denomination: string | null;
  adresse_enregistree: string | null;
  tranche_effectif: string | null;
  phone: string | null;
  website: string | null;
};

export async function POST(
  req: NextRequest,
  ctx: { params: Promise<{ id: string }> },
) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const limited = await rateLimit("heavy", `user:${guard.id}`);
  if (limited) return limited;
  await ensureTertiary();

  const { id } = await ctx.params;
  const buildingId = Number(id);
  if (!Number.isFinite(buildingId)) {
    return NextResponse.json({ error: "id invalide" }, { status: 400 });
  }

  const building = await db.get<{ lat: number | null; lon: number | null }>(
    `SELECT lat, lon FROM tertiary_buildings WHERE id = ?`,
    [buildingId],
  );
  if (!building) return NextResponse.json({ error: "Bâtiment introuvable" }, { status: 404 });

  const body = await req.json().catch(() => ({})) as { onlyEmployers?: boolean };
  const onlyEmployers = Boolean(body.onlyEmployers);

  // Récupère les occupants à enrichir (skip ceux qui ont déjà phone OU website)
  let where = "building_id = ? AND (phone IS NULL OR phone = '') AND (website IS NULL OR website = '')";
  const args: (string | number | null)[] = [buildingId];
  if (onlyEmployers) {
    where += " AND tranche_effectif IS NOT NULL AND tranche_effectif != 'NN' AND tranche_effectif != '00'";
  }
  const rows = await db.all<Row>(
    `SELECT id, denomination, adresse_enregistree, tranche_effectif, phone, website
     FROM tertiary_occupants WHERE ${where} LIMIT 50`,
    args,
  );

  let enrichedOk = 0;
  let enrichedFail = 0;

  // Enrichissement en parallèle (limité à ~5 concurrent pour ne pas surcharger Overpass)
  const CONCURRENCY = 5;
  for (let i = 0; i < rows.length; i += CONCURRENCY) {
    const batch = rows.slice(i, i + CONCURRENCY);
    await Promise.all(
      batch.map(async (o) => {
        if (!o.denomination) {
          enrichedFail++;
          return;
        }
        try {
          const contact = await findContactInfo({
            denomination: o.denomination,
            address: o.adresse_enregistree ?? "",
            lat: building.lat ?? undefined,
            lon: building.lon ?? undefined,
          });
          await db.run(
            `UPDATE tertiary_occupants SET
               phone = ?, website = ?, email = ?, hours = ?,
               contact_source = ?, contact_fetched_at = unixepoch()
             WHERE id = ?`,
            [
              contact.phone,
              contact.website,
              contact.email,
              contact.hours,
              contact.source,
              o.id,
            ],
          );
          if (contact.source !== "none") enrichedOk++;
          else enrichedFail++;
        } catch {
          enrichedFail++;
        }
      }),
    );
  }

  return NextResponse.json({
    candidats: rows.length,
    enrichis: enrichedOk,
    sans_contact: enrichedFail,
  });
}
