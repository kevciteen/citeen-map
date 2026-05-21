/**
 * POST /api/tertiaire/[id]/create-prospect — crée un prospect lié au
 * bâtiment tertiaire, ou retourne celui existant (idempotent par building).
 */
import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import { ensureTertiary } from "@/lib/db/ensure-tertiary";
import { ensureAuth } from "@/lib/auth/guards";

export const runtime = "nodejs";

type Building = {
  id: number;
  label: string | null;
  adresse: string | null;
  lat: number | null;
  lon: number | null;
};

export async function POST(
  _req: NextRequest,
  ctx: { params: Promise<{ id: string }> },
) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  await ensureTertiary();

  const { id } = await ctx.params;
  const buildingId = Number(id);
  if (!Number.isFinite(buildingId)) {
    return NextResponse.json({ error: "id invalide" }, { status: 400 });
  }

  const building = await db.get<Building>(
    `SELECT id, label, adresse, lat, lon FROM tertiary_buildings WHERE id = ?`,
    [buildingId],
  );
  if (!building) {
    return NextResponse.json({ error: "Bâtiment introuvable" }, { status: 404 });
  }

  // Idempotent : si prospect existe déjà, le renvoyer
  const existing = await db.get<{ id: number; stage: string }>(
    `SELECT id, stage FROM prospects WHERE tertiary_building_id = ? LIMIT 1`,
    [buildingId],
  );
  if (existing) {
    return NextResponse.json({ prospect: existing, created: false });
  }

  const label = building.label ?? building.adresse ?? `Bâtiment tertiaire #${buildingId}`;
  const res = await db.run(
    `INSERT INTO prospects (
       tertiary_building_id, custom_label, custom_address, custom_lat, custom_lon,
       stage, priority
     ) VALUES (?, ?, ?, ?, ?, 'to_contact', 2)`,
    [
      buildingId,
      label,
      building.adresse,
      building.lat,
      building.lon,
    ],
  );

  // Activity log
  await db.run(
    `INSERT INTO activities (prospect_id, type, payload, author)
     VALUES (?, 'created_from_tertiary', ?, ?)`,
    [
      res.lastInsertRowid,
      JSON.stringify({ buildingId, label }),
      String(guard.id),
    ],
  );

  return NextResponse.json({
    prospect: { id: res.lastInsertRowid, stage: "to_contact" },
    created: true,
  });
}
