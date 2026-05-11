import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";

export const runtime = "nodejs";

export async function GET(_req: NextRequest, { params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const coproId = Number(id);
  if (!Number.isFinite(coproId)) {
    return NextResponse.json({ error: "Invalid id" }, { status: 400 });
  }

  const copro = await db.get(
    `SELECT c.*, e.classe_reelle, e.classe_simulee, e.classe_finale,
            e.ge_score_moyen, e.conso_moyenne, e.nb_dpe_individuels, e.rayon_recherche, e.computed_at
     FROM copros c
     LEFT JOIN dpe_estimates e ON e.copro_id = c.id
     WHERE c.id = ?`,
    [coproId],
  );

  if (!copro) {
    return NextResponse.json({ error: "Not found" }, { status: 404 });
  }

  const prospect = await db.get(
    `SELECT id, stage, priority, estimated_value, next_action_at, next_action_label
     FROM prospects WHERE copro_id = ? ORDER BY id DESC LIMIT 1`,
    [coproId],
  );

  return NextResponse.json({ copro, prospect: prospect ?? null });
}
