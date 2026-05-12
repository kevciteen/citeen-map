import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import type { InValue } from "@libsql/client";
import { z } from "zod";
import { ensureAuth } from "@/lib/auth/guards";

export const runtime = "nodejs";

const schema = z
  .object({
    nom_copro: z.string().nullable().optional(),
    adresse: z.string().min(1).nullable().optional(),
    code_postal: z.string().min(1).nullable().optional(),
    commune: z.string().min(1).nullable().optional(),
    syndic: z.string().nullable().optional(),
    lat: z.number().finite().nullable().optional(),
    lon: z.number().finite().nullable().optional(),
    nb_lots_habitation: z.number().int().nullable().optional(),
    periode_construction: z.string().nullable().optional(),
  })
  .strict();

export async function PATCH(req: NextRequest, { params }: { params: Promise<{ id: string }> }) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const { id } = await params;
  const coproId = Number(id);
  if (!Number.isFinite(coproId)) {
    return NextResponse.json({ error: "Invalid id" }, { status: 400 });
  }

  const body = await req.json();
  const parsed = schema.safeParse(body);
  if (!parsed.success) {
    return NextResponse.json({ error: parsed.error.format() }, { status: 400 });
  }
  const d = parsed.data;

  const sets: string[] = [];
  const vals: InValue[] = [];
  const keys = [
    "nom_copro",
    "adresse",
    "code_postal",
    "commune",
    "syndic",
    "lat",
    "lon",
    "nb_lots_habitation",
    "periode_construction",
  ] as const;
  for (const k of keys) {
    if (k in d) {
      sets.push(`${k} = ?`);
      vals.push(((d as Record<string, unknown>)[k] ?? null) as InValue);
    }
  }
  if (sets.length === 0) {
    return NextResponse.json({ error: "Aucun champ à mettre à jour" }, { status: 400 });
  }
  vals.push(coproId);

  const info = await db.run(
    `UPDATE copros SET ${sets.join(", ")} WHERE id = ?`,
    vals,
  );
  if (info.changes === 0) {
    return NextResponse.json({ error: "Copro introuvable" }, { status: 404 });
  }

  // Si lat/lon ou adresse changent, on invalide le cache DPE pour forcer une réestimation
  if ("lat" in d || "lon" in d || "adresse" in d || "code_postal" in d || "commune" in d) {
    await db.run("DELETE FROM dpe_estimates WHERE copro_id = ?", [coproId]);
  }

  return NextResponse.json({ ok: true });
}
