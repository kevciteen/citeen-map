/**
 * POST /api/overrides/notes?type=...&ref=...  body: { body } → ajoute une note
 */
import { NextRequest, NextResponse } from "next/server";
import { ensureAuth } from "@/lib/auth/guards";
import { addNote } from "@/lib/services/entity-overrides";
import type { OverrideEntityType } from "@/lib/db/ensure-entity-overrides";

export const runtime = "nodejs";

export async function POST(req: NextRequest) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const type = req.nextUrl.searchParams.get("type")?.trim() as OverrideEntityType | null;
  const ref = req.nextUrl.searchParams.get("ref")?.trim();
  if (!type || !ref) {
    return NextResponse.json({ error: "type + ref requis" }, { status: 400 });
  }
  const body = (await req.json().catch(() => ({}))) as { body?: string };
  if (!body.body || typeof body.body !== "string" || !body.body.trim()) {
    return NextResponse.json({ error: "body requis" }, { status: 400 });
  }
  const note = await addNote({ entityType: type, entityRef: ref }, body.body, guard.id);
  return NextResponse.json({ note });
}
