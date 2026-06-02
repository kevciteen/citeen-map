/**
 * POST   /api/overrides/tags?type=...&ref=...  body: { tag }     → ajoute un tag
 * DELETE /api/overrides/tags?type=...&ref=...&tag=...             → supprime
 */
import { NextRequest, NextResponse } from "next/server";
import { ensureAuth } from "@/lib/auth/guards";
import { addTag, removeTag } from "@/lib/services/entity-overrides";
import type { OverrideEntityType } from "@/lib/db/ensure-entity-overrides";

export const runtime = "nodejs";

function parseKey(req: NextRequest) {
  const type = req.nextUrl.searchParams.get("type")?.trim() as OverrideEntityType | null;
  const ref = req.nextUrl.searchParams.get("ref")?.trim();
  if (!type || !ref) return null;
  return { entityType: type, entityRef: ref };
}

export async function POST(req: NextRequest) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const key = parseKey(req);
  if (!key) return NextResponse.json({ error: "type + ref requis" }, { status: 400 });
  const body = (await req.json().catch(() => ({}))) as { tag?: string };
  if (!body.tag) return NextResponse.json({ error: "tag requis" }, { status: 400 });
  const tag = await addTag(key, body.tag, guard.id);
  return NextResponse.json({ tag });
}

export async function DELETE(req: NextRequest) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const key = parseKey(req);
  if (!key) return NextResponse.json({ error: "type + ref requis" }, { status: 400 });
  const tag = req.nextUrl.searchParams.get("tag")?.trim();
  if (!tag) return NextResponse.json({ error: "tag requis" }, { status: 400 });
  await removeTag(key, tag);
  return NextResponse.json({ ok: true });
}
