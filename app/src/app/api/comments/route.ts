import { NextRequest, NextResponse } from "next/server";
import { requireUser } from "@/lib/auth/session";
import { createComment, listComments } from "@/lib/services/comments";

export const runtime = "nodejs";

const ENTITY_TYPES = new Set(["prospect", "copro", "maison", "syndic"]);

export async function GET(req: NextRequest) {
  try {
    await requireUser();
  } catch {
    return NextResponse.json({ error: "Non authentifié" }, { status: 401 });
  }
  const sp = req.nextUrl.searchParams;
  const entityType = sp.get("entity_type") ?? "";
  const entityId = sp.get("entity_id") ?? "";
  if (!ENTITY_TYPES.has(entityType) || !entityId) {
    return NextResponse.json(
      { error: "entity_type + entity_id requis" },
      { status: 400 },
    );
  }
  const items = await listComments(entityType, entityId);
  return NextResponse.json({ items });
}

export async function POST(req: NextRequest) {
  let me;
  try {
    me = await requireUser();
  } catch {
    return NextResponse.json({ error: "Non authentifié" }, { status: 401 });
  }
  const body = (await req.json().catch(() => null)) as {
    entity_type?: string;
    entity_id?: string;
    body?: string;
    parent_id?: number | null;
    link?: string;
  } | null;
  if (
    !body?.entity_type ||
    !body?.entity_id ||
    !ENTITY_TYPES.has(body.entity_type) ||
    !body.body?.trim()
  ) {
    return NextResponse.json(
      { error: "entity_type, entity_id, body requis" },
      { status: 400 },
    );
  }
  // Default link selon le type d'entité
  const link =
    body.link ??
    (body.entity_type === "prospect"
      ? `/prospects/${body.entity_id}`
      : body.entity_type === "copro"
        ? `/copros/${body.entity_id}`
        : body.entity_type === "syndic"
          ? `/syndics/${body.entity_id}`
          : `/maisons`);
  const res = await createComment({
    entityType: body.entity_type,
    entityId: body.entity_id,
    authorId: me.id,
    authorName: me.name ?? me.email,
    body: body.body,
    parentId: body.parent_id ?? null,
    link,
  });
  return NextResponse.json(res);
}
