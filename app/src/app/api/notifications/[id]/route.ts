import { NextRequest, NextResponse } from "next/server";
import { requireUser } from "@/lib/auth/session";
import { markNotificationRead } from "@/lib/services/comments";

export const runtime = "nodejs";

export async function POST(
  _req: NextRequest,
  { params }: { params: Promise<{ id: string }> },
) {
  let me;
  try {
    me = await requireUser();
  } catch {
    return NextResponse.json({ error: "Non authentifié" }, { status: 401 });
  }
  const { id: rawId } = await params;
  const id = Number(rawId);
  if (!Number.isFinite(id)) {
    return NextResponse.json({ error: "ID invalide" }, { status: 400 });
  }
  await markNotificationRead(me.id, id);
  return NextResponse.json({ ok: true });
}
