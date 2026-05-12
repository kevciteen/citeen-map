import { NextResponse } from "next/server";
import { requireUser } from "@/lib/auth/session";
import { markAllNotificationsRead } from "@/lib/services/comments";

export const runtime = "nodejs";

export async function POST() {
  let me;
  try {
    me = await requireUser();
  } catch {
    return NextResponse.json({ error: "Non authentifié" }, { status: 401 });
  }
  await markAllNotificationsRead(me.id);
  return NextResponse.json({ ok: true });
}
