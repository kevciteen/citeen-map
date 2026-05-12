import { NextRequest, NextResponse } from "next/server";
import { requireUser } from "@/lib/auth/session";
import {
  countUnreadNotifications,
  listNotifications,
} from "@/lib/services/comments";

export const runtime = "nodejs";

export async function GET(req: NextRequest) {
  let me;
  try {
    me = await requireUser();
  } catch {
    return NextResponse.json({ error: "Non authentifié" }, { status: 401 });
  }
  const sp = req.nextUrl.searchParams;
  const onlyUnread = sp.get("unread") === "1";
  const limit = Number(sp.get("limit") ?? 30);
  const [items, unreadCount] = await Promise.all([
    listNotifications(me.id, { limit, onlyUnread }),
    countUnreadNotifications(me.id),
  ]);
  return NextResponse.json({ items, unreadCount });
}
