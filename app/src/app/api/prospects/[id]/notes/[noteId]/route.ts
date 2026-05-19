/**
 * DELETE /api/prospects/[id]/notes/[noteId] → supprime une note
 */
import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import { ensureAuth } from "@/lib/auth/guards";

export const runtime = "nodejs";

export async function DELETE(
  _req: NextRequest,
  { params }: { params: Promise<{ id: string; noteId: string }> },
) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const { id, noteId } = await params;
  const pid = Number(id);
  const nid = Number(noteId);
  if (!Number.isFinite(pid) || !Number.isFinite(nid)) {
    return NextResponse.json({ error: "bad id" }, { status: 400 });
  }
  await db.run("DELETE FROM notes WHERE id = ? AND prospect_id = ?", [nid, pid]);
  return NextResponse.json({ ok: true });
}
