/**
 * DELETE /api/prospects/[id]/contacts/[contactId] → supprime un contact
 */
import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import { ensureAuth } from "@/lib/auth/guards";

export const runtime = "nodejs";

export async function DELETE(
  _req: NextRequest,
  { params }: { params: Promise<{ id: string; contactId: string }> },
) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const { id, contactId } = await params;
  const pid = Number(id);
  const cid = Number(contactId);
  if (!Number.isFinite(pid) || !Number.isFinite(cid)) {
    return NextResponse.json({ error: "bad id" }, { status: 400 });
  }
  await db.run("DELETE FROM contacts WHERE id = ? AND prospect_id = ?", [cid, pid]);
  return NextResponse.json({ ok: true });
}
