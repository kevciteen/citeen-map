/**
 * DELETE /api/prospects/[id]/documents/[docId] → supprime un document
 */
import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import { ensureAuth } from "@/lib/auth/guards";
import { ensureDocuments } from "@/lib/db/ensure-documents";

export const runtime = "nodejs";

export async function DELETE(
  _req: NextRequest,
  { params }: { params: Promise<{ id: string; docId: string }> },
) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  await ensureDocuments();
  const { id, docId } = await params;
  const pid = Number(id);
  const did = Number(docId);
  if (!Number.isFinite(pid) || !Number.isFinite(did)) {
    return NextResponse.json({ error: "bad id" }, { status: 400 });
  }
  await db.run("DELETE FROM documents WHERE id = ? AND prospect_id = ?", [did, pid]);
  return NextResponse.json({ ok: true });
}
