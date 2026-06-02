/**
 * DELETE /api/overrides/notes/:id → supprime une note
 */
import { NextResponse } from "next/server";
import { ensureAuth } from "@/lib/auth/guards";
import { deleteNote } from "@/lib/services/entity-overrides";

export const runtime = "nodejs";

export async function DELETE(
  _req: Request,
  { params }: { params: Promise<{ id: string }> },
) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const { id } = await params;
  const noteId = Number(id);
  if (!Number.isFinite(noteId)) {
    return NextResponse.json({ error: "id invalide" }, { status: 400 });
  }
  await deleteNote(noteId);
  return NextResponse.json({ ok: true });
}
