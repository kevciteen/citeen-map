/**
 * GET  /api/prospects/[id]/documents → liste des documents du prospect
 * POST /api/prospects/[id]/documents → ajoute un document (URL externe + métadonnées)
 *
 * Pour la phase B, on stocke uniquement une URL externe (Google Drive,
 * Dropbox, OneDrive...). Pas d'upload de fichier — l'utilisateur partage
 * le lien après avoir uploadé ailleurs.
 */
import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import { z } from "zod";
import { ensureAuth } from "@/lib/auth/guards";
import { ensureDocuments } from "@/lib/db/ensure-documents";

export const runtime = "nodejs";

const schema = z.object({
  name: z.string().min(1),
  url: z.string().url(),
  kind: z.string().optional().nullable(),
  description: z.string().optional().nullable(),
});

type DocumentRow = {
  id: number;
  prospect_id: number;
  name: string;
  url: string;
  kind: string | null;
  description: string | null;
  created_at: number;
  created_by: string | null;
};

export async function GET(
  _req: NextRequest,
  { params }: { params: Promise<{ id: string }> },
) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  await ensureDocuments();
  const { id } = await params;
  const pid = Number(id);
  if (!Number.isFinite(pid)) {
    return NextResponse.json({ error: "bad id" }, { status: 400 });
  }
  const rows = await db.all<DocumentRow>(
    "SELECT id, prospect_id, name, url, kind, description, created_at, created_by FROM documents WHERE prospect_id = ? ORDER BY id DESC",
    [pid],
  );
  return NextResponse.json({ documents: rows });
}

export async function POST(
  req: NextRequest,
  { params }: { params: Promise<{ id: string }> },
) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  await ensureDocuments();
  const { id } = await params;
  const pid = Number(id);
  if (!Number.isFinite(pid)) {
    return NextResponse.json({ error: "bad id" }, { status: 400 });
  }
  const parsed = schema.safeParse(await req.json());
  if (!parsed.success) {
    return NextResponse.json({ error: parsed.error.format() }, { status: 400 });
  }
  const d = parsed.data;
  const info = await db.run(
    "INSERT INTO documents (prospect_id, name, url, kind, description, created_by) VALUES (?, ?, ?, ?, ?, ?)",
    [
      pid,
      d.name,
      d.url,
      d.kind ?? null,
      d.description ?? null,
      guard.email ?? null,
    ],
  );
  await db.run("UPDATE prospects SET updated_at = unixepoch() WHERE id = ?", [pid]);
  return NextResponse.json({ id: info.lastInsertRowid }, { status: 201 });
}
