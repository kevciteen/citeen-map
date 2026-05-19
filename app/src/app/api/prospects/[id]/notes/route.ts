import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import { z } from "zod";
import { ensureAuth } from "@/lib/auth/guards";

export const runtime = "nodejs";

const schema = z.object({ body: z.string().min(1), author: z.string().optional() });

type NoteRow = {
  id: number;
  prospect_id: number;
  body: string;
  author: string | null;
  created_at: number | null;
};

export async function GET(
  _req: NextRequest,
  { params }: { params: Promise<{ id: string }> },
) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const { id } = await params;
  const pid = Number(id);
  if (!Number.isFinite(pid)) {
    return NextResponse.json({ error: "bad id" }, { status: 400 });
  }
  const rows = await db.all<NoteRow>(
    "SELECT id, prospect_id, body, author, created_at FROM notes WHERE prospect_id = ? ORDER BY id DESC",
    [pid],
  );
  return NextResponse.json({ notes: rows });
}

export async function POST(req: NextRequest, { params }: { params: Promise<{ id: string }> }) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const { id } = await params;
  const pid = Number(id);
  if (!Number.isFinite(pid)) return NextResponse.json({ error: "bad id" }, { status: 400 });
  const parsed = schema.safeParse(await req.json());
  if (!parsed.success) return NextResponse.json({ error: parsed.error.format() }, { status: 400 });

  const info = await db.run(
    "INSERT INTO notes (prospect_id, body, author) VALUES (?, ?, ?)",
    [pid, parsed.data.body, parsed.data.author ?? null],
  );
  await db.run(
    "INSERT INTO activities (prospect_id, type, payload) VALUES (?, 'note_added', ?)",
    [pid, JSON.stringify({ noteId: info.lastInsertRowid })],
  );
  await db.run("UPDATE prospects SET updated_at = unixepoch() WHERE id = ?", [pid]);

  return NextResponse.json({ id: info.lastInsertRowid }, { status: 201 });
}
