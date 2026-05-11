import { NextRequest, NextResponse } from "next/server";
import { sqlite } from "@/lib/db/client";
import { z } from "zod";

export const runtime = "nodejs";

const schema = z.object({ body: z.string().min(1), author: z.string().optional() });

export async function POST(req: NextRequest, { params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const pid = Number(id);
  if (!Number.isFinite(pid)) return NextResponse.json({ error: "bad id" }, { status: 400 });
  const parsed = schema.safeParse(await req.json());
  if (!parsed.success) return NextResponse.json({ error: parsed.error.format() }, { status: 400 });

  const info = sqlite
    .prepare("INSERT INTO notes (prospect_id, body, author) VALUES (?, ?, ?)")
    .run(pid, parsed.data.body, parsed.data.author ?? null);
  sqlite
    .prepare("INSERT INTO activities (prospect_id, type, payload) VALUES (?, 'note_added', ?)")
    .run(pid, JSON.stringify({ noteId: Number(info.lastInsertRowid) }));
  sqlite.prepare("UPDATE prospects SET updated_at = unixepoch() WHERE id = ?").run(pid);

  return NextResponse.json({ id: Number(info.lastInsertRowid) }, { status: 201 });
}
