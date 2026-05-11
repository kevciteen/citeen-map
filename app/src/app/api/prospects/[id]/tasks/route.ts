import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import { z } from "zod";

export const runtime = "nodejs";

const schema = z.object({
  title: z.string().min(1),
  kind: z.enum(["call", "email", "visit", "meeting", "other"]).optional(),
  dueAt: z.number().nullable().optional(),
});

export async function POST(req: NextRequest, { params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const pid = Number(id);
  if (!Number.isFinite(pid)) return NextResponse.json({ error: "bad id" }, { status: 400 });
  const parsed = schema.safeParse(await req.json());
  if (!parsed.success) return NextResponse.json({ error: parsed.error.format() }, { status: 400 });

  const info = await db.run(
    "INSERT INTO tasks (prospect_id, title, kind, due_at) VALUES (?, ?, ?, ?)",
    [pid, parsed.data.title, parsed.data.kind ?? null, parsed.data.dueAt ?? null],
  );
  return NextResponse.json({ id: info.lastInsertRowid }, { status: 201 });
}

export async function PATCH(req: NextRequest, { params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const pid = Number(id);
  if (!Number.isFinite(pid)) return NextResponse.json({ error: "bad id" }, { status: 400 });
  const body = await req.json();
  const taskId = Number(body?.taskId);
  if (!Number.isFinite(taskId)) return NextResponse.json({ error: "bad taskId" }, { status: 400 });

  if (body.done === true) {
    await db.run(
      "UPDATE tasks SET done_at = unixepoch() WHERE id = ? AND prospect_id = ?",
      [taskId, pid],
    );
  } else if (body.done === false) {
    await db.run(
      "UPDATE tasks SET done_at = NULL WHERE id = ? AND prospect_id = ?",
      [taskId, pid],
    );
  }
  return NextResponse.json({ ok: true });
}
