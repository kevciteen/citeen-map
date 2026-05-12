import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import { ensureAuth } from "@/lib/auth/guards";

export const runtime = "nodejs";

export async function PATCH(
  req: NextRequest,
  { params }: { params: Promise<{ id: string }> },
) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const { id } = await params;
  const tid = Number(id);
  if (!Number.isFinite(tid))
    return NextResponse.json({ error: "bad id" }, { status: 400 });
  const body = (await req.json().catch(() => null)) as {
    done?: boolean;
    title?: string;
    dueAt?: number | null;
  } | null;
  if (!body) return NextResponse.json({ error: "JSON requis" }, { status: 400 });

  const sets: string[] = [];
  const vals: (string | number | null)[] = [];
  if (body.done !== undefined) {
    sets.push("done_at = ?");
    vals.push(body.done ? Math.floor(Date.now() / 1000) : null);
  }
  if (body.title !== undefined) {
    sets.push("title = ?");
    vals.push(body.title);
  }
  if (body.dueAt !== undefined) {
    sets.push("due_at = ?");
    vals.push(body.dueAt);
  }
  if (sets.length === 0)
    return NextResponse.json({ error: "Aucun changement" }, { status: 400 });
  vals.push(tid);
  await db.run(`UPDATE tasks SET ${sets.join(", ")} WHERE id = ?`, vals);
  return NextResponse.json({ ok: true });
}

export async function DELETE(
  _req: NextRequest,
  { params }: { params: Promise<{ id: string }> },
) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const { id } = await params;
  const tid = Number(id);
  if (!Number.isFinite(tid))
    return NextResponse.json({ error: "bad id" }, { status: 400 });
  await db.run("DELETE FROM tasks WHERE id = ?", [tid]);
  return NextResponse.json({ ok: true });
}
