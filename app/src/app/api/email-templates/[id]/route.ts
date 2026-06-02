/**
 * GET    /api/email-templates/:id
 * PATCH  /api/email-templates/:id
 * DELETE /api/email-templates/:id
 *
 * Règle d'autorisation : un user peut éditer/supprimer ses propres templates.
 * Un admin peut éditer/supprimer tous. Les autres peuvent juste lire les
 * templates partagés.
 */
import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";
import { db } from "@/lib/db/client";
import { ensureAuth } from "@/lib/auth/guards";
import { ensureEmailTemplates } from "@/lib/db/ensure-email-templates";

export const runtime = "nodejs";

const patchSchema = z.object({
  name: z.string().min(1).max(120).optional(),
  subject: z.string().min(1).max(255).optional(),
  body: z.string().min(1).max(20000).optional(),
  scope: z.enum(["prospect", "syndic", "copro", "generic"]).optional(),
  isShared: z.boolean().optional(),
});

async function fetchOne(id: number) {
  return db.get<{
    id: number;
    name: string;
    subject: string;
    body: string;
    scope: string;
    is_shared: number;
    created_by: number | null;
    created_at: number;
    updated_at: number;
  }>(`SELECT * FROM email_templates WHERE id = ?`, [id]);
}

export async function GET(
  _req: NextRequest,
  ctx: { params: Promise<{ id: string }> },
) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  await ensureEmailTemplates();
  const { id } = await ctx.params;
  const row = await fetchOne(Number(id));
  if (!row) return NextResponse.json({ error: "not_found" }, { status: 404 });
  if (!row.is_shared && row.created_by !== guard.id && guard.role !== "admin") {
    return NextResponse.json({ error: "forbidden" }, { status: 403 });
  }
  return NextResponse.json(row);
}

export async function PATCH(
  req: NextRequest,
  ctx: { params: Promise<{ id: string }> },
) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  await ensureEmailTemplates();
  const { id } = await ctx.params;
  const row = await fetchOne(Number(id));
  if (!row) return NextResponse.json({ error: "not_found" }, { status: 404 });
  if (row.created_by !== guard.id && guard.role !== "admin") {
    return NextResponse.json({ error: "forbidden" }, { status: 403 });
  }
  const json = await req.json().catch(() => null);
  const parsed = patchSchema.safeParse(json);
  if (!parsed.success) {
    return NextResponse.json({ error: parsed.error.flatten() }, { status: 400 });
  }
  const d = parsed.data;
  const sets: string[] = [];
  const args: (string | number)[] = [];
  if (d.name !== undefined) { sets.push("name = ?"); args.push(d.name); }
  if (d.subject !== undefined) { sets.push("subject = ?"); args.push(d.subject); }
  if (d.body !== undefined) { sets.push("body = ?"); args.push(d.body); }
  if (d.scope !== undefined) { sets.push("scope = ?"); args.push(d.scope); }
  if (d.isShared !== undefined) { sets.push("is_shared = ?"); args.push(d.isShared ? 1 : 0); }
  if (sets.length === 0) return NextResponse.json({ ok: true });
  sets.push("updated_at = unixepoch()");
  args.push(Number(id));
  await db.run(`UPDATE email_templates SET ${sets.join(", ")} WHERE id = ?`, args);
  return NextResponse.json({ ok: true });
}

export async function DELETE(
  _req: NextRequest,
  ctx: { params: Promise<{ id: string }> },
) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  await ensureEmailTemplates();
  const { id } = await ctx.params;
  const row = await fetchOne(Number(id));
  if (!row) return NextResponse.json({ error: "not_found" }, { status: 404 });
  if (row.created_by !== guard.id && guard.role !== "admin") {
    return NextResponse.json({ error: "forbidden" }, { status: 403 });
  }
  await db.run(`DELETE FROM email_templates WHERE id = ?`, [Number(id)]);
  return NextResponse.json({ ok: true });
}
