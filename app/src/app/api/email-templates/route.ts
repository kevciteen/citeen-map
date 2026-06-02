/**
 * GET  /api/email-templates       liste (tous les partagés + privés du user)
 * POST /api/email-templates       crée un nouveau template
 */
import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";
import { db } from "@/lib/db/client";
import { ensureAuth } from "@/lib/auth/guards";
import { ensureEmailTemplates } from "@/lib/db/ensure-email-templates";

export const runtime = "nodejs";

const SCOPES = ["prospect", "syndic", "copro", "generic"] as const;

const createSchema = z.object({
  name: z.string().min(1).max(120),
  subject: z.string().min(1).max(255),
  body: z.string().min(1).max(20000),
  scope: z.enum(SCOPES).default("generic"),
  isShared: z.boolean().default(true),
});

export async function GET(req: NextRequest) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  await ensureEmailTemplates();

  const sp = req.nextUrl.searchParams;
  const scope = sp.get("scope");
  const filters: string[] = ["(is_shared = 1 OR created_by = ?)"];
  const args: (string | number)[] = [guard.id];
  if (scope && (SCOPES as readonly string[]).includes(scope)) {
    filters.push("(scope = ? OR scope = 'generic')");
    args.push(scope);
  }
  const items = await db.all<{
    id: number;
    name: string;
    subject: string;
    body: string;
    scope: string;
    is_shared: number;
    created_by: number | null;
    created_at: number;
    updated_at: number;
  }>(
    `SELECT id, name, subject, body, scope, is_shared, created_by, created_at, updated_at
     FROM email_templates
     WHERE ${filters.join(" AND ")}
     ORDER BY scope, name`,
    args,
  );
  return NextResponse.json({ items });
}

export async function POST(req: NextRequest) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  await ensureEmailTemplates();
  const json = await req.json().catch(() => null);
  const parsed = createSchema.safeParse(json);
  if (!parsed.success) {
    return NextResponse.json({ error: parsed.error.flatten() }, { status: 400 });
  }
  const d = parsed.data;
  const res = await db.run(
    `INSERT INTO email_templates (name, subject, body, scope, is_shared, created_by)
     VALUES (?, ?, ?, ?, ?, ?)`,
    [d.name, d.subject, d.body, d.scope, d.isShared ? 1 : 0, guard.id],
  );
  const id = Number(res.lastInsertRowid);
  return NextResponse.json({ id }, { status: 201 });
}
