import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import type { InValue } from "@libsql/client";
import { z } from "zod";
import { ensureProspectExtras } from "@/lib/db/ensure-prospect-extras";
import { getCurrentUser } from "@/lib/auth/session";

export const runtime = "nodejs";

const STAGE_VALUES = [
  "lead",
  "to_contact",
  "contacted",
  "meeting",
  "proposal",
  "audit",
  "ag_vote",
  "won",
  "lost",
] as const;

const createSchema = z.object({
  coproId: z.number().int().positive().optional(),
  customLabel: z.string().min(1).optional(),
  customAddress: z.string().optional(),
  customLat: z.number().optional(),
  customLon: z.number().optional(),
  stage: z.enum(STAGE_VALUES).default("lead"),
  priority: z.number().int().min(1).max(3).default(2),
  estimatedValue: z.number().nullable().optional(),
  assignedTo: z.string().optional(),
  assignedUserId: z.number().int().positive().nullable().optional(),
  tags: z.array(z.string()).optional(),
});

export async function GET(req: NextRequest) {
  await ensureProspectExtras();
  const sp = req.nextUrl.searchParams;
  const stage = sp.get("stage");
  const mineOnly = sp.get("mine") === "1";

  const where: string[] = [];
  const params: InValue[] = [];
  if (stage) {
    where.push("p.stage = ?");
    params.push(stage);
  }
  if (mineOnly) {
    const me = await getCurrentUser();
    if (me) {
      where.push("p.assigned_user_id = ?");
      params.push(me.id);
    }
  }

  const rows = await db.all(
    `SELECT
       p.id, p.copro_id, p.custom_label, p.custom_address, p.custom_lat, p.custom_lon,
       p.stage, p.priority, p.estimated_value, p.expected_close_date,
       p.next_action_at, p.next_action_label, p.assigned_to, p.assigned_user_id,
       p.won_reason, p.lost_reason, p.tags,
       p.created_at, p.updated_at,
       c.nom_copro, c.adresse, c.code_postal, c.commune, c.syndic, c.lat as copro_lat, c.lon as copro_lon,
       e.classe_finale,
       u.name AS assigned_user_name, u.email AS assigned_user_email
     FROM prospects p
     LEFT JOIN copros c ON c.id = p.copro_id
     LEFT JOIN dpe_estimates e ON e.copro_id = p.copro_id
     LEFT JOIN users u ON u.id = p.assigned_user_id
     ${where.length ? "WHERE " + where.join(" AND ") : ""}
     ORDER BY p.updated_at DESC`,
    params,
  );

  return NextResponse.json({ items: rows });
}

export async function POST(req: NextRequest) {
  await ensureProspectExtras();
  const body = await req.json();
  const parsed = createSchema.safeParse(body);
  if (!parsed.success) {
    return NextResponse.json({ error: parsed.error.format() }, { status: 400 });
  }
  const d = parsed.data;

  if (d.coproId) {
    const existing = await db.get<{ id: number }>(
      "SELECT id FROM prospects WHERE copro_id = ?",
      [d.coproId],
    );
    if (existing) {
      return NextResponse.json(
        { error: "already_exists", prospectId: existing.id },
        { status: 409 },
      );
    }
  }

  const tagsJson = d.tags ? JSON.stringify(d.tags) : null;

  const me = await getCurrentUser();
  // Auto-assign au user qui crée si pas explicite
  const assignedUserId = d.assignedUserId ?? me?.id ?? null;
  const info = await db.run(
    `INSERT INTO prospects
      (copro_id, custom_label, custom_address, custom_lat, custom_lon,
       stage, priority, estimated_value, assigned_to, assigned_user_id, tags)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      d.coproId ?? null,
      d.customLabel ?? null,
      d.customAddress ?? null,
      d.customLat ?? null,
      d.customLon ?? null,
      d.stage,
      d.priority,
      d.estimatedValue ?? null,
      d.assignedTo ?? null,
      assignedUserId,
      tagsJson,
    ],
  );

  const id = info.lastInsertRowid;
  await db.run(
    `INSERT INTO activities (prospect_id, type, payload, author) VALUES (?, 'created', NULL, NULL)`,
    [id],
  );

  return NextResponse.json({ id }, { status: 201 });
}
