import { NextRequest, NextResponse } from "next/server";
import { sqlite } from "@/lib/db/client";
import { z } from "zod";

export const runtime = "nodejs";

const createSchema = z.object({
  coproId: z.number().int().positive().optional(),
  customLabel: z.string().min(1).optional(),
  customAddress: z.string().optional(),
  customLat: z.number().optional(),
  customLon: z.number().optional(),
  stage: z
    .enum(["lead", "to_contact", "contacted", "meeting", "proposal", "won", "lost"])
    .default("lead"),
  priority: z.number().int().min(1).max(3).default(2),
  estimatedValue: z.number().nullable().optional(),
  assignedTo: z.string().optional(),
  tags: z.array(z.string()).optional(),
});

export async function GET(req: NextRequest) {
  const sp = req.nextUrl.searchParams;
  const stage = sp.get("stage");

  const where: string[] = [];
  const params: unknown[] = [];
  if (stage) {
    where.push("p.stage = ?");
    params.push(stage);
  }

  const rows = sqlite
    .prepare(
      `SELECT
         p.id, p.copro_id, p.custom_label, p.custom_address, p.custom_lat, p.custom_lon,
         p.stage, p.priority, p.estimated_value, p.expected_close_date,
         p.next_action_at, p.next_action_label, p.assigned_to, p.tags,
         p.created_at, p.updated_at,
         c.nom_copro, c.adresse, c.code_postal, c.commune, c.syndic, c.lat as copro_lat, c.lon as copro_lon,
         e.classe_finale
       FROM prospects p
       LEFT JOIN copros c ON c.id = p.copro_id
       LEFT JOIN dpe_estimates e ON e.copro_id = p.copro_id
       ${where.length ? "WHERE " + where.join(" AND ") : ""}
       ORDER BY p.updated_at DESC`,
    )
    .all(...params);

  return NextResponse.json({ items: rows });
}

export async function POST(req: NextRequest) {
  const body = await req.json();
  const parsed = createSchema.safeParse(body);
  if (!parsed.success) {
    return NextResponse.json({ error: parsed.error.format() }, { status: 400 });
  }
  const d = parsed.data;

  if (d.coproId) {
    const existing = sqlite
      .prepare("SELECT id FROM prospects WHERE copro_id = ?")
      .get(d.coproId) as { id: number } | undefined;
    if (existing) {
      return NextResponse.json(
        { error: "already_exists", prospectId: existing.id },
        { status: 409 },
      );
    }
  }

  const tagsJson = d.tags ? JSON.stringify(d.tags) : null;

  const insert = sqlite.prepare(
    `INSERT INTO prospects
      (copro_id, custom_label, custom_address, custom_lat, custom_lon,
       stage, priority, estimated_value, assigned_to, tags)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  );
  const info = insert.run(
    d.coproId ?? null,
    d.customLabel ?? null,
    d.customAddress ?? null,
    d.customLat ?? null,
    d.customLon ?? null,
    d.stage,
    d.priority,
    d.estimatedValue ?? null,
    d.assignedTo ?? null,
    tagsJson,
  );

  const id = Number(info.lastInsertRowid);
  sqlite
    .prepare(
      `INSERT INTO activities (prospect_id, type, payload, author) VALUES (?, 'created', NULL, NULL)`,
    )
    .run(id);

  return NextResponse.json({ id }, { status: 201 });
}
