import { NextRequest, NextResponse } from "next/server";
import { sqlite } from "@/lib/db/client";
import { z } from "zod";

export const runtime = "nodejs";

const updateSchema = z.object({
  stage: z
    .enum(["lead", "to_contact", "contacted", "meeting", "proposal", "won", "lost"])
    .optional(),
  priority: z.number().int().min(1).max(3).optional(),
  estimatedValue: z.number().nullable().optional(),
  nextActionAt: z.number().nullable().optional(),
  nextActionLabel: z.string().nullable().optional(),
  assignedTo: z.string().nullable().optional(),
  tags: z.array(z.string()).optional(),
});

export async function GET(_req: NextRequest, { params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const pid = Number(id);
  if (!Number.isFinite(pid)) return NextResponse.json({ error: "bad id" }, { status: 400 });

  const prospect = sqlite
    .prepare(
      `SELECT p.*, c.nom_copro, c.adresse, c.code_postal, c.commune, c.syndic,
              c.nb_lots, c.lat as copro_lat, c.lon as copro_lon,
              e.classe_finale, e.conso_moyenne, e.nb_dpe_individuels
       FROM prospects p
       LEFT JOIN copros c ON c.id = p.copro_id
       LEFT JOIN dpe_estimates e ON e.copro_id = p.copro_id
       WHERE p.id = ?`,
    )
    .get(pid);
  if (!prospect) return NextResponse.json({ error: "not found" }, { status: 404 });

  const contacts = sqlite
    .prepare("SELECT * FROM contacts WHERE prospect_id = ? ORDER BY id")
    .all(pid);
  const notes = sqlite
    .prepare("SELECT * FROM notes WHERE prospect_id = ? ORDER BY created_at DESC")
    .all(pid);
  const tasks = sqlite
    .prepare("SELECT * FROM tasks WHERE prospect_id = ? ORDER BY due_at, id")
    .all(pid);
  const activities = sqlite
    .prepare("SELECT * FROM activities WHERE prospect_id = ? ORDER BY created_at DESC LIMIT 50")
    .all(pid);

  return NextResponse.json({ prospect, contacts, notes, tasks, activities });
}

export async function PATCH(req: NextRequest, { params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const pid = Number(id);
  if (!Number.isFinite(pid)) return NextResponse.json({ error: "bad id" }, { status: 400 });
  const body = await req.json();
  const parsed = updateSchema.safeParse(body);
  if (!parsed.success) return NextResponse.json({ error: parsed.error.format() }, { status: 400 });
  const d = parsed.data;

  const sets: string[] = ["updated_at = unixepoch()"];
  const vals: unknown[] = [];

  if (d.stage !== undefined) {
    sets.push("stage = ?");
    vals.push(d.stage);
  }
  if (d.priority !== undefined) {
    sets.push("priority = ?");
    vals.push(d.priority);
  }
  if (d.estimatedValue !== undefined) {
    sets.push("estimated_value = ?");
    vals.push(d.estimatedValue);
  }
  if (d.nextActionAt !== undefined) {
    sets.push("next_action_at = ?");
    vals.push(d.nextActionAt);
  }
  if (d.nextActionLabel !== undefined) {
    sets.push("next_action_label = ?");
    vals.push(d.nextActionLabel);
  }
  if (d.assignedTo !== undefined) {
    sets.push("assigned_to = ?");
    vals.push(d.assignedTo);
  }
  if (d.tags !== undefined) {
    sets.push("tags = ?");
    vals.push(JSON.stringify(d.tags));
  }

  vals.push(pid);
  sqlite.prepare(`UPDATE prospects SET ${sets.join(", ")} WHERE id = ?`).run(...vals);

  if (d.stage) {
    sqlite
      .prepare(
        `INSERT INTO activities (prospect_id, type, payload) VALUES (?, 'stage_change', ?)`,
      )
      .run(pid, JSON.stringify({ stage: d.stage }));
  }

  return NextResponse.json({ ok: true });
}

export async function DELETE(_req: NextRequest, { params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const pid = Number(id);
  if (!Number.isFinite(pid)) return NextResponse.json({ error: "bad id" }, { status: 400 });
  sqlite.prepare("DELETE FROM prospects WHERE id = ?").run(pid);
  return NextResponse.json({ ok: true });
}
