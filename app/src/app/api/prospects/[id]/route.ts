import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import type { InValue } from "@libsql/client";
import { z } from "zod";
import { ensureProspectExtras } from "@/lib/db/ensure-prospect-extras";

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

const updateSchema = z.object({
  stage: z.enum(STAGE_VALUES).optional(),
  priority: z.number().int().min(1).max(3).optional(),
  estimatedValue: z.number().nullable().optional(),
  nextActionAt: z.number().nullable().optional(),
  nextActionLabel: z.string().nullable().optional(),
  assignedTo: z.string().nullable().optional(),
  assignedUserId: z.number().int().positive().nullable().optional(),
  wonReason: z.string().nullable().optional(),
  lostReason: z.string().nullable().optional(),
  tags: z.array(z.string()).optional(),
});

export async function GET(_req: NextRequest, { params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const pid = Number(id);
  if (!Number.isFinite(pid)) return NextResponse.json({ error: "bad id" }, { status: 400 });

  const prospect = await db.get(
    `SELECT p.*, c.nom_copro, c.adresse, c.code_postal, c.commune, c.syndic,
            c.nb_lots, c.lat as copro_lat, c.lon as copro_lon,
            e.classe_finale, e.conso_moyenne, e.nb_dpe_individuels
     FROM prospects p
     LEFT JOIN copros c ON c.id = p.copro_id
     LEFT JOIN dpe_estimates e ON e.copro_id = p.copro_id
     WHERE p.id = ?`,
    [pid],
  );
  if (!prospect) return NextResponse.json({ error: "not found" }, { status: 404 });

  const [contacts, notes, tasks, activities] = await Promise.all([
    db.all("SELECT * FROM contacts WHERE prospect_id = ? ORDER BY id", [pid]),
    db.all(
      "SELECT * FROM notes WHERE prospect_id = ? ORDER BY created_at DESC",
      [pid],
    ),
    db.all("SELECT * FROM tasks WHERE prospect_id = ? ORDER BY due_at, id", [pid]),
    db.all(
      "SELECT * FROM activities WHERE prospect_id = ? ORDER BY created_at DESC LIMIT 50",
      [pid],
    ),
  ]);

  return NextResponse.json({ prospect, contacts, notes, tasks, activities });
}

export async function PATCH(req: NextRequest, { params }: { params: Promise<{ id: string }> }) {
  await ensureProspectExtras();
  const { id } = await params;
  const pid = Number(id);
  if (!Number.isFinite(pid)) return NextResponse.json({ error: "bad id" }, { status: 400 });
  const body = await req.json();
  const parsed = updateSchema.safeParse(body);
  if (!parsed.success) return NextResponse.json({ error: parsed.error.format() }, { status: 400 });
  const d = parsed.data;

  const sets: string[] = ["updated_at = unixepoch()"];
  const vals: InValue[] = [];

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
  if (d.assignedUserId !== undefined) {
    sets.push("assigned_user_id = ?");
    vals.push(d.assignedUserId);
  }
  if (d.wonReason !== undefined) {
    sets.push("won_reason = ?");
    vals.push(d.wonReason);
  }
  if (d.lostReason !== undefined) {
    sets.push("lost_reason = ?");
    vals.push(d.lostReason);
  }
  if (d.tags !== undefined) {
    sets.push("tags = ?");
    vals.push(JSON.stringify(d.tags));
  }

  vals.push(pid);
  await db.run(
    `UPDATE prospects SET ${sets.join(", ")} WHERE id = ?`,
    vals,
  );

  if (d.stage) {
    await db.run(
      `INSERT INTO activities (prospect_id, type, payload) VALUES (?, 'stage_change', ?)`,
      [pid, JSON.stringify({ stage: d.stage })],
    );
  }

  return NextResponse.json({ ok: true });
}

export async function DELETE(_req: NextRequest, { params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const pid = Number(id);
  if (!Number.isFinite(pid)) return NextResponse.json({ error: "bad id" }, { status: 400 });
  await db.run("DELETE FROM prospects WHERE id = ?", [pid]);
  return NextResponse.json({ ok: true });
}
