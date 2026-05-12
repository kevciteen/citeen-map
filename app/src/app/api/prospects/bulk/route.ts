import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import { z } from "zod";
import { ensureAuth } from "@/lib/auth/guards";

export const runtime = "nodejs";

const schema = z.object({
  coproIds: z.array(z.number().int().positive()).min(1).max(1000),
  stage: z
    .enum(["lead", "to_contact", "contacted", "meeting", "proposal", "won", "lost"])
    .default("to_contact"),
  priority: z.number().int().min(1).max(3).default(2),
  tags: z.array(z.string()).optional(),
});

export async function POST(req: NextRequest) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const body = await req.json();
  const parsed = schema.safeParse(body);
  if (!parsed.success) {
    return NextResponse.json({ error: parsed.error.format() }, { status: 400 });
  }
  const d = parsed.data;
  const tagsJson = d.tags ? JSON.stringify(d.tags) : null;

  // 1. Find existing prospects for these copros in one query
  const placeholders = d.coproIds.map(() => "?").join(",");
  const existing = await db.all<{ copro_id: number; id: number }>(
    `SELECT copro_id, id FROM prospects WHERE copro_id IN (${placeholders})`,
    d.coproIds,
  );
  const existingMap = new Map(existing.map((e) => [e.copro_id, e.id]));

  let created = 0;
  let alreadyExists = 0;
  const ids: number[] = [];

  // 2. Build batch of inserts for non-existing
  const batchStatements: Array<{ sql: string; args: unknown[] }> = [];
  for (const coproId of d.coproIds) {
    const existingId = existingMap.get(coproId);
    if (existingId) {
      alreadyExists++;
      ids.push(existingId);
      continue;
    }
    // Insert prospect + activity in sequence (need lastInsertRowid)
    const info = await db.run(
      "INSERT INTO prospects (copro_id, stage, priority, tags) VALUES (?, ?, ?, ?)",
      [coproId, d.stage, d.priority, tagsJson],
    );
    const pid = info.lastInsertRowid;
    await db.run(
      `INSERT INTO activities (prospect_id, type, payload, author) VALUES (?, 'created', '{"bulk":true}', NULL)`,
      [pid],
    );
    created++;
    ids.push(pid);
  }
  void batchStatements; // not used in async version (sequential)

  return NextResponse.json({
    created,
    alreadyExists,
    total: d.coproIds.length,
    prospectIds: ids,
  });
}
