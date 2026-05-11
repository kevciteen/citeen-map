import { NextRequest, NextResponse } from "next/server";
import { sqlite } from "@/lib/db/client";
import { z } from "zod";

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
  const body = await req.json();
  const parsed = schema.safeParse(body);
  if (!parsed.success) {
    return NextResponse.json({ error: parsed.error.format() }, { status: 400 });
  }
  const d = parsed.data;
  const tagsJson = d.tags ? JSON.stringify(d.tags) : null;

  const existsStmt = sqlite.prepare("SELECT id FROM prospects WHERE copro_id = ?");
  const insertStmt = sqlite.prepare(
    `INSERT INTO prospects (copro_id, stage, priority, tags) VALUES (?, ?, ?, ?)`,
  );
  const activityStmt = sqlite.prepare(
    `INSERT INTO activities (prospect_id, type, payload, author) VALUES (?, 'created', '{"bulk":true}', NULL)`,
  );

  let created = 0;
  let alreadyExists = 0;
  const ids: number[] = [];

  const trx = sqlite.transaction(() => {
    for (const coproId of d.coproIds) {
      const existing = existsStmt.get(coproId) as { id: number } | undefined;
      if (existing) {
        alreadyExists++;
        ids.push(existing.id);
        continue;
      }
      const info = insertStmt.run(coproId, d.stage, d.priority, tagsJson);
      const pid = Number(info.lastInsertRowid);
      activityStmt.run(pid);
      created++;
      ids.push(pid);
    }
  });
  trx();

  return NextResponse.json({
    created,
    alreadyExists,
    total: d.coproIds.length,
    prospectIds: ids,
  });
}
