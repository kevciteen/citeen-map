/**
 * GET  /api/prospects/[id]/contacts → liste des contacts du prospect
 * POST /api/prospects/[id]/contacts → ajoute un contact
 */
import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import { z } from "zod";
import { ensureAuth } from "@/lib/auth/guards";

export const runtime = "nodejs";

const schema = z.object({
  role: z.string().optional().nullable(),
  fullName: z.string().optional().nullable(),
  email: z.string().optional().nullable(),
  phone: z.string().optional().nullable(),
  company: z.string().optional().nullable(),
  notes: z.string().optional().nullable(),
});

type ContactRow = {
  id: number;
  prospect_id: number;
  role: string | null;
  full_name: string | null;
  email: string | null;
  phone: string | null;
  company: string | null;
  notes: string | null;
  created_at: number | null;
};

export async function GET(
  _req: NextRequest,
  { params }: { params: Promise<{ id: string }> },
) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const { id } = await params;
  const pid = Number(id);
  if (!Number.isFinite(pid)) {
    return NextResponse.json({ error: "bad id" }, { status: 400 });
  }
  const rows = await db.all<ContactRow>(
    "SELECT id, prospect_id, role, full_name, email, phone, company, notes, created_at FROM contacts WHERE prospect_id = ? ORDER BY id DESC",
    [pid],
  );
  return NextResponse.json({ contacts: rows });
}

export async function POST(
  req: NextRequest,
  { params }: { params: Promise<{ id: string }> },
) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const { id } = await params;
  const pid = Number(id);
  if (!Number.isFinite(pid)) {
    return NextResponse.json({ error: "bad id" }, { status: 400 });
  }
  const parsed = schema.safeParse(await req.json());
  if (!parsed.success) {
    return NextResponse.json({ error: parsed.error.format() }, { status: 400 });
  }
  const d = parsed.data;
  const info = await db.run(
    "INSERT INTO contacts (prospect_id, role, full_name, email, phone, company, notes) VALUES (?, ?, ?, ?, ?, ?, ?)",
    [
      pid,
      d.role ?? null,
      d.fullName ?? null,
      d.email ?? null,
      d.phone ?? null,
      d.company ?? null,
      d.notes ?? null,
    ],
  );
  await db.run(
    "INSERT INTO activities (prospect_id, type, payload) VALUES (?, 'contact_added', ?)",
    [pid, JSON.stringify({ contactId: info.lastInsertRowid, role: d.role })],
  );
  await db.run("UPDATE prospects SET updated_at = unixepoch() WHERE id = ?", [pid]);
  return NextResponse.json({ id: info.lastInsertRowid }, { status: 201 });
}
