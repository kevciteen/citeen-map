import { NextRequest, NextResponse } from "next/server";
import { requireUser } from "@/lib/auth/session";
import { db } from "@/lib/db/client";
import { ensureUsersTable } from "@/lib/db/ensure-users";

export const runtime = "nodejs";

/**
 * Autocomplete utilisateurs pour l'input @mention.
 * Filtre sur les comptes actifs uniquement.
 */
export async function GET(req: NextRequest) {
  try {
    await requireUser();
  } catch {
    return NextResponse.json({ error: "Non authentifié" }, { status: 401 });
  }
  await ensureUsersTable();
  const q = req.nextUrl.searchParams.get("q")?.trim().toLowerCase() ?? "";
  const limit = Math.min(Number(req.nextUrl.searchParams.get("limit") ?? 8), 20);
  const where = q
    ? "WHERE active = 1 AND (LOWER(email) LIKE ? OR LOWER(COALESCE(name, '')) LIKE ?)"
    : "WHERE active = 1";
  const params = q ? [`%${q}%`, `%${q}%`] : [];
  const rows = await db.all<{ id: number; email: string; name: string | null }>(
    `SELECT id, email, name FROM users ${where} ORDER BY name, email LIMIT ?`,
    [...params, limit],
  );
  return NextResponse.json({ items: rows });
}
