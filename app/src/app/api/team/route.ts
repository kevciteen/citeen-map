/**
 * GET /api/team
 *
 * Renvoie la liste des utilisateurs actifs (équipe) pour les selects
 * d'assignement. Champs minimalistes — pas de password_hash etc.
 */
import { NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import { ensureAuth } from "@/lib/auth/guards";
import { ensureUsersTable } from "@/lib/db/ensure-users";

export const runtime = "nodejs";

export async function GET() {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  await ensureUsersTable();
  const team = await db.all<{
    id: number;
    email: string;
    name: string | null;
    role: "admin" | "member";
  }>(
    `SELECT id, email, name, role
     FROM users
     WHERE active = 1
     ORDER BY COALESCE(name, email)`,
  );
  return NextResponse.json({ team, me: { id: guard.id, email: guard.email, name: guard.name } });
}
