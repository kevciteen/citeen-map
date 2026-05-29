/**
 * POST /api/directory/sync (admin)
 *
 * Resynchronise l'annuaire `directory` depuis les tables canoniques :
 * copros, tertiary_occupants (+ tertiary_buildings), syndic_contacts,
 * prospects.custom_*. Idempotent via UPSERT sur (entity_type, entity_ref).
 *
 * À appeler après import massif ou enrichissement batch. Pas de pagination
 * pour l'instant — testé sur ~100k rows en moins de 60s.
 */
import { NextResponse } from "next/server";
import { ensureAdmin } from "@/lib/auth/guards";
import { syncDirectoryAll } from "@/lib/services/directory-sync";
import { rateLimit } from "@/lib/rate-limit";

export const runtime = "nodejs";
export const maxDuration = 60;

export async function POST() {
  const guard = await ensureAdmin();
  if (guard instanceof NextResponse) return guard;
  const limited = await rateLimit("heavy", `user:${guard.id}`);
  if (limited) return limited;

  const result = await syncDirectoryAll();
  return NextResponse.json(result);
}
