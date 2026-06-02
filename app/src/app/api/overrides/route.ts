/**
 * GET  /api/overrides?type=...&ref=...  → renvoie overlay complet
 * PATCH /api/overrides?type=...&ref=...  body: { field, value } → upsert override
 */
import { NextRequest, NextResponse } from "next/server";
import { ensureAuth } from "@/lib/auth/guards";
import {
  getEntityOverlay,
  setOverride,
  type EntityKey,
} from "@/lib/services/entity-overrides";
import type { OverrideEntityType } from "@/lib/db/ensure-entity-overrides";

export const runtime = "nodejs";

const VALID_TYPES: OverrideEntityType[] = [
  "copro", "tertiary_building", "occupant", "dpe",
  "address", "maison", "appartement", "syndic",
];

function parseKey(req: NextRequest): EntityKey | NextResponse {
  const type = req.nextUrl.searchParams.get("type")?.trim();
  const ref = req.nextUrl.searchParams.get("ref")?.trim();
  if (!type || !ref) {
    return NextResponse.json({ error: "type + ref requis" }, { status: 400 });
  }
  if (!VALID_TYPES.includes(type as OverrideEntityType)) {
    return NextResponse.json({ error: `type invalide (autorisés: ${VALID_TYPES.join(", ")})` }, { status: 400 });
  }
  return { entityType: type as OverrideEntityType, entityRef: ref };
}

export async function GET(req: NextRequest) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const key = parseKey(req);
  if (key instanceof NextResponse) return key;
  const overlay = await getEntityOverlay(key);
  return NextResponse.json(overlay);
}

export async function PATCH(req: NextRequest) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const key = parseKey(req);
  if (key instanceof NextResponse) return key;
  const body = (await req.json().catch(() => ({}))) as {
    field?: string;
    value?: string | null;
  };
  if (!body.field || typeof body.field !== "string") {
    return NextResponse.json({ error: "field requis" }, { status: 400 });
  }
  await setOverride(key, body.field, body.value ?? null, guard.id);
  return NextResponse.json({ ok: true });
}
