import { NextRequest, NextResponse } from "next/server";
import { requireAdmin } from "@/lib/auth/session";
import { setUserPassword, updateUser } from "@/lib/auth/users";

export const runtime = "nodejs";

export async function PATCH(
  req: NextRequest,
  { params }: { params: Promise<{ id: string }> },
) {
  try {
    await requireAdmin();
  } catch {
    return NextResponse.json({ error: "Accès admin requis" }, { status: 403 });
  }
  const { id: rawId } = await params;
  const id = Number(rawId);
  if (!Number.isFinite(id)) {
    return NextResponse.json({ error: "ID invalide" }, { status: 400 });
  }
  const body = (await req.json().catch(() => null)) as {
    name?: string | null;
    role?: "admin" | "member";
    active?: boolean;
    resetPassword?: string;
  } | null;
  if (!body) return NextResponse.json({ error: "JSON requis" }, { status: 400 });

  const patch: Parameters<typeof updateUser>[1] = {};
  if ("name" in body) patch.name = body.name ?? null;
  if (body.role) patch.role = body.role;
  if ("active" in body) patch.active = !!body.active;
  await updateUser(id, patch);

  if (body.resetPassword && body.resetPassword.length >= 8) {
    await setUserPassword(id, body.resetPassword, false);
  }
  return NextResponse.json({ ok: true });
}
