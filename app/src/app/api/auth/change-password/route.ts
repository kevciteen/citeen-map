import { NextRequest, NextResponse } from "next/server";
import { requireUser } from "@/lib/auth/session";
import { findUserById, setUserPassword } from "@/lib/auth/users";
import { verifyPassword } from "@/lib/auth/password";

export const runtime = "nodejs";

export async function POST(req: NextRequest) {
  let me;
  try {
    me = await requireUser();
  } catch {
    return NextResponse.json({ error: "Non authentifié" }, { status: 401 });
  }
  const body = (await req.json().catch(() => null)) as
    | { current?: string; next?: string }
    | null;
  if (!body?.current || !body?.next || body.next.length < 8) {
    return NextResponse.json(
      { error: "Mot de passe actuel + nouveau (min 8 caractères) requis" },
      { status: 400 },
    );
  }
  const user = await findUserById(me.id);
  if (!user) {
    return NextResponse.json({ error: "Utilisateur introuvable" }, { status: 404 });
  }
  const ok = await verifyPassword(body.current, user.password_hash);
  if (!ok) {
    return NextResponse.json(
      { error: "Mot de passe actuel incorrect" },
      { status: 400 },
    );
  }
  await setUserPassword(user.id, body.next, true);
  return NextResponse.json({ ok: true });
}
