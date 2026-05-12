import { NextRequest, NextResponse } from "next/server";
import { requireAdmin } from "@/lib/auth/session";
import { createUser, listUsers } from "@/lib/auth/users";

export const runtime = "nodejs";

export async function GET() {
  try {
    await requireAdmin();
  } catch {
    return NextResponse.json({ error: "Accès admin requis" }, { status: 403 });
  }
  const users = await listUsers();
  // Ne JAMAIS exposer le hash en clair
  return NextResponse.json({
    users: users.map((u) => ({
      id: u.id,
      email: u.email,
      role: u.role,
      name: u.name,
      active: u.active === 1,
      must_change_password: u.must_change_password === 1,
      created_at: u.created_at,
      last_login_at: u.last_login_at,
    })),
  });
}

export async function POST(req: NextRequest) {
  let admin;
  try {
    admin = await requireAdmin();
  } catch {
    return NextResponse.json({ error: "Accès admin requis" }, { status: 403 });
  }
  const body = (await req.json().catch(() => null)) as {
    email?: string;
    password?: string;
    role?: "admin" | "member";
    name?: string;
  } | null;
  if (!body?.email || !body?.password) {
    return NextResponse.json(
      { error: "email + password requis" },
      { status: 400 },
    );
  }
  if (body.password.length < 8) {
    return NextResponse.json(
      { error: "Mot de passe : 8 caractères minimum" },
      { status: 400 },
    );
  }
  try {
    const id = await createUser({
      email: body.email,
      password: body.password,
      role: body.role ?? "member",
      name: body.name,
      createdBy: admin.id,
      mustChangePassword: true,
    });
    return NextResponse.json({ id });
  } catch (err) {
    const msg = (err as Error).message;
    if (msg.includes("UNIQUE")) {
      return NextResponse.json(
        { error: "Cet email est déjà utilisé" },
        { status: 409 },
      );
    }
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}
