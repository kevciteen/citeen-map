import { NextRequest, NextResponse } from "next/server";
import { authenticateUser, ensureAdminUser } from "@/lib/auth/users";
import { getSession } from "@/lib/auth/session";
import { rateLimit } from "@/lib/rate-limit";

export const runtime = "nodejs";

export async function POST(req: NextRequest) {
  // Brute-force protection : 5 tentatives / minute / IP
  const ip =
    req.headers.get("x-forwarded-for")?.split(",")[0]?.trim() ??
    req.headers.get("x-real-ip") ??
    "unknown";
  const limited = await rateLimit("login", `ip:${ip}`);
  if (limited) return limited;
  // Garantit que le compte admin est seed (idempotent) avant toute tentative
  try {
    await ensureAdminUser();
  } catch {
    // pas fatal — on continue même si seed fail
  }

  const body = (await req.json().catch(() => null)) as
    | { email?: string; password?: string }
    | null;
  if (!body?.email || !body?.password) {
    return NextResponse.json(
      { error: "email + password requis" },
      { status: 400 },
    );
  }

  const user = await authenticateUser(body.email, body.password);
  if (!user) {
    return NextResponse.json(
      { error: "Identifiants invalides" },
      { status: 401 },
    );
  }

  const session = await getSession();
  session.user = {
    id: user.id,
    email: user.email,
    role: user.role,
    name: user.name,
  };
  await session.save();

  return NextResponse.json({
    user: session.user,
    mustChangePassword: user.must_change_password === 1,
  });
}
