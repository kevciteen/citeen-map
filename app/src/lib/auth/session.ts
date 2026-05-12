import { getIronSession, type IronSession } from "iron-session";
import { cookies } from "next/headers";

export type SessionUser = {
  id: number;
  email: string;
  role: "admin" | "member";
  name: string | null;
};

export type AppSession = {
  user?: SessionUser;
};

const SESSION_COOKIE = "citeen_session";

function sessionOptions() {
  const password = process.env.SESSION_SECRET;
  if (!password || password.length < 32) {
    throw new Error(
      "SESSION_SECRET env var manquante ou < 32 caractères. Génère un secret long et ajoute-le dans Vercel.",
    );
  }
  return {
    password,
    cookieName: SESSION_COOKIE,
    cookieOptions: {
      secure: process.env.NODE_ENV === "production",
      httpOnly: true,
      sameSite: "lax" as const,
      maxAge: 60 * 60 * 24 * 30, // 30 jours
      path: "/",
    },
  };
}

export async function getSession(): Promise<IronSession<AppSession>> {
  const cookieStore = await cookies();
  return getIronSession<AppSession>(cookieStore, sessionOptions());
}

export async function getCurrentUser(): Promise<SessionUser | null> {
  const s = await getSession();
  return s.user ?? null;
}

export async function requireUser(): Promise<SessionUser> {
  const u = await getCurrentUser();
  if (!u) throw new Error("Non authentifié");
  return u;
}

export async function requireAdmin(): Promise<SessionUser> {
  const u = await requireUser();
  if (u.role !== "admin") throw new Error("Accès admin requis");
  return u;
}
