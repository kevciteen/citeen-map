import { NextRequest, NextResponse } from "next/server";
import { unsealData } from "iron-session";

/**
 * Middleware d'auth — bloque les pages app et les API métier si pas connecté.
 *
 * Pages PUBLIQUES (autorisées sans session) :
 *   - /login
 *   - /api/auth/*
 *   - assets _next, favicon, etc.
 *
 * Tout le reste exige une session valide. Si pas de session → redirige
 * vers /login pour les pages, ou renvoie 401 pour les API.
 */
type SessionShape = { user?: { id: number; email: string; role: string } };

const SESSION_COOKIE = "citeen_session";

const PUBLIC_PATHS = [
  "/login",
  "/api/auth/login",
  "/api/auth/logout",
  "/api/auth/me",
  // Cron routes : protégées par CRON_SECRET côté handler (Vercel ajoute
  // le header Authorization automatiquement), pas par session cookie.
  "/api/cron",
];

function isPublic(pathname: string): boolean {
  if (PUBLIC_PATHS.some((p) => pathname === p || pathname.startsWith(p + "/")))
    return true;
  if (pathname.startsWith("/_next")) return true;
  if (pathname.startsWith("/favicon")) return true;
  if (pathname.match(/\.(svg|png|jpg|jpeg|gif|ico|webp|css|js|map)$/)) return true;
  return false;
}

export async function middleware(req: NextRequest) {
  const { pathname } = req.nextUrl;
  if (isPublic(pathname)) return NextResponse.next();

  const cookieValue = req.cookies.get(SESSION_COOKIE)?.value;
  if (!cookieValue) return redirectOr401(req, pathname);

  const password = process.env.SESSION_SECRET;
  if (!password || password.length < 32) return redirectOr401(req, pathname);

  try {
    const session = await unsealData<SessionShape>(cookieValue, { password });
    if (!session?.user) return redirectOr401(req, pathname);
    const res = NextResponse.next();
    res.headers.set("x-user-id", String(session.user.id));
    res.headers.set("x-user-role", session.user.role);
    return res;
  } catch {
    return redirectOr401(req, pathname);
  }
}

function redirectOr401(req: NextRequest, pathname: string) {
  if (pathname.startsWith("/api/")) {
    return NextResponse.json({ error: "Non authentifié" }, { status: 401 });
  }
  const url = req.nextUrl.clone();
  url.pathname = "/login";
  url.searchParams.set("next", pathname);
  return NextResponse.redirect(url);
}

export const config = {
  matcher: [
    "/((?!_next/static|_next/image|favicon.ico).*)",
  ],
};
