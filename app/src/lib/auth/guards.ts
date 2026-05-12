import { NextResponse } from "next/server";
import { getCurrentUser, type SessionUser } from "./session";

/**
 * Defense-in-depth : à appeler en début de chaque route API métier.
 * Le middleware gate déjà ces routes au niveau cookie, mais on ajoute
 * une 2e ligne de défense au niveau du handler au cas où :
 *  - le middleware serait mal configuré
 *  - la route serait appelée depuis un contexte interne sans passer
 *    par le middleware (rare mais possible avec Edge → Node)
 *  - on souhaite logger l'identité de l'appelant dans les logs métier
 *
 * Usage :
 *   const guard = await ensureAuth();
 *   if (guard instanceof NextResponse) return guard;
 *   const user = guard; // SessionUser
 */
export async function ensureAuth(): Promise<SessionUser | NextResponse> {
  const me = await getCurrentUser().catch(() => null);
  if (!me) {
    return NextResponse.json(
      { error: "Non authentifié" },
      { status: 401 },
    );
  }
  return me;
}

export async function ensureAdmin(): Promise<SessionUser | NextResponse> {
  const me = await getCurrentUser().catch(() => null);
  if (!me) {
    return NextResponse.json(
      { error: "Non authentifié" },
      { status: 401 },
    );
  }
  if (me.role !== "admin") {
    return NextResponse.json(
      { error: "Accès admin requis" },
      { status: 403 },
    );
  }
  return me;
}
