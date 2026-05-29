import { NextRequest, NextResponse } from "next/server";
import { getOrFetchSirene, getSyndicRecord } from "@/lib/services/syndic-storage";
import { slugifySyndic } from "@/lib/db/ensure-syndic-contacts";
import { ensureAuth } from "@/lib/auth/guards";

export const runtime = "nodejs";
export const maxDuration = 15;

/**
 * Résout le syndic et MERGE avec les enrichissements user (table
 * syndic_contacts) — l'overrid manuel a la priorité absolue sur les données
 * Sirene. Comme ça quand tu mets à jour email/tel sur la fiche syndic, ça
 * remonte automatiquement partout dans le CRM (panel copro, etc.).
 */
export async function GET(req: NextRequest) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const name = req.nextUrl.searchParams.get("name")?.trim();
  if (!name) {
    return NextResponse.json({ error: "?name= requis" }, { status: 400 });
  }
  try {
    const slug = slugifySyndic(name);
    const stored = await getSyndicRecord(slug);
    // getOrFetchSirene fait DB-first + fallback live → 1 seul roundtrip
    // en cas de cache hit (premier appel : 200-400ms, suivants : ~20ms)
    const sirene = await getOrFetchSirene(slug, name).catch(() => null);

    if (!sirene && !stored) {
      return NextResponse.json(
        { contact: null },
        { headers: { "Cache-Control": "private, max-age=60, stale-while-revalidate=300" } },
      );
    }

    // Merge : user-edits écrasent les données Sirene
    const merged = {
      source: stored ? "merged" : "recherche-entreprises",
      slug,
      nomComplet: sirene?.nomComplet ?? stored?.name ?? name,
      siren: sirene?.siren ?? "",
      siretSiege: sirene?.siretSiege ?? null,
      adresse: stored?.address_override ?? sirene?.adresse ?? null,
      codePostal: sirene?.codePostal ?? null,
      commune: sirene?.commune ?? null,
      departement: sirene?.departement ?? null,
      codeApe: sirene?.codeApe ?? null,
      libelleApe: sirene?.libelleApe ?? null,
      dirigeant: sirene?.dirigeant ?? null,
      trancheEffectif: sirene?.trancheEffectif ?? null,
      matchScore: sirene?.matchScore ?? 0,
      // Champs user-editables — priorité absolue, partout dans le CRM
      email: stored?.email ?? null,
      phone: stored?.phone ?? null,
      contactPerson: stored?.contact_person ?? null,
      website: stored?.website ?? null,
      notes: stored?.notes ?? null,
      hasUserEdits: Boolean(
        stored && (stored.email || stored.phone || stored.contact_person ||
          stored.website || stored.address_override || stored.notes),
      ),
    };
    return NextResponse.json(
      { contact: merged },
      {
        headers: {
          // 60s frais côté navigateur (revisits instantanées), 5 min servi
          // depuis cache pendant revalidation en arrière-plan.
          "Cache-Control":
            "private, max-age=60, stale-while-revalidate=300",
        },
      },
    );
  } catch (err) {
    return NextResponse.json(
      { error: (err as Error).message, contact: null },
      { status: 500 },
    );
  }
}
