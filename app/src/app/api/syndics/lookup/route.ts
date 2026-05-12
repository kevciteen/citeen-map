import { NextRequest, NextResponse } from "next/server";
import { resolveSyndicByName } from "@/lib/services/syndic-contact";
import { getSyndicRecord } from "@/lib/services/syndic-storage";
import { slugifySyndic } from "@/lib/db/ensure-syndic-contacts";

export const runtime = "nodejs";
export const maxDuration = 15;

/**
 * Résout le syndic et MERGE avec les enrichissements user (table
 * syndic_contacts) — l'overrid manuel a la priorité absolue sur les données
 * Sirene. Comme ça quand tu mets à jour email/tel sur la fiche syndic, ça
 * remonte automatiquement partout dans le CRM (panel copro, etc.).
 */
export async function GET(req: NextRequest) {
  const name = req.nextUrl.searchParams.get("name")?.trim();
  if (!name) {
    return NextResponse.json({ error: "?name= requis" }, { status: 400 });
  }
  try {
    const slug = slugifySyndic(name);
    const [sirene, stored] = await Promise.all([
      resolveSyndicByName(name).catch(() => null),
      getSyndicRecord(slug),
    ]);

    // Si on a aucune donnée du tout, on renvoie null
    if (!sirene && !stored) {
      return NextResponse.json({ contact: null });
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
    return NextResponse.json({ contact: merged });
  } catch (err) {
    return NextResponse.json(
      { error: (err as Error).message, contact: null },
      { status: 500 },
    );
  }
}
