import { NextRequest, NextResponse } from "next/server";
import { sqlite } from "@/lib/db/client";
import { estimateDpeForCopro } from "@/lib/services/dpe";

export const runtime = "nodejs";

/**
 * Returns the DPE estimation for a copro.
 * Always re-runs the strict matching algorithm (no DB cache shortcut), but
 * the underlying ADEME fetch is cached in memory for 10 min per (lat, lon, r).
 * The result is persisted to `dpe_estimates` for the list / kanban / exports.
 */
export async function GET(_req: NextRequest, { params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const coproId = Number(id);
  if (!Number.isFinite(coproId)) {
    return NextResponse.json({ error: "Invalid id" }, { status: 400 });
  }

  const copro = sqlite
    .prepare(
      `SELECT id, lat, lon, adresse, code_postal, commune,
              numero_immatriculation, code_insee_commune, section, numero_parcelle
       FROM copros WHERE id = ?`,
    )
    .get(coproId) as
    | {
        id: number;
        lat: number | null;
        lon: number | null;
        adresse: string | null;
        code_postal: string | null;
        commune: string | null;
        numero_immatriculation: string | null;
        code_insee_commune: string | null;
        section: string | null;
        numero_parcelle: string | null;
      }
    | undefined;

  if (!copro) return NextResponse.json({ error: "Copro not found" }, { status: 404 });
  if (copro.lat == null || copro.lon == null)
    return NextResponse.json({ error: "Copro sans coordonnées" }, { status: 422 });

  const est = await estimateDpeForCopro({
    lat: copro.lat,
    lon: copro.lon,
    address: copro.adresse,
    codePostal: copro.code_postal,
    commune: copro.commune,
    numeroImmatriculation: copro.numero_immatriculation,
    codeInseeCommune: copro.code_insee_commune,
    section: copro.section,
    numeroParcelle: copro.numero_parcelle,
  });

  sqlite
    .prepare(
      `INSERT INTO dpe_estimates
       (copro_id, classe_reelle, classe_simulee, classe_finale, conso_moyenne, nb_dpe_individuels, rayon_recherche, quality_level, computed_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, unixepoch())
       ON CONFLICT(copro_id) DO UPDATE SET
         classe_reelle = excluded.classe_reelle,
         classe_simulee = excluded.classe_simulee,
         classe_finale = excluded.classe_finale,
         conso_moyenne = excluded.conso_moyenne,
         nb_dpe_individuels = excluded.nb_dpe_individuels,
         rayon_recherche = excluded.rayon_recherche,
         quality_level = excluded.quality_level,
         computed_at = unixepoch()`,
    )
    .run(
      coproId,
      est.collectifReel?.classe ?? null,
      est.immeubleSimule?.classe ?? null,
      est.immeubleFinal.classe,
      est.immeubleFinal.conso ?? est.immeubleSimule?.consoMoyenne ?? null,
      est.totalDpeMatched,
      est.rayonM,
      est.quality.level,
    );

  return NextResponse.json(est);
}
