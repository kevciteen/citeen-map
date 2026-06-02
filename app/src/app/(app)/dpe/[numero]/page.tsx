import { Topbar } from "@/components/layout/topbar";
import { DpeDetailView } from "@/components/dpe/dpe-detail-view";
import { DpeTertiaireDetailView } from "@/components/dpe/dpe-tertiaire-detail-view";
import { fetchAdemeDpeByNumero } from "@/lib/services/ademe";
import { fetchDpeTertiaireByNumero } from "@/lib/services/dpe-tertiaire";
import { fetchDpeLegacyByNumero, type DpeLegacyRecord } from "@/lib/services/dpe-legacy";
import type { DpeTertiaireRecord } from "@/lib/services/dpe-tertiaire";
import { requireUser } from "@/lib/auth/session";
import { redirect, notFound } from "next/navigation";
import Link from "next/link";
import { ArrowLeft } from "lucide-react";
import { PrintButton } from "@/components/common/print-button";

export const dynamic = "force-dynamic";

/** Adapte un record legacy au format attendu par DpeTertiaireDetailView */
function legacyToTertRecord(r: DpeLegacyRecord): DpeTertiaireRecord {
  return {
    numero_dpe: r.numero_dpe,
    classe_consommation_energie: r.classe_consommation_energie,
    classe_estimation_ges: r.classe_estimation_ges,
    consommation_energie: r.consommation_energie,
    estimation_ges: r.estimation_ges,
    surface_utile: r.surface_thermique_lot ?? r.surface_habitable,
    surface_habitable: r.surface_habitable,
    secteur_activite: "Logement résidentiel (méthode pré-2021)",
    tr002_type_batiment_libelle: r.tr002_type_batiment_libelle ?? "Résidentiel",
    geo_adresse: r.geo_adresse,
    nom_rue: r.nom_rue,
    commune: r.commune,
    code_postal: r.code_postal,
    code_insee_commune: r.code_insee_commune,
    latitude: r.latitude,
    longitude: r.longitude,
    _geopoint: r._geopoint,
    annee_construction: r.annee_construction,
    date_etablissement_dpe: r.date_etablissement_dpe,
    date_reception_dpe: r.date_reception_dpe,
  };
}

export default async function DpeDetailPage({
  params,
}: {
  params: Promise<{ numero: string }>;
}) {
  try {
    await requireUser();
  } catch {
    redirect("/login");
  }
  const { numero } = await params;

  // Essai des 3 datasets ADEME en parallèle
  const [actuel, tert, legacy] = await Promise.all([
    fetchAdemeDpeByNumero(numero).catch(() => null),
    fetchDpeTertiaireByNumero(numero).catch(() => null),
    fetchDpeLegacyByNumero(numero).catch(() => null),
  ]);

  if (!actuel && !tert && !legacy) notFound();

  // Priorité d'affichage : actuel > tertiaire > legacy
  const subtitleByDataset = actuel
    ? "Fiche officielle ADEME — Diagnostic de Performance Énergétique"
    : tert
      ? "Fiche DPE — Dataset ADEME tertiaire (méthode 2007-2021)"
      : "Fiche DPE — Méthode résidentielle pré-2021 (dataset dpe-france)";

  return (
    <div className="flex h-full flex-col">
      <Topbar title={`DPE ${numero}`} subtitle={subtitleByDataset} />
      <div className="flex-1 overflow-y-auto bg-secondary/30 p-6">
        <div className="mx-auto max-w-4xl space-y-3">
          <div className="flex items-center justify-between gap-2">
            <Link
              href="/dpe"
              className="inline-flex items-center gap-1 text-xs text-muted-foreground hover:text-primary"
            >
              <ArrowLeft className="h-3 w-3" />
              Retour à la recherche par adresse
            </Link>
            <PrintButton />
          </div>
          {actuel ? (
            <DpeDetailView record={actuel} />
          ) : tert ? (
            <DpeTertiaireDetailView record={tert} />
          ) : legacy ? (
            <DpeTertiaireDetailView record={legacyToTertRecord(legacy)} />
          ) : null}
        </div>
      </div>
    </div>
  );
}
