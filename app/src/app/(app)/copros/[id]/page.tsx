import { notFound } from "next/navigation";
import { db } from "@/lib/db/client";
import { Topbar } from "@/components/layout/topbar";
import { CoproFiche } from "@/components/crm/copro-fiche";

export const dynamic = "force-dynamic";

type DbRow = {
  id: number;
  numero_immatriculation: string;
  nom_copro: string | null;
  adresse: string | null;
  code_postal: string | null;
  commune: string | null;
  departement: string | null;
  syndic: string | null;
  nb_lots: number | null;
  nb_lots_habitation: number | null;
  periode_construction: string | null;
  lat: number | null;
  lon: number | null;
  code_insee_commune: string | null;
  section: string | null;
  numero_parcelle: string | null;
  reference_cadastrale: string | null;
  classe_finale: string | null;
  classe_reelle: string | null;
  classe_simulee: string | null;
  conso_moyenne: number | null;
  nb_dpe_individuels: number | null;
  rayon_recherche: number | null;
};

export default async function CoproDetailPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = await params;
  const coproId = Number(id);
  if (!Number.isFinite(coproId)) notFound();

  const copro = await db.get<DbRow>(
    `SELECT c.*, e.classe_finale, e.classe_reelle, e.classe_simulee,
            e.conso_moyenne, e.nb_dpe_individuels, e.rayon_recherche
     FROM copros c
     LEFT JOIN dpe_estimates e ON e.copro_id = c.id
     WHERE c.id = ?`,
    [coproId],
  );

  if (!copro) notFound();

  const prospect = await db.get<{ id: number; stage: string }>(
    "SELECT id, stage FROM prospects WHERE copro_id = ? ORDER BY id DESC LIMIT 1",
    [coproId],
  );

  return (
    <div className="flex h-full flex-col">
      <Topbar
        title={copro.nom_copro || copro.adresse || `Copro #${copro.id}`}
        subtitle={`${copro.adresse ?? ""} · ${copro.code_postal ?? ""} ${copro.commune ?? ""}`}
      />
      <div className="flex-1 overflow-y-auto bg-secondary/30">
        <CoproFiche copro={copro} prospect={prospect ?? null} />
      </div>
    </div>
  );
}
