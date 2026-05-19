import { notFound } from "next/navigation";
import { db } from "@/lib/db/client";
import { Topbar } from "@/components/layout/topbar";
import { ProspectDetail } from "@/components/crm/prospect-detail";
import { ensureProspectDpe } from "@/lib/db/ensure-prospect-dpe";

export const dynamic = "force-dynamic";

export default async function ProspectPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const pid = Number(id);
  if (!Number.isFinite(pid)) notFound();

  await ensureProspectDpe();
  const prospect = await db.get(
    `SELECT p.*, c.nom_copro, c.adresse, c.code_postal, c.commune, c.syndic,
            c.nb_lots, c.nb_lots_habitation, c.periode_construction,
            c.lat as copro_lat, c.lon as copro_lon, c.numero_immatriculation,
            e.classe_finale, e.classe_reelle, e.classe_simulee, e.conso_moyenne, e.nb_dpe_individuels
     FROM prospects p
     LEFT JOIN copros c ON c.id = p.copro_id
     LEFT JOIN dpe_estimates e ON e.copro_id = p.copro_id
     WHERE p.id = ?`,
    [pid],
  );

  if (!prospect) notFound();

  return (
    <div className="flex h-full flex-col">
      <Topbar title="Fiche prospect" subtitle={`#${pid}`} />
      <div className="flex-1 overflow-y-auto bg-secondary/30 p-6">
        <ProspectDetail initialProspect={prospect as Record<string, unknown>} id={pid} />
      </div>
    </div>
  );
}
