import { Topbar } from "@/components/layout/topbar";
import { TertiaireBuildingDetail } from "@/components/crm/tertiaire-building-detail";
import { db } from "@/lib/db/client";
import { ensureTertiary } from "@/lib/db/ensure-tertiary";
import { requireUser } from "@/lib/auth/session";
import { redirect, notFound } from "next/navigation";
import Link from "next/link";
import { ArrowLeft } from "lucide-react";
import { PrintButton } from "@/components/common/print-button";

export const dynamic = "force-dynamic";

type Building = {
  id: number;
  adresse: string | null;
  code_postal: string | null;
  commune: string | null;
  code_insee_commune: string | null;
  departement: string | null;
  lat: number | null;
  lon: number | null;
  secteur: string | null;
  type_usage: string | null;
  surface_m2: number | null;
  annee_construction: number | null;
};

export default async function TertiaireBuildingDetailPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  try {
    await requireUser();
  } catch {
    redirect("/login");
  }
  await ensureTertiary();
  const { id } = await params;
  const buildingId = Number(id);
  if (!Number.isFinite(buildingId)) notFound();

  const building = await db.get<Building>(
    `SELECT id, adresse, code_postal, commune, code_insee_commune, departement,
            lat, lon, secteur, type_usage, surface_m2, annee_construction
     FROM tertiary_buildings WHERE id = ?`,
    [buildingId],
  );
  if (!building) notFound();

  return (
    <div className="flex h-full flex-col">
      <Topbar
        title={building.adresse ?? `Bâtiment #${building.id}`}
        subtitle={`${building.secteur ?? "Tertiaire"} · ${building.code_postal ?? ""} ${building.commune ?? ""}`}
      />
      <div className="flex-1 overflow-y-auto bg-secondary/30 p-6">
        <div className="mx-auto max-w-5xl space-y-3">
          <div className="flex items-center justify-between gap-2">
            <Link
              href="/tertiaire"
              className="inline-flex items-center gap-1 text-xs text-muted-foreground hover:text-primary"
            >
              <ArrowLeft className="h-3 w-3" />
              Retour à la recherche par adresse
            </Link>
            <PrintButton />
          </div>
          <TertiaireBuildingDetail buildingId={building.id} initialBuilding={building} />
        </div>
      </div>
    </div>
  );
}
