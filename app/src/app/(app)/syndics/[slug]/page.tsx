import { Topbar } from "@/components/layout/topbar";
import { SyndicDetail } from "@/components/crm/syndic-detail";
import { getSyndicFullDetail } from "@/lib/services/syndic-storage";
import { resolveSyndicByName } from "@/lib/services/syndic-contact";
import { notFound } from "next/navigation";

export const dynamic = "force-dynamic";

export default async function SyndicDetailPage({
  params,
  searchParams,
}: {
  params: Promise<{ slug: string }>;
  searchParams: Promise<{ name?: string }>;
}) {
  const { slug } = await params;
  const sp = await searchParams;

  // 1re visite : on a slug + name (passé en query par les liens depuis
  // SyndicsBrowser / CoproPanel). Une fois la fiche éditée, le name est
  // persisté en DB et plus besoin du query param.
  const detail = await getSyndicFullDetail(slug, sp.name);
  if (!detail) notFound();

  // On enrichit avec la Sirene si pas encore fait (pré-calcul SSR pour le 1er render)
  const sirene = detail.sirene ?? (await resolveSyndicByName(detail.name).catch(() => null));

  return (
    <div className="flex h-full flex-col">
      <Topbar
        title={detail.name}
        subtitle={`Fiche syndic · ${
          detail.aggregate?.nb_copros ?? 0
        } copros gérées · ${detail.aggregate?.lots_total ?? 0} lots`}
      />
      <div className="flex-1 overflow-y-auto bg-secondary/30 p-6">
        <div className="mx-auto max-w-5xl">
          <SyndicDetail initial={{ ...detail, sirene }} />
        </div>
      </div>
    </div>
  );
}
