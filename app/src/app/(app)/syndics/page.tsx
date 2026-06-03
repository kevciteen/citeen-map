import { Topbar } from "@/components/layout/topbar";
import { SyndicsBrowser } from "@/components/crm/syndics-browser";
import { getCoproSyndicCounts } from "@/lib/services/global-counts";

export const dynamic = "force-dynamic";

export default async function SyndicsPage() {
  const { totalSyndics: total } = await getCoproSyndicCounts();

  return (
    <div className="flex h-full flex-col">
      <Topbar
        title="Syndics"
        subtitle={`${total.toLocaleString("fr-FR")} syndics référencés — partez du gestionnaire pour identifier votre cible commerciale`}
      />
      <div className="flex-1 overflow-hidden bg-secondary/30">
        <SyndicsBrowser totalInDb={total} />
      </div>
    </div>
  );
}
