import { Topbar } from "@/components/layout/topbar";
import { CoprosSyndicsTabs } from "@/components/crm/copros-syndics-tabs";
import { getCoproSyndicCounts } from "@/lib/services/global-counts";

export const dynamic = "force-dynamic";

export default async function CoprosPage({
  searchParams,
}: {
  searchParams: Promise<{ tab?: string }>;
}) {
  const [{ totalCopros, totalSyndics }, params] = await Promise.all([
    getCoproSyndicCounts(),
    searchParams,
  ]);
  const initialTab = params.tab === "syndics" ? "syndics" : "copros";

  return (
    <div className="flex h-full flex-col">
      <Topbar
        title="Copropriétés"
        subtitle={`${totalCopros.toLocaleString("fr-FR")} immeubles · ${totalSyndics.toLocaleString("fr-FR")} syndics — registre national IDF`}
      />
      <div className="flex-1 overflow-hidden bg-secondary/30">
        <CoprosSyndicsTabs
          totalCopros={totalCopros}
          totalSyndics={totalSyndics}
          initialTab={initialTab}
        />
      </div>
    </div>
  );
}
