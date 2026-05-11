import { Topbar } from "@/components/layout/topbar";
import { SyndicsBrowser } from "@/components/crm/syndics-browser";
import { db } from "@/lib/db/client";

export const dynamic = "force-dynamic";

export default async function SyndicsPage() {
  const row = await db.get<{ c: number }>(
    `SELECT COUNT(DISTINCT TRIM(syndic)) AS c
     FROM copros
     WHERE syndic IS NOT NULL AND LOWER(syndic) != 'non connu'`,
  );
  const total = row?.c ?? 0;

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
