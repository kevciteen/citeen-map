import { Topbar } from "@/components/layout/topbar";
import { CoprosBrowser } from "@/components/crm/copros-browser";
import { db } from "@/lib/db/client";

export const dynamic = "force-dynamic";

export default async function CoprosPage() {
  const row = await db.get<{ c: number }>("SELECT COUNT(*) AS c FROM copros");
  const total = row?.c ?? 0;

  return (
    <div className="flex h-full flex-col">
      <Topbar
        title="Copropriétés"
        subtitle={`${total.toLocaleString("fr-FR")} immeubles en base — registre national IDF`}
      />
      <div className="flex-1 overflow-hidden bg-secondary/30">
        <CoprosBrowser totalInDb={total} />
      </div>
    </div>
  );
}
