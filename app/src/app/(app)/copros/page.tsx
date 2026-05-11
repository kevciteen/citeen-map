import { Topbar } from "@/components/layout/topbar";
import { CoprosBrowser } from "@/components/crm/copros-browser";
import { sqlite } from "@/lib/db/client";

export const dynamic = "force-dynamic";

export default function CoprosPage() {
  const total = (
    sqlite.prepare("SELECT COUNT(*) AS c FROM copros").get() as { c: number }
  ).c;

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
