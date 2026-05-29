import { Topbar } from "@/components/layout/topbar";
import { DpeAtAddress } from "@/components/dpe/dpe-at-address";
import { requireUser } from "@/lib/auth/session";
import { redirect } from "next/navigation";

export const dynamic = "force-dynamic";

export default async function DpeLookupPage() {
  try {
    await requireUser();
  } catch {
    redirect("/login");
  }
  return (
    <div className="flex h-full flex-col">
      <Topbar
        title="Recherche DPE par adresse"
        subtitle="Reflet exhaustif ADEME : DPE collectifs réels d'immeuble + appartements individuels + maisons — sectionné par type, sans agrégation cachée"
      />
      <div className="flex-1 overflow-y-auto bg-secondary/30 p-6">
        <div className="mx-auto max-w-3xl">
          <DpeAtAddress autoSearch={false} />
        </div>
      </div>
    </div>
  );
}
