import { Topbar } from "@/components/layout/topbar";
import { CampagneBuilder } from "@/components/crm/campagne-builder";
import { requireUser } from "@/lib/auth/session";
import { redirect } from "next/navigation";

export const dynamic = "force-dynamic";

export default async function CampagnesPage() {
  try {
    await requireUser();
  } catch {
    redirect("/login");
  }
  return (
    <div className="flex h-full flex-col">
      <Topbar
        title="Campagnes de prospection"
        subtitle="Cible des copropriétés en F/G + lots min + zone → preview géolocalisé → lance le batch de prospects en 1 clic"
      />
      <div className="flex-1 overflow-y-auto bg-secondary/30 p-6">
        <div className="mx-auto max-w-6xl">
          <CampagneBuilder />
        </div>
      </div>
    </div>
  );
}
