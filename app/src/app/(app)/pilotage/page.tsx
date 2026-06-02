import { Topbar } from "@/components/layout/topbar";
import { CockpitView } from "@/components/crm/cockpit-view";
import { requireUser } from "@/lib/auth/session";
import { redirect } from "next/navigation";

export const dynamic = "force-dynamic";

export default async function PilotagePage() {
  let me;
  try {
    me = await requireUser();
  } catch {
    redirect("/login");
  }
  const firstName = me.name?.split(" ")[0] ?? me.email.split("@")[0];

  return (
    <div className="flex h-full flex-col">
      <Topbar
        title={`Cockpit — ${firstName}`}
        subtitle="Tout ce qui compte aujourd'hui pour la prospection copropriété"
      />
      <div className="flex-1 overflow-y-auto bg-secondary/30">
        <CockpitView />
      </div>
    </div>
  );
}
