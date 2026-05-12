import { Topbar } from "@/components/layout/topbar";
import { TodayBoard } from "@/components/crm/today-board";
import { requireUser } from "@/lib/auth/session";
import { redirect } from "next/navigation";

export const dynamic = "force-dynamic";

export default async function TodayPage() {
  let me;
  try {
    me = await requireUser();
  } catch {
    redirect("/login");
  }
  return (
    <div className="flex h-full flex-col">
      <Topbar
        title={`Bonjour ${me.name?.split(" ")[0] ?? me.email.split("@")[0]}`}
        subtitle="Ta journée commerciale en un coup d'œil — relances, tâches, deals en cours"
      />
      <div className="flex-1 overflow-y-auto bg-secondary/30 p-6">
        <div className="mx-auto max-w-7xl">
          <TodayBoard />
        </div>
      </div>
    </div>
  );
}
