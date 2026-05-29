import { Topbar } from "@/components/layout/topbar";
import { CoordsHealthBrowser } from "@/components/admin/coords-health-browser";
import { requireAdmin } from "@/lib/auth/session";
import { redirect } from "next/navigation";

export const dynamic = "force-dynamic";

export default async function AdminCoordsHealthPage() {
  try {
    await requireAdmin();
  } catch {
    redirect("/dashboard");
  }
  return (
    <div className="flex h-full flex-col">
      <Topbar
        title="Santé des coordonnées"
        subtitle="Backfills géocodage copros, quota Google Places, sync annuaire unifié"
      />
      <div className="flex-1 overflow-y-auto bg-secondary/30 p-6">
        <div className="mx-auto max-w-5xl">
          <CoordsHealthBrowser />
        </div>
      </div>
    </div>
  );
}
