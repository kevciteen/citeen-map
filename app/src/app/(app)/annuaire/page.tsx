import { Topbar } from "@/components/layout/topbar";
import { AnnuaireBrowser } from "@/components/annuaire/annuaire-browser";
import { requireUser } from "@/lib/auth/session";
import { redirect } from "next/navigation";

export const dynamic = "force-dynamic";

export default async function AnnuairePage() {
  try {
    await requireUser();
  } catch {
    redirect("/login");
  }
  return (
    <div className="flex h-full flex-col">
      <Topbar
        title="Annuaire unifié"
        subtitle="Recherche cross-entité : copros, sociétés tertiaires, syndics, prospects libres"
      />
      <div className="flex-1 overflow-y-auto bg-secondary/30 p-6">
        <div className="mx-auto max-w-6xl">
          <AnnuaireBrowser />
        </div>
      </div>
    </div>
  );
}
