import { Topbar } from "@/components/layout/topbar";
import { DpeDetailView } from "@/components/dpe/dpe-detail-view";
import { DpeTertiaireDetailView } from "@/components/dpe/dpe-tertiaire-detail-view";
import { fetchAdemeDpeByNumero } from "@/lib/services/ademe";
import { fetchDpeTertiaireByNumero } from "@/lib/services/dpe-tertiaire";
import { requireUser } from "@/lib/auth/session";
import { redirect, notFound } from "next/navigation";
import Link from "next/link";
import { ArrowLeft } from "lucide-react";

export const dynamic = "force-dynamic";

export default async function DpeDetailPage({
  params,
}: {
  params: Promise<{ numero: string }>;
}) {
  try {
    await requireUser();
  } catch {
    redirect("/login");
  }
  const { numero } = await params;

  // Essai dataset résidentiel actuel puis tertiaire (anciens DPE + tertiaire)
  const [actuel, tert] = await Promise.all([
    fetchAdemeDpeByNumero(numero).catch(() => null),
    fetchDpeTertiaireByNumero(numero).catch(() => null),
  ]);

  if (!actuel && !tert) notFound();

  return (
    <div className="flex h-full flex-col">
      <Topbar
        title={`DPE ${numero}`}
        subtitle={
          actuel
            ? "Fiche officielle ADEME — Diagnostic de Performance Énergétique"
            : "Fiche DPE — Dataset ADEME tertiaire (méthode 2007-2021)"
        }
      />
      <div className="flex-1 overflow-y-auto bg-secondary/30 p-6">
        <div className="mx-auto max-w-4xl space-y-3">
          <Link
            href="/dpe"
            className="inline-flex items-center gap-1 text-xs text-muted-foreground hover:text-primary"
          >
            <ArrowLeft className="h-3 w-3" />
            Retour à la recherche par adresse
          </Link>
          {actuel ? (
            <DpeDetailView record={actuel} />
          ) : tert ? (
            <DpeTertiaireDetailView record={tert} />
          ) : null}
        </div>
      </div>
    </div>
  );
}
