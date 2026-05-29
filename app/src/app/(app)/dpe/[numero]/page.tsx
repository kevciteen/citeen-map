import { Topbar } from "@/components/layout/topbar";
import { DpeDetailView } from "@/components/dpe/dpe-detail-view";
import { fetchAdemeDpeByNumero } from "@/lib/services/ademe";
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
  const record = await fetchAdemeDpeByNumero(numero);
  if (!record) notFound();

  return (
    <div className="flex h-full flex-col">
      <Topbar
        title={`DPE ${numero}`}
        subtitle="Fiche officielle ADEME — Diagnostic de Performance Énergétique"
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
          <DpeDetailView record={record} />
        </div>
      </div>
    </div>
  );
}
