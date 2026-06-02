import { Topbar } from "@/components/layout/topbar";
import { db } from "@/lib/db/client";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Building2, Kanban, Activity, TrendingUp, Calendar } from "lucide-react";
import { PIPELINE_ORDER, stageMeta, formatCurrency } from "@/lib/utils";
import Link from "next/link";
import { DpeBadge } from "@/components/ui/dpe-badge";
import { TodayBoard } from "@/components/crm/today-board";
import { PipelineSection } from "@/components/crm/pipeline-section";
import { PilotageTabs } from "@/components/crm/pilotage-tabs";
import { requireUser } from "@/lib/auth/session";
import { redirect } from "next/navigation";

export const dynamic = "force-dynamic";

type Tab = "today" | "overview" | "pipeline";

function isTab(v: string | string[] | undefined): v is Tab {
  return v === "today" || v === "overview" || v === "pipeline";
}

export default async function PilotagePage({
  searchParams,
}: {
  searchParams: Promise<{ tab?: string }>;
}) {
  let me;
  try {
    me = await requireUser();
  } catch {
    redirect("/login");
  }
  const params = await searchParams;
  const tab: Tab = isTab(params.tab) ? params.tab : "today";

  const titleByTab: Record<Tab, { title: string; subtitle: string }> = {
    today: {
      title: `Bonjour ${me.name?.split(" ")[0] ?? me.email.split("@")[0]}`,
      subtitle: "Ta journée commerciale en un coup d'œil",
    },
    overview: {
      title: "Vue d'ensemble",
      subtitle: "Tableau de bord prospection — pipeline, relances, performance",
    },
    pipeline: {
      title: "Pipeline commercial",
      subtitle: "Tableau de bord des prospects — kanban / tableau",
    },
  };

  return (
    <div className="flex h-full flex-col">
      <Topbar title={titleByTab[tab].title} subtitle={titleByTab[tab].subtitle} />
      <PilotageTabs current={tab} />
      <div className="flex-1 overflow-y-auto bg-secondary/30">
        {tab === "today" ? <TodaySection /> : null}
        {tab === "overview" ? <OverviewSection /> : null}
        {tab === "pipeline" ? <PipelineSection /> : null}
      </div>
    </div>
  );
}

function TodaySection() {
  return (
    <div className="mx-auto max-w-7xl p-6">
      <TodayBoard />
    </div>
  );
}

async function OverviewSection() {
  const [coproCountRow, prospectCountRow, wonCountRow, dpeEstimatedRow] =
    await Promise.all([
      db.get<{ c: number }>("SELECT COUNT(*) AS c FROM copros"),
      db.get<{ c: number }>("SELECT COUNT(*) AS c FROM prospects"),
      db.get<{ c: number }>(
        "SELECT COUNT(*) AS c FROM prospects WHERE stage='won'",
      ),
      db.get<{ c: number }>("SELECT COUNT(*) AS c FROM dpe_estimates"),
    ]);
  const coproCount = coproCountRow?.c ?? 0;
  const prospectCount = prospectCountRow?.c ?? 0;
  const wonCount = wonCountRow?.c ?? 0;
  const dpeEstimated = dpeEstimatedRow?.c ?? 0;

  const byStage = await db.all<{ stage: string; c: number; total: number }>(
    "SELECT stage, COUNT(*) AS c, COALESCE(SUM(estimated_value), 0) AS total FROM prospects GROUP BY stage",
  );

  const stageMap = new Map(byStage.map((r) => [r.stage, r]));
  const totalPipeline = byStage
    .filter((s) => s.stage !== "lost")
    .reduce((s, x) => s + x.total, 0);

  const upcoming = await db.all<{
    id: number;
    next_action_at: number;
    next_action_label: string | null;
    nom_copro: string | null;
    adresse: string | null;
    commune: string | null;
    classe_finale: string | null;
  }>(
    `SELECT p.id, p.next_action_at, p.next_action_label,
            c.nom_copro, c.adresse, c.commune, e.classe_finale
     FROM prospects p
     LEFT JOIN copros c ON c.id = p.copro_id
     LEFT JOIN dpe_estimates e ON e.copro_id = p.copro_id
     WHERE p.next_action_at IS NOT NULL AND p.stage NOT IN ('won','lost')
     ORDER BY p.next_action_at ASC LIMIT 6`,
  );

  return (
    <div className="mx-auto max-w-7xl space-y-4 p-6">
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <KpiCard
          icon={<Building2 className="h-4 w-4" />}
          label="Copropriétés en base"
          value={coproCount.toLocaleString("fr-FR")}
          hint="Registre national IDF (75–95)"
        />
        <KpiCard
          icon={<Kanban className="h-4 w-4" />}
          label="Prospects actifs"
          value={prospectCount.toLocaleString("fr-FR")}
          hint={`${wonCount} signés`}
        />
        <KpiCard
          icon={<TrendingUp className="h-4 w-4" />}
          label="Pipeline en cours"
          value={formatCurrency(totalPipeline)}
          hint="Hors prospects perdus"
        />
        <KpiCard
          icon={<Activity className="h-4 w-4" />}
          label="DPE estimés"
          value={dpeEstimated.toLocaleString("fr-FR")}
          hint="Copros avec calcul ADEME"
        />
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-sm">Pipeline par étape</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            {PIPELINE_ORDER.map((s) => {
              const meta = stageMeta(s);
              const data = stageMap.get(s);
              const count = data?.c ?? 0;
              const pct = prospectCount > 0 ? (count / prospectCount) * 100 : 0;
              return (
                <div key={s} className="flex items-center gap-3">
                  <span className="w-28 text-xs font-medium">{meta.label}</span>
                  <div className="relative h-6 flex-1 overflow-hidden rounded-full bg-secondary">
                    <div
                      className="h-full transition-all"
                      style={{
                        width: `${Math.max(pct, count > 0 ? 4 : 0)}%`,
                        background: meta.color,
                      }}
                    />
                  </div>
                  <span className="w-12 text-right text-xs font-bold">
                    {count}
                  </span>
                  <span className="w-24 text-right text-xs text-muted-foreground">
                    {formatCurrency(data?.total ?? 0)}
                  </span>
                </div>
              );
            })}
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-sm">Prochaines relances</CardTitle>
        </CardHeader>
        <CardContent>
          {upcoming.length === 0 ? (
            <p className="text-xs text-muted-foreground">
              Aucune relance programmée. Programmez-en depuis une fiche prospect.
            </p>
          ) : (
            <ul className="space-y-2">
              {upcoming.map((u) => (
                <li
                  key={u.id}
                  className="flex items-center justify-between rounded-lg border border-border p-3"
                >
                  <div>
                    <Link
                      href={`/prospects/${u.id}`}
                      className="text-sm font-semibold hover:text-primary"
                    >
                      {u.nom_copro || u.adresse || `Prospect #${u.id}`}
                    </Link>
                    <p className="text-[11px] text-muted-foreground">
                      {u.commune} · {u.next_action_label ?? "À relancer"}
                    </p>
                  </div>
                  <div className="flex items-center gap-2">
                    <DpeBadge classe={u.classe_finale} size="sm" />
                    <span className="flex items-center gap-1 text-[11px] text-muted-foreground">
                      <Calendar className="h-3 w-3" />
                      {new Date(u.next_action_at * 1000).toLocaleDateString("fr-FR")}
                    </span>
                  </div>
                </li>
              ))}
            </ul>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function KpiCard({
  icon,
  label,
  value,
  hint,
}: {
  icon: React.ReactNode;
  label: string;
  value: string;
  hint?: string;
}) {
  return (
    <Card>
      <CardContent className="flex flex-col gap-1 p-5">
        <div className="flex items-center gap-2 text-xs uppercase tracking-wider text-muted-foreground">
          {icon}
          {label}
        </div>
        <div className="text-2xl font-black tracking-tight">{value}</div>
        {hint ? (
          <p className="text-[11px] text-muted-foreground">{hint}</p>
        ) : null}
      </CardContent>
    </Card>
  );
}
