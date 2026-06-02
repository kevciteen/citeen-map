"use client";
/**
 * Cockpit pilotage — page unique opinionated pour la prospection rénovation
 * énergétique copropriété.
 *
 * 6 sections en grille, layout fixe :
 *  1. KPIs (4 cards essentielles)
 *  2. À faire aujourd'hui (next_actions <= today)
 *  3. Kanban compact prospects actifs
 *  4. Top syndics à relancer (heuristique F/G non-pipeline)
 *  5. Activité récente cross-entité
 *  6. Mini-carte des prospects actifs
 */
import { useQuery } from "@tanstack/react-query";
import Link from "next/link";
import {
  Activity, AlertCircle, Calendar, ChevronRight, Flame,
  Kanban, Loader2, MapPin, Phone, Sparkles, StickyNote,
  Tag as TagIcon, TrendingUp, Users,
} from "lucide-react";
import { jsonFetcher } from "@/lib/fetcher";
import { Skeleton } from "@/components/ui/skeleton";
import { DpeBadge } from "@/components/ui/dpe-badge";
import { stageMeta, formatCurrency, PIPELINE_ORDER } from "@/lib/utils";
import {
  AnnuaireMap, DPE_COLORS, type AnnuaireMapPoint,
} from "@/components/annuaire/annuaire-map";

type Stage = "lead" | "to_contact" | "contacted" | "meeting" | "proposal" | "won" | "lost";

type CockpitData = {
  kpis: {
    passoiresActives: number;
    relancesJour: number;
    pipelineValue: number;
    tauxConversion: number;
  };
  tasks: Array<{
    id: number;
    label: string | null;
    stage: Stage;
    next_action_at: number | null;
    next_action_label: string | null;
    copro_id: number | null;
    copro_nom: string | null;
    copro_adresse: string | null;
    copro_commune: string | null;
    classe_finale: string | null;
  }>;
  kanban: {
    counts: Array<{ stage: Stage; n: number }>;
    samples: Array<{ id: number; stage: Stage; label: string | null; classe_finale: string | null }>;
  };
  topSyndics: Array<{ syndic: string; nb_fg: number; nb_pipeline: number; potentiel: number }>;
  recentActivity: Array<{
    kind: "note" | "tag";
    entity_type: string;
    entity_ref: string;
    body_or_tag: string;
    created_at: number;
  }>;
  prospectsMap: Array<{
    id: number;
    lat: number;
    lon: number;
    stage: Stage;
    classe_finale: string | null;
  }>;
};

export function CockpitView() {
  const { data, isPending } = useQuery({
    queryKey: ["cockpit"],
    queryFn: ({ signal }) => jsonFetcher<CockpitData>("/api/cockpit", signal),
    staleTime: 30 * 1000,
    refetchInterval: 60 * 1000,
  });

  return (
    <div className="mx-auto max-w-7xl space-y-4 p-6">
      {/* ============= KPIs ============= */}
      <section className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <KpiCard
          label="Passoires actives"
          value={data?.kpis.passoiresActives}
          hint="F+G en pipeline"
          icon={Flame}
          accent="rose"
          loading={isPending}
        />
        <KpiCard
          label="Relances dues"
          value={data?.kpis.relancesJour}
          hint="aujourd'hui ou en retard"
          icon={Phone}
          accent="amber"
          loading={isPending}
        />
        <KpiCard
          label="Pipeline value"
          value={data?.kpis.pipelineValue}
          format="currency"
          hint="propositions + RDV"
          icon={TrendingUp}
          accent="emerald"
          loading={isPending}
        />
        <KpiCard
          label="Taux conversion"
          value={data?.kpis.tauxConversion}
          format="percent"
          hint="won / non-lead"
          icon={Sparkles}
          accent="primary"
          loading={isPending}
        />
      </section>

      {/* ============= À FAIRE AUJOURD'HUI ============= */}
      <Section
        title="À faire aujourd'hui"
        icon={Calendar}
        right={
          data ? (
            <span className="text-xs text-muted-foreground">
              {data.tasks.length} tâche{data.tasks.length > 1 ? "s" : ""}
            </span>
          ) : null
        }
      >
        {isPending ? (
          <Skeleton className="h-24 w-full" />
        ) : data && data.tasks.length === 0 ? (
          <p className="rounded-md border border-border bg-secondary/30 p-4 text-center text-xs text-muted-foreground">
            Aucune tâche pour aujourd&apos;hui ✨
          </p>
        ) : (
          <ul className="space-y-1.5">
            {data?.tasks.map((t) => (
              <li
                key={t.id}
                className="flex items-center gap-2 rounded-md border border-border bg-card p-2 text-xs hover:bg-secondary/30"
              >
                <div className="shrink-0 rounded bg-amber-100 p-1 text-amber-900">
                  <Phone className="h-3 w-3" />
                </div>
                <div className="min-w-0 flex-1">
                  <Link
                    href={`/prospects/${t.id}`}
                    className="block truncate font-medium hover:text-primary"
                  >
                    {t.copro_nom || t.copro_adresse || t.label || `Prospect #${t.id}`}
                  </Link>
                  <p className="truncate text-[10px] text-muted-foreground">
                    {t.next_action_label || "À relancer"}
                    {t.copro_commune ? ` · ${t.copro_commune}` : ""}
                  </p>
                </div>
                {t.classe_finale ? (
                  <DpeBadge classe={t.classe_finale} className="!h-5 !min-w-[20px] !text-[10px]" />
                ) : null}
                <span
                  className="shrink-0 rounded px-1.5 py-0.5 text-[9px] uppercase tracking-wider"
                  style={{
                    background: stageMeta(t.stage).color,
                    color: "white",
                  }}
                >
                  {stageMeta(t.stage).label}
                </span>
                <ChevronRight className="h-3 w-3 text-muted-foreground" />
              </li>
            ))}
          </ul>
        )}
      </Section>

      {/* ============= KANBAN COMPACT ============= */}
      <Section
        title="Pipeline (vue compacte)"
        icon={Kanban}
        right={
          <Link
            href="/pilotage-legacy?tab=pipeline"
            className="text-xs text-primary hover:underline"
          >
            Vue kanban complète →
          </Link>
        }
      >
        {isPending ? (
          <div className="grid grid-cols-5 gap-2">
            <Skeleton className="h-32" />
            <Skeleton className="h-32" />
            <Skeleton className="h-32" />
            <Skeleton className="h-32" />
            <Skeleton className="h-32" />
          </div>
        ) : (
          <div className="grid grid-cols-2 gap-2 sm:grid-cols-5">
            {PIPELINE_ORDER.filter((s) => s !== "won" && s !== "lost").map((stage) => {
              const count = data?.kanban.counts.find((c) => c.stage === stage)?.n ?? 0;
              const samples = data?.kanban.samples.filter((s) => s.stage === stage) ?? [];
              return (
                <div
                  key={stage}
                  className="rounded-lg border border-border bg-card p-2"
                  style={{ borderTopWidth: 3, borderTopColor: stageMeta(stage).color }}
                >
                  <div className="mb-1.5 flex items-center justify-between gap-1">
                    <p className="truncate text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
                      {stageMeta(stage).label}
                    </p>
                    <span className="rounded bg-secondary px-1.5 py-0.5 text-[10px] font-bold">
                      {count}
                    </span>
                  </div>
                  <ul className="space-y-0.5">
                    {samples.slice(0, 3).map((s) => (
                      <li key={s.id}>
                        <Link
                          href={`/prospects/${s.id}`}
                          className="flex items-center gap-1 rounded p-1 text-[10px] hover:bg-secondary"
                        >
                          {s.classe_finale ? (
                            <span
                              className="inline-block h-1.5 w-1.5 shrink-0 rounded-full"
                              style={{ background: DPE_COLORS[s.classe_finale] ?? "#94a3b8" }}
                            />
                          ) : null}
                          <span className="truncate">{s.label ?? `#${s.id}`}</span>
                        </Link>
                      </li>
                    ))}
                    {count > 3 ? (
                      <li className="px-1 text-[9px] text-muted-foreground">
                        + {count - 3} autres
                      </li>
                    ) : null}
                  </ul>
                </div>
              );
            })}
          </div>
        )}
      </Section>

      {/* ============= TOP SYNDICS À RELANCER + ACTIVITÉ RÉCENTE ============= */}
      <div className="grid gap-4 lg:grid-cols-2">
        <Section title="Syndics à attaquer" icon={Users} subtitle="Forte concentration F/G non encore travaillée">
          {isPending ? (
            <Skeleton className="h-48 w-full" />
          ) : data && data.topSyndics.length > 0 ? (
            <ul className="space-y-1.5">
              {data.topSyndics.map((s) => (
                <li key={s.syndic} className="flex items-center gap-2 rounded-md border border-border bg-card p-2 text-xs">
                  <div className="shrink-0 rounded-full bg-rose-100 p-1 text-rose-900">
                    <Flame className="h-3 w-3" />
                  </div>
                  <div className="min-w-0 flex-1">
                    <Link
                      href={`/syndics/${slugify(s.syndic)}?name=${encodeURIComponent(s.syndic)}`}
                      className="block truncate font-semibold hover:text-primary"
                    >
                      {s.syndic}
                    </Link>
                    <p className="text-[10px] text-muted-foreground">
                      {s.nb_fg} copros F/G · {s.nb_pipeline} déjà en pipeline
                    </p>
                  </div>
                  <span className="shrink-0 rounded-full bg-emerald-100 px-2 py-0.5 text-[10px] font-bold text-emerald-900">
                    +{s.potentiel} potentiel
                  </span>
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-xs text-muted-foreground">
              Aucun syndic identifié — ajoute des copros via le registre.
            </p>
          )}
        </Section>

        <Section title="Activité récente" icon={Activity} subtitle="Notes + tags des 7 derniers jours">
          {isPending ? (
            <Skeleton className="h-48 w-full" />
          ) : data && data.recentActivity.length > 0 ? (
            <ul className="space-y-1.5">
              {data.recentActivity.map((a, i) => (
                <li key={i} className="flex items-start gap-2 rounded-md border border-border bg-card p-2 text-xs">
                  <div
                    className={
                      "shrink-0 rounded p-1 " +
                      (a.kind === "note"
                        ? "bg-blue-100 text-blue-900"
                        : "bg-purple-100 text-purple-900")
                    }
                  >
                    {a.kind === "note" ? <StickyNote className="h-3 w-3" /> : <TagIcon className="h-3 w-3" />}
                  </div>
                  <div className="min-w-0 flex-1">
                    <p className="line-clamp-2 text-[11px]">{a.body_or_tag}</p>
                    <p className="text-[10px] text-muted-foreground">
                      {a.entity_type} · {a.entity_ref}{" "}
                      <span className="opacity-70">
                        · {new Date(a.created_at * 1000).toLocaleString("fr-FR", { dateStyle: "short", timeStyle: "short" })}
                      </span>
                    </p>
                  </div>
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-xs text-muted-foreground">
              Aucune activité récente.
            </p>
          )}
        </Section>
      </div>

      {/* ============= MINI-CARTE PROSPECTS ============= */}
      <Section title="Prospects actifs sur la carte" icon={MapPin} subtitle="Coloration par classe DPE">
        {isPending ? (
          <Skeleton className="h-72 w-full" />
        ) : data && data.prospectsMap.length > 0 ? (
          <div className="space-y-2">
            <AnnuaireMap
              colorMode="dpe"
              points={data.prospectsMap.map((p): AnnuaireMapPoint => ({
                id: p.id,
                entity_type: "copro",
                entity_ref: String(p.id),
                display_name: `Prospect #${p.id}`,
                display_subtitle: stageMeta(p.stage).label,
                lat: p.lat,
                lon: p.lon,
                phone: null,
                email: null,
                website: null,
                dpe_class: p.classe_finale,
              }))}
            />
            <p className="text-[11px] text-muted-foreground">
              {data.prospectsMap.length} prospects affichés (max 500).
            </p>
          </div>
        ) : (
          <p className="rounded-md border border-border bg-secondary/30 p-4 text-center text-xs text-muted-foreground">
            Aucun prospect actif avec coordonnées.
          </p>
        )}
      </Section>
    </div>
  );
}

/* ============================== SOUS-COMPOSANTS ============================== */

function KpiCard({
  label, value, hint, icon: Icon, accent, loading, format,
}: {
  label: string;
  value: number | undefined;
  hint?: string;
  icon: React.ComponentType<{ className?: string }>;
  accent: "rose" | "amber" | "emerald" | "primary";
  loading: boolean;
  format?: "currency" | "percent";
}) {
  const accentClass = {
    rose: "border-rose-200 bg-rose-50",
    amber: "border-amber-200 bg-amber-50",
    emerald: "border-emerald-200 bg-emerald-50",
    primary: "border-primary/30 bg-primary/5",
  }[accent];
  const iconClass = {
    rose: "bg-rose-200 text-rose-900",
    amber: "bg-amber-200 text-amber-900",
    emerald: "bg-emerald-200 text-emerald-900",
    primary: "bg-primary/20 text-primary",
  }[accent];

  const displayValue = (() => {
    if (loading || value === undefined) return null;
    if (format === "currency") return formatCurrency(value);
    if (format === "percent") return `${value.toFixed(0)} %`;
    return value.toLocaleString("fr-FR");
  })();

  return (
    <div className={`flex items-center gap-3 rounded-xl border ${accentClass} p-3 shadow-sm`}>
      <div className={`rounded-lg p-2 ${iconClass}`}>
        <Icon className="h-4 w-4" />
      </div>
      <div className="min-w-0 flex-1">
        <p className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</p>
        {loading ? (
          <Skeleton className="h-7 w-20" />
        ) : (
          <p className="text-2xl font-black tracking-tight">{displayValue}</p>
        )}
        {hint ? <p className="text-[10px] text-muted-foreground">{hint}</p> : null}
      </div>
    </div>
  );
}

function Section({
  title, subtitle, icon: Icon, right, children,
}: {
  title: string;
  subtitle?: string;
  icon: React.ComponentType<{ className?: string }>;
  right?: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <section className="rounded-xl border border-border bg-card p-4 shadow-sm">
      <div className="mb-3 flex items-start justify-between gap-2">
        <div className="flex items-start gap-2">
          <Icon className="mt-0.5 h-4 w-4 shrink-0 text-primary" />
          <div>
            <h2 className="text-sm font-bold">{title}</h2>
            {subtitle ? (
              <p className="text-[11px] text-muted-foreground">{subtitle}</p>
            ) : null}
          </div>
        </div>
        {right}
      </div>
      {children}
    </section>
  );
}

function slugify(name: string): string {
  return name
    .trim()
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80);
}
