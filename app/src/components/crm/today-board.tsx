"use client";
import { useEffect, useState } from "react";
import Link from "next/link";
import {
  AlertCircle,
  Calendar,
  CheckCircle2,
  Clock,
  Flame,
  Loader2,
  TrendingUp,
} from "lucide-react";
import { DpeBadge } from "@/components/ui/dpe-badge";
import { formatCurrency, formatDate, stageMeta, PIPELINE_ORDER, type PipelineStageKey } from "@/lib/utils";
import { toast } from "sonner";

type Prospect = {
  id: number;
  stage: PipelineStageKey;
  priority: number;
  estimated_value: number | null;
  next_action_at: number | null;
  next_action_label: string | null;
  custom_label: string | null;
  custom_address: string | null;
  nom_copro: string | null;
  adresse: string | null;
  code_postal: string | null;
  commune: string | null;
  syndic: string | null;
  classe_finale: string | null;
};

type Task = {
  id: number;
  title: string;
  kind: string | null;
  due_at: number;
  done_at: number | null;
  prospect_id: number | null;
  custom_label: string | null;
  nom_copro: string | null;
  adresse: string | null;
};

type ByStage = { stage: PipelineStageKey; n: number; total: number };

type Today = {
  user: { id: number; name: string | null; email: string };
  now: number;
  overdue: Prospect[];
  today: Prospect[];
  thisWeek: Prospect[];
  byStage: ByStage[];
  tasksToday: Task[];
  tasksOverdue: Task[];
};

export function TodayBoard() {
  const [data, setData] = useState<Today | null>(null);
  const [loading, setLoading] = useState(true);

  const load = async () => {
    setLoading(true);
    try {
      const r = await fetch("/api/today");
      if (r.ok) setData(await r.json());
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void load();
  }, []);

  const markTaskDone = async (taskId: number) => {
    const r = await fetch(`/api/tasks/${taskId}`, {
      method: "PATCH",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ done: true }),
    });
    if (r.ok) {
      toast.success("Tâche terminée");
      await load();
    } else toast.error("Erreur");
  };

  if (loading || !data) {
    return (
      <div className="flex h-64 items-center justify-center">
        <Loader2 className="h-6 w-6 animate-spin text-primary" />
      </div>
    );
  }

  const totalPipeline = data.byStage
    .filter((s) => s.stage !== "won" && s.stage !== "lost")
    .reduce((acc, s) => acc + s.total, 0);
  const wonTotal = data.byStage.find((s) => s.stage === "won")?.total ?? 0;
  const wonCount = data.byStage.find((s) => s.stage === "won")?.n ?? 0;

  return (
    <div className="space-y-6">
      {/* KPIs en tête */}
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <Kpi
          icon={<AlertCircle className="h-4 w-4" />}
          color="red"
          label="En retard"
          value={`${data.overdue.length + data.tasksOverdue.length}`}
          hint="prospects/tâches"
        />
        <Kpi
          icon={<Calendar className="h-4 w-4" />}
          color="blue"
          label="Aujourd'hui"
          value={`${data.today.length + data.tasksToday.length}`}
          hint="à traiter"
        />
        <Kpi
          icon={<TrendingUp className="h-4 w-4" />}
          color="emerald"
          label="Pipeline"
          value={formatCurrency(totalPipeline)}
          hint={`${data.byStage.filter((s) => s.stage !== "won" && s.stage !== "lost").reduce((a, b) => a + b.n, 0)} deals`}
        />
        <Kpi
          icon={<CheckCircle2 className="h-4 w-4" />}
          color="primary"
          label="Signés"
          value={formatCurrency(wonTotal)}
          hint={`${wonCount} deals`}
        />
      </div>

      {/* En retard — priorité 1 */}
      {data.overdue.length > 0 || data.tasksOverdue.length > 0 ? (
        <Section
          title="🔥 En retard"
          tone="red"
          subtitle="À traiter en priorité — ces prospects/tâches ont dépassé leur deadline"
        >
          {data.tasksOverdue.map((t) => (
            <TaskCard key={`t-${t.id}`} task={t} overdue onDone={() => markTaskDone(t.id)} />
          ))}
          {data.overdue.map((p) => (
            <ProspectCard key={p.id} prospect={p} overdue />
          ))}
        </Section>
      ) : null}

      {/* Aujourd'hui */}
      <Section
        title="📅 Aujourd'hui"
        tone="blue"
        subtitle="Tes actions prévues pour aujourd'hui"
      >
        {data.tasksToday.length === 0 && data.today.length === 0 ? (
          <p className="rounded-lg bg-card p-6 text-center text-sm text-muted-foreground">
            Rien de prévu aujourd'hui. Profite-en pour relancer un prospect 💪
          </p>
        ) : (
          <>
            {data.tasksToday.map((t) => (
              <TaskCard key={`t-${t.id}`} task={t} onDone={() => markTaskDone(t.id)} />
            ))}
            {data.today.map((p) => (
              <ProspectCard key={p.id} prospect={p} />
            ))}
          </>
        )}
      </Section>

      {/* Cette semaine */}
      {data.thisWeek.length > 0 ? (
        <Section
          title="📆 Cette semaine"
          tone="neutral"
          subtitle="Prochains 7 jours — anticipe pour pas être en retard"
        >
          {data.thisWeek.map((p) => (
            <ProspectCard key={p.id} prospect={p} />
          ))}
        </Section>
      ) : null}

      {/* Pipeline par étape */}
      <div className="rounded-xl border border-border bg-card p-5">
        <h2 className="mb-3 text-sm font-bold uppercase tracking-wider text-muted-foreground">
          Mon pipeline par étape
        </h2>
        <div className="grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-5">
          {PIPELINE_ORDER.filter((s) => s !== "lost").map((stage) => {
            const row = data.byStage.find((x) => x.stage === stage);
            const meta = stageMeta(stage);
            return (
              <Link
                key={stage}
                href={`/prospects?stage=${stage}`}
                className="rounded-lg border border-border bg-background p-3 transition-colors hover:border-primary/40 hover:bg-secondary/30"
              >
                <div className="flex items-center gap-1.5 text-[10px] uppercase tracking-wider text-muted-foreground">
                  <span
                    className="inline-block h-2 w-2 rounded-full"
                    style={{ background: meta.color }}
                  />
                  {meta.label}
                </div>
                <div className="mt-1 text-lg font-black">{row?.n ?? 0}</div>
                <div className="text-[11px] text-muted-foreground">
                  {formatCurrency(row?.total ?? 0)}
                </div>
              </Link>
            );
          })}
        </div>
      </div>
    </div>
  );
}

function Kpi({
  icon,
  label,
  value,
  hint,
  color,
}: {
  icon: React.ReactNode;
  label: string;
  value: string;
  hint: string;
  color: "red" | "blue" | "emerald" | "primary";
}) {
  const colorMap = {
    red: "from-red-500/10 text-red-700",
    blue: "from-blue-500/10 text-blue-700",
    emerald: "from-emerald-500/10 text-emerald-700",
    primary: "from-primary/10 text-primary",
  };
  return (
    <div className={`rounded-xl border border-border bg-gradient-to-br ${colorMap[color]} to-card p-4`}>
      <div className="flex items-center gap-2 text-[10px] font-semibold uppercase tracking-wider">
        {icon}
        {label}
      </div>
      <div className="mt-1 text-2xl font-black text-foreground">{value}</div>
      <div className="text-[10px] text-muted-foreground">{hint}</div>
    </div>
  );
}

function Section({
  title,
  subtitle,
  tone,
  children,
}: {
  title: string;
  subtitle: string;
  tone: "red" | "blue" | "neutral";
  children: React.ReactNode;
}) {
  const border =
    tone === "red"
      ? "border-l-4 border-l-red-500"
      : tone === "blue"
        ? "border-l-4 border-l-blue-500"
        : "";
  return (
    <section className={`rounded-xl border border-border bg-card/40 p-4 ${border}`}>
      <div className="mb-3">
        <h2 className="text-sm font-bold">{title}</h2>
        <p className="text-[11px] text-muted-foreground">{subtitle}</p>
      </div>
      <div className="space-y-2">{children}</div>
    </section>
  );
}

function ProspectCard({ prospect: p, overdue }: { prospect: Prospect; overdue?: boolean }) {
  const title = p.nom_copro || p.custom_label || p.adresse || `Prospect #${p.id}`;
  const subtitle = p.nom_copro
    ? `${p.adresse ?? ""}${p.adresse ? " · " : ""}${p.code_postal ?? ""} ${p.commune ?? ""}`
    : p.custom_address ?? "";
  const meta = stageMeta(p.stage);
  return (
    <Link
      href={`/prospects/${p.id}`}
      className="flex items-start gap-3 rounded-lg border border-border bg-card p-3 transition-colors hover:border-primary/40 hover:bg-secondary/30"
    >
      <span
        className="mt-1 inline-block h-2 w-2 shrink-0 rounded-full"
        style={{ background: meta.color }}
        title={meta.label}
      />
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-2">
          <p className="truncate text-sm font-semibold">{title}</p>
          {p.classe_finale ? <DpeBadge classe={p.classe_finale} size="sm" /> : null}
          {p.priority === 1 ? (
            <Flame className="h-3 w-3 text-red-500" />
          ) : null}
        </div>
        <p className="truncate text-[11px] text-muted-foreground">{subtitle}</p>
        {p.next_action_at ? (
          <p className={`mt-1 flex items-center gap-1 text-[11px] ${overdue ? "font-bold text-red-700" : "text-muted-foreground"}`}>
            <Clock className="h-3 w-3" />
            {p.next_action_label ?? "Relance"} · {formatDate(p.next_action_at)}
          </p>
        ) : null}
      </div>
      <span
        className="shrink-0 rounded-full px-2 py-0.5 text-[10px] font-bold text-white"
        style={{ background: meta.color }}
      >
        {meta.label}
      </span>
      {p.estimated_value ? (
        <div className="shrink-0 text-right text-xs font-semibold">
          {formatCurrency(p.estimated_value)}
        </div>
      ) : null}
    </Link>
  );
}

function TaskCard({
  task: t,
  overdue,
  onDone,
}: {
  task: Task;
  overdue?: boolean;
  onDone: () => void;
}) {
  const ctx = t.nom_copro || t.custom_label || (t.prospect_id ? `Prospect #${t.prospect_id}` : "Standalone");
  return (
    <div className="flex items-center gap-3 rounded-lg border border-border bg-card p-3">
      <button
        onClick={onDone}
        className="flex h-5 w-5 shrink-0 items-center justify-center rounded-full border-2 border-border bg-background hover:border-emerald-500 hover:bg-emerald-50"
        title="Marquer comme fait"
      >
        <CheckCircle2 className="h-3 w-3 text-emerald-600 opacity-0 hover:opacity-100" />
      </button>
      <div className="min-w-0 flex-1">
        <p className="truncate text-sm font-medium">{t.title}</p>
        <p className="truncate text-[11px] text-muted-foreground">
          {t.kind ? `${t.kind} · ` : ""}
          {ctx}
        </p>
      </div>
      <span className={`shrink-0 text-[11px] ${overdue ? "font-bold text-red-700" : "text-muted-foreground"}`}>
        {formatDate(t.due_at)}
      </span>
    </div>
  );
}
