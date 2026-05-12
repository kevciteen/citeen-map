"use client";
import { useEffect, useState } from "react";
import Link from "next/link";
import { Building2, Calendar, MapPin, AlertCircle, User, Users } from "lucide-react";
import { PIPELINE_ORDER, stageMeta, formatCurrency, type PipelineStageKey } from "@/lib/utils";
import { DpeBadge } from "@/components/ui/dpe-badge";
import { toast } from "sonner";
import { cn } from "@/lib/utils";

type ProspectRow = {
  id: number;
  copro_id: number | null;
  custom_label: string | null;
  custom_address: string | null;
  stage: PipelineStageKey;
  priority: number;
  estimated_value: number | null;
  next_action_at: number | null;
  next_action_label: string | null;
  assigned_to: string | null;
  assigned_user_id: number | null;
  assigned_user_name: string | null;
  assigned_user_email: string | null;
  tags: string | null;
  nom_copro: string | null;
  adresse: string | null;
  code_postal: string | null;
  commune: string | null;
  syndic: string | null;
  classe_finale: string | null;
};

export function KanbanBoard() {
  const [rows, setRows] = useState<ProspectRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [dragId, setDragId] = useState<number | null>(null);
  const [mineOnly, setMineOnly] = useState(false);

  const load = async () => {
    setLoading(true);
    const url = mineOnly ? "/api/prospects?mine=1" : "/api/prospects";
    const r = await fetch(url);
    const j = await r.json();
    setRows(j.items ?? []);
    setLoading(false);
  };

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mineOnly]);

  const onDragStart = (id: number) => setDragId(id);
  const onDragEnd = () => setDragId(null);
  const onDrop = async (stage: PipelineStageKey) => {
    if (dragId == null) return;
    const id = dragId;
    setDragId(null);
    setRows((rs) => rs.map((r) => (r.id === id ? { ...r, stage } : r)));
    const r = await fetch(`/api/prospects/${id}`, {
      method: "PATCH",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ stage }),
    });
    if (!r.ok) {
      toast.error("Erreur lors du déplacement");
      load();
    }
  };

  const grouped = PIPELINE_ORDER.map((s) => ({
    stage: s,
    items: rows.filter((r) => r.stage === s),
  }));

  const totalPipeline = rows
    .filter((r) => r.stage !== "won" && r.stage !== "lost")
    .reduce((s, x) => s + (x.estimated_value ?? 0), 0);

  return (
    <div className="flex h-full flex-col">
      {/* Toolbar : filtre "mes prospects" + total pipeline */}
      <div className="flex items-center justify-between gap-3 border-b border-border bg-card/30 px-4 py-2 text-xs">
        <div className="flex items-center gap-1 rounded-lg border border-border bg-background p-0.5">
          <button
            onClick={() => setMineOnly(false)}
            className={cn(
              "flex items-center gap-1.5 rounded-md px-2.5 py-1 font-semibold transition-colors",
              !mineOnly
                ? "bg-primary text-primary-foreground shadow-sm"
                : "text-muted-foreground hover:bg-secondary",
            )}
          >
            <Users className="h-3 w-3" />
            Toute l&apos;équipe
          </button>
          <button
            onClick={() => setMineOnly(true)}
            className={cn(
              "flex items-center gap-1.5 rounded-md px-2.5 py-1 font-semibold transition-colors",
              mineOnly
                ? "bg-primary text-primary-foreground shadow-sm"
                : "text-muted-foreground hover:bg-secondary",
            )}
          >
            <User className="h-3 w-3" />
            Mes prospects
          </button>
        </div>
        <div className="flex items-center gap-3 text-muted-foreground">
          <span>
            <strong className="text-foreground">{rows.length}</strong> prospects affichés
          </span>
          <span>·</span>
          <span>
            Pipeline ouvert :{" "}
            <strong className="text-emerald-700">{formatCurrency(totalPipeline)}</strong>
          </span>
        </div>
      </div>
      <div className="flex h-full gap-3 overflow-x-auto p-4">
        {grouped.map(({ stage, items }) => {
          const meta = stageMeta(stage);
          const totalValue = items.reduce((s, x) => s + (x.estimated_value ?? 0), 0);
          return (
            <div
              key={stage}
              className="flex h-full w-[300px] shrink-0 flex-col"
              onDragOver={(e) => {
                e.preventDefault();
              }}
              onDrop={() => onDrop(stage as PipelineStageKey)}
            >
              <div
                className="mb-2 flex items-center justify-between rounded-lg border border-border bg-card px-3 py-2 shadow-sm"
                style={{ borderTopColor: meta.color, borderTopWidth: 3 }}
              >
                <div className="flex items-center gap-2">
                  <span
                    className="inline-block h-2 w-2 rounded-full"
                    style={{ background: meta.color }}
                  />
                  <span className="text-sm font-bold">{meta.label}</span>
                  <span className="rounded-full bg-secondary px-1.5 py-0.5 text-[10px] font-semibold text-muted-foreground">
                    {items.length}
                  </span>
                </div>
                {totalValue > 0 ? (
                  <span className="text-[10px] font-semibold text-muted-foreground">
                    {formatCurrency(totalValue)}
                  </span>
                ) : null}
              </div>

              <div className="flex-1 space-y-2 overflow-y-auto pb-4">
                {loading ? (
                  <div className="rounded-lg border border-dashed border-border p-8 text-center text-xs text-muted-foreground">
                    Chargement…
                  </div>
                ) : items.length === 0 ? (
                  <div className="rounded-lg border border-dashed border-border p-8 text-center text-xs text-muted-foreground">
                    Glissez une carte ici
                  </div>
                ) : (
                  items.map((p) => (
                    <ProspectCard
                      key={p.id}
                      prospect={p}
                      isDragging={dragId === p.id}
                      onDragStart={() => onDragStart(p.id)}
                      onDragEnd={onDragEnd}
                    />
                  ))
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function ProspectCard({
  prospect: p,
  isDragging,
  onDragStart,
  onDragEnd,
}: {
  prospect: ProspectRow;
  isDragging: boolean;
  onDragStart: () => void;
  onDragEnd: () => void;
}) {
  const title =
    p.nom_copro ||
    p.adresse ||
    p.custom_label ||
    p.custom_address ||
    `Prospect #${p.id}`;
  const subtitle =
    p.code_postal || p.commune
      ? `${p.code_postal ?? ""} ${p.commune ?? ""}`.trim()
      : null;
  const overdue =
    p.next_action_at && p.next_action_at * 1000 < Date.now() ? true : false;

  return (
    <Link
      href={`/prospects/${p.id}`}
      draggable
      onDragStart={onDragStart}
      onDragEnd={onDragEnd}
      className={cn(
        "block cursor-grab rounded-lg border border-border bg-card p-3 shadow-sm transition-all hover:border-primary/50 hover:shadow-md active:cursor-grabbing",
        isDragging && "opacity-50",
      )}
    >
      <div className="flex items-start justify-between gap-2">
        <div className="flex items-start gap-2">
          <Building2 className="mt-0.5 h-3.5 w-3.5 shrink-0 text-primary" />
          <h4 className="text-xs font-bold leading-tight">{title}</h4>
        </div>
        <DpeBadge classe={p.classe_finale} size="sm" />
      </div>
      {subtitle ? (
        <p className="mt-1 flex items-center gap-1 text-[11px] text-muted-foreground">
          <MapPin className="h-3 w-3" />
          {subtitle}
        </p>
      ) : null}
      {p.syndic ? (
        <p className="mt-1 truncate text-[10px] text-muted-foreground">
          Syndic · {p.syndic}
        </p>
      ) : null}

      <div className="mt-2 flex items-center justify-between text-[10px]">
        {p.estimated_value ? (
          <span className="font-semibold text-emerald-700">
            {formatCurrency(p.estimated_value)}
          </span>
        ) : (
          <span className="text-muted-foreground">—</span>
        )}
        {p.next_action_at ? (
          <span
            className={cn(
              "flex items-center gap-1",
              overdue ? "text-destructive" : "text-muted-foreground",
            )}
          >
            {overdue ? <AlertCircle className="h-3 w-3" /> : <Calendar className="h-3 w-3" />}
            {new Date(p.next_action_at * 1000).toLocaleDateString("fr-FR", {
              day: "2-digit",
              month: "short",
            })}
          </span>
        ) : null}
      </div>
    </Link>
  );
}
