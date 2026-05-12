"use client";
import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import {
  ArrowDown,
  ArrowUp,
  Clock,
  Flame,
  Loader2,
  User as UserIcon,
  Users,
} from "lucide-react";
import { DpeBadge } from "@/components/ui/dpe-badge";
import {
  PIPELINE_ORDER,
  stageMeta,
  formatCurrency,
  formatDate,
  type PipelineStageKey,
} from "@/lib/utils";
import { cn } from "@/lib/utils";
import { toast } from "sonner";

type Row = {
  id: number;
  copro_id: number | null;
  custom_label: string | null;
  custom_address: string | null;
  stage: PipelineStageKey;
  priority: number;
  estimated_value: number | null;
  next_action_at: number | null;
  next_action_label: string | null;
  assigned_user_id: number | null;
  assigned_user_name: string | null;
  assigned_user_email: string | null;
  updated_at: number;
  nom_copro: string | null;
  adresse: string | null;
  code_postal: string | null;
  commune: string | null;
  syndic: string | null;
  classe_finale: string | null;
};

type SortKey =
  | "name_asc"
  | "name_desc"
  | "stage_asc"
  | "value_desc"
  | "value_asc"
  | "next_action_asc"
  | "next_action_desc"
  | "updated_desc";

export function PipelineTable() {
  const [rows, setRows] = useState<Row[]>([]);
  const [loading, setLoading] = useState(true);
  const [mineOnly, setMineOnly] = useState(false);
  const [stageFilter, setStageFilter] = useState<string>("");
  const [search, setSearch] = useState("");
  const [sort, setSort] = useState<SortKey>("updated_desc");

  const load = async () => {
    setLoading(true);
    const url = mineOnly ? "/api/prospects?mine=1" : "/api/prospects";
    const r = await fetch(url);
    const j = await r.json();
    setRows(j.items ?? []);
    setLoading(false);
  };

  useEffect(() => {
    void load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mineOnly]);

  const filtered = useMemo(() => {
    let out = rows;
    if (stageFilter) out = out.filter((r) => r.stage === stageFilter);
    if (search.trim()) {
      const q = search.trim().toLowerCase();
      out = out.filter((r) => {
        return (
          (r.nom_copro ?? "").toLowerCase().includes(q) ||
          (r.adresse ?? "").toLowerCase().includes(q) ||
          (r.custom_label ?? "").toLowerCase().includes(q) ||
          (r.commune ?? "").toLowerCase().includes(q) ||
          (r.syndic ?? "").toLowerCase().includes(q)
        );
      });
    }
    return out.slice().sort((a, b) => {
      switch (sort) {
        case "name_asc":
        case "name_desc": {
          const an = (a.nom_copro ?? a.custom_label ?? a.adresse ?? "").toLowerCase();
          const bn = (b.nom_copro ?? b.custom_label ?? b.adresse ?? "").toLowerCase();
          return (sort === "name_asc" ? 1 : -1) * an.localeCompare(bn);
        }
        case "stage_asc":
          return stageMeta(a.stage).order - stageMeta(b.stage).order;
        case "value_desc":
          return (b.estimated_value ?? 0) - (a.estimated_value ?? 0);
        case "value_asc":
          return (a.estimated_value ?? 0) - (b.estimated_value ?? 0);
        case "next_action_asc":
          return (a.next_action_at ?? Infinity) - (b.next_action_at ?? Infinity);
        case "next_action_desc":
          return (b.next_action_at ?? -Infinity) - (a.next_action_at ?? -Infinity);
        case "updated_desc":
        default:
          return b.updated_at - a.updated_at;
      }
    });
  }, [rows, stageFilter, search, sort]);

  const totalValue = filtered
    .filter((r) => r.stage !== "won" && r.stage !== "lost")
    .reduce((s, r) => s + (r.estimated_value ?? 0), 0);

  const updateStage = async (id: number, stage: PipelineStageKey) => {
    setRows((rs) =>
      rs.map((r) => (r.id === id ? { ...r, stage, updated_at: Math.floor(Date.now() / 1000) } : r)),
    );
    const r = await fetch(`/api/prospects/${id}`, {
      method: "PATCH",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ stage }),
    });
    if (!r.ok) {
      toast.error("Erreur");
      void load();
    } else toast.success("Étape mise à jour");
  };

  return (
    <div className="flex h-full flex-col">
      {/* Toolbar */}
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-border bg-card/30 px-4 py-2 text-xs">
        <div className="flex flex-wrap items-center gap-2">
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
              <UserIcon className="h-3 w-3" />
              Mes prospects
            </button>
          </div>
          <select
            value={stageFilter}
            onChange={(e) => setStageFilter(e.target.value)}
            className="h-8 rounded-lg border border-input bg-background px-2 text-xs"
          >
            <option value="">Toutes les étapes</option>
            {PIPELINE_ORDER.map((s) => (
              <option key={s} value={s}>
                {stageMeta(s).label}
              </option>
            ))}
          </select>
          <input
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Rechercher copro / syndic / commune…"
            className="h-8 w-64 rounded-lg border border-input bg-background px-3 text-xs"
          />
        </div>
        <div className="flex items-center gap-3 text-muted-foreground">
          <span>
            <strong className="text-foreground">{filtered.length}</strong> sur {rows.length}
          </span>
          <span>·</span>
          <span>
            Pipeline ouvert :{" "}
            <strong className="text-emerald-700">{formatCurrency(totalValue)}</strong>
          </span>
        </div>
      </div>

      {/* Table */}
      <div className="flex-1 overflow-auto">
        {loading ? (
          <div className="flex h-32 items-center justify-center">
            <Loader2 className="h-5 w-5 animate-spin text-primary" />
          </div>
        ) : filtered.length === 0 ? (
          <p className="p-8 text-center text-sm text-muted-foreground">
            Aucun prospect ne correspond.
          </p>
        ) : (
          <table className="w-full text-sm">
            <thead className="sticky top-0 z-10 border-b border-border bg-card text-[10px] uppercase tracking-wider text-muted-foreground">
              <tr>
                <SortHeader label="Prospect" col="name_asc" alt="name_desc" sort={sort} setSort={setSort} />
                <SortHeader label="Étape" col="stage_asc" sort={sort} setSort={setSort} />
                <SortHeader label="DPE" col="updated_desc" sort={sort} setSort={setSort} sortable={false} />
                <SortHeader label="Valeur" col="value_desc" alt="value_asc" sort={sort} setSort={setSort} align="right" />
                <SortHeader label="Prochaine action" col="next_action_asc" alt="next_action_desc" sort={sort} setSort={setSort} />
                <SortHeader label="Assigné à" col="updated_desc" sort={sort} setSort={setSort} sortable={false} />
                <SortHeader label="Maj" col="updated_desc" sort={sort} setSort={setSort} align="right" />
              </tr>
            </thead>
            <tbody>
              {filtered.map((r) => (
                <RowView key={r.id} row={r} onStage={(s) => updateStage(r.id, s)} />
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

function SortHeader({
  label,
  col,
  alt,
  sort,
  setSort,
  align,
  sortable = true,
}: {
  label: string;
  col: SortKey;
  alt?: SortKey;
  sort: SortKey;
  setSort: (s: SortKey) => void;
  align?: "right";
  sortable?: boolean;
}) {
  const active = sort === col || sort === alt;
  const onClick = sortable
    ? () => setSort(sort === col && alt ? alt : col)
    : undefined;
  return (
    <th
      onClick={onClick}
      className={cn(
        "px-3 py-2 font-semibold",
        align === "right" ? "text-right" : "text-left",
        sortable ? "cursor-pointer hover:text-foreground" : "",
        active ? "text-foreground" : "",
      )}
    >
      <span className="inline-flex items-center gap-1">
        {label}
        {active && sort === col ? <ArrowDown className="h-2.5 w-2.5" /> : null}
        {active && sort === alt ? <ArrowUp className="h-2.5 w-2.5" /> : null}
      </span>
    </th>
  );
}

function RowView({ row: r, onStage }: { row: Row; onStage: (s: PipelineStageKey) => void }) {
  const title = r.nom_copro || r.custom_label || r.adresse || `Prospect #${r.id}`;
  const sub = r.nom_copro
    ? `${r.adresse ?? ""}${r.adresse ? " · " : ""}${r.code_postal ?? ""} ${r.commune ?? ""}`
    : r.custom_address ?? "";
  const meta = stageMeta(r.stage);
  const overdue =
    r.next_action_at != null && r.next_action_at < Math.floor(Date.now() / 1000);
  return (
    <tr className="border-b border-border/40 hover:bg-secondary/30">
      <td className="px-3 py-2.5">
        <div className="flex items-center gap-2">
          {r.priority === 1 ? <Flame className="h-3 w-3 shrink-0 text-red-500" /> : null}
          <div className="min-w-0">
            <Link
              href={`/prospects/${r.id}`}
              className="block truncate font-semibold hover:text-primary"
            >
              {title}
            </Link>
            <p className="truncate text-[11px] text-muted-foreground">{sub}</p>
            {r.syndic ? (
              <p className="truncate text-[10px] text-muted-foreground">
                Syndic : {r.syndic}
              </p>
            ) : null}
          </div>
        </div>
      </td>
      <td className="px-3 py-2.5">
        <select
          value={r.stage}
          onChange={(e) => onStage(e.target.value as PipelineStageKey)}
          className="rounded-md border border-border bg-background px-2 py-1 text-[11px] font-semibold"
          style={{ borderLeft: `3px solid ${meta.color}` }}
        >
          {PIPELINE_ORDER.map((s) => (
            <option key={s} value={s}>
              {stageMeta(s).label}
            </option>
          ))}
        </select>
      </td>
      <td className="px-3 py-2.5">
        {r.classe_finale ? <DpeBadge classe={r.classe_finale} size="sm" /> : "—"}
      </td>
      <td className="px-3 py-2.5 text-right font-semibold">
        {formatCurrency(r.estimated_value)}
      </td>
      <td className="px-3 py-2.5">
        {r.next_action_at ? (
          <div className={cn("flex items-center gap-1 text-[11px]", overdue && "font-bold text-red-700")}>
            <Clock className="h-3 w-3" />
            <span>{r.next_action_label ?? "Relance"}</span>
            <span className="text-muted-foreground">·</span>
            <span>{formatDate(r.next_action_at)}</span>
          </div>
        ) : (
          <span className="text-muted-foreground">—</span>
        )}
      </td>
      <td className="px-3 py-2.5 text-[11px]">
        {r.assigned_user_name || r.assigned_user_email ? (
          <span className="rounded-full bg-secondary px-2 py-0.5">
            {r.assigned_user_name ?? r.assigned_user_email}
          </span>
        ) : (
          <span className="text-muted-foreground">Non assigné</span>
        )}
      </td>
      <td className="px-3 py-2.5 text-right text-[11px] text-muted-foreground">
        {formatDate(r.updated_at)}
      </td>
    </tr>
  );
}
