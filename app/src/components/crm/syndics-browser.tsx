"use client";
import { useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import {
  Search,
  Loader2,
  ChevronLeft,
  ChevronRight,
  ChevronDown,
  ChevronUp,
  X,
  Users,
  Building2,
  MapPin,
  FileSpreadsheet,
} from "lucide-react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

type Row = {
  syndic: string;
  nb_copros: number;
  lots_total: number;
  nb_communes: number;
  nb_departements: number;
  dpe_a: number;
  dpe_b: number;
  dpe_c: number;
  dpe_d: number;
  dpe_e: number;
  dpe_f: number;
  dpe_g: number;
  dpe_nc: number;
  in_pipeline: number;
  avg_conso: number | null;
  dept_list: string | null;
};

type SortKey = "nb_copros_desc" | "nb_copros_asc" | "lots_desc" | "name_asc" | "name_desc";

const PAGE_SIZE = 30;
const DEPARTEMENTS = ["", "75", "77", "78", "91", "92", "93", "94", "95"];

const DPE_BG: Record<string, string> = {
  a: "#1f9d55",
  b: "#7cb342",
  c: "#cddc39",
  d: "#ffeb3b",
  e: "#ffb300",
  f: "#fb8c00",
  g: "#e53935",
};

export function SyndicsBrowser({ totalInDb }: { totalInDb: number }) {
  const [q, setQ] = useState("");
  const [dept, setDept] = useState("");
  const [sort, setSort] = useState<SortKey>("nb_copros_desc");
  const [page, setPage] = useState(1);

  const [rows, setRows] = useState<Row[]>([]);
  const [count, setCount] = useState(0);
  const [loading, setLoading] = useState(false);
  const seq = useRef(0);

  const qs = useMemo(() => {
    const sp = new URLSearchParams();
    if (q.trim()) sp.set("q", q.trim());
    if (dept) sp.set("dept", dept);
    if (sort) sp.set("sort", sort);
    sp.set("limit", String(PAGE_SIZE));
    sp.set("offset", String((page - 1) * PAGE_SIZE));
    return sp.toString();
  }, [q, dept, sort, page]);

  useEffect(() => {
    const t = setTimeout(async () => {
      const n = ++seq.current;
      setLoading(true);
      try {
        const r = await fetch(`/api/syndics?${qs}`);
        if (!r.ok) return;
        const j = await r.json();
        if (n !== seq.current) return;
        setRows(j.items as Row[]);
        setCount(j.total);
      } finally {
        if (n === seq.current) setLoading(false);
      }
    }, 220);
    return () => clearTimeout(t);
  }, [qs]);

  useEffect(() => setPage(1), [q, dept, sort]);

  const pages = Math.max(1, Math.ceil(count / PAGE_SIZE));

  return (
    <div className="flex h-full flex-col">
      {/* Search bar */}
      <div className="flex flex-wrap items-center gap-2 border-b border-border bg-card p-3">
        <div className="relative min-w-[260px] flex-1">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="Rechercher un syndic…"
            className="pl-9"
          />
          {loading && q ? (
            <Loader2 className="absolute right-3 top-1/2 h-4 w-4 -translate-y-1/2 animate-spin text-primary" />
          ) : null}
        </div>
        <select
          value={dept}
          onChange={(e) => setDept(e.target.value)}
          className="h-10 rounded-lg border border-input bg-background px-3 text-sm"
        >
          {DEPARTEMENTS.map((d) => (
            <option key={d} value={d}>
              {d ? `Dpt ${d}` : "Tous dpt"}
            </option>
          ))}
        </select>
        <select
          value={sort}
          onChange={(e) => setSort(e.target.value as SortKey)}
          className="h-10 rounded-lg border border-input bg-background px-3 text-sm"
        >
          <option value="nb_copros_desc">Plus de copros</option>
          <option value="nb_copros_asc">Moins de copros</option>
          <option value="lots_desc">Plus de lots</option>
          <option value="name_asc">Nom (A → Z)</option>
          <option value="name_desc">Nom (Z → A)</option>
        </select>
        {q || dept ? (
          <Button
            variant="ghost"
            size="sm"
            onClick={() => {
              setQ("");
              setDept("");
            }}
          >
            <X className="h-3.5 w-3.5" /> Réinitialiser
          </Button>
        ) : null}
      </div>

      {/* Header */}
      <div className="flex items-center justify-between gap-2 border-b border-border bg-card/40 px-4 py-2 text-xs">
        <div className="flex items-center gap-3 text-muted-foreground">
          {loading ? <Loader2 className="h-3 w-3 animate-spin text-primary" /> : null}
          <span>
            <span className="font-bold text-foreground">
              {count.toLocaleString("fr-FR")}
            </span>{" "}
            syndic{count > 1 ? "s" : ""}
            {" · "}
            <span className="text-muted-foreground">
              {totalInDb.toLocaleString("fr-FR")} référencés en base
            </span>
          </span>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            disabled={page === 1}
            onClick={() => setPage((p) => Math.max(1, p - 1))}
          >
            <ChevronLeft className="h-3.5 w-3.5" />
          </Button>
          <span className="text-muted-foreground">
            Page <span className="font-bold text-foreground">{page}</span> / {pages}
          </span>
          <Button
            variant="outline"
            size="sm"
            disabled={page >= pages}
            onClick={() => setPage((p) => Math.min(pages, p + 1))}
          >
            <ChevronRight className="h-3.5 w-3.5" />
          </Button>
        </div>
      </div>

      {/* Cards grid */}
      <div className="flex-1 overflow-auto p-4">
        <div
          className={cn(
            "grid gap-3 transition-opacity sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4",
            loading && "opacity-50",
          )}
        >
          {rows.map((s) => (
            <SyndicCard key={s.syndic} syndic={s} />
          ))}
        </div>
        {!loading && rows.length === 0 ? (
          <p className="p-8 text-center text-sm text-muted-foreground">
            Aucun syndic ne correspond.
          </p>
        ) : null}
      </div>
    </div>
  );
}

function SyndicCard({ syndic: s }: { syndic: Row }) {
  const totalDpe = s.dpe_a + s.dpe_b + s.dpe_c + s.dpe_d + s.dpe_e + s.dpe_f + s.dpe_g;
  const pipelinePct = s.nb_copros > 0 ? (s.in_pipeline / s.nb_copros) * 100 : 0;
  const filterUrl = `/copros?syndic=${encodeURIComponent(s.syndic)}`;
  const exportUrl = `/api/export/copros-by-filter.xlsx?syndic=${encodeURIComponent(s.syndic)}`;

  return (
    <div className="overflow-hidden rounded-xl border border-border bg-card shadow-sm transition-all hover:border-primary/40 hover:shadow-md">
      <div className="border-b border-border bg-gradient-to-br from-primary/5 to-secondary/30 p-4">
        <div className="flex items-start gap-3">
          <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-lg bg-primary text-primary-foreground">
            <Users className="h-5 w-5" />
          </div>
          <div className="min-w-0 flex-1">
            <Link
              href={filterUrl}
              className="block truncate text-sm font-bold leading-tight hover:text-primary"
              title={s.syndic}
            >
              {s.syndic}
            </Link>
            <p className="mt-0.5 truncate text-[10px] text-muted-foreground">
              {s.dept_list ? `Dpt ${s.dept_list}` : ""}
            </p>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-3 gap-2 border-b border-border p-3 text-center">
        <div>
          <div className="text-lg font-black text-primary">{s.nb_copros}</div>
          <div className="text-[10px] uppercase tracking-wider text-muted-foreground">
            copros
          </div>
        </div>
        <div>
          <div className="text-lg font-black text-foreground">
            {s.lots_total.toLocaleString("fr-FR")}
          </div>
          <div className="text-[10px] uppercase tracking-wider text-muted-foreground">
            lots
          </div>
        </div>
        <div>
          <div className="text-lg font-black text-foreground">{s.nb_communes}</div>
          <div className="text-[10px] uppercase tracking-wider text-muted-foreground">
            communes
          </div>
        </div>
      </div>

      {totalDpe > 0 ? (
        <div className="border-b border-border p-3">
          <p className="mb-1.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
            Répartition DPE ({totalDpe} estimés)
          </p>
          <div className="flex h-3 w-full overflow-hidden rounded-full">
            {(["a", "b", "c", "d", "e", "f", "g"] as const).map((cls) => {
              const v = (s as unknown as Record<string, number>)[`dpe_${cls}`];
              if (!v) return null;
              const pct = (v / totalDpe) * 100;
              return (
                <span
                  key={cls}
                  title={`Classe ${cls.toUpperCase()} : ${v} copros`}
                  style={{ width: `${pct}%`, background: DPE_BG[cls] }}
                />
              );
            })}
          </div>
          {s.avg_conso ? (
            <p className="mt-1 text-[10px] text-muted-foreground">
              Conso moy. <strong>{Math.round(s.avg_conso)}</strong> kWhep/m²/an
            </p>
          ) : null}
        </div>
      ) : null}

      <div className="space-y-1 p-3">
        <div className="flex items-center justify-between text-[11px]">
          <span className="text-muted-foreground">Dans pipeline</span>
          <span className="font-semibold">
            {s.in_pipeline} / {s.nb_copros} ({Math.round(pipelinePct)}%)
          </span>
        </div>
        <div className="h-1 w-full overflow-hidden rounded-full bg-secondary">
          <div
            className="h-full bg-primary"
            style={{ width: `${Math.max(2, pipelinePct)}%` }}
          />
        </div>
      </div>

      <div className="flex items-center gap-1 border-t border-border bg-secondary/30 p-2">
        <Link
          href={filterUrl}
          className="flex flex-1 items-center justify-center gap-1 rounded-md bg-primary px-2 py-1.5 text-[11px] font-bold text-primary-foreground hover:bg-primary/90"
        >
          <Building2 className="h-3 w-3" />
          Voir les copros
        </Link>
        <a
          href={exportUrl}
          className="flex items-center justify-center gap-1 rounded-md border border-border bg-background px-2 py-1.5 text-[11px] font-medium hover:bg-secondary"
          title={`Exporter les ${s.nb_copros} copros gérées par ${s.syndic}`}
        >
          <FileSpreadsheet className="h-3 w-3" />
          Excel
        </a>
      </div>
    </div>
  );
}

void ChevronDown;
void ChevronUp;
void MapPin;
