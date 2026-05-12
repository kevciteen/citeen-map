"use client";
import { useState } from "react";
import { Loader2, Search, Home, ExternalLink, Plus, Download } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { DpeBadge } from "@/components/ui/dpe-badge";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { toast } from "sonner";

type MaisonDpe = {
  numero_dpe: string;
  classe: string;
  ges: string;
  conso: number | null;
  surface: number | null;
  date: string | null;
  annee_construction: number | null;
  energie_principale_chauffage: string | null;
  type_batiment: string | null;
  address: {
    housenumber: string | null;
    street: string | null;
    postcode: string | null;
    city: string | null;
    label: string;
  };
  lat: number | null;
  lon: number | null;
  ademe_url: string;
};

const DPE_CLASSES = ["A", "B", "C", "D", "E", "F", "G"] as const;

export function MaisonsZoneSearch() {
  const [cp, setCp] = useState("");
  const [commune, setCommune] = useState("");
  const [dpeClasses, setDpeClasses] = useState<Set<string>>(new Set(["F", "G"]));
  const [consoMin, setConsoMin] = useState("");
  const [consoMax, setConsoMax] = useState("");
  const [yearMin, setYearMin] = useState("");
  const [yearMax, setYearMax] = useState("");

  const [items, setItems] = useState<MaisonDpe[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [bulkAdding, setBulkAdding] = useState(false);

  const toggleClass = (c: string) => {
    const next = new Set(dpeClasses);
    if (next.has(c)) next.delete(c);
    else next.add(c);
    setDpeClasses(next);
  };
  const toggleSelected = (numero_dpe: string) => {
    const next = new Set(selected);
    if (next.has(numero_dpe)) next.delete(numero_dpe);
    else next.add(numero_dpe);
    setSelected(next);
  };

  const search = async () => {
    if (!cp.trim() && !commune.trim()) {
      toast.error("Saisissez au moins un CP ou une commune");
      return;
    }
    setLoading(true);
    setItems([]);
    setSelected(new Set());
    try {
      const sp = new URLSearchParams();
      if (cp.trim()) sp.set("cp", cp.trim());
      if (commune.trim()) sp.set("commune", commune.trim());
      if (dpeClasses.size > 0) sp.set("dpe", [...dpeClasses].join(","));
      if (consoMin.trim()) sp.set("consoMin", consoMin.trim());
      if (consoMax.trim()) sp.set("consoMax", consoMax.trim());
      if (yearMin.trim()) sp.set("yearMin", yearMin.trim());
      if (yearMax.trim()) sp.set("yearMax", yearMax.trim());
      sp.set("limit", "500");

      const r = await fetch(`/api/maisons/search?${sp}`);
      const j = await r.json();
      if (r.ok) {
        setItems(j.items);
        setTotal(j.total);
      } else toast.error(j?.error || "Erreur");
    } finally {
      setLoading(false);
    }
  };

  const bulkAdd = async () => {
    if (selected.size === 0) return;
    setBulkAdding(true);
    try {
      let ok = 0;
      for (const numero of selected) {
        const m = items.find((x) => x.numero_dpe === numero);
        if (!m) continue;
        const r = await fetch("/api/prospects", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            customLabel: `Maison · ${m.address.label}`,
            customAddress: m.address.label,
            customLat: m.lat,
            customLon: m.lon,
            stage: "to_contact",
            priority: 2,
            tags: ["maison", `dpe-${m.classe}`],
          }),
        });
        if (r.ok || r.status === 409) ok++;
      }
      toast.success(`${ok}/${selected.size} ajoutés au pipeline`);
      setSelected(new Set());
    } finally {
      setBulkAdding(false);
    }
  };

  const exportCsv = () => {
    const rows = [
      ["numero_dpe","classe","ges","conso","surface","date","annee","energie","housenumber","street","cp","commune","lat","lon","ademe_url"],
      ...items.map((m) => [
        m.numero_dpe, m.classe, m.ges, m.conso ?? "", m.surface ?? "",
        m.date ?? "", m.annee_construction ?? "", m.energie_principale_chauffage ?? "",
        m.address.housenumber ?? "", m.address.street ?? "",
        m.address.postcode ?? "", m.address.city ?? "",
        m.lat ?? "", m.lon ?? "", m.ademe_url,
      ]),
    ]
      .map((r) => r.map((v) => `"${String(v).replace(/"/g, '""')}"`).join(","))
      .join("\n");
    const blob = new Blob(["﻿" + rows], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `maisons-${cp || commune}-${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="flex h-full flex-col">
      {/* Filters */}
      <div className="grid gap-3 border-b border-border bg-card p-4 lg:grid-cols-3 xl:grid-cols-4">
        <Field label="Code postal">
          <Input value={cp} onChange={(e) => setCp(e.target.value)} placeholder="ex. 75011" inputMode="numeric" />
        </Field>
        <Field label="Commune">
          <Input value={commune} onChange={(e) => setCommune(e.target.value)} placeholder="ex. Paris 11e" />
        </Field>
        <Field label="Classes DPE">
          <div className="flex flex-wrap gap-1">
            {DPE_CLASSES.map((c) => (
              <button
                key={c}
                onClick={() => toggleClass(c)}
                className={cn(
                  "transition-transform",
                  dpeClasses.has(c) ? "scale-110 ring-2 ring-foreground/70 ring-offset-1" : "opacity-50 hover:opacity-100",
                )}
              >
                <DpeBadge classe={c} size="sm" />
              </button>
            ))}
          </div>
        </Field>
        <Field label="Conso min / max (kWhep)">
          <div className="flex gap-1">
            <Input value={consoMin} onChange={(e) => setConsoMin(e.target.value)} placeholder="min" className="w-20" inputMode="numeric" />
            <Input value={consoMax} onChange={(e) => setConsoMax(e.target.value)} placeholder="max" className="w-20" inputMode="numeric" />
          </div>
        </Field>
        <Field label="Année construction min / max">
          <div className="flex gap-1">
            <Input value={yearMin} onChange={(e) => setYearMin(e.target.value)} placeholder="min" className="w-20" inputMode="numeric" />
            <Input value={yearMax} onChange={(e) => setYearMax(e.target.value)} placeholder="max" className="w-20" inputMode="numeric" />
          </div>
        </Field>
        <div className="flex items-end gap-2 lg:col-span-2 xl:col-span-1">
          <Button onClick={search} disabled={loading} className="flex-1">
            {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Search className="h-4 w-4" />}
            Rechercher
          </Button>
        </div>
      </div>

      {/* Header */}
      <div className="flex items-center justify-between gap-2 border-b border-border bg-card/40 px-4 py-2 text-xs">
        <div className="flex items-center gap-2 text-muted-foreground">
          {loading ? <Loader2 className="h-3 w-3 animate-spin text-primary" /> : null}
          <span>
            <span className="font-bold text-foreground">{items.length.toLocaleString("fr-FR")}</span>{" "}
            résultats affichés · {total.toLocaleString("fr-FR")} maisons matchées au total
          </span>
        </div>
        {items.length > 0 ? (
          <div className="flex items-center gap-2">
            <span className="text-muted-foreground">
              {selected.size} sélectionné{selected.size > 1 ? "s" : ""}
            </span>
            {selected.size > 0 ? (
              <Button size="sm" onClick={bulkAdd} disabled={bulkAdding}>
                {bulkAdding ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Plus className="h-3.5 w-3.5" />}
                Ajouter {selected.size} au pipeline
              </Button>
            ) : null}
            <Button size="sm" variant="outline" onClick={exportCsv}>
              <Download className="h-3.5 w-3.5" /> CSV
            </Button>
          </div>
        ) : null}
      </div>

      {/* List */}
      <div className="flex-1 overflow-auto p-4">
        {items.length === 0 && !loading ? (
          <p className="p-8 text-center text-sm text-muted-foreground">
            Définissez un CP ou une commune, choisissez les classes DPE cibles (par défaut F+G = passoires thermiques), et cliquez Rechercher.
          </p>
        ) : (
          <div className="grid gap-2">
            {items.map((m) => (
              <MaisonRow
                key={m.numero_dpe}
                maison={m}
                checked={selected.has(m.numero_dpe)}
                onToggle={() => toggleSelected(m.numero_dpe)}
              />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

function MaisonRow({
  maison: m,
  checked,
  onToggle,
}: {
  maison: MaisonDpe;
  checked: boolean;
  onToggle: () => void;
}) {
  return (
    <div className={cn("flex items-center gap-3 rounded-lg border border-border bg-card p-3", checked && "border-primary bg-primary/5")}>
      <input type="checkbox" checked={checked} onChange={onToggle} className="h-4 w-4" />
      <DpeBadge classe={m.classe} size="md" />
      <Home className="h-3.5 w-3.5 text-muted-foreground" />
      <div className="min-w-0 flex-1">
        <p className="truncate text-sm font-semibold">
          {m.address.housenumber} {m.address.street}
        </p>
        <p className="truncate text-[11px] text-muted-foreground">
          {m.address.postcode} {m.address.city}
        </p>
      </div>
      <div className="hidden items-center gap-3 text-xs sm:flex">
        {m.conso ? (
          <span>
            <span className="font-bold">{m.conso}</span>
            <span className="text-muted-foreground"> kWhep</span>
          </span>
        ) : null}
        {m.surface ? (
          <span>
            <span className="font-bold">{m.surface}</span>
            <span className="text-muted-foreground"> m²</span>
          </span>
        ) : null}
        {m.annee_construction ? (
          <Badge variant="outline" className="text-[10px]">
            {m.annee_construction}
          </Badge>
        ) : null}
      </div>
      <a
        href={m.ademe_url}
        target="_blank"
        rel="noreferrer"
        className="flex h-7 w-7 items-center justify-center rounded-md text-muted-foreground hover:bg-secondary hover:text-foreground"
        title="Fiche DPE officielle ADEME"
      >
        <ExternalLink className="h-3.5 w-3.5" />
      </a>
    </div>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <label className="mb-1 block text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
        {label}
      </label>
      {children}
    </div>
  );
}

void Card;
void CardContent;
