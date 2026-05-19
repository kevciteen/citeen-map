"use client";
import { useState } from "react";
import { Loader2, Search, Home, Building, ExternalLink, Plus, Download, Eye, SlidersHorizontal, List, Map as MapIcon } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { DpeBadge } from "@/components/ui/dpe-badge";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { toast } from "sonner";
import { MaisonDetailSheet } from "@/components/crm/maison-detail-sheet";
import { MaisonsMap } from "@/components/crm/maisons-map";

type MaisonDpe = {
  numero_dpe: string;
  classe: string;
  ges: string;
  conso: number | null;
  emission_ges: number | null;
  surface: number | null;
  date: string | null;
  date_visite: string | null;
  date_fin_validite: string | null;
  annee_construction: number | null;
  nb_niveaux: number | null;
  nb_pieces: number | null;
  hauteur_sous_plafond: number | null;
  type_batiment: string | null;
  methode_dpe: string | null;
  energie_principale_chauffage: string | null;
  energie_n2: string | null;
  energie_n3: string | null;
  installation_chauffage: string | null;
  generateur_chauffage: string | null;
  generateur_chauffage_desc: string | null;
  emetteur_chauffage: string | null;
  generateur_ecs: string | null;
  description_ecs: string | null;
  volume_ballon_ecs: number | null;
  energie_climatisation: string | null;
  surface_climatisee: number | null;
  type_ventilation: string | null;
  ventilation_recente: boolean | null;
  zone_climatique: string | null;
  deperdition_murs: number | null;
  deperdition_baies: number | null;
  deperdition_plancher_bas: number | null;
  deperdition_plancher_haut: number | null;
  cout_total: number | null;
  cout_chauffage: number | null;
  cout_ecs: number | null;
  cout_eclairage: number | null;
  cout_refroidissement: number | null;
  isolation_enveloppe: string | null;
  isolation_murs: string | null;
  isolation_toiture: string | null;
  isolation_plancher_bas: string | null;
  isolation_menuiseries: string | null;
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
const ENERGIES = [
  ["gaz", "Gaz naturel"],
  ["fioul", "Fioul"],
  ["electricite", "Électricité"],
  ["bois", "Bois"],
  ["reseau de chaleur", "Réseau de chaleur"],
  ["gpl", "GPL / Propane"],
] as const;

export type ZoneSearchEvent = {
  items: MaisonDpe[];
  total: number;
};

type BatimentType = "maison" | "appartement";

export function MaisonsZoneSearch({
  onResults,
  typeBatiment = "maison",
}: {
  onResults?: (e: ZoneSearchEvent) => void;
  typeBatiment?: BatimentType;
} = {}) {
  const apiSegment = typeBatiment === "appartement" ? "appartements" : "maisons";
  const typeLabel = typeBatiment === "appartement" ? "Appartement" : "Maison";
  const typeLabelPluralFr =
    typeBatiment === "appartement" ? "appartements" : "maisons";

  const [cp, setCp] = useState("");
  const [commune, setCommune] = useState("");
  const [dpeClasses, setDpeClasses] = useState<Set<string>>(new Set(["F", "G"]));
  const [gesClasses, setGesClasses] = useState<Set<string>>(new Set());
  const [consoMin, setConsoMin] = useState("");
  const [consoMax, setConsoMax] = useState("");
  const [surfaceMin, setSurfaceMin] = useState("");
  const [surfaceMax, setSurfaceMax] = useState("");
  const [yearMin, setYearMin] = useState("");
  const [yearMax, setYearMax] = useState("");
  const [energies, setEnergies] = useState<Set<string>>(new Set());
  const [isolationMurs, setIsolationMurs] = useState(false);
  const [dpeAncien, setDpeAncien] = useState("");
  const [advanced, setAdvanced] = useState(false);

  const [items, setItems] = useState<MaisonDpe[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [bulkAdding, setBulkAdding] = useState(false);
  const [view, setView] = useState<"list" | "map">("list");

  const toggleClass = (c: string) => {
    const next = new Set(dpeClasses);
    if (next.has(c)) next.delete(c);
    else next.add(c);
    setDpeClasses(next);
  };
  const toggleGes = (c: string) => {
    const next = new Set(gesClasses);
    if (next.has(c)) next.delete(c);
    else next.add(c);
    setGesClasses(next);
  };
  const toggleEnergie = (e: string) => {
    const next = new Set(energies);
    if (next.has(e)) next.delete(e);
    else next.add(e);
    setEnergies(next);
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
      if (gesClasses.size > 0) sp.set("ges", [...gesClasses].join(","));
      if (consoMin.trim()) sp.set("consoMin", consoMin.trim());
      if (consoMax.trim()) sp.set("consoMax", consoMax.trim());
      if (surfaceMin.trim()) sp.set("surfaceMin", surfaceMin.trim());
      if (surfaceMax.trim()) sp.set("surfaceMax", surfaceMax.trim());
      if (yearMin.trim()) sp.set("yearMin", yearMin.trim());
      if (yearMax.trim()) sp.set("yearMax", yearMax.trim());
      if (energies.size > 0) sp.set("energie", [...energies].join(","));
      if (isolationMurs) sp.set("isolationMursMauvaise", "1");
      if (dpeAncien.trim()) sp.set("dpeAncienAnnees", dpeAncien.trim());
      sp.set("limit", "500");

      const r = await fetch(`/api/${apiSegment}/search?${sp}`);
      const j = await r.json();
      if (r.ok) {
        setItems(j.items);
        setTotal(j.total);
        onResults?.({ items: j.items, total: j.total });
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
            customLabel: `${typeLabel} · ${m.address.label}`,
            customAddress: m.address.label,
            customLat: m.lat,
            customLon: m.lon,
            stage: "to_contact",
            priority: 2,
            tags: [typeBatiment, `dpe-${m.classe}`],
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
    a.download = `${typeLabelPluralFr}-${cp || commune}-${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="flex h-full flex-col">
      {/* Filters */}
      <div className="border-b border-border bg-card p-4">
        <div className="grid gap-3 lg:grid-cols-3 xl:grid-cols-4">
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
          <div className="flex items-end gap-2">
            <Button onClick={search} disabled={loading} className="flex-1">
              {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Search className="h-4 w-4" />}
              Rechercher
            </Button>
            <Button
              variant={advanced ? "default" : "outline"}
              onClick={() => setAdvanced((v) => !v)}
              size="sm"
            >
              <SlidersHorizontal className="h-3.5 w-3.5" />
              Filtres avancés
            </Button>
          </div>
        </div>

        {advanced ? (
          <div className="mt-3 grid gap-3 rounded-lg border border-border bg-secondary/40 p-3 lg:grid-cols-3 xl:grid-cols-4">
            <Field label="Classes GES">
              <div className="flex flex-wrap gap-1">
                {DPE_CLASSES.map((c) => (
                  <button
                    key={c}
                    onClick={() => toggleGes(c)}
                    className={cn(
                      "transition-transform",
                      gesClasses.has(c)
                        ? "scale-110 ring-2 ring-foreground/70 ring-offset-1"
                        : "opacity-50 hover:opacity-100",
                    )}
                  >
                    <DpeBadge classe={c} size="sm" />
                  </button>
                ))}
              </div>
            </Field>
            <Field label="Énergie de chauffage">
              <div className="flex flex-wrap gap-1">
                {ENERGIES.map(([key, label]) => (
                  <button
                    key={key}
                    onClick={() => toggleEnergie(key)}
                    className={cn(
                      "rounded-full border px-2 py-0.5 text-[11px] font-medium transition-colors",
                      energies.has(key)
                        ? "border-primary bg-primary text-primary-foreground"
                        : "border-border bg-background hover:bg-secondary",
                    )}
                  >
                    {label}
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
            <Field label="Surface m² min / max">
              <div className="flex gap-1">
                <Input value={surfaceMin} onChange={(e) => setSurfaceMin(e.target.value)} placeholder="min" className="w-20" inputMode="numeric" />
                <Input value={surfaceMax} onChange={(e) => setSurfaceMax(e.target.value)} placeholder="max" className="w-20" inputMode="numeric" />
              </div>
            </Field>
            <Field label="Année construction min / max">
              <div className="flex gap-1">
                <Input value={yearMin} onChange={(e) => setYearMin(e.target.value)} placeholder="min" className="w-20" inputMode="numeric" />
                <Input value={yearMax} onChange={(e) => setYearMax(e.target.value)} placeholder="max" className="w-20" inputMode="numeric" />
              </div>
            </Field>
            <Field label="DPE de plus de N années">
              <Input value={dpeAncien} onChange={(e) => setDpeAncien(e.target.value)} placeholder="ex. 7" className="w-24" inputMode="numeric" />
            </Field>
            <Field label="Cibles passoires">
              <label className="flex cursor-pointer items-center gap-2 text-xs">
                <input
                  type="checkbox"
                  checked={isolationMurs}
                  onChange={(e) => setIsolationMurs(e.target.checked)}
                  className="h-3.5 w-3.5"
                />
                Isolation murs ≤ moyenne
              </label>
            </Field>
          </div>
        ) : null}
      </div>

      {/* Header */}
      <div className="flex items-center justify-between gap-2 border-b border-border bg-card/40 px-4 py-2 text-xs">
        <div className="flex items-center gap-2 text-muted-foreground">
          {loading ? <Loader2 className="h-3 w-3 animate-spin text-primary" /> : null}
          <span>
            <span className="font-bold text-foreground">{items.length.toLocaleString("fr-FR")}</span>{" "}
            résultats affichés · {total.toLocaleString("fr-FR")} {typeLabelPluralFr} {typeBatiment === "appartement" ? "matchés" : "matchées"} au total
          </span>
        </div>
        <div className="flex items-center gap-3">
          {items.length > 0 ? (
            <div className="flex items-center gap-1 rounded-md border border-border bg-background p-0.5">
              <button
                onClick={() => setView("list")}
                className={cn(
                  "flex items-center gap-1 rounded-sm px-2 py-1 text-[11px] font-medium",
                  view === "list" ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:bg-secondary",
                )}
              >
                <List className="h-3 w-3" /> Liste
              </button>
              <button
                onClick={() => setView("map")}
                className={cn(
                  "flex items-center gap-1 rounded-sm px-2 py-1 text-[11px] font-medium",
                  view === "map" ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:bg-secondary",
                )}
              >
                <MapIcon className="h-3 w-3" /> Carte
              </button>
            </div>
          ) : null}
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
      </div>

      {/* Results — inline styles (comme la carte prospection qui fonctionne)
          parce que Tailwind h-full ne cascade pas fiablement dans ce flex */}
      <div
        className="relative overflow-hidden"
        style={{ flex: "1 1 0", minHeight: 0, height: "100%" }}
      >
        {items.length === 0 && !loading ? (
          <p className="p-8 text-center text-sm text-muted-foreground">
            Définissez un CP ou une commune, choisissez les classes DPE cibles (par défaut F+G = passoires thermiques), et cliquez Rechercher.
          </p>
        ) : view === "list" ? (
          <div
            className="overflow-auto p-4"
            style={{ position: "absolute", inset: 0 }}
          >
            <div className="grid gap-2">
              {items.map((m) => (
                <MaisonRow
                  key={m.numero_dpe}
                  maison={m}
                  checked={selected.has(m.numero_dpe)}
                  onToggle={() => toggleSelected(m.numero_dpe)}
                  typeBatiment={typeBatiment}
                />
              ))}
            </div>
          </div>
        ) : (
          <MaisonsMap items={items} typeBatiment={typeBatiment} />
        )}
      </div>
    </div>
  );
}

function MaisonRow({
  maison: m,
  checked,
  onToggle,
  typeBatiment = "maison",
}: {
  maison: MaisonDpe;
  checked: boolean;
  onToggle: () => void;
  typeBatiment?: BatimentType;
}) {
  return (
    <div className={cn("flex items-center gap-3 rounded-lg border border-border bg-card p-3", checked && "border-primary bg-primary/5")}>
      <input type="checkbox" checked={checked} onChange={onToggle} className="h-4 w-4" />
      <DpeBadge classe={m.classe} size="md" />
      {typeBatiment === "appartement" ? (
        <Building className="h-3.5 w-3.5 text-muted-foreground" />
      ) : (
        <Home className="h-3.5 w-3.5 text-muted-foreground" />
      )}
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
        {m.energie_principale_chauffage ? (
          <Badge variant="secondary" className="text-[10px]">
            {m.energie_principale_chauffage}
          </Badge>
        ) : null}
      </div>
      <MaisonDetailSheet
        maison={m}
        typeBatiment={typeBatiment}
        trigger={
          <button
            className="flex h-7 items-center gap-1 rounded-md border border-border bg-background px-2 text-[11px] font-medium hover:bg-secondary"
            title="Voir tous les détails (chauffage, coûts, isolation, dates)"
          >
            <Eye className="h-3 w-3" />
            Détail
          </button>
        }
      />
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
