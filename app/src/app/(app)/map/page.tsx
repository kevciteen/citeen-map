"use client";
import { useCallback, useRef, useState } from "react";
import { Topbar } from "@/components/layout/topbar";
import { MapView, type CoproPoint, type MaisonPoint, type MapBounds } from "@/components/map/map-view";
import {
  FiltersBar,
  DEFAULT_FILTERS,
  type MapFilters,
} from "@/components/map/filters-bar";
import { AddressSearch } from "@/components/map/address-search";
import { CoproPanel } from "@/components/map/copro-panel";
import { MaisonDetailSheet } from "@/components/crm/maison-detail-sheet";
import { Loader2 } from "lucide-react";

// On garde une référence aux derniers items maisons pour résoudre le clic carte.
type MaisonFull = MaisonPoint & {
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
  ademe_url: string;
};

export default function MapPage() {
  const [, setBounds] = useState<MapBounds | null>(null);
  const [filters, setFilters] = useState<MapFilters>(DEFAULT_FILTERS);
  const [points, setPoints] = useState<CoproPoint[]>([]);
  const [maisons, setMaisons] = useState<MaisonPoint[]>([]);
  const [loading, setLoading] = useState(false);
  const [count, setCount] = useState(0);
  const [maisonsCount, setMaisonsCount] = useState(0);
  const [hasSearched, setHasSearched] = useState(false);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [selectedMaison, setSelectedMaison] = useState<MaisonFull | null>(null);
  const [maisonSheetKey, setMaisonSheetKey] = useState(0);
  const [flyTo, setFlyTo] = useState<{ lat: number; lon: number; zoom?: number } | null>(null);
  const fetchSeq = useRef(0);
  const maisonsCache = useRef<Map<string, MaisonFull>>(new Map());

  // Recherche déclenchée uniquement par l'utilisateur (bouton Rechercher / Enter)
  const runSearch = useCallback(async (f: MapFilters) => {
    const seq = ++fetchSeq.current;
    setLoading(true);
    setHasSearched(true);
    try {
      const wantsCopros = f.mode === "copros" || f.mode === "both";
      const wantsMaisons = f.mode === "maisons" || f.mode === "both";

      const tasks: Array<Promise<unknown>> = [];

      if (wantsCopros) {
        const sp = new URLSearchParams();
        sp.set("limit", "1500");
        if (f.q) sp.set("q", f.q);
        if (f.cp) sp.set("cp", f.cp);
        if (f.syndic) sp.set("syndic", f.syndic);
        if (f.dept) sp.set("dept", f.dept);
        if (f.dpeClasses.length) sp.set("dpe", f.dpeClasses.join(","));
        if (f.minLots != null) sp.set("minLots", String(f.minLots));
        if (f.periode) sp.set("periode", f.periode);
        tasks.push(
          fetch(`/api/copros?${sp}`)
            .then((r) => (r.ok ? r.json() : { items: [], count: 0 }))
            .then((j) => {
              if (seq !== fetchSeq.current) return;
              setPoints(j.items as CoproPoint[]);
              setCount(j.count);
            }),
        );
      } else {
        setPoints([]);
        setCount(0);
      }

      if (wantsMaisons) {
        // Pour les maisons on doit fournir CP ou commune (extrait du q si possible)
        const sp = new URLSearchParams();
        if (f.cp) sp.set("cp", f.cp);
        // Si q ressemble à un CP, on l'utilise comme tel
        if (!f.cp && /^\d{5}$/.test(f.q.trim())) sp.set("cp", f.q.trim());
        else if (f.q) sp.set("commune", f.q.trim());
        if (f.dpeClasses.length) sp.set("dpe", f.dpeClasses.join(","));
        sp.set("limit", "500");

        if (sp.has("cp") || sp.has("commune")) {
          tasks.push(
            fetch(`/api/maisons/search?${sp}`)
              .then((r) => (r.ok ? r.json() : { items: [], total: 0 }))
              .then((j) => {
                if (seq !== fetchSeq.current) return;
                const items = j.items as MaisonFull[];
                maisonsCache.current = new Map(items.map((m) => [m.numero_dpe, m]));
                setMaisons(
                  items
                    .filter((m) => m.lat != null && m.lon != null)
                    .map((m) => ({
                      numero_dpe: m.numero_dpe,
                      lat: m.lat!,
                      lon: m.lon!,
                      classe: m.classe,
                      address: m.address.label,
                    })),
                );
                setMaisonsCount(j.total ?? items.length);
              }),
          );
        } else {
          setMaisons([]);
          setMaisonsCount(0);
        }
      } else {
        setMaisons([]);
        setMaisonsCount(0);
      }

      await Promise.all(tasks);
    } finally {
      if (seq === fetchSeq.current) setLoading(false);
    }
  }, []);

  const handleSelectMaison = useCallback((numeroDpe: string) => {
    const m = maisonsCache.current.get(numeroDpe);
    if (m) {
      setSelectedMaison(m);
      setMaisonSheetKey((n) => n + 1);
    }
  }, []);

  return (
    <div
      className="flex flex-col"
      style={{ height: "100%", minHeight: 0 }}
    >
      <Topbar
        title="Carte prospection"
        subtitle="Carte unifiée : copropriétés + maisons individuelles. La recherche déclenche l'affichage."
      />
      <div
        className="relative"
        style={{ flex: "1 1 0", minHeight: 0, height: "100%" }}
      >
        <MapView
          points={points}
          maisons={maisons}
          flyTo={flyTo}
          selectedId={selectedId}
          onBoundsChange={setBounds}
          onSelectCopro={(id) => setSelectedId(id)}
          onSelectMaison={handleSelectMaison}
        />

        <FiltersBar
          value={filters}
          onChange={setFilters}
          onSubmit={() => runSearch(filters)}
          onReset={() => {
            setFilters(DEFAULT_FILTERS);
            setPoints([]);
            setMaisons([]);
            setCount(0);
            setMaisonsCount(0);
            setHasSearched(false);
          }}
          resultCount={count + maisonsCount}
        />

        <div className="absolute right-4 top-4 z-10 w-[420px] max-w-[calc(100vw-2rem)]">
          {selectedId == null ? (
            <div className="premium-card p-3">
              <AddressSearch
                onSelect={(r) => setFlyTo({ lat: r.lat, lon: r.lon, zoom: 17 })}
              />
              <p className="mt-2 text-[11px] text-muted-foreground">
                Recherche d&apos;adresse (BAN) — clic sur un point pour ouvrir la fiche.
              </p>
            </div>
          ) : null}
        </div>

        {selectedId != null ? (
          <CoproPanel
            coproId={selectedId}
            onClose={() => setSelectedId(null)}
          />
        ) : null}

        {/* Sheet maison déclenchée par clic sur un marker maison */}
        {selectedMaison ? (
          <MaisonDetailSheet
            key={`${selectedMaison.numero_dpe}-${maisonSheetKey}`}
            maison={selectedMaison}
            trigger={
              <button
                id="__map_maison_sheet_trigger__"
                style={{ display: "none" }}
              />
            }
          />
        ) : null}
        <MaisonSheetOpener depKey={maisonSheetKey} />

        {/* Bandeau de compteur + état recherche */}
        <div className="pointer-events-none absolute bottom-4 left-1/2 z-10 -translate-x-1/2">
          <div className="pointer-events-auto flex items-center gap-2 rounded-full border border-border bg-card/90 px-4 py-1.5 text-xs font-medium shadow-sm backdrop-blur">
            {loading ? <Loader2 className="h-3 w-3 animate-spin text-primary" /> : null}
            {!hasSearched ? (
              <span className="text-muted-foreground">
                Lancez une recherche pour afficher les résultats sur la carte
              </span>
            ) : (
              <span>
                <span className="font-bold text-primary">{count}</span> copros ·{" "}
                <span className="font-bold text-emerald-600">{maisonsCount}</span> maisons
              </span>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

// Petit effet pour cliquer le trigger caché du sheet quand selectedMaison change
function MaisonSheetOpener({ depKey }: { depKey: number }) {
  // eslint-disable-next-line react-hooks/exhaustive-deps
  if (typeof window !== "undefined" && depKey > 0) {
    queueMicrotask(() => {
      const btn = document.getElementById("__map_maison_sheet_trigger__");
      if (btn) (btn as HTMLButtonElement).click();
    });
  }
  return null;
}
