"use client";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Topbar } from "@/components/layout/topbar";
import { MapView, type CoproPoint, type MapBounds } from "@/components/map/map-view";
import {
  FiltersBar,
  DEFAULT_FILTERS,
  type MapFilters,
} from "@/components/map/filters-bar";
import { AddressSearch } from "@/components/map/address-search";
import { CoproPanel } from "@/components/map/copro-panel";
import { Loader2 } from "lucide-react";

export default function MapPage() {
  const [bounds, setBounds] = useState<MapBounds | null>(null);
  const [filters, setFilters] = useState<MapFilters>(DEFAULT_FILTERS);
  const [appliedFilters, setAppliedFilters] = useState<MapFilters>(DEFAULT_FILTERS);
  const [points, setPoints] = useState<CoproPoint[]>([]);
  const [loading, setLoading] = useState(false);
  const [count, setCount] = useState(0);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [flyTo, setFlyTo] = useState<{ lat: number; lon: number; zoom?: number } | null>(null);
  const fetchSeq = useRef(0);

  const queryString = useMemo(() => {
    if (!bounds) return null;
    const sp = new URLSearchParams();
    sp.set("minLat", String(bounds.minLat));
    sp.set("maxLat", String(bounds.maxLat));
    sp.set("minLon", String(bounds.minLon));
    sp.set("maxLon", String(bounds.maxLon));
    sp.set("limit", "1500");
    if (appliedFilters.q) sp.set("q", appliedFilters.q);
    if (appliedFilters.cp) sp.set("cp", appliedFilters.cp);
    if (appliedFilters.syndic) sp.set("syndic", appliedFilters.syndic);
    if (appliedFilters.dept) sp.set("dept", appliedFilters.dept);
    if (appliedFilters.dpeClasses.length)
      sp.set("dpe", appliedFilters.dpeClasses.join(","));
    if (appliedFilters.minLots != null)
      sp.set("minLots", String(appliedFilters.minLots));
    if (appliedFilters.periode) sp.set("periode", appliedFilters.periode);
    return sp.toString();
  }, [bounds, appliedFilters]);

  const reload = useCallback(async () => {
    if (!queryString) return;
    const seq = ++fetchSeq.current;
    setLoading(true);
    try {
      const r = await fetch(`/api/copros?${queryString}`);
      if (!r.ok) return;
      const j = await r.json();
      if (seq !== fetchSeq.current) return;
      setPoints(j.items as CoproPoint[]);
      setCount(j.count);
    } finally {
      if (seq === fetchSeq.current) setLoading(false);
    }
  }, [queryString]);

  useEffect(() => {
    const t = setTimeout(reload, 250);
    return () => clearTimeout(t);
  }, [reload]);

  return (
    <div
      className="flex flex-col"
      style={{ height: "100%", minHeight: 0 }}
    >
      <Topbar
        title="Carte prospection"
        subtitle="Explorer les copropriétés et qualifier vos cibles"
      />
      <div
        className="relative"
        style={{ flex: "1 1 0", minHeight: 0, height: "100%" }}
      >
        <MapView
          points={points}
          flyTo={flyTo}
          selectedId={selectedId}
          onBoundsChange={setBounds}
          onSelectCopro={(id) => setSelectedId(id)}
        />

        <FiltersBar
          value={filters}
          onChange={setFilters}
          onSubmit={() => setAppliedFilters(filters)}
          onReset={() => {
            setFilters(DEFAULT_FILTERS);
            setAppliedFilters(DEFAULT_FILTERS);
          }}
          resultCount={count}
        />

        <div className="absolute right-4 top-4 z-10 w-[420px] max-w-[calc(100vw-2rem)]">
          {selectedId == null ? (
            <div className="premium-card p-3">
              <AddressSearch
                onSelect={(r) => setFlyTo({ lat: r.lat, lon: r.lon, zoom: 17 })}
              />
              <p className="mt-2 text-[11px] text-muted-foreground">
                Recherche d'adresse (BAN) — clic sur une copro pour ouvrir la fiche.
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

        <div className="pointer-events-none absolute bottom-4 left-1/2 z-10 -translate-x-1/2">
          <div className="pointer-events-auto flex items-center gap-2 rounded-full border border-border bg-card/90 px-4 py-1.5 text-xs font-medium shadow-sm backdrop-blur">
            {loading ? <Loader2 className="h-3 w-3 animate-spin text-primary" /> : null}
            <span>
              <span className="font-bold text-primary">{count}</span> copros affichées
            </span>
          </div>
        </div>
      </div>
    </div>
  );
}
