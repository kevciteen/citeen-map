"use client";
import { useCallback, useRef, useState } from "react";
import { Loader2, Search, SlidersHorizontal } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { DpeBadge } from "@/components/ui/dpe-badge";
import { cn } from "@/lib/utils";
import { toast } from "sonner";
import { MaisonsMap, type MaisonDpe, type MaisonsMapBounds } from "@/components/crm/maisons-map";
import { MaisonDetailBody } from "@/components/crm/maison-detail-sheet";
import { AddressAutocomplete } from "@/components/crm/address-autocomplete";

const DPE_CLASSES = ["A", "B", "C", "D", "E", "F", "G"] as const;
const ENERGIES = [
  ["gaz", "Gaz naturel"],
  ["fioul", "Fioul"],
  ["electricite", "Électricité"],
  ["bois", "Bois"],
  ["reseau de chaleur", "Réseau de chaleur"],
  ["gpl", "GPL / Propane"],
] as const;

// En-deçà de ce zoom le viewport couvre une zone trop large pour une requête
// ADEME pertinente — on invite à zoomer (comme le ZOOM_MIN du tertiaire).
const ZOOM_MIN = 12;
const MAX_RADIUS_M = 8000;
const ADDRESS_RADIUS_M = 700;

type BatimentType = "maison" | "appartement";

/**
 * Vue « carte d'abord » des logements individuels, calquée sur le module
 * tertiaire : la carte réagit au viewport. On interroge ADEME par
 * geo_distance autour du centre visible (bouton « Rechercher cette zone »)
 * ou autour d'une adresse choisie (avec vol + petit rayon), au lieu de
 * charger un code postal entier.
 */
export function MaisonsMapView({
  typeBatiment = "maison",
}: {
  typeBatiment?: BatimentType;
}) {
  const apiSegment = typeBatiment === "appartement" ? "appartements" : "maisons";
  const typeLabelPluralFr = typeBatiment === "appartement" ? "appartements" : "maisons";

  const [addressQuery, setAddressQuery] = useState("");
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
  const [showAdvanced, setShowAdvanced] = useState(false);

  const [items, setItems] = useState<MaisonDpe[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [searched, setSearched] = useState(false);
  const [zoom, setZoom] = useState(11);
  const [flyTo, setFlyTo] = useState<{ lat: number; lon: number; zoom?: number } | null>(null);
  const [selected, setSelected] = useState<MaisonDpe | null>(null);

  const boundsRef = useRef<MaisonsMapBounds | null>(null);
  const onBoundsChange = useCallback((b: MaisonsMapBounds) => {
    boundsRef.current = b;
    setZoom(b.zoom);
  }, []);

  const toggle = (set: Set<string>, setter: (s: Set<string>) => void, v: string) => {
    const next = new Set(set);
    if (next.has(v)) next.delete(v);
    else next.add(v);
    setter(next);
  };

  const fetchAround = useCallback(
    async (lat: number, lon: number, radiusM: number) => {
      setLoading(true);
      setItems([]);
      try {
        const sp = new URLSearchParams();
        sp.set("lat", String(lat));
        sp.set("lon", String(lon));
        sp.set("r", String(Math.round(Math.min(radiusM, MAX_RADIUS_M))));
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
        sp.set("limit", "800");

        const r = await fetch(`/api/${apiSegment}/around?${sp}`);
        const j = await r.json();
        if (r.ok) {
          setItems(j.items);
          setTotal(j.total);
          setSearched(true);
        } else toast.error(j?.error || "Erreur");
      } catch (err) {
        toast.error((err as Error).message);
      } finally {
        setLoading(false);
      }
    },
    [apiSegment, dpeClasses, gesClasses, consoMin, consoMax, surfaceMin, surfaceMax, yearMin, yearMax, energies, isolationMurs, dpeAncien],
  );

  const searchArea = () => {
    const b = boundsRef.current;
    if (!b) return;
    if (b.zoom < ZOOM_MIN) {
      toast.error("Zoome davantage pour rechercher dans cette zone");
      return;
    }
    fetchAround(b.centerLat, b.centerLon, b.radiusM);
  };

  return (
    <div className="flex h-full w-full">
      <div className="relative h-full flex-1">
      {/* Panneau de filtres flottant (calqué sur le tertiaire) */}
      <div className="absolute left-3 top-3 z-10 w-[360px] max-w-[calc(100vw-2rem)] rounded-lg border border-border bg-card/95 p-3 shadow-md backdrop-blur">
        <div className="mb-2">
          <AddressAutocomplete
            value={addressQuery}
            onChange={setAddressQuery}
            onSelect={(s) => {
              setAddressQuery(s.label);
              setFlyTo({ lat: s.lat, lon: s.lon, zoom: 16 });
              fetchAround(s.lat, s.lon, ADDRESS_RADIUS_M);
            }}
            placeholder="🔍 Tape une adresse (suggestions BAN)"
            disabled={loading}
          />
        </div>

        <div>
          <p className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
            Classes DPE cibles
          </p>
          <div className="flex flex-wrap gap-1">
            {DPE_CLASSES.map((c) => (
              <button
                key={c}
                onClick={() => toggle(dpeClasses, setDpeClasses, c)}
                className={cn(
                  "transition-transform",
                  dpeClasses.has(c)
                    ? "scale-110 ring-2 ring-foreground/70 ring-offset-1"
                    : "opacity-50 hover:opacity-100",
                )}
              >
                <DpeBadge classe={c} size="sm" />
              </button>
            ))}
          </div>
        </div>

        <div className="mt-2 flex items-center gap-2">
          <Button
            size="sm"
            onClick={searchArea}
            disabled={loading || zoom < ZOOM_MIN}
            className="h-8 flex-1 gap-1.5 text-xs"
          >
            {loading ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Search className="h-3.5 w-3.5" />}
            Rechercher cette zone
          </Button>
          <Button
            size="sm"
            variant={showAdvanced ? "default" : "outline"}
            onClick={() => setShowAdvanced((v) => !v)}
            className="h-8 px-2"
            title="Filtres avancés"
          >
            <SlidersHorizontal className="h-3.5 w-3.5" />
          </Button>
        </div>

        {showAdvanced ? (
          <div className="mt-2 space-y-2 border-t border-border/50 pt-2">
            <div>
              <p className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
                Classes GES
              </p>
              <div className="flex flex-wrap gap-1">
                {DPE_CLASSES.map((c) => (
                  <button
                    key={c}
                    onClick={() => toggle(gesClasses, setGesClasses, c)}
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
            </div>
            <div>
              <p className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
                Énergie de chauffage
              </p>
              <div className="flex flex-wrap gap-1">
                {ENERGIES.map(([key, label]) => (
                  <button
                    key={key}
                    onClick={() => toggle(energies, setEnergies, key)}
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
            </div>
            <div className="grid grid-cols-2 gap-2">
              <Input value={consoMin} onChange={(e) => setConsoMin(e.target.value)} placeholder="Conso min" inputMode="numeric" className="h-8 text-xs" />
              <Input value={consoMax} onChange={(e) => setConsoMax(e.target.value)} placeholder="Conso max" inputMode="numeric" className="h-8 text-xs" />
              <Input value={surfaceMin} onChange={(e) => setSurfaceMin(e.target.value)} placeholder="Surface min m²" inputMode="numeric" className="h-8 text-xs" />
              <Input value={surfaceMax} onChange={(e) => setSurfaceMax(e.target.value)} placeholder="Surface max m²" inputMode="numeric" className="h-8 text-xs" />
              <Input value={yearMin} onChange={(e) => setYearMin(e.target.value)} placeholder="Année min" inputMode="numeric" className="h-8 text-xs" />
              <Input value={yearMax} onChange={(e) => setYearMax(e.target.value)} placeholder="Année max" inputMode="numeric" className="h-8 text-xs" />
            </div>
            <Input value={dpeAncien} onChange={(e) => setDpeAncien(e.target.value)} placeholder="DPE de plus de N années" inputMode="numeric" className="h-8 text-xs" />
            <label className="flex cursor-pointer items-center gap-2 text-xs">
              <input
                type="checkbox"
                checked={isolationMurs}
                onChange={(e) => setIsolationMurs(e.target.checked)}
                className="h-3.5 w-3.5"
              />
              Isolation murs ≤ moyenne (cibles passoires)
            </label>
          </div>
        ) : null}

        <p className="mt-2 text-[10px] text-muted-foreground">
          {zoom < ZOOM_MIN ? (
            "Zoome ou tape une adresse pour cibler une zone, puis lance la recherche."
          ) : searched ? (
            <>
              <span className="font-bold text-foreground">{items.length.toLocaleString("fr-FR")}</span>{" "}
              affichés · {total.toLocaleString("fr-FR")} {typeLabelPluralFr} dans le rayon
            </>
          ) : (
            "Recadre la carte sur la zone voulue puis « Rechercher cette zone »."
          )}
        </p>
      </div>

      {/* Carte plein écran — pilotée par le viewport */}
      <MaisonsMap
        items={items}
        typeBatiment={typeBatiment}
        showLegend={false}
        autoFit={false}
        onBoundsChange={onBoundsChange}
        flyTo={flyTo}
        onSelectMaison={setSelected}
      />

      {/* État vide */}
      {!searched && !loading ? (
        <div className="pointer-events-none absolute left-1/2 top-24 z-10 -translate-x-1/2 rounded-lg border border-border bg-card/95 px-4 py-2 text-xs text-muted-foreground shadow-md">
          Tape une adresse ou zoome sur une zone, puis lance la recherche.
        </div>
      ) : null}
      </div>

      {/* Panneau latéral docké (style tertiaire — rétrécit la carte) */}
      {selected ? (
        <div className="h-full w-[420px] max-w-[calc(100vw-2rem)] shrink-0 border-l border-border">
          <MaisonDetailBody
            key={selected.numero_dpe}
            maison={selected}
            typeBatiment={typeBatiment}
            onClose={() => setSelected(null)}
          />
        </div>
      ) : null}
    </div>
  );
}
