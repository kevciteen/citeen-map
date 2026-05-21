"use client";
import { useEffect, useRef, useState } from "react";
import maplibregl, { type Map } from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

const DPE_COLORS: Record<string, string> = {
  A: "#1f9d55",
  B: "#7cb342",
  C: "#cddc39",
  D: "#ffeb3b",
  E: "#ffb300",
  F: "#fb8c00",
  G: "#e53935",
  NC: "#94a3b8",
};

const SECTOR_ICONS: Record<string, string> = {
  Bureaux: "■",
  Commerces: "●",
  "Hotellerie / Restauration": "▲",
  Sante: "+",
  Enseignement: "♦",
  "Autres secteurs": "•",
};

export type TertiairePoint = {
  id: number;
  label: string | null;
  adresse: string | null;
  lat: number;
  lon: number;
  secteur: string | null;
  etiquette_dpe: string | null;
  surface_m2: number | null;
};

export type MapBounds = {
  minLat: number;
  maxLat: number;
  minLon: number;
  maxLon: number;
  zoom: number;
};

const BASE_STYLE_URL = "https://tiles.openfreemap.org/styles/liberty";

export function TertiaireMap({
  points,
  onBoundsChange,
  onSelectBuilding,
  selectedId,
  initialCenter,
}: {
  points: TertiairePoint[];
  onBoundsChange?: (b: MapBounds) => void;
  onSelectBuilding: (id: number) => void;
  selectedId?: number | null;
  initialCenter?: { lat: number; lon: number; zoom?: number };
}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<Map | null>(null);
  const [ready, setReady] = useState(false);

  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;
    const center = initialCenter
      ? ([initialCenter.lon, initialCenter.lat] as [number, number])
      : ([2.3522, 48.8566] as [number, number]);
    const map = new maplibregl.Map({
      container: containerRef.current,
      style: BASE_STYLE_URL,
      center,
      zoom: initialCenter?.zoom ?? 11,
    });
    mapRef.current = map;

    map.on("load", () => {
      map.addSource("tertiary", {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] },
        cluster: true,
        clusterMaxZoom: 14,
        clusterRadius: 40,
      });

      // Clusters
      map.addLayer({
        id: "tert-clusters",
        type: "circle",
        source: "tertiary",
        filter: ["has", "point_count"],
        paint: {
          "circle-color": [
            "step", ["get", "point_count"],
            "#3b82f6", 50, "#f59e0b", 200, "#ef4444",
          ],
          "circle-radius": [
            "step", ["get", "point_count"],
            16, 50, 22, 200, 28,
          ],
          "circle-stroke-width": 2,
          "circle-stroke-color": "#ffffff",
        },
      });
      map.addLayer({
        id: "tert-cluster-count",
        type: "symbol",
        source: "tertiary",
        filter: ["has", "point_count"],
        layout: {
          "text-field": "{point_count_abbreviated}",
          "text-size": 12,
          "text-font": ["Noto Sans Bold"],
        },
        paint: { "text-color": "#ffffff" },
      });

      // Unclustered points colorés par DPE
      map.addLayer({
        id: "tert-points",
        type: "circle",
        source: "tertiary",
        filter: ["!", ["has", "point_count"]],
        paint: {
          "circle-color": [
            "match", ["get", "dpe"],
            "A", DPE_COLORS.A, "B", DPE_COLORS.B, "C", DPE_COLORS.C,
            "D", DPE_COLORS.D, "E", DPE_COLORS.E, "F", DPE_COLORS.F, "G", DPE_COLORS.G,
            DPE_COLORS.NC,
          ],
          "circle-radius": [
            "case", ["==", ["get", "selected"], true], 10, 7,
          ],
          "circle-stroke-width": [
            "case", ["==", ["get", "selected"], true], 3, 2,
          ],
          "circle-stroke-color": "#1e293b",
        },
      });

      map.on("click", "tert-clusters", (e) => {
        const features = map.queryRenderedFeatures(e.point, { layers: ["tert-clusters"] });
        const clusterId = features[0]?.properties?.cluster_id;
        if (clusterId == null) return;
        const source = map.getSource("tertiary") as maplibregl.GeoJSONSource;
        source.getClusterExpansionZoom(clusterId).then((zoom) => {
          map.easeTo({ center: (features[0].geometry as GeoJSON.Point).coordinates as [number, number], zoom });
        });
      });

      map.on("click", "tert-points", (e) => {
        const f = e.features?.[0];
        const id = f?.properties?.id;
        if (typeof id === "number") onSelectBuilding(id);
        else if (typeof id === "string" && Number.isFinite(Number(id))) onSelectBuilding(Number(id));
      });

      map.on("mouseenter", "tert-points", () => { map.getCanvas().style.cursor = "pointer"; });
      map.on("mouseleave", "tert-points", () => { map.getCanvas().style.cursor = ""; });
      map.on("mouseenter", "tert-clusters", () => { map.getCanvas().style.cursor = "pointer"; });
      map.on("mouseleave", "tert-clusters", () => { map.getCanvas().style.cursor = ""; });

      const emitBounds = () => {
        const b = map.getBounds();
        onBoundsChange?.({
          minLat: b.getSouth(),
          maxLat: b.getNorth(),
          minLon: b.getWest(),
          maxLon: b.getEast(),
          zoom: map.getZoom(),
        });
      };
      map.on("moveend", emitBounds);
      emitBounds();
      setReady(true);
    });

    return () => {
      mapRef.current?.remove();
      mapRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Mise à jour des features
  useEffect(() => {
    if (!ready || !mapRef.current) return;
    const source = mapRef.current.getSource("tertiary") as maplibregl.GeoJSONSource | undefined;
    if (!source) return;
    const features: GeoJSON.Feature[] = points
      .filter(
        (p) =>
          typeof p.lat === "number" &&
          typeof p.lon === "number" &&
          Number.isFinite(p.lat) &&
          Number.isFinite(p.lon),
      )
      .map((p) => ({
        type: "Feature",
        geometry: { type: "Point", coordinates: [p.lon, p.lat] },
        properties: {
          id: p.id,
          label: p.label ?? p.adresse ?? `#${p.id}`,
          dpe: p.etiquette_dpe ?? "NC",
          secteur: p.secteur ?? "",
          surface: p.surface_m2 ?? null,
          selected: p.id === selectedId,
        },
      }));
    source.setData({ type: "FeatureCollection", features });
  }, [points, selectedId, ready]);

  return (
    <div className="relative h-full w-full">
      <div ref={containerRef} className="h-full w-full" />
      {!ready ? (
        <div className="absolute inset-0 flex items-center justify-center bg-secondary/30 text-sm text-muted-foreground">
          Chargement de la carte…
        </div>
      ) : null}
      {/* Légende DPE */}
      <div className="absolute right-3 bottom-3 rounded-lg border border-border bg-card/95 p-3 shadow-md backdrop-blur">
        <p className="mb-2 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
          Classe DPE
        </p>
        <div className="flex items-center gap-1">
          {(["A", "B", "C", "D", "E", "F", "G"] as const).map((c) => (
            <div key={c} className="flex flex-col items-center">
              <span
                className="inline-block h-4 w-4 rounded-full border-2 border-stone-700"
                style={{ backgroundColor: DPE_COLORS[c] }}
              />
              <span className="mt-0.5 text-[9px] font-semibold">{c}</span>
            </div>
          ))}
          <div className="ml-2 flex flex-col items-center">
            <span
              className="inline-block h-4 w-4 rounded-full border-2 border-stone-700"
              style={{ backgroundColor: DPE_COLORS.NC }}
            />
            <span className="mt-0.5 text-[9px] font-semibold">NC</span>
          </div>
        </div>
      </div>
    </div>
  );
}

// Référence non utilisée pour éviter un dead-code warning sur SECTOR_ICONS
export const _sectorIcons = SECTOR_ICONS;
