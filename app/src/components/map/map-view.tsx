"use client";
import { useEffect, useRef, useState } from "react";
import maplibregl, { type Map, type MapGeoJSONFeature } from "maplibre-gl";
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

export type CoproPoint = {
  id: number;
  numero_immatriculation: string;
  nom_copro: string | null;
  adresse: string | null;
  code_postal: string | null;
  commune: string | null;
  syndic: string | null;
  lat: number;
  lon: number;
  classe_finale?: string | null;
};

export type MapBounds = {
  minLat: number;
  maxLat: number;
  minLon: number;
  maxLon: number;
  zoom: number;
};

/* Vector basemap — OpenFreeMap "liberty" : style coloré (eau bleue,
   bâtiments crème, parcs verts), beaucoup plus lisible que positron pour
   un usage CRM premium. Style+glyphs+tiles fournis par une seule URL. */
const BASE_STYLE_URL = "https://tiles.openfreemap.org/styles/liberty";

export function MapView({
  points,
  flyTo,
  onBoundsChange,
  onSelectCopro,
  selectedId,
}: {
  points: CoproPoint[];
  flyTo?: { lat: number; lon: number; zoom?: number } | null;
  onBoundsChange?: (b: MapBounds) => void;
  onSelectCopro: (id: number) => void;
  selectedId?: number | null;
}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<Map | null>(null);
  const [ready, setReady] = useState(false);

  useEffect(() => {
    /* eslint-disable no-console */
    console.log("[map] effect mount, container=", containerRef.current);
    if (!containerRef.current) {
      console.error("[map] containerRef.current is null — aborting init");
      return;
    }
    if (mapRef.current) {
      console.log("[map] already initialized — skipping");
      return;
    }

    let map: Map;
    try {
      map = new maplibregl.Map({
        container: containerRef.current,
        style: BASE_STYLE_URL,
        center: [2.3522, 48.8566],
        zoom: 11,
      });
      console.log("[map] instance created", map);
    } catch (err) {
      console.error("[map] failed to construct:", err);
      return;
    }
    mapRef.current = map;

    map.on("error", (e: maplibregl.ErrorEvent) => {
      console.warn("[map error]", e?.error?.message || e);
    });
    map.on("styledata", () => console.log("[map] styledata"));
    map.on("load", () => console.log("[map] load fired"));

    map.addControl(new maplibregl.NavigationControl({ visualizePitch: false }), "bottom-right");
    map.addControl(new maplibregl.ScaleControl({ unit: "metric" }), "bottom-left");

    /* MapLibre calcule la taille du canvas WebGL au mount. Si le flexbox parent
       grandit après (chargement async, ouverture DevTools, redimensionnement),
       le canvas reste petit → laisse un bandeau blanc. Triple-bretelle :
       1) ResizeObserver sur le container (mutations CSS)
       2) Listener sur window resize (Devtools / fenêtre)
       3) Force resize sur quelques frames consécutives au mount (latence flex) */
    const safeResize = () => {
      try {
        map.resize();
      } catch {
        /* ignore — map déjà détachée */
      }
    };
    const ro = new ResizeObserver(safeResize);
    ro.observe(containerRef.current);
    window.addEventListener("resize", safeResize);

    // Force-resize sur les premières 1.5s pour rattraper les remontées flex async
    const resizeTimers: number[] = [];
    for (const delay of [50, 150, 350, 700, 1200, 1800]) {
      resizeTimers.push(window.setTimeout(safeResize, delay));
    }

    map.on("load", () => {
      map.addSource("copros", {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] },
        cluster: true,
        clusterRadius: 45,
        clusterMaxZoom: 14,
      });

      map.addLayer({
        id: "clusters",
        type: "circle",
        source: "copros",
        filter: ["has", "point_count"],
        paint: {
          "circle-color": [
            "step",
            ["get", "point_count"],
            "#3b82f6",
            10,
            "#2563eb",
            50,
            "#1d4ed8",
            200,
            "#1e3a8a",
          ],
          "circle-radius": ["step", ["get", "point_count"], 14, 10, 18, 50, 24, 200, 32],
          "circle-stroke-color": "#ffffff",
          "circle-stroke-width": 2,
          "circle-opacity": 0.92,
        },
      });

      map.addLayer({
        id: "cluster-count",
        type: "symbol",
        source: "copros",
        filter: ["has", "point_count"],
        layout: {
          "text-field": ["get", "point_count_abbreviated"],
          "text-size": 12,
          "text-font": ["Noto Sans Bold"],
          "text-allow-overlap": true,
        },
        paint: { "text-color": "#ffffff" },
      });

      map.addLayer({
        id: "copro-halo",
        type: "circle",
        source: "copros",
        filter: ["!", ["has", "point_count"]],
        paint: {
          "circle-radius": ["interpolate", ["linear"], ["zoom"], 11, 4, 16, 11],
          "circle-color": [
            "match",
            ["coalesce", ["get", "dpe"], "NC"],
            "A", DPE_COLORS.A,
            "B", DPE_COLORS.B,
            "C", DPE_COLORS.C,
            "D", DPE_COLORS.D,
            "E", DPE_COLORS.E,
            "F", DPE_COLORS.F,
            "G", DPE_COLORS.G,
            DPE_COLORS.NC,
          ],
          "circle-opacity": 0.25,
        },
      });

      map.addLayer({
        id: "copro-dot",
        type: "circle",
        source: "copros",
        filter: ["!", ["has", "point_count"]],
        paint: {
          "circle-radius": ["interpolate", ["linear"], ["zoom"], 11, 3, 16, 7],
          "circle-color": [
            "match",
            ["coalesce", ["get", "dpe"], "NC"],
            "A", DPE_COLORS.A,
            "B", DPE_COLORS.B,
            "C", DPE_COLORS.C,
            "D", DPE_COLORS.D,
            "E", DPE_COLORS.E,
            "F", DPE_COLORS.F,
            "G", DPE_COLORS.G,
            DPE_COLORS.NC,
          ],
          "circle-stroke-color": "#ffffff",
          "circle-stroke-width": [
            "case",
            ["boolean", ["feature-state", "selected"], false],
            3,
            1.5,
          ],
        },
      });

      map.on("click", "copro-dot", (e) => {
        const f = e.features?.[0] as MapGeoJSONFeature | undefined;
        const id = f?.properties?.id;
        if (typeof id === "number") onSelectCopro(id);
        else if (id) onSelectCopro(Number(id));
      });

      map.on("click", "clusters", (e) => {
        const f = e.features?.[0];
        const clusterId = f?.properties?.cluster_id;
        if (clusterId == null) return;
        const src = map.getSource("copros") as maplibregl.GeoJSONSource;
        src.getClusterExpansionZoom(clusterId).then((zoom) => {
          const c = (f!.geometry as GeoJSON.Point).coordinates as [number, number];
          map.easeTo({ center: c, zoom });
        });
      });

      map.on("mouseenter", "copro-dot", () => (map.getCanvas().style.cursor = "pointer"));
      map.on("mouseleave", "copro-dot", () => (map.getCanvas().style.cursor = ""));
      map.on("mouseenter", "clusters", () => (map.getCanvas().style.cursor = "pointer"));
      map.on("mouseleave", "clusters", () => (map.getCanvas().style.cursor = ""));

      const fireBounds = () => {
        const b = map.getBounds();
        onBoundsChange?.({
          minLat: b.getSouth(),
          maxLat: b.getNorth(),
          minLon: b.getWest(),
          maxLon: b.getEast(),
          zoom: map.getZoom(),
        });
      };
      map.on("moveend", fireBounds);
      setReady(true);
      fireBounds();
    });

    return () => {
      resizeTimers.forEach((id) => window.clearTimeout(id));
      ro.disconnect();
      window.removeEventListener("resize", safeResize);
      map.remove();
      mapRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Update source data
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !ready) return;
    const src = map.getSource("copros") as maplibregl.GeoJSONSource | undefined;
    if (!src) return;
    src.setData({
      type: "FeatureCollection",
      features: points.map((p) => ({
        type: "Feature",
        geometry: { type: "Point", coordinates: [p.lon, p.lat] },
        properties: {
          id: p.id,
          nom: p.nom_copro,
          adr: p.adresse,
          cp: p.code_postal,
          ville: p.commune,
          dpe: p.classe_finale ?? "NC",
        },
      })),
    });
  }, [points, ready]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !flyTo) return;
    map.flyTo({
      center: [flyTo.lon, flyTo.lat],
      zoom: flyTo.zoom ?? 16,
      speed: 1.4,
    });
  }, [flyTo]);

  // Mark selected
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !ready) return;
    // simple visual cue handled by stroke-width via feature-state; we set on currently visible features
    if (selectedId == null) return;
    const features = map.querySourceFeatures("copros", { filter: ["==", ["get", "id"], selectedId] });
    for (const f of features) {
      if (f.id != null) map.setFeatureState({ source: "copros", id: f.id }, { selected: true });
    }
  }, [selectedId, ready]);

  return (
    <div
      ref={containerRef}
      style={{
        position: "absolute",
        top: 0,
        left: 0,
        right: 0,
        bottom: 0,
        width: "100%",
        height: "100%",
        background: "#eef2f7",
      }}
    />
  );
}
