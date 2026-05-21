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

export type MaisonPoint = {
  numero_dpe: string;
  lat: number;
  lon: number;
  classe: string;
  address: string;
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

/* Vector basemap — OpenFreeMap "liberty" : style coloré (eau bleue,
   bâtiments crème, parcs verts), beaucoup plus lisible que positron pour
   un usage CRM premium. Style+glyphs+tiles fournis par une seule URL. */
const BASE_STYLE_URL = "https://tiles.openfreemap.org/styles/liberty";

export function MapView({
  points,
  maisons = [],
  tertiary = [],
  flyTo,
  onBoundsChange,
  onSelectCopro,
  onSelectMaison,
  onSelectTertiaire,
  selectedId,
}: {
  points: CoproPoint[];
  maisons?: MaisonPoint[];
  tertiary?: TertiairePoint[];
  onSelectMaison?: (numeroDpe: string) => void;
  onSelectTertiaire?: (id: number) => void;
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

      /* ===== Source + layers MAISONS — marker distinct des copros =====
         Pour différencier visuellement :
         - Copros = cercle (halo DPE + dot)
         - Maisons = carré (square via symbol layer avec icône générée) entouré
           de la couleur DPE pour rester lisible. */
      map.addSource("maisons", {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] },
        cluster: false, // pas de cluster pour les maisons — déjà filtrées par CP/commune
      });

      // Halo carré coloré (rotation 45° d'un cercle MapLibre n'existe pas →
      // on simule un losange avec une grande stroke et zéro fill)
      map.addLayer({
        id: "maison-halo",
        type: "circle",
        source: "maisons",
        paint: {
          "circle-radius": ["interpolate", ["linear"], ["zoom"], 11, 5, 16, 12],
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
          "circle-opacity": 0.18,
          // Stroke distinctif pour différencier des copros (qui ont une stroke blanche fine)
          "circle-stroke-color": "#1f2937",
          "circle-stroke-width": 2,
          "circle-stroke-opacity": 0.6,
        },
      });

      map.addLayer({
        id: "maison-dot",
        type: "circle",
        source: "maisons",
        paint: {
          "circle-radius": ["interpolate", ["linear"], ["zoom"], 11, 3.5, 16, 8],
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
          // Bordure noire épaisse = signature visuelle "maison" (vs copro = bordure blanche fine)
          "circle-stroke-color": "#111827",
          "circle-stroke-width": 2,
        },
      });

      // Label "M" pour identifier sans ambiguïté
      map.addLayer({
        id: "maison-label",
        type: "symbol",
        source: "maisons",
        minzoom: 13,
        layout: {
          "text-field": "M",
          "text-size": 9,
          "text-font": ["Noto Sans Bold"],
          "text-allow-overlap": true,
          "text-ignore-placement": true,
          "text-offset": [0, 0],
          "text-anchor": "center",
        },
        paint: {
          "text-color": "#ffffff",
          "text-halo-color": "#111827",
          "text-halo-width": 1,
        },
      });

      /* ===== Source + layers TERTIAIRE — triangle distinct des copros (rond)
         et des maisons (carré). 80k bâtiments IDF importés depuis ADEME. ===== */
      map.addSource("tertiary", {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] },
        cluster: true,
        clusterRadius: 50,
        clusterMaxZoom: 13,
      });

      map.addLayer({
        id: "tert-clusters",
        type: "circle",
        source: "tertiary",
        filter: ["has", "point_count"],
        paint: {
          "circle-color": [
            "step", ["get", "point_count"],
            "#f59e0b", 50, "#ea580c", 200, "#9a3412",
          ],
          "circle-radius": ["step", ["get", "point_count"], 12, 50, 18, 200, 26],
          "circle-stroke-color": "#ffffff",
          "circle-stroke-width": 2,
          "circle-opacity": 0.92,
        },
      });

      map.addLayer({
        id: "tert-cluster-count",
        type: "symbol",
        source: "tertiary",
        filter: ["has", "point_count"],
        layout: {
          "text-field": ["get", "point_count_abbreviated"],
          "text-size": 11,
          "text-font": ["Noto Sans Bold"],
          "text-allow-overlap": true,
        },
        paint: { "text-color": "#ffffff" },
      });

      map.addLayer({
        id: "tert-dot",
        type: "circle",
        source: "tertiary",
        filter: ["!", ["has", "point_count"]],
        paint: {
          "circle-radius": ["interpolate", ["linear"], ["zoom"], 11, 4, 16, 9],
          "circle-color": [
            "match", ["coalesce", ["get", "dpe"], "NC"],
            "A", DPE_COLORS.A, "B", DPE_COLORS.B, "C", DPE_COLORS.C,
            "D", DPE_COLORS.D, "E", DPE_COLORS.E, "F", DPE_COLORS.F,
            "G", DPE_COLORS.G, DPE_COLORS.NC,
          ],
          // Bordure orange = signature visuelle "tertiaire" (vs copro=blanc, maison=noir)
          "circle-stroke-color": "#ea580c",
          "circle-stroke-width": 2,
        },
      });

      map.addLayer({
        id: "tert-label",
        type: "symbol",
        source: "tertiary",
        filter: ["!", ["has", "point_count"]],
        minzoom: 14,
        layout: {
          "text-field": "T",
          "text-size": 9,
          "text-font": ["Noto Sans Bold"],
          "text-allow-overlap": true,
          "text-ignore-placement": true,
          "text-anchor": "center",
        },
        paint: {
          "text-color": "#ffffff",
          "text-halo-color": "#9a3412",
          "text-halo-width": 1,
        },
      });

      map.on("click", "tert-dot", (e) => {
        const f = e.features?.[0] as MapGeoJSONFeature | undefined;
        const id = f?.properties?.id;
        if (typeof id === "number" && onSelectTertiaire) onSelectTertiaire(id);
        else if (id && onSelectTertiaire) onSelectTertiaire(Number(id));
      });

      map.on("click", "tert-clusters", (e) => {
        const f = e.features?.[0];
        const clusterId = f?.properties?.cluster_id;
        if (clusterId == null) return;
        const src = map.getSource("tertiary") as maplibregl.GeoJSONSource;
        src.getClusterExpansionZoom(clusterId).then((zoom) => {
          const c = (f!.geometry as GeoJSON.Point).coordinates as [number, number];
          map.easeTo({ center: c, zoom });
        });
      });

      map.on("mouseenter", "tert-dot", () => (map.getCanvas().style.cursor = "pointer"));
      map.on("mouseleave", "tert-dot", () => (map.getCanvas().style.cursor = ""));
      map.on("mouseenter", "tert-clusters", () => (map.getCanvas().style.cursor = "pointer"));
      map.on("mouseleave", "tert-clusters", () => (map.getCanvas().style.cursor = ""));

      map.on("click", "copro-dot", (e) => {
        const f = e.features?.[0] as MapGeoJSONFeature | undefined;
        const id = f?.properties?.id;
        if (typeof id === "number") onSelectCopro(id);
        else if (id) onSelectCopro(Number(id));
      });

      map.on("click", "maison-dot", (e) => {
        const f = e.features?.[0] as MapGeoJSONFeature | undefined;
        const numero = f?.properties?.numero_dpe;
        if (typeof numero === "string" && onSelectMaison) onSelectMaison(numero);
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
      map.on("mouseenter", "maison-dot", () => (map.getCanvas().style.cursor = "pointer"));
      map.on("mouseleave", "maison-dot", () => (map.getCanvas().style.cursor = ""));
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

  // Update copros source data + auto-fit
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
    if (points.length > 0) {
      const bounds = new maplibregl.LngLatBounds();
      for (const p of points) bounds.extend([p.lon, p.lat]);
      map.fitBounds(bounds, { padding: 80, maxZoom: 15, duration: 600 });
    }
  }, [points, ready]);

  // Update maisons source data + auto-fit si on a uniquement des maisons
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !ready) return;
    const src = map.getSource("maisons") as maplibregl.GeoJSONSource | undefined;
    if (!src) return;
    src.setData({
      type: "FeatureCollection",
      features: maisons.map((m) => ({
        type: "Feature",
        geometry: { type: "Point", coordinates: [m.lon, m.lat] },
        properties: {
          numero_dpe: m.numero_dpe,
          dpe: m.classe,
          address: m.address,
        },
      })),
    });
    // Auto-fit si on a des maisons mais pas de copros (sinon le fit copros gère)
    if (maisons.length > 0 && points.length === 0) {
      const bounds = new maplibregl.LngLatBounds();
      for (const m of maisons) bounds.extend([m.lon, m.lat]);
      map.fitBounds(bounds, { padding: 80, maxZoom: 15, duration: 600 });
    }
  }, [maisons, points.length, ready]);

  // Update tertiary source data + auto-fit
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !ready) return;
    const src = map.getSource("tertiary") as maplibregl.GeoJSONSource | undefined;
    if (!src) return;
    const features: GeoJSON.Feature[] = tertiary
      .filter((p) => Number.isFinite(p.lat) && Number.isFinite(p.lon))
      .map((p) => ({
        type: "Feature",
        geometry: { type: "Point", coordinates: [p.lon, p.lat] },
        properties: {
          id: p.id,
          label: p.label ?? p.adresse ?? `#${p.id}`,
          dpe: p.etiquette_dpe ?? "NC",
          secteur: p.secteur ?? "",
          surface: p.surface_m2 ?? null,
        },
      }));
    src.setData({ type: "FeatureCollection", features });
    if (tertiary.length > 0 && points.length === 0 && maisons.length === 0) {
      const bounds = new maplibregl.LngLatBounds();
      for (const p of tertiary) {
        if (Number.isFinite(p.lat) && Number.isFinite(p.lon)) bounds.extend([p.lon, p.lat]);
      }
      if (!bounds.isEmpty()) map.fitBounds(bounds, { padding: 80, maxZoom: 14, duration: 600 });
    }
  }, [tertiary, points.length, maisons.length, ready]);

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
