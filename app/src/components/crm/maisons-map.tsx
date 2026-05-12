"use client";
import { useEffect, useRef, useState } from "react";
import maplibregl, { type Map, type MapGeoJSONFeature } from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { MaisonDetailSheet } from "@/components/crm/maison-detail-sheet";

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

export function MaisonsMap({ items }: { items: MaisonDpe[] }) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<Map | null>(null);
  const [ready, setReady] = useState(false);
  const [selected, setSelected] = useState<MaisonDpe | null>(null);
  const [sheetTrigger, setSheetTrigger] = useState(0);

  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;
    const container = containerRef.current;

    // MapLibre refuse de s'initialiser si le container fait 0×0 — on attend
    // que le layout flex propage une hauteur réelle avant d'instancier.
    let cancelled = false;
    let timers: number[] = [];

    const safeResize = () => {
      try { mapRef.current?.resize(); } catch {}
    };

    const tryInit = () => {
      if (cancelled || mapRef.current) return;
      const w = container.clientWidth;
      const h = container.clientHeight;
      if (w < 10 || h < 10) return;

      const m = new maplibregl.Map({
        container,
        style: "https://tiles.openfreemap.org/styles/liberty",
        center: [2.3522, 48.8566],
        zoom: 11,
      });
      mapRef.current = m;
      m.addControl(new maplibregl.NavigationControl({ visualizePitch: false }), "bottom-right");
      m.addControl(new maplibregl.ScaleControl({ unit: "metric" }), "bottom-left");
      timers = [50, 200, 500, 1000].map((d) => window.setTimeout(safeResize, d));

      m.on("error", (e) => {
        // eslint-disable-next-line no-console
        console.error("[MaisonsMap] map error", e);
      });

      m.on("load", () => {
        m.addSource("maisons", {
          type: "geojson",
          data: { type: "FeatureCollection", features: [] },
          cluster: true,
          clusterRadius: 40,
          clusterMaxZoom: 14,
        });
        m.addLayer({
          id: "clusters",
          type: "circle",
          source: "maisons",
          filter: ["has", "point_count"],
          paint: {
            "circle-color": ["step", ["get", "point_count"], "#3b82f6", 10, "#2563eb", 50, "#1d4ed8"],
            "circle-radius": ["step", ["get", "point_count"], 14, 10, 18, 50, 24],
            "circle-stroke-color": "#ffffff",
            "circle-stroke-width": 2,
          },
        });
        m.addLayer({
          id: "cluster-count",
          type: "symbol",
          source: "maisons",
          filter: ["has", "point_count"],
          layout: {
            "text-field": ["get", "point_count_abbreviated"],
            "text-size": 12,
            "text-font": ["Noto Sans Bold"],
            "text-allow-overlap": true,
          },
          paint: { "text-color": "#ffffff" },
        });
        m.addLayer({
          id: "maison-halo",
          type: "circle",
          source: "maisons",
          filter: ["!", ["has", "point_count"]],
          paint: {
            "circle-radius": ["interpolate", ["linear"], ["zoom"], 11, 6, 18, 16],
            "circle-color": [
              "match", ["coalesce", ["get", "dpe"], "NC"],
              "A", DPE_COLORS.A, "B", DPE_COLORS.B, "C", DPE_COLORS.C, "D", DPE_COLORS.D,
              "E", DPE_COLORS.E, "F", DPE_COLORS.F, "G", DPE_COLORS.G, DPE_COLORS.NC,
            ],
            "circle-opacity": 0.3,
          },
        });
        m.addLayer({
          id: "maison-dot",
          type: "circle",
          source: "maisons",
          filter: ["!", ["has", "point_count"]],
          paint: {
            "circle-radius": ["interpolate", ["linear"], ["zoom"], 11, 4, 18, 10],
            "circle-color": [
              "match", ["coalesce", ["get", "dpe"], "NC"],
              "A", DPE_COLORS.A, "B", DPE_COLORS.B, "C", DPE_COLORS.C, "D", DPE_COLORS.D,
              "E", DPE_COLORS.E, "F", DPE_COLORS.F, "G", DPE_COLORS.G, DPE_COLORS.NC,
            ],
            "circle-stroke-color": "#ffffff",
            "circle-stroke-width": 1.5,
          },
        });

        m.on("click", "maison-dot", (e) => {
          const f = e.features?.[0] as MapGeoJSONFeature | undefined;
          const numero = f?.properties?.numero_dpe;
          if (typeof numero === "string") {
            const found = (window as unknown as { __maisonsCache?: MaisonDpe[] }).__maisonsCache?.find(
              (x) => x.numero_dpe === numero,
            );
            if (found) {
              setSelected(found);
              setSheetTrigger((n) => n + 1);
            }
          }
        });

        m.on("click", "clusters", (e) => {
          const f = e.features?.[0];
          const clusterId = f?.properties?.cluster_id;
          if (clusterId == null) return;
          const src = m.getSource("maisons") as maplibregl.GeoJSONSource;
          src.getClusterExpansionZoom(clusterId).then((zoom) => {
            const c = (f!.geometry as GeoJSON.Point).coordinates as [number, number];
            m.easeTo({ center: c, zoom });
          });
        });

        m.on("mouseenter", "maison-dot", () => (m.getCanvas().style.cursor = "pointer"));
        m.on("mouseleave", "maison-dot", () => (m.getCanvas().style.cursor = ""));
        m.on("mouseenter", "clusters", () => (m.getCanvas().style.cursor = "pointer"));
        m.on("mouseleave", "clusters", () => (m.getCanvas().style.cursor = ""));

        setReady(true);
      });
    };

    const ro = new ResizeObserver(() => {
      if (!mapRef.current) tryInit();
      else safeResize();
    });
    ro.observe(container);
    window.addEventListener("resize", safeResize);
    tryInit(); // tentative immédiate

    return () => {
      cancelled = true;
      timers.forEach((id) => window.clearTimeout(id));
      ro.disconnect();
      window.removeEventListener("resize", safeResize);
      mapRef.current?.remove();
      mapRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Cache items globally so the click handler can resolve them
  useEffect(() => {
    (window as unknown as { __maisonsCache?: MaisonDpe[] }).__maisonsCache = items;
  }, [items]);

  // Update source data
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !ready) return;
    const src = map.getSource("maisons") as maplibregl.GeoJSONSource | undefined;
    if (!src) return;
    const features = items
      .filter((m) => m.lat != null && m.lon != null)
      .map((m) => ({
        type: "Feature" as const,
        geometry: { type: "Point" as const, coordinates: [m.lon!, m.lat!] },
        properties: {
          numero_dpe: m.numero_dpe,
          dpe: m.classe,
          address: m.address.label,
        },
      }));
    src.setData({ type: "FeatureCollection", features });

    // Auto-fit on first load
    if (features.length > 0) {
      const bounds = new maplibregl.LngLatBounds();
      for (const f of features) bounds.extend(f.geometry.coordinates as [number, number]);
      map.fitBounds(bounds, { padding: 50, maxZoom: 16, duration: 600 });
    }
  }, [items, ready]);

  return (
    <div
      style={{
        position: "absolute",
        top: 0,
        left: 0,
        right: 0,
        bottom: 0,
        width: "100%",
        height: "100%",
      }}
    >
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

      {/* DPE color legend */}
      <div className="absolute left-3 top-3 z-10 rounded-lg border border-border bg-card/95 p-2 shadow backdrop-blur">
        <p className="mb-1 text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
          Classe DPE
        </p>
        <div className="flex items-center gap-1">
          {(["A", "B", "C", "D", "E", "F", "G"] as const).map((c) => (
            <span
              key={c}
              className="flex h-6 w-6 items-center justify-center rounded text-[11px] font-black text-white"
              style={{ background: DPE_COLORS[c], color: c === "C" || c === "D" ? "#1f2937" : "#fff" }}
            >
              {c}
            </span>
          ))}
        </div>
      </div>

      {/* Hidden trigger to open the sheet imperatively when user clicks a marker */}
      {selected ? (
        <MaisonDetailSheet
          key={`${selected.numero_dpe}-${sheetTrigger}`}
          maison={selected}
          trigger={<button id="__maison_sheet_trigger" style={{ display: "none" }} />}
        />
      ) : null}
      {/* Auto-click the hidden trigger when selected changes */}
      <OpenSheetEffect dep={sheetTrigger} />
    </div>
  );
}

function OpenSheetEffect({ dep }: { dep: number }) {
  useEffect(() => {
    if (dep === 0) return;
    const btn = document.getElementById("__maison_sheet_trigger");
    if (btn) (btn as HTMLButtonElement).click();
  }, [dep]);
  return null;
}
