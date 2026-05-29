"use client";
/**
 * Carte MapLibre dédiée à l'annuaire unifié.
 *
 * Affiche les entités directory en cercles colorés par type :
 *   - copro      : bleu (immeuble résidentiel)
 *   - occupant   : orange (société tertiaire)
 *   - syndic     : vert émeraude (entreprise de gestion)
 *   - prospect_custom : violet (adresse libre)
 *
 * Émet `onBoundsChange` au pan/zoom pour que le parent puisse refetch
 * avec un filtre bbox (debounced ~400ms).
 *
 * Volontairement plus simple que le MapView principal (qui gère 3 types
 * spécifiques copros/maisons/tertiaire avec DPE coloring) — on peut faire
 * converger plus tard.
 */
import { useEffect, useRef } from "react";
import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

const BASE_STYLE_URL = "https://tiles.openfreemap.org/styles/liberty";

export const TYPE_COLORS: Record<string, string> = {
  copro: "#2563eb",            // blue-600
  occupant: "#f97316",         // orange-500
  syndic: "#10b981",           // emerald-500
  prospect_custom: "#a855f7",  // purple-500
};

export type AnnuaireMapPoint = {
  id: number;
  entity_type: string;
  entity_ref: string;
  display_name: string;
  display_subtitle: string | null;
  lat: number;
  lon: number;
  phone: string | null;
  email: string | null;
  website: string | null;
};

export type MapBounds = {
  minLat: number;
  maxLat: number;
  minLon: number;
  maxLon: number;
};

const SOURCE_ID = "directory-src";
const LAYER_ID = "directory-circles";

export function AnnuaireMap({
  points,
  onBoundsChange,
  onSelect,
}: {
  points: AnnuaireMapPoint[];
  onBoundsChange?: (b: MapBounds) => void;
  onSelect?: (key: string) => void;
}) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const popupRef = useRef<maplibregl.Popup | null>(null);
  const boundsTimerRef = useRef<number | null>(null);

  // Init carte (1 fois)
  useEffect(() => {
    if (mapRef.current || !containerRef.current) return;
    const map = new maplibregl.Map({
      container: containerRef.current,
      style: BASE_STYLE_URL,
      center: [2.3522, 48.8566], // Paris
      zoom: 11,
      attributionControl: false,
    });
    map.addControl(
      new maplibregl.NavigationControl({ showCompass: false }),
      "top-right",
    );
    map.addControl(
      new maplibregl.AttributionControl({ compact: true }),
      "bottom-right",
    );

    map.on("load", () => {
      map.addSource(SOURCE_ID, {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] },
      });
      map.addLayer({
        id: LAYER_ID,
        type: "circle",
        source: SOURCE_ID,
        paint: {
          "circle-radius": [
            "interpolate", ["linear"], ["zoom"],
            10, 4, 14, 7, 16, 10,
          ],
          "circle-color": [
            "match",
            ["get", "entity_type"],
            "copro", TYPE_COLORS.copro,
            "occupant", TYPE_COLORS.occupant,
            "syndic", TYPE_COLORS.syndic,
            "prospect_custom", TYPE_COLORS.prospect_custom,
            "#64748b",
          ],
          "circle-stroke-width": 1.5,
          "circle-stroke-color": "#ffffff",
          "circle-opacity": 0.9,
        },
      });
    });

    // Click → popup
    map.on("click", LAYER_ID, (e) => {
      const f = e.features?.[0];
      if (!f || f.geometry.type !== "Point") return;
      const p = f.properties as {
        display_name: string;
        display_subtitle: string | null;
        entity_type: string;
        entity_ref: string;
        phone: string | null;
        email: string | null;
        website: string | null;
      };
      const key = `${p.entity_type}-${p.entity_ref}`;
      onSelect?.(key);

      const detailHref = getDetailHref(p.entity_type, p.entity_ref);
      const [lon, lat] = (f.geometry.coordinates as [number, number]);

      const channels: string[] = [];
      if (p.phone) channels.push(`📞 <a href="tel:${p.phone}">${escapeHtml(p.phone)}</a>`);
      if (p.email)
        channels.push(`✉️ <a href="mailto:${p.email}">${escapeHtml(p.email)}</a>`);
      if (p.website) {
        const url = p.website.startsWith("http") ? p.website : `https://${p.website}`;
        channels.push(
          `🌐 <a href="${escapeHtml(url)}" target="_blank" rel="noreferrer">Site</a>`,
        );
      }

      const html = `
        <div style="min-width:200px;font-family:system-ui,sans-serif;">
          <div style="font-weight:600;color:#0f172a;">${escapeHtml(p.display_name)}</div>
          ${p.display_subtitle ? `<div style="font-size:11px;color:#64748b;">${escapeHtml(p.display_subtitle)}</div>` : ""}
          ${channels.length > 0 ? `<div style="margin-top:6px;font-size:11px;display:flex;flex-direction:column;gap:2px;">${channels.join("")}</div>` : ""}
          ${detailHref ? `<a href="${detailHref}" style="display:inline-block;margin-top:8px;font-size:11px;font-weight:600;color:#2563eb;text-decoration:none;">Ouvrir la fiche →</a>` : ""}
        </div>
      `;

      popupRef.current?.remove();
      popupRef.current = new maplibregl.Popup({ offset: 12, closeButton: true })
        .setLngLat([lon, lat])
        .setHTML(html)
        .addTo(map);
    });

    map.on("mouseenter", LAYER_ID, () => {
      map.getCanvas().style.cursor = "pointer";
    });
    map.on("mouseleave", LAYER_ID, () => {
      map.getCanvas().style.cursor = "";
    });

    const handleBounds = () => {
      if (!onBoundsChange) return;
      const b = map.getBounds();
      // Debounce ~400ms pour éviter de spammer le parent
      if (boundsTimerRef.current != null) window.clearTimeout(boundsTimerRef.current);
      boundsTimerRef.current = window.setTimeout(() => {
        onBoundsChange({
          minLat: b.getSouth(),
          maxLat: b.getNorth(),
          minLon: b.getWest(),
          maxLon: b.getEast(),
        });
      }, 400);
    };
    map.on("moveend", handleBounds);
    map.on("zoomend", handleBounds);

    mapRef.current = map;
    return () => {
      if (boundsTimerRef.current != null) window.clearTimeout(boundsTimerRef.current);
      map.remove();
      mapRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Update source data quand points change
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    const src = map.getSource(SOURCE_ID) as maplibregl.GeoJSONSource | undefined;
    if (!src) {
      const onLoad = () => updateSource(map, points);
      map.once("load", onLoad);
      return;
    }
    updateSource(map, points);
  }, [points]);

  return (
    <div
      ref={containerRef}
      className="h-[480px] w-full overflow-hidden rounded-xl border border-border bg-card shadow-sm"
    />
  );
}

function updateSource(map: maplibregl.Map, points: AnnuaireMapPoint[]) {
  const src = map.getSource(SOURCE_ID) as maplibregl.GeoJSONSource | undefined;
  if (!src) return;
  const features = points
    .filter((p) => Number.isFinite(p.lat) && Number.isFinite(p.lon))
    .map((p) => ({
      type: "Feature" as const,
      geometry: { type: "Point" as const, coordinates: [p.lon, p.lat] },
      properties: {
        display_name: p.display_name,
        display_subtitle: p.display_subtitle,
        entity_type: p.entity_type,
        entity_ref: p.entity_ref,
        phone: p.phone,
        email: p.email,
        website: p.website,
      },
    }));
  src.setData({ type: "FeatureCollection", features });
}

function getDetailHref(entity_type: string, entity_ref: string): string | null {
  switch (entity_type) {
    case "copro":
      return `/copros/${entity_ref}`;
    case "syndic":
      return `/syndics/${entity_ref}`;
    case "occupant":
      return null;
    case "prospect_custom":
      return `/prospects/${entity_ref}`;
    default:
      return null;
  }
}

function escapeHtml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}
