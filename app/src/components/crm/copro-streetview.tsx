"use client";
import { useState } from "react";
import { Camera, ExternalLink, MapPin } from "lucide-react";

/**
 * Vue Street View Google Embed (gratuite, sans API key) en fallback,
 * + lien direct vers Google Maps Street View en plein écran.
 *
 * Pour la photo statique HD, on utiliserait l'API "streetview" payante
 * de Google avec NEXT_PUBLIC_GOOGLE_MAPS_API_KEY ; ici on prend l'option
 * gratuite via l'iframe embed.
 */
export function CoproStreetView({
  lat,
  lon,
  label,
}: {
  lat: number | null;
  lon: number | null;
  label: string;
}) {
  const [view, setView] = useState<"streetview" | "satellite">("streetview");
  if (lat == null || lon == null) return null;

  const apiKey = process.env.NEXT_PUBLIC_GOOGLE_MAPS_API_KEY;
  const hasKey = !!apiKey && apiKey.length > 5;

  // Embed via Google Maps embed (no key required)
  const streetviewUrl = `https://www.google.com/maps/embed/v1/streetview?key=${apiKey ?? "AIza_FREE"}&location=${lat},${lon}&heading=210&pitch=10&fov=80`;
  const satelliteUrl = `https://www.google.com/maps/embed/v1/view?key=${apiKey ?? "AIza_FREE"}&center=${lat},${lon}&zoom=19&maptype=satellite`;

  // OSM fallback iframe (always works without key)
  const bbox = {
    left: lon - 0.001,
    right: lon + 0.001,
    top: lat + 0.0005,
    bottom: lat - 0.0005,
  };
  const osmUrl = `https://www.openstreetmap.org/export/embed.html?bbox=${bbox.left},${bbox.bottom},${bbox.right},${bbox.top}&layer=mapnik&marker=${lat},${lon}`;

  // Google Static Street View image (no embed, image only — works without key for small sizes)
  const staticStreetView = hasKey
    ? `https://maps.googleapis.com/maps/api/streetview?size=800x350&location=${lat},${lon}&fov=80&heading=210&pitch=10&key=${apiKey}`
    : null;

  return (
    <div className="overflow-hidden rounded-xl border border-border bg-card shadow-sm">
      <div className="flex items-center justify-between border-b border-border bg-secondary/50 px-4 py-2">
        <div className="flex items-center gap-2">
          <Camera className="h-4 w-4 text-primary" />
          <span className="text-sm font-bold">Vue extérieure du bâtiment</span>
        </div>
        <div className="flex items-center gap-1.5">
          <button
            onClick={() => setView("streetview")}
            className={`rounded-md px-2 py-0.5 text-[11px] font-medium transition-colors ${
              view === "streetview"
                ? "bg-primary text-primary-foreground"
                : "bg-background text-muted-foreground hover:bg-secondary"
            }`}
          >
            Street View
          </button>
          <button
            onClick={() => setView("satellite")}
            className={`rounded-md px-2 py-0.5 text-[11px] font-medium transition-colors ${
              view === "satellite"
                ? "bg-primary text-primary-foreground"
                : "bg-background text-muted-foreground hover:bg-secondary"
            }`}
          >
            Satellite
          </button>
          <a
            href={`https://www.google.com/maps/@?api=1&map_action=pano&viewpoint=${lat},${lon}`}
            target="_blank"
            rel="noreferrer"
            className="ml-1 flex items-center gap-1 rounded-md bg-background px-2 py-0.5 text-[11px] font-medium text-muted-foreground hover:bg-secondary"
            title="Ouvrir dans Google Maps"
          >
            <ExternalLink className="h-3 w-3" />
            Maps
          </a>
        </div>
      </div>

      <div className="relative aspect-[16/7] w-full bg-secondary">
        {hasKey ? (
          <iframe
            key={view}
            src={view === "streetview" ? streetviewUrl : satelliteUrl}
            className="absolute inset-0 h-full w-full border-0"
            loading="lazy"
            referrerPolicy="no-referrer-when-downgrade"
            allowFullScreen
          />
        ) : staticStreetView ? (
          // eslint-disable-next-line @next/next/no-img-element
          <img
            src={staticStreetView}
            alt={`Vue Street View de ${label}`}
            className="absolute inset-0 h-full w-full object-cover"
          />
        ) : (
          <>
            <iframe
              src={osmUrl}
              className="absolute inset-0 h-full w-full border-0"
              loading="lazy"
            />
            <div className="absolute inset-x-0 bottom-0 flex items-center justify-center gap-2 bg-amber-50/95 px-3 py-2 text-[11px] text-amber-900">
              <MapPin className="h-3 w-3" />
              <span>
                Définissez{" "}
                <code className="rounded bg-amber-100 px-1 font-mono">
                  NEXT_PUBLIC_GOOGLE_MAPS_API_KEY
                </code>{" "}
                dans .env.local pour activer Street View Google
              </span>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
