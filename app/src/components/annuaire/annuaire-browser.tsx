"use client";
import { useState } from "react";
import Link from "next/link";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import {
  Loader2, Search, MapPin, Phone, Mail, Globe, Filter,
  Building2, Briefcase, IdCard, Users, Map as MapIcon, List,
} from "lucide-react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { useDebouncedValue } from "@/lib/hooks/use-debounced-value";
import { jsonFetcher } from "@/lib/fetcher";
import {
  AnnuaireMap,
  TYPE_COLORS,
  type AnnuaireMapPoint,
  type MapBounds,
} from "./annuaire-map";

type DirectoryRow = {
  id: number;
  entity_type: "occupant" | "copro" | "syndic" | "prospect_custom";
  entity_ref: string;
  display_name: string;
  display_subtitle: string | null;
  address: string | null;
  postcode: string | null;
  city: string | null;
  departement: string | null;
  lat: number | null;
  lon: number | null;
  coords_source: string | null;
  phone: string | null;
  phone_source: string | null;
  email: string | null;
  email_source: string | null;
  website: string | null;
  website_source: string | null;
  parent_copro_id: number | null;
  parent_building_id: number | null;
  synced_at: number;
};

type TypeFilter = "all" | "copro" | "occupant" | "syndic" | "prospect_custom";

const TYPE_LABELS: Record<DirectoryRow["entity_type"], string> = {
  copro: "Copropriété",
  occupant: "Société tertiaire",
  syndic: "Syndic",
  prospect_custom: "Adresse libre",
};

const TYPE_ICONS: Record<DirectoryRow["entity_type"], typeof Building2> = {
  copro: Building2,
  occupant: Briefcase,
  syndic: IdCard,
  prospect_custom: Users,
};

export function AnnuaireBrowser() {
  const [q, setQ] = useState("");
  const [cp, setCp] = useState("");
  const [type, setType] = useState<TypeFilter>("all");
  const [onlyWithContact, setOnlyWithContact] = useState(false);
  const [onlyWithCoords, setOnlyWithCoords] = useState(false);
  const [showMap, setShowMap] = useState(true);
  const [bounds, setBounds] = useState<MapBounds | null>(null);
  const [highlighted, setHighlighted] = useState<string | null>(null);

  // Debounce 250ms sur les inputs texte : pas de fetch tant que le user
  // tape encore. Les CP et toggles changent rarement → pas debounced.
  const debouncedQ = useDebouncedValue(q, 250);
  const debouncedCp = useDebouncedValue(cp, 250);

  const listQuery = useQuery({
    queryKey: [
      "directory-list",
      debouncedQ.trim(),
      debouncedCp.trim(),
      type,
      onlyWithContact,
      onlyWithCoords || showMap,
      showMap && bounds
        ? `${bounds.minLat.toFixed(4)},${bounds.maxLat.toFixed(4)},${bounds.minLon.toFixed(4)},${bounds.maxLon.toFixed(4)}`
        : "",
      showMap ? 500 : 300,
    ],
    queryFn: ({ signal }) => {
      const url = new URL("/api/directory", window.location.origin);
      url.searchParams.set("limit", showMap ? "500" : "300");
      if (debouncedQ.trim()) url.searchParams.set("q", debouncedQ.trim());
      if (debouncedCp.trim()) url.searchParams.set("cp", debouncedCp.trim());
      if (type !== "all") url.searchParams.set("types", type);
      if (onlyWithContact) url.searchParams.set("onlyWithContact", "1");
      if (onlyWithCoords || showMap) url.searchParams.set("onlyWithCoords", "1");
      if (showMap && bounds) {
        url.searchParams.set("minLat", String(bounds.minLat));
        url.searchParams.set("maxLat", String(bounds.maxLat));
        url.searchParams.set("minLon", String(bounds.minLon));
        url.searchParams.set("maxLon", String(bounds.maxLon));
      }
      return jsonFetcher<{ count: number; items: DirectoryRow[] }>(
        url.toString(),
        signal,
      );
    },
    placeholderData: keepPreviousData, // pas de flash à vide entre 2 fetches
  });

  // Stats : très long staleTime — change rarement, peut être servi du cache
  const statsQuery = useQuery({
    queryKey: ["directory-stats"],
    queryFn: ({ signal }) =>
      jsonFetcher<{ total: number }>("/api/directory/stats", signal),
    staleTime: 60 * 1000,
  });

  const items: DirectoryRow[] = listQuery.data?.items ?? [];
  const total = statsQuery.data?.total ?? null;
  const isFetching = listQuery.isFetching;
  const isInitialLoad = listQuery.isPending;

  // Points pour la carte = items ayant des coordonnées
  const mapPoints: AnnuaireMapPoint[] = items
    .filter((i) => i.lat != null && i.lon != null)
    .map((i) => ({
      id: i.id,
      entity_type: i.entity_type,
      entity_ref: i.entity_ref,
      display_name: i.display_name,
      display_subtitle: i.display_subtitle,
      lat: i.lat as number,
      lon: i.lon as number,
      phone: i.phone,
      email: i.email,
      website: i.website,
    }));

  return (
    <div className="space-y-4">
      {/* Toolbar filtres */}
      <div className="space-y-3 rounded-xl border border-border bg-card p-4 shadow-sm">
        <div className="flex flex-wrap items-center gap-2">
          <div className="relative flex-1 min-w-[200px]">
            <Search className="absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              placeholder="Rechercher nom ou adresse…"
              value={q}
              onChange={(e) => setQ(e.target.value)}
              className="pl-8"
            />
          </div>
          <Input
            placeholder="CP"
            value={cp}
            onChange={(e) => setCp(e.target.value)}
            className="w-24"
            maxLength={5}
          />
          <select
            value={type}
            onChange={(e) => setType(e.target.value as TypeFilter)}
            className="rounded-md border border-input bg-background px-2 py-1.5 text-sm"
          >
            <option value="all">Tous types</option>
            <option value="copro">Copropriétés</option>
            <option value="occupant">Sociétés tertiaires</option>
            <option value="syndic">Syndics</option>
            <option value="prospect_custom">Adresses libres</option>
          </select>
        </div>

        <div className="flex flex-wrap items-center gap-3 text-xs">
          <label className="flex items-center gap-1.5 cursor-pointer">
            <input
              type="checkbox"
              checked={onlyWithContact}
              onChange={(e) => setOnlyWithContact(e.target.checked)}
              className="rounded"
            />
            Avec contact (tel/mail/site)
          </label>
          <label className="flex items-center gap-1.5 cursor-pointer">
            <input
              type="checkbox"
              checked={onlyWithCoords}
              onChange={(e) => setOnlyWithCoords(e.target.checked)}
              className="rounded"
              disabled={showMap}
            />
            Avec coordonnées GPS
            {showMap ? (
              <span className="text-[10px] text-muted-foreground">
                (implicite avec carte)
              </span>
            ) : null}
          </label>
          <Button
            variant={showMap ? "default" : "outline"}
            size="sm"
            onClick={() => setShowMap((v) => !v)}
            className="ml-auto"
          >
            {showMap ? (
              <>
                <List className="h-4 w-4" /> Vue liste
              </>
            ) : (
              <>
                <MapIcon className="h-4 w-4" /> Vue carte
              </>
            )}
          </Button>
          <div className="text-muted-foreground">
            <strong className="text-foreground">{items.length}</strong> affichés
            {total !== null ? (
              <span> · {total.toLocaleString("fr-FR")} en annuaire</span>
            ) : null}
            {isFetching && !isInitialLoad ? (
              <Loader2 className="ml-1.5 inline h-3 w-3 animate-spin text-muted-foreground" />
            ) : null}
          </div>
        </div>
      </div>

      {/* Carte */}
      {showMap ? (
        <>
          <AnnuaireMap
            points={mapPoints}
            onBoundsChange={setBounds}
            onSelect={(key) => {
              setHighlighted(key);
              const el = document.getElementById(`row-${key}`);
              if (el) el.scrollIntoView({ behavior: "smooth", block: "center" });
            }}
          />
          <div className="flex flex-wrap gap-3 text-[11px] text-muted-foreground">
            {Object.entries(TYPE_COLORS).map(([k, color]) => (
              <span key={k} className="flex items-center gap-1.5">
                <span
                  className="inline-block h-2.5 w-2.5 rounded-full"
                  style={{ background: color }}
                />
                {TYPE_LABELS[k as DirectoryRow["entity_type"]]}
              </span>
            ))}
          </div>
        </>
      ) : null}

      {/* Résultats */}
      {isInitialLoad ? (
        <div className="space-y-2">
          {Array.from({ length: 5 }).map((_, i) => (
            <div
              key={i}
              className="flex gap-3 rounded-lg border border-border bg-card p-3 shadow-sm"
            >
              <Skeleton className="h-8 w-8 shrink-0 rounded-md" />
              <div className="flex-1 space-y-1.5">
                <Skeleton className="h-3.5 w-2/3" />
                <Skeleton className="h-3 w-1/3" />
                <Skeleton className="h-3 w-4/5" />
              </div>
            </div>
          ))}
        </div>
      ) : null}

      {!isInitialLoad && items.length === 0 ? (
        <div className="rounded-xl border border-border bg-card p-8 text-center text-sm text-muted-foreground">
          <Filter className="mx-auto mb-2 h-5 w-5" />
          Aucun résultat.{" "}
          {total === 0 ? (
            <>
              L&apos;annuaire est vide — un admin doit déclencher la sync
              initiale via{" "}
              <Link href="/admin/coords-health" className="text-primary hover:underline">
                Santé coordonnées
              </Link>
              .
            </>
          ) : (
            "Élargis les filtres."
          )}
        </div>
      ) : null}

      {!isInitialLoad ? (
        <div className="space-y-2">
          {items.map((r) => {
            const key = `${r.entity_type}-${r.entity_ref}`;
            return (
              <DirectoryRowCard
                key={key}
                row={r}
                highlighted={highlighted === key}
              />
            );
          })}
        </div>
      ) : null}
    </div>
  );
}

function DirectoryRowCard({
  row,
  highlighted,
}: {
  row: DirectoryRow;
  highlighted?: boolean;
}) {
  const Icon = TYPE_ICONS[row.entity_type];
  const detailHref = getDetailHref(row);
  return (
    <div
      id={`row-${row.entity_type}-${row.entity_ref}`}
      className={
        "rounded-lg border bg-card p-3 shadow-sm transition-colors hover:bg-secondary/20 " +
        (highlighted
          ? "border-primary ring-1 ring-primary/30"
          : "border-border")
      }
    >
      <div className="flex items-start gap-3">
        <div className="rounded-md bg-primary/10 p-2 text-primary">
          <Icon className="h-4 w-4" />
        </div>
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2">
            {detailHref ? (
              <Link
                href={detailHref}
                className="truncate text-sm font-semibold text-foreground hover:underline"
              >
                {row.display_name}
              </Link>
            ) : (
              <span className="truncate text-sm font-semibold text-foreground">
                {row.display_name}
              </span>
            )}
            <span className="shrink-0 rounded-full bg-secondary/60 px-2 py-0.5 text-[9px] font-medium uppercase tracking-wider text-muted-foreground">
              {TYPE_LABELS[row.entity_type]}
            </span>
          </div>
          {row.display_subtitle ? (
            <p className="text-xs text-muted-foreground">{row.display_subtitle}</p>
          ) : null}
          {row.address || row.postcode ? (
            <p className="mt-1 flex items-start gap-1 text-xs text-muted-foreground">
              <MapPin className="mt-0.5 h-3 w-3 shrink-0" />
              <span>
                {row.address ? row.address : null}
                {row.postcode ? `, ${row.postcode}` : ""}
                {row.city ? ` ${row.city}` : ""}
              </span>
              {row.coords_source ? (
                <span className="ml-auto shrink-0 rounded bg-secondary/60 px-1 py-0.5 text-[9px] uppercase">
                  {row.coords_source}
                </span>
              ) : null}
            </p>
          ) : null}

          {/* Canaux contact */}
          {row.phone || row.email || row.website ? (
            <div className="mt-2 flex flex-wrap items-center gap-3 text-xs">
              {row.phone ? (
                <a
                  href={`tel:${row.phone}`}
                  className="flex items-center gap-1 text-foreground hover:text-primary"
                >
                  <Phone className="h-3 w-3" /> {row.phone}
                </a>
              ) : null}
              {row.email ? (
                <a
                  href={`mailto:${row.email}`}
                  className="flex items-center gap-1 text-foreground hover:text-primary"
                >
                  <Mail className="h-3 w-3" /> {row.email}
                </a>
              ) : null}
              {row.website ? (
                <a
                  href={
                    row.website.startsWith("http")
                      ? row.website
                      : `https://${row.website}`
                  }
                  target="_blank"
                  rel="noreferrer"
                  className="flex items-center gap-1 text-foreground hover:text-primary"
                >
                  <Globe className="h-3 w-3" /> Site
                </a>
              ) : null}
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}

function getDetailHref(row: DirectoryRow): string | null {
  switch (row.entity_type) {
    case "copro":
      return `/copros/${row.entity_ref}`;
    case "syndic":
      return `/syndics/${row.entity_ref}`;
    case "occupant":
      return row.parent_building_id
        ? `/tertiaire/${row.parent_building_id}`
        : null;
    case "prospect_custom":
      return `/prospects/${row.entity_ref}`;
  }
}
