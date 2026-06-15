"use client";
import { useState } from "react";
import { MapPin, Search, Map as MapIcon } from "lucide-react";
import { cn } from "@/lib/utils";
import { MaisonsAddressSearch } from "@/components/crm/maisons-address-search";
import { MaisonsZoneSearch } from "@/components/crm/maisons-zone-search";
import { MaisonsMapView } from "@/components/crm/maisons-map-view";

type Tab = "map" | "address" | "zone";
export type BatimentType = "maison" | "appartement";

export function MaisonsModule({
  typeBatiment = "maison",
}: { typeBatiment?: BatimentType } = {}) {
  const [tab, setTab] = useState<Tab>("map");

  return (
    <div className="flex h-full flex-col">
      <div className="flex items-center gap-1 border-b border-border bg-card p-2">
        <TabBtn active={tab === "map"} onClick={() => setTab("map")} icon={<MapIcon className="h-3.5 w-3.5" />}>
          Carte
        </TabBtn>
        <TabBtn active={tab === "address"} onClick={() => setTab("address")} icon={<MapPin className="h-3.5 w-3.5" />}>
          Recherche par adresse
        </TabBtn>
        <TabBtn active={tab === "zone"} onClick={() => setTab("zone")} icon={<Search className="h-3.5 w-3.5" />}>
          Recherche par zone (CP / commune)
        </TabBtn>
      </div>
      <div className="flex-1 overflow-hidden">
        {tab === "map" ? (
          <MaisonsMapView typeBatiment={typeBatiment} />
        ) : tab === "address" ? (
          <MaisonsAddressSearch typeBatiment={typeBatiment} />
        ) : (
          <MaisonsZoneSearch typeBatiment={typeBatiment} />
        )}
      </div>
    </div>
  );
}

function TabBtn({
  active,
  onClick,
  icon,
  children,
}: {
  active: boolean;
  onClick: () => void;
  icon: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <button
      onClick={onClick}
      className={cn(
        "flex items-center gap-2 rounded-md px-3 py-1.5 text-sm font-medium transition-colors",
        active
          ? "bg-primary text-primary-foreground"
          : "text-muted-foreground hover:bg-secondary hover:text-foreground",
      )}
    >
      {icon}
      {children}
    </button>
  );
}
