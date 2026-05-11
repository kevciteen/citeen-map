"use client";
import { useEffect, useRef, useState } from "react";
import { MapPin, Loader2 } from "lucide-react";
import { Input } from "@/components/ui/input";

type Result = {
  lat: number;
  lon: number;
  label: string;
  postcode?: string;
  city?: string;
};

export function AddressSearch({
  onSelect,
}: {
  onSelect: (r: Result) => void;
}) {
  const [q, setQ] = useState("");
  const [items, setItems] = useState<Result[]>([]);
  const [loading, setLoading] = useState(false);
  const [open, setOpen] = useState(false);
  const tid = useRef<number | null>(null);

  useEffect(() => {
    if (tid.current) window.clearTimeout(tid.current);
    if (q.trim().length < 3) {
      setItems([]);
      return;
    }
    tid.current = window.setTimeout(async () => {
      setLoading(true);
      try {
        const r = await fetch(`/api/search/places?q=${encodeURIComponent(q)}`);
        const j = await r.json();
        setItems(j.items ?? []);
        setOpen(true);
      } finally {
        setLoading(false);
      }
    }, 220);
  }, [q]);

  return (
    <div className="relative">
      <div className="relative">
        <MapPin className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
        <Input
          value={q}
          onChange={(e) => setQ(e.target.value)}
          onFocus={() => items.length && setOpen(true)}
          onBlur={() => setTimeout(() => setOpen(false), 180)}
          placeholder="Rechercher une adresse, immeuble, commune…"
          className="pl-9"
        />
        {loading ? (
          <Loader2 className="absolute right-3 top-1/2 h-4 w-4 -translate-y-1/2 animate-spin text-muted-foreground" />
        ) : null}
      </div>
      {open && items.length > 0 ? (
        <div className="absolute z-20 mt-1.5 w-full overflow-hidden rounded-lg border border-border bg-popover shadow-lg">
          {items.map((it, i) => (
            <button
              key={i}
              type="button"
              className="flex w-full items-start gap-2 px-3 py-2 text-left text-sm hover:bg-secondary"
              onMouseDown={(e) => {
                e.preventDefault();
                onSelect(it);
                setQ(it.label);
                setOpen(false);
              }}
            >
              <MapPin className="mt-0.5 h-3.5 w-3.5 shrink-0 text-primary" />
              <div className="flex-1">
                <div className="font-medium leading-tight">{it.label}</div>
                {it.postcode || it.city ? (
                  <div className="text-xs text-muted-foreground">
                    {it.postcode} {it.city}
                  </div>
                ) : null}
              </div>
            </button>
          ))}
        </div>
      ) : null}
    </div>
  );
}
