"use client";
import { useEffect, useRef, useState } from "react";
import { Search, Loader2, MapPin } from "lucide-react";
import { Input } from "@/components/ui/input";

type Suggestion = {
  label: string;
  lat: number;
  lon: number;
  postcode?: string;
  city?: string;
  citycode?: string;
};

/**
 * Input d'adresse avec autocomplete BAN (api-adresse.data.gouv.fr).
 * Émet `onSelect` quand l'utilisateur choisit une suggestion OU submit le formulaire.
 */
export function AddressAutocomplete({
  value,
  onChange,
  onSelect,
  onSubmit,
  placeholder = "Adresse complète",
  disabled,
}: {
  value: string;
  onChange: (v: string) => void;
  onSelect?: (s: Suggestion) => void;
  onSubmit?: () => void;
  placeholder?: string;
  disabled?: boolean;
}) {
  const [suggestions, setSuggestions] = useState<Suggestion[]>([]);
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [activeIdx, setActiveIdx] = useState(-1);
  const debounce = useRef<ReturnType<typeof setTimeout> | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const q = value.trim();
    if (debounce.current) clearTimeout(debounce.current);
    if (q.length < 3) {
      setSuggestions([]);
      setOpen(false);
      return;
    }
    debounce.current = setTimeout(async () => {
      setLoading(true);
      try {
        const url = new URL("https://api-adresse.data.gouv.fr/search/");
        url.searchParams.set("q", q);
        url.searchParams.set("limit", "6");
        url.searchParams.set("autocomplete", "1");
        const res = await fetch(url.toString());
        if (!res.ok) return;
        const json = await res.json() as {
          features?: Array<{
            geometry?: { coordinates?: [number, number] };
            properties?: {
              label?: string;
              postcode?: string;
              city?: string;
              citycode?: string;
            };
          }>;
        };
        const items: Suggestion[] = (json.features ?? [])
          .map((f) => {
            const c = f.geometry?.coordinates;
            if (!c || c.length !== 2) return null;
            return {
              label: f.properties?.label ?? q,
              lon: Number(c[0]),
              lat: Number(c[1]),
              postcode: f.properties?.postcode,
              city: f.properties?.city,
              citycode: f.properties?.citycode,
            } as Suggestion;
          })
          .filter((x): x is Suggestion => x !== null);
        setSuggestions(items);
        setOpen(items.length > 0);
        setActiveIdx(-1);
      } catch {
        // silencieux : on garde l'input fonctionnel même si BAN down
      } finally {
        setLoading(false);
      }
    }, 200);
    return () => {
      if (debounce.current) clearTimeout(debounce.current);
    };
  }, [value]);

  // Click outside fermer
  useEffect(() => {
    function onDocClick(e: MouseEvent) {
      if (!containerRef.current) return;
      if (!containerRef.current.contains(e.target as Node)) setOpen(false);
    }
    document.addEventListener("mousedown", onDocClick);
    return () => document.removeEventListener("mousedown", onDocClick);
  }, []);

  const choose = (s: Suggestion) => {
    onChange(s.label);
    setOpen(false);
    onSelect?.(s);
  };

  const onKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (!open || suggestions.length === 0) {
      if (e.key === "Enter") {
        e.preventDefault();
        onSubmit?.();
      }
      return;
    }
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setActiveIdx((i) => Math.min(i + 1, suggestions.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActiveIdx((i) => Math.max(i - 1, -1));
    } else if (e.key === "Enter") {
      e.preventDefault();
      if (activeIdx >= 0 && activeIdx < suggestions.length) {
        choose(suggestions[activeIdx]);
      } else {
        onSubmit?.();
      }
    } else if (e.key === "Escape") {
      setOpen(false);
    }
  };

  return (
    <div ref={containerRef} className="relative flex-1">
      <Input
        value={value}
        onChange={(e) => onChange(e.target.value)}
        onFocus={() => suggestions.length > 0 && setOpen(true)}
        onKeyDown={onKeyDown}
        placeholder={placeholder}
        disabled={disabled}
        className="pr-10"
      />
      <div className="pointer-events-none absolute inset-y-0 right-3 flex items-center text-muted-foreground">
        {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Search className="h-4 w-4" />}
      </div>
      {open && suggestions.length > 0 ? (
        <ul className="absolute left-0 right-0 top-full z-30 mt-1 max-h-72 overflow-auto rounded-lg border border-border bg-card shadow-lg">
          {suggestions.map((s, i) => (
            <li key={`${s.label}-${i}`}>
              <button
                type="button"
                onMouseEnter={() => setActiveIdx(i)}
                onClick={() => choose(s)}
                className={`flex w-full items-start gap-2 px-3 py-2 text-left text-sm transition-colors ${
                  i === activeIdx ? "bg-secondary" : "hover:bg-secondary/60"
                }`}
              >
                <MapPin className="mt-0.5 h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                <span className="min-w-0 flex-1">{s.label}</span>
              </button>
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  );
}
