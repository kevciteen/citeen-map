"use client";
/**
 * Champ de saisie d'adresse avec autocomplete BAN universel.
 *
 * Utilisable partout dans l'app (DPE, maisons, appartements, tertiaire,
 * campagnes, etc.) — appelle /api/search/places (proxy BAN) avec debounce
 * 220ms, montre les suggestions, ferme au choix ou au clic extérieur.
 *
 * onSelect reçoit la suggestion choisie (label + lat/lon + housenumber).
 * onChange est appelé à chaque frappe (utile pour piloter une recherche
 * temps réel sans attendre la sélection).
 */
import { useEffect, useRef, useState } from "react";
import { Loader2, MapPin, Search } from "lucide-react";
import { Input } from "@/components/ui/input";

export type AddressSuggestion = {
  label: string;
  housenumber: string | null;
  postcode: string | null;
  city: string | null;
  lat: number;
  lon: number;
  score: number;
};

export function AddressAutocomplete({
  value,
  onChange,
  onSelect,
  placeholder = "Adresse (ex: 12 rue Voltaire 75011 Paris)",
  autoFocus = false,
  className,
  minLengthSuggest = 3,
}: {
  value: string;
  onChange: (v: string) => void;
  onSelect?: (s: AddressSuggestion) => void;
  placeholder?: string;
  autoFocus?: boolean;
  className?: string;
  minLengthSuggest?: number;
}) {
  const [suggestions, setSuggestions] = useState<AddressSuggestion[]>([]);
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [highlight, setHighlight] = useState(0);
  const tidRef = useRef<number | null>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    if (autoFocus) inputRef.current?.focus();
  }, [autoFocus]);

  // Debounced fetch
  useEffect(() => {
    if (tidRef.current) window.clearTimeout(tidRef.current);
    const q = value.trim();
    if (q.length < minLengthSuggest) {
      setSuggestions([]);
      setOpen(false);
      return;
    }
    tidRef.current = window.setTimeout(async () => {
      setLoading(true);
      try {
        const r = await fetch(`/api/search/places?q=${encodeURIComponent(q)}`);
        if (r.ok) {
          const j = (await r.json()) as { items?: AddressSuggestion[] };
          setSuggestions(j.items ?? []);
          setOpen(true);
          setHighlight(0);
        }
      } finally {
        setLoading(false);
      }
    }, 220);
    return () => {
      if (tidRef.current) window.clearTimeout(tidRef.current);
    };
  }, [value, minLengthSuggest]);

  // Click extérieur ferme
  useEffect(() => {
    const onDoc = (e: MouseEvent) => {
      if (!containerRef.current?.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, []);

  const pick = (s: AddressSuggestion) => {
    onChange(s.label);
    setOpen(false);
    onSelect?.(s);
  };

  const onKeyDown = (e: React.KeyboardEvent) => {
    if (!open || suggestions.length === 0) return;
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setHighlight((i) => Math.min(i + 1, suggestions.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setHighlight((i) => Math.max(i - 1, 0));
    } else if (e.key === "Enter" && suggestions[highlight]) {
      e.preventDefault();
      pick(suggestions[highlight]);
    } else if (e.key === "Escape") {
      setOpen(false);
    }
  };

  return (
    <div ref={containerRef} className={"relative " + (className ?? "")}>
      <div className="relative">
        <Search className="absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
        <Input
          ref={inputRef}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          onFocus={() => suggestions.length > 0 && setOpen(true)}
          onKeyDown={onKeyDown}
          placeholder={placeholder}
          className="pl-8 pr-8"
        />
        {loading ? (
          <Loader2 className="absolute right-2.5 top-1/2 h-4 w-4 -translate-y-1/2 animate-spin text-muted-foreground" />
        ) : null}
      </div>
      {open && suggestions.length > 0 ? (
        <ul className="absolute z-30 mt-1 max-h-72 w-full overflow-y-auto rounded-md border border-border bg-card shadow-lg">
          {suggestions.map((s, i) => (
            <li key={`${s.label}-${i}`}>
              <button
                type="button"
                onClick={() => pick(s)}
                onMouseEnter={() => setHighlight(i)}
                className={
                  "flex w-full items-start gap-2 px-3 py-2 text-left text-xs transition-colors " +
                  (i === highlight ? "bg-primary/10" : "hover:bg-secondary/40")
                }
              >
                <MapPin className="mt-0.5 h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                <div className="min-w-0 flex-1">
                  <p className="truncate font-medium text-foreground">
                    {s.label}
                  </p>
                  <p className="text-[10px] text-muted-foreground">
                    {s.city} {s.postcode}
                    {" · "}score {Math.round(s.score * 100)} %
                  </p>
                </div>
              </button>
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  );
}
