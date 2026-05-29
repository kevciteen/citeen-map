"use client";
/**
 * Palette de commande cross-entité (CMD+K / CTRL+K).
 *
 * Cherche dans `directory` via /api/directory en FTS5 (sub-10ms côté serveur)
 * et affiche les matches avec navigation au clavier (↑↓ + Entrée).
 *
 * Pas de dépendance externe (pas de cmdk) — modal HTML simple pour rester
 * léger et cohérent avec les composants shadcn déjà en place.
 */
import { useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { useQuery } from "@tanstack/react-query";
import {
  Search, Loader2, Building2, Briefcase, IdCard, Users, Command,
} from "lucide-react";
import { useDebouncedValue } from "@/lib/hooks/use-debounced-value";
import { jsonFetcher } from "@/lib/fetcher";

type DirectoryRow = {
  id: number;
  entity_type: "occupant" | "copro" | "syndic" | "prospect_custom";
  entity_ref: string;
  display_name: string;
  display_subtitle: string | null;
  postcode: string | null;
  city: string | null;
  dpe_class: string | null;
};

const TYPE_LABELS: Record<DirectoryRow["entity_type"], string> = {
  copro: "Copropriété",
  occupant: "Société tertiaire",
  syndic: "Syndic",
  prospect_custom: "Adresse libre",
};

const TYPE_ICONS = {
  copro: Building2,
  occupant: Briefcase,
  syndic: IdCard,
  prospect_custom: Users,
};

function getHref(row: DirectoryRow): string | null {
  switch (row.entity_type) {
    case "copro":
      return `/copros/${row.entity_ref}`;
    case "syndic":
      return `/syndics/${row.entity_ref}`;
    case "occupant":
      return null;
    case "prospect_custom":
      return `/prospects/${row.entity_ref}`;
  }
}

export function GlobalSearch() {
  const router = useRouter();
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [highlightIndex, setHighlightIndex] = useState(0);
  const inputRef = useRef<HTMLInputElement | null>(null);
  const debounced = useDebouncedValue(query, 200);

  // Raccourci clavier global Cmd+K / Ctrl+K + event custom pour ouverture
  // depuis un autre composant (bouton sidebar par exemple).
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setOpen((v) => !v);
      } else if (e.key === "Escape") {
        setOpen(false);
      }
    };
    const onCustomOpen = () => setOpen(true);
    window.addEventListener("keydown", onKey);
    window.addEventListener("global-search:open", onCustomOpen);
    return () => {
      window.removeEventListener("keydown", onKey);
      window.removeEventListener("global-search:open", onCustomOpen);
    };
  }, []);

  // Focus auto à l'ouverture
  useEffect(() => {
    if (open) {
      setTimeout(() => inputRef.current?.focus(), 10);
      setHighlightIndex(0);
    }
  }, [open]);

  const { data, isFetching } = useQuery({
    queryKey: ["global-search", debounced],
    queryFn: ({ signal }) =>
      jsonFetcher<{ items: DirectoryRow[] }>(
        `/api/directory?q=${encodeURIComponent(debounced)}&limit=20`,
        signal,
      ),
    enabled: open && debounced.length >= 2,
    staleTime: 30 * 1000,
  });

  const items = data?.items ?? [];

  const navigate = (row: DirectoryRow) => {
    const href = getHref(row);
    if (href) {
      router.push(href);
      setOpen(false);
      setQuery("");
    }
  };

  const onKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setHighlightIndex((i) => Math.min(i + 1, items.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setHighlightIndex((i) => Math.max(i - 1, 0));
    } else if (e.key === "Enter" && items[highlightIndex]) {
      e.preventDefault();
      navigate(items[highlightIndex]);
    }
  };

  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-50 flex items-start justify-center bg-black/40 pt-[10vh] backdrop-blur-sm"
      onClick={() => setOpen(false)}
    >
      <div
        className="w-full max-w-xl overflow-hidden rounded-xl border border-border bg-card shadow-2xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center gap-2 border-b border-border px-3">
          <Search className="h-4 w-4 text-muted-foreground" />
          <input
            ref={inputRef}
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={onKeyDown}
            placeholder="Rechercher copro, syndic, société, adresse…"
            className="h-12 flex-1 border-0 bg-transparent text-sm focus:outline-none"
          />
          {isFetching ? (
            <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
          ) : null}
          <kbd className="rounded border border-border bg-secondary px-1.5 py-0.5 text-[10px] text-muted-foreground">
            ESC
          </kbd>
        </div>

        <div className="max-h-[60vh] overflow-y-auto">
          {debounced.length < 2 ? (
            <div className="p-6 text-center text-xs text-muted-foreground">
              <p>Tape au moins 2 caractères.</p>
              <p className="mt-2 flex items-center justify-center gap-1">
                Astuce :{" "}
                <kbd className="inline-flex items-center gap-0.5 rounded border border-border bg-secondary px-1 text-[10px]">
                  <Command className="h-2.5 w-2.5" />K
                </kbd>{" "}
                ouvre cette recherche depuis n&apos;importe quelle page.
              </p>
            </div>
          ) : items.length === 0 && !isFetching ? (
            <div className="p-6 text-center text-xs text-muted-foreground">
              Aucun résultat pour <strong>&quot;{debounced}&quot;</strong>.
            </div>
          ) : (
            <ul className="py-1">
              {items.map((row, i) => {
                const Icon = TYPE_ICONS[row.entity_type];
                const isActive = i === highlightIndex;
                const href = getHref(row);
                return (
                  <li key={`${row.entity_type}-${row.entity_ref}`}>
                    <button
                      type="button"
                      onClick={() => navigate(row)}
                      onMouseEnter={() => setHighlightIndex(i)}
                      disabled={!href}
                      className={
                        "flex w-full items-center gap-3 px-4 py-2.5 text-left text-xs transition-colors " +
                        (isActive
                          ? "bg-primary/10"
                          : "hover:bg-secondary/40") +
                        (href ? " cursor-pointer" : " cursor-not-allowed opacity-60")
                      }
                    >
                      <div className="rounded-md bg-secondary/60 p-1.5 text-primary">
                        <Icon className="h-3.5 w-3.5" />
                      </div>
                      <div className="min-w-0 flex-1">
                        <p className="truncate font-semibold text-foreground">
                          {row.display_name}
                        </p>
                        <p className="truncate text-[11px] text-muted-foreground">
                          {TYPE_LABELS[row.entity_type]}
                          {row.city ? ` · ${row.city}` : ""}
                          {row.postcode ? ` ${row.postcode}` : ""}
                          {row.dpe_class ? ` · DPE ${row.dpe_class}` : ""}
                        </p>
                      </div>
                      {isActive ? (
                        <span className="shrink-0 text-[10px] text-muted-foreground">
                          ↵
                        </span>
                      ) : null}
                    </button>
                  </li>
                );
              })}
            </ul>
          )}
        </div>

        <div className="flex items-center justify-between gap-3 border-t border-border px-4 py-2 text-[10px] text-muted-foreground">
          <div className="flex items-center gap-2">
            <kbd className="rounded border border-border bg-secondary px-1 py-0.5">↑↓</kbd>
            naviguer
            <kbd className="rounded border border-border bg-secondary px-1 py-0.5">↵</kbd>
            ouvrir
          </div>
          <span>{items.length} résultat(s)</span>
        </div>
      </div>
    </div>
  );
}
