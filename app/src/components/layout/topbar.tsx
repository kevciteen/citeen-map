"use client";
import { useEffect, useState } from "react";
import { useRouter, usePathname } from "next/navigation";
import { ArrowLeft, Search } from "lucide-react";
import { Input } from "@/components/ui/input";
import { NotificationBell } from "@/components/collab/notification-bell";
import { UserMenu } from "@/components/layout/user-menu";

export function Topbar({ title, subtitle }: { title: string; subtitle?: string }) {
  const router = useRouter();
  const pathname = usePathname();
  const [canGoBack, setCanGoBack] = useState(false);
  const [globalSearch, setGlobalSearch] = useState("");

  // Détecte si on a un historique de navigation (au moins une page avant)
  useEffect(() => {
    setCanGoBack(window.history.length > 1);
  }, [pathname]);

  // Cacher le bouton sur la home / pages racine
  const isRoot = pathname === "/" || pathname === "/dashboard" || pathname === "/map";

  return (
    <header className="flex h-16 shrink-0 items-center justify-between gap-3 border-b border-border bg-card/40 px-4 backdrop-blur sm:px-6">
      <div className="flex min-w-0 items-center gap-3">
        {!isRoot && canGoBack ? (
          <button
            onClick={() => router.back()}
            aria-label="Retour"
            title="Retour à la page précédente"
            className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg border border-border bg-background text-muted-foreground transition-colors hover:bg-secondary hover:text-foreground"
          >
            <ArrowLeft className="h-4 w-4" />
          </button>
        ) : null}
        <div className="min-w-0">
          <h1 className="truncate text-base font-bold tracking-tight">{title}</h1>
          {subtitle ? (
            <p className="truncate text-xs text-muted-foreground">{subtitle}</p>
          ) : null}
        </div>
      </div>
      <div className="flex items-center gap-3">
        <form
          onSubmit={(e) => {
            e.preventDefault();
            if (globalSearch.trim()) {
              router.push(`/copros?q=${encodeURIComponent(globalSearch.trim())}`);
            }
          }}
          className="relative hidden md:block"
        >
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            value={globalSearch}
            onChange={(e) => setGlobalSearch(e.target.value)}
            placeholder="Rechercher copro, syndic, adresse…"
            className="h-9 w-80 pl-9 text-sm"
          />
        </form>
        <NotificationBell />
        <UserMenu />
      </div>
    </header>
  );
}
