"use client";
import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { useEffect, useState } from "react";
import {
  Map as MapIcon,
  LayoutDashboard,
  Building2,
  Building,
  Briefcase,
  Home,
  Settings,
  FileDown,
  PlusCircle,
  Shield,
  LogOut,
  KeyRound,
  BookOpen,
  Activity,
  ScanSearch,
  Search,
  Command,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { CiteenLogoWordmark } from "@/components/layout/citeen-logo";

type Me = { id: number; email: string; role: "admin" | "member"; name: string | null };

const items = [
  { href: "/pilotage", label: "Pilotage", icon: LayoutDashboard },
  { href: "/map", label: "Carte prospection", icon: MapIcon },
  { href: "/copros", label: "Copropriétés", icon: Building2 },
  { href: "/maisons", label: "Maisons individuelles", icon: Home },
  { href: "/appartements", label: "Appartements", icon: Building },
  { href: "/tertiaire", label: "Tertiaire", icon: Briefcase },
  { href: "/annuaire", label: "Annuaire unifié", icon: BookOpen },
  { href: "/dpe", label: "DPE par adresse", icon: ScanSearch },
  { href: "/exports", label: "Exports", icon: FileDown },
];

export function Sidebar() {
  const pathname = usePathname();
  const router = useRouter();
  const [me, setMe] = useState<Me | null>(null);

  useEffect(() => {
    fetch("/api/auth/me")
      .then((r) => r.json())
      .then((j) => setMe(j.user ?? null))
      .catch(() => setMe(null));
  }, []);

  const logout = async () => {
    await fetch("/api/auth/logout", { method: "POST" });
    router.push("/login");
    router.refresh();
  };

  return (
    <aside className="flex h-screen w-64 shrink-0 flex-col border-r border-border bg-card">
      <div className="flex h-16 items-center border-b border-border px-4">
        <div className="flex flex-1 flex-col items-start leading-tight">
          <CiteenLogoWordmark height={32} />
          <span className="ml-1 mt-0.5 text-[9px] uppercase tracking-wider text-muted-foreground">
            CRM Prospection
          </span>
        </div>
      </div>

      <div className="space-y-2 px-3 pt-4">
        <Link
          href="/prospects/new"
          className="flex w-full items-center justify-center gap-2 rounded-lg bg-primary px-3 py-2 text-sm font-semibold text-primary-foreground shadow-sm transition-colors hover:bg-primary/90"
        >
          <PlusCircle className="h-4 w-4" />
          Nouveau prospect
        </Link>
        <button
          onClick={() => window.dispatchEvent(new Event("global-search:open"))}
          className="flex w-full items-center justify-between gap-2 rounded-lg border border-border bg-card px-3 py-1.5 text-xs text-muted-foreground transition-colors hover:bg-secondary"
        >
          <span className="inline-flex items-center gap-1.5">
            <Search className="h-3 w-3" /> Recherche globale
          </span>
          <kbd className="inline-flex items-center gap-0.5 rounded border border-border bg-secondary px-1 text-[10px]">
            <Command className="h-2.5 w-2.5" />K
          </kbd>
        </button>
      </div>

      <nav className="flex-1 space-y-1 px-3 py-4">
        {items.map((item) => {
          const active = pathname === item.href || pathname.startsWith(item.href + "/");
          const Icon = item.icon;
          return (
            <Link
              key={item.href}
              href={item.href}
              className={cn("sidebar-item", active && "sidebar-item-active")}
            >
              <Icon className="h-4 w-4" />
              {item.label}
            </Link>
          );
        })}
        {me?.role === "admin" ? (
          <>
            <Link
              href="/admin/users"
              className={cn(
                "sidebar-item mt-3 border-t border-border pt-3",
                pathname === "/admin/users" && "sidebar-item-active",
              )}
            >
              <Shield className="h-4 w-4" />
              Gestion équipe
            </Link>
            <Link
              href="/admin/coords-health"
              className={cn(
                "sidebar-item",
                pathname.startsWith("/admin/coords-health") && "sidebar-item-active",
              )}
            >
              <Activity className="h-4 w-4" />
              Santé coordonnées
            </Link>
          </>
        ) : null}
      </nav>

      <div className="space-y-1 border-t border-border p-3">
        {me ? (
          <div className="rounded-lg bg-secondary/50 p-2 text-[11px]">
            <p className="truncate font-semibold">{me.name || me.email}</p>
            <p className="truncate text-[10px] text-muted-foreground">
              {me.role === "admin" ? "Admin" : "Membre"} · {me.email}
            </p>
          </div>
        ) : null}
        <Link href="/settings/password" className="sidebar-item">
          <KeyRound className="h-4 w-4" />
          Mot de passe
        </Link>
        <Link href="/settings" className="sidebar-item">
          <Settings className="h-4 w-4" />
          Paramètres
        </Link>
        <button
          onClick={logout}
          className="sidebar-item w-full text-left text-red-700 hover:bg-red-50 hover:text-red-900"
        >
          <LogOut className="h-4 w-4" />
          Déconnexion
        </button>
      </div>
    </aside>
  );
}
