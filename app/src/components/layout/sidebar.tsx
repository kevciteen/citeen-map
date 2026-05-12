"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  Map as MapIcon,
  Kanban,
  LayoutDashboard,
  Building2,
  Home,
  Settings,
  FileDown,
  PlusCircle,
  Users,
} from "lucide-react";
import { cn } from "@/lib/utils";

const items = [
  { href: "/dashboard", label: "Vue d'ensemble", icon: LayoutDashboard },
  { href: "/map", label: "Carte prospection", icon: MapIcon },
  { href: "/prospects", label: "Pipeline", icon: Kanban },
  { href: "/copros", label: "Copropriétés", icon: Building2 },
  { href: "/maisons", label: "Maisons individuelles", icon: Home },
  { href: "/syndics", label: "Syndics", icon: Users },
  { href: "/exports", label: "Exports", icon: FileDown },
];

export function Sidebar() {
  const pathname = usePathname();
  return (
    <aside className="flex h-screen w-64 shrink-0 flex-col border-r border-border bg-card">
      <div className="flex h-16 items-center gap-2 border-b border-border px-5">
        <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-primary text-primary-foreground">
          <span className="text-lg font-black">C</span>
        </div>
        <div className="flex flex-col leading-tight">
          <span className="text-sm font-bold tracking-tight">Citeen</span>
          <span className="text-[10px] uppercase tracking-wider text-muted-foreground">
            CRM Prospection
          </span>
        </div>
      </div>

      <div className="px-3 pt-4">
        <Link
          href="/prospects/new"
          className="flex w-full items-center justify-center gap-2 rounded-lg bg-primary px-3 py-2 text-sm font-semibold text-primary-foreground shadow-sm transition-colors hover:bg-primary/90"
        >
          <PlusCircle className="h-4 w-4" />
          Nouveau prospect
        </Link>
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
      </nav>

      <div className="border-t border-border p-3">
        <Link href="/settings" className="sidebar-item">
          <Settings className="h-4 w-4" />
          Paramètres
        </Link>
      </div>
    </aside>
  );
}
