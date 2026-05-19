"use client";
import Link from "next/link";
import { Sun, LayoutDashboard, Kanban } from "lucide-react";
import { cn } from "@/lib/utils";

type Tab = "today" | "overview" | "pipeline";

export function PilotageTabs({ current }: { current: Tab }) {
  const tabs: { id: Tab; label: string; icon: React.ReactNode }[] = [
    { id: "today", label: "Aujourd'hui", icon: <Sun className="h-3.5 w-3.5" /> },
    {
      id: "overview",
      label: "Vue d'ensemble",
      icon: <LayoutDashboard className="h-3.5 w-3.5" />,
    },
    {
      id: "pipeline",
      label: "Pipeline",
      icon: <Kanban className="h-3.5 w-3.5" />,
    },
  ];

  return (
    <div className="flex items-center gap-1 border-b border-border bg-card/60 px-4 py-1.5">
      {tabs.map((t) => (
        <Link
          key={t.id}
          href={`/pilotage?tab=${t.id}`}
          className={cn(
            "flex items-center gap-1.5 rounded-md px-3 py-1.5 text-xs font-semibold transition-colors",
            current === t.id
              ? "bg-primary text-primary-foreground shadow-sm"
              : "text-muted-foreground hover:bg-secondary hover:text-foreground",
          )}
        >
          {t.icon}
          {t.label}
        </Link>
      ))}
    </div>
  );
}
