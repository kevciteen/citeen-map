"use client";
import { useState, Suspense } from "react";
import { Building2, Users } from "lucide-react";
import { cn } from "@/lib/utils";
import { CoprosBrowser } from "@/components/crm/copros-browser";
import { SyndicsBrowser } from "@/components/crm/syndics-browser";

type Tab = "copros" | "syndics";

/**
 * Page Copropriétés avec onglets intégrés (Copros | Syndics).
 *
 * Précédemment, /syndics était une page séparée dans la sidebar — on l'a
 * remontée comme onglet ici car les syndics ne sont pas une entité standalone :
 * ce sont des gestionnaires de copros, pertinents à explorer dans ce contexte.
 */
export function CoprosSyndicsTabs({
  totalCopros,
  totalSyndics,
  initialTab = "copros",
}: {
  totalCopros: number;
  totalSyndics: number;
  initialTab?: Tab;
}) {
  const [tab, setTab] = useState<Tab>(initialTab);

  return (
    <div className="flex h-full flex-col">
      <div className="border-b border-border bg-card/40 backdrop-blur">
        <div className="flex gap-1 px-4">
          <TabButton
            active={tab === "copros"}
            onClick={() => setTab("copros")}
            icon={<Building2 className="h-3.5 w-3.5" />}
            label="Copropriétés"
            count={totalCopros}
          />
          <TabButton
            active={tab === "syndics"}
            onClick={() => setTab("syndics")}
            icon={<Users className="h-3.5 w-3.5" />}
            label="Syndics"
            count={totalSyndics}
          />
        </div>
      </div>

      <div className="flex-1 overflow-hidden">
        {tab === "copros" ? (
          <Suspense fallback={null}>
            <CoprosBrowser totalInDb={totalCopros} />
          </Suspense>
        ) : (
          <SyndicsBrowser totalInDb={totalSyndics} />
        )}
      </div>
    </div>
  );
}

function TabButton({
  active,
  onClick,
  icon,
  label,
  count,
}: {
  active: boolean;
  onClick: () => void;
  icon: React.ReactNode;
  label: string;
  count: number;
}) {
  return (
    <button
      onClick={onClick}
      className={cn(
        "relative flex items-center gap-1.5 px-4 py-3 text-xs font-semibold uppercase tracking-wider transition-colors",
        active ? "text-primary" : "text-muted-foreground hover:text-foreground",
      )}
    >
      {icon}
      {label}
      <span className={cn(
        "rounded-full px-1.5 py-0.5 text-[10px] font-bold",
        active ? "bg-primary/10 text-primary" : "bg-secondary text-muted-foreground",
      )}>
        {count.toLocaleString("fr-FR")}
      </span>
      {active ? <span className="absolute inset-x-0 bottom-0 h-0.5 bg-primary" /> : null}
    </button>
  );
}
