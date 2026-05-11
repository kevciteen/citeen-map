import { Topbar } from "@/components/layout/topbar";
import { KanbanBoard } from "@/components/crm/kanban-board";
import { Download } from "lucide-react";

export default function ProspectsPage() {
  return (
    <div className="flex h-full flex-col">
      <Topbar
        title="Pipeline commercial"
        subtitle="Pilotez votre prospection — glissez les cartes pour faire avancer un deal"
      />
      <div className="flex items-center justify-end gap-2 border-b border-border bg-card/40 px-4 py-2">
        <a
          href="/api/export/prospects.csv"
          className="inline-flex items-center gap-2 rounded-lg border border-border bg-background px-3 py-1.5 text-xs font-medium hover:bg-secondary"
        >
          <Download className="h-3.5 w-3.5" />
          Exporter CSV
        </a>
      </div>
      <div className="flex-1 overflow-hidden bg-secondary/30">
        <KanbanBoard />
      </div>
    </div>
  );
}
