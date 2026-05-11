import { Topbar } from "@/components/layout/topbar";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Download, FileText } from "lucide-react";

const EXPORTS = [
  {
    title: "Prospects pipeline (CSV)",
    description:
      "Tous les prospects avec leurs informations copropriété, DPE, syndic, étape pipeline et valeur estimée. Idéal pour reporting, import dans Excel, traitements externes.",
    href: "/api/export/prospects.csv",
    filename: "prospects-pipeline.csv",
  },
];

export default function ExportsPage() {
  return (
    <div className="flex h-full flex-col">
      <Topbar title="Exports" subtitle="Données enrichies prêtes à exploiter" />
      <div className="flex-1 overflow-y-auto bg-secondary/30 p-6">
        <div className="mx-auto grid max-w-5xl gap-3 sm:grid-cols-2">
          {EXPORTS.map((e) => (
            <Card key={e.href}>
              <CardHeader>
                <div className="flex items-center gap-3">
                  <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-primary/10 text-primary">
                    <FileText className="h-5 w-5" />
                  </div>
                  <div>
                    <CardTitle className="text-sm">{e.title}</CardTitle>
                    <CardDescription className="text-xs">{e.filename}</CardDescription>
                  </div>
                </div>
              </CardHeader>
              <CardContent>
                <p className="mb-3 text-xs text-muted-foreground">{e.description}</p>
                <a
                  href={e.href}
                  className="inline-flex items-center gap-2 rounded-lg bg-primary px-3 py-1.5 text-xs font-semibold text-primary-foreground hover:bg-primary/90"
                >
                  <Download className="h-3.5 w-3.5" />
                  Télécharger
                </a>
              </CardContent>
            </Card>
          ))}
        </div>
      </div>
    </div>
  );
}
