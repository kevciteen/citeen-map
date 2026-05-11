import { Topbar } from "@/components/layout/topbar";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export default function SettingsPage() {
  return (
    <div className="flex h-full flex-col">
      <Topbar title="Paramètres" subtitle="Configuration de l'espace de travail" />
      <div className="flex-1 overflow-y-auto bg-secondary/30 p-6">
        <div className="mx-auto max-w-3xl space-y-3">
          <Card>
            <CardHeader>
              <CardTitle className="text-sm">Sources de données</CardTitle>
            </CardHeader>
            <CardContent className="space-y-2 text-xs">
              <Row label="Registre national des copropriétés" value="data.gouv.fr · 8 départements IDF" />
              <Row label="DPE ADEME" value="dpe03existant (logements existants)" />
              <Row label="Géocodage adresse" value="Base Adresse Nationale (BAN)" />
              <Row label="Persistance" value="SQLite local · ./data/citeen.db" />
            </CardContent>
          </Card>
          <Card>
            <CardHeader>
              <CardTitle className="text-sm">À venir</CardTitle>
            </CardHeader>
            <CardContent className="text-xs text-muted-foreground">
              <ul className="list-disc pl-4">
                <li>Authentification multi-utilisateur (NextAuth + RLS)</li>
                <li>Intégration SIRENE pour enrichir les syndics</li>
                <li>DVF prix au m² pour valorisation</li>
                <li>Notification de relances (mail / push)</li>
                <li>Import depuis Google Places</li>
              </ul>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}

function Row({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between border-b border-border py-2 last:border-0">
      <span className="text-muted-foreground">{label}</span>
      <span className="font-medium">{value}</span>
    </div>
  );
}
