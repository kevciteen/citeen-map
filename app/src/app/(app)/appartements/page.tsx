import { Topbar } from "@/components/layout/topbar";
import { MaisonsModule } from "@/components/crm/maisons-module";

export default function AppartementsPage() {
  return (
    <div className="flex h-full flex-col">
      <Topbar
        title="Appartements"
        subtitle="Trouvez n'importe quel appartement par adresse précise ou par zone (CP, commune)"
      />
      <div className="flex-1 overflow-hidden bg-secondary/30">
        <MaisonsModule typeBatiment="appartement" />
      </div>
    </div>
  );
}
