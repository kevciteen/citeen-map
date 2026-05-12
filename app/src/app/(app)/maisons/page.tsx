import { Topbar } from "@/components/layout/topbar";
import { MaisonsModule } from "@/components/crm/maisons-module";

export default function MaisonsPage() {
  return (
    <div className="flex h-full flex-col">
      <Topbar
        title="Maisons individuelles"
        subtitle="Trouvez n'importe quelle maison française par adresse précise ou par zone (CP, commune)"
      />
      <div className="flex-1 overflow-hidden bg-secondary/30">
        <MaisonsModule />
      </div>
    </div>
  );
}
