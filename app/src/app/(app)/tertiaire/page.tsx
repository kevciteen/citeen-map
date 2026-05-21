import { Topbar } from "@/components/layout/topbar";
import { TertiaireModule } from "@/components/crm/tertiaire-module";

export default function TertiairePage() {
  return (
    <div className="flex h-full flex-col">
      <Topbar
        title="Bâtiments tertiaires"
        subtitle="Bureaux, commerces, hôtels, santé, enseignement — BDNB + DPE ADEME + occupants SIRENE + simulateur CEE BAT-*"
      />
      <div className="flex-1 overflow-hidden bg-secondary/30">
        <TertiaireModule />
      </div>
    </div>
  );
}
