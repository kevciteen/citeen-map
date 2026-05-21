import { Topbar } from "@/components/layout/topbar";
import { TertiaireTestModule } from "@/components/crm/tertiaire-test-module";

export default function TertiairePage() {
  return (
    <div className="flex h-full flex-col">
      <Topbar
        title="Bâtiments tertiaires"
        subtitle="Bureaux, commerces, hôtels, santé, enseignement — sourcing BDNB + DPE tertiaire ADEME + occupants SIRENE"
      />
      <div className="flex-1 overflow-hidden bg-secondary/30">
        <TertiaireTestModule />
      </div>
    </div>
  );
}
