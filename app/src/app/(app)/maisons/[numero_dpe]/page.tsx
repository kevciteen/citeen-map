import { Topbar } from "@/components/layout/topbar";
import { MaisonFichePage } from "@/components/crm/maison-fiche-page";

export default async function MaisonDetailRoute({
  params,
}: {
  params: Promise<{ numero_dpe: string }>;
}) {
  const { numero_dpe } = await params;
  return (
    <div className="flex h-full flex-col">
      <Topbar
        title="Fiche maison"
        subtitle={`N° DPE : ${numero_dpe}`}
      />
      <div className="flex-1 overflow-y-auto bg-secondary/30">
        <MaisonFichePage typeBatiment="maison" numeroDpe={numero_dpe} />
      </div>
    </div>
  );
}
