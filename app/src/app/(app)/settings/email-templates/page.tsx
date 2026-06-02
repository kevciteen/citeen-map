import { Topbar } from "@/components/layout/topbar";
import { EmailTemplatesManager } from "@/components/crm/email-templates-manager";
import { requireUser } from "@/lib/auth/session";
import { redirect } from "next/navigation";

export const dynamic = "force-dynamic";

export default async function EmailTemplatesPage() {
  try {
    await requireUser();
  } catch {
    redirect("/login");
  }
  return (
    <div className="flex h-full flex-col">
      <Topbar
        title="Modèles d'email"
        subtitle="Crée et partage des templates de prospection — variables {{nom_copro}}, {{syndic}}…"
      />
      <div className="flex-1 overflow-y-auto bg-secondary/30 p-6">
        <div className="mx-auto max-w-5xl">
          <EmailTemplatesManager />
        </div>
      </div>
    </div>
  );
}
