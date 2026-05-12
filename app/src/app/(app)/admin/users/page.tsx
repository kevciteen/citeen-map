import { Topbar } from "@/components/layout/topbar";
import { AdminUsersBrowser } from "@/components/admin/users-browser";
import { requireAdmin } from "@/lib/auth/session";
import { redirect } from "next/navigation";

export const dynamic = "force-dynamic";

export default async function AdminUsersPage() {
  try {
    await requireAdmin();
  } catch {
    redirect("/dashboard");
  }
  return (
    <div className="flex h-full flex-col">
      <Topbar
        title="Gestion utilisateurs"
        subtitle="Comptes admin et collaborateurs — créer, désactiver, réinitialiser un mot de passe"
      />
      <div className="flex-1 overflow-y-auto bg-secondary/30 p-6">
        <div className="mx-auto max-w-5xl">
          <AdminUsersBrowser />
        </div>
      </div>
    </div>
  );
}
