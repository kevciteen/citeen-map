"use client";
import { useEffect, useState } from "react";
import { Plus, Loader2, UserPlus, Shield, User, Key, X } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { toast } from "sonner";
import { cn } from "@/lib/utils";

type UserRow = {
  id: number;
  email: string;
  role: "admin" | "member";
  name: string | null;
  active: boolean;
  must_change_password: boolean;
  created_at: number;
  last_login_at: number | null;
};

export function AdminUsersBrowser() {
  const [users, setUsers] = useState<UserRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [showCreate, setShowCreate] = useState(false);
  const [resetFor, setResetFor] = useState<UserRow | null>(null);

  const load = async () => {
    setLoading(true);
    try {
      const r = await fetch("/api/admin/users");
      const j = await r.json();
      if (r.ok) setUsers(j.users);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void load();
  }, []);

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <p className="text-sm text-muted-foreground">
          <strong className="text-foreground">{users.length}</strong> utilisateur(s)
        </p>
        <Button onClick={() => setShowCreate(true)} size="sm">
          <UserPlus className="h-4 w-4" />
          Nouveau collaborateur
        </Button>
      </div>

      <div className="overflow-hidden rounded-xl border border-border bg-card">
        {loading ? (
          <div className="flex h-24 items-center justify-center">
            <Loader2 className="h-5 w-5 animate-spin text-primary" />
          </div>
        ) : users.length === 0 ? (
          <p className="p-6 text-center text-sm text-muted-foreground">
            Aucun utilisateur. Crée le premier collaborateur.
          </p>
        ) : (
          <table className="w-full text-sm">
            <thead className="border-b border-border bg-secondary/40 text-[10px] uppercase tracking-wider text-muted-foreground">
              <tr>
                <th className="px-4 py-2 text-left">Utilisateur</th>
                <th className="px-4 py-2 text-left">Rôle</th>
                <th className="px-4 py-2 text-left">Statut</th>
                <th className="px-4 py-2 text-left">Dernière connexion</th>
                <th className="px-4 py-2 text-right">Actions</th>
              </tr>
            </thead>
            <tbody>
              {users.map((u) => (
                <UserRowView
                  key={u.id}
                  user={u}
                  onReset={() => setResetFor(u)}
                  onUpdated={load}
                />
              ))}
            </tbody>
          </table>
        )}
      </div>

      {showCreate ? (
        <CreateUserModal
          onClose={() => setShowCreate(false)}
          onCreated={() => {
            setShowCreate(false);
            void load();
          }}
        />
      ) : null}

      {resetFor ? (
        <ResetPasswordModal
          user={resetFor}
          onClose={() => setResetFor(null)}
          onDone={() => {
            setResetFor(null);
            void load();
          }}
        />
      ) : null}
    </div>
  );
}

function UserRowView({
  user,
  onReset,
  onUpdated,
}: {
  user: UserRow;
  onReset: () => void;
  onUpdated: () => void;
}) {
  const toggleActive = async () => {
    const r = await fetch(`/api/admin/users/${user.id}`, {
      method: "PATCH",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ active: !user.active }),
    });
    if (r.ok) {
      toast.success(user.active ? "Désactivé" : "Réactivé");
      onUpdated();
    } else toast.error("Erreur");
  };
  const toggleRole = async () => {
    const newRole = user.role === "admin" ? "member" : "admin";
    const r = await fetch(`/api/admin/users/${user.id}`, {
      method: "PATCH",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ role: newRole }),
    });
    if (r.ok) {
      toast.success(`Rôle changé en ${newRole}`);
      onUpdated();
    } else toast.error("Erreur");
  };

  const lastLogin = user.last_login_at
    ? new Date(user.last_login_at * 1000).toLocaleDateString("fr-FR")
    : "—";

  return (
    <tr className="border-b border-border/50 last:border-0">
      <td className="px-4 py-3">
        <div className="font-medium">{user.name || user.email}</div>
        {user.name ? (
          <div className="text-[11px] text-muted-foreground">{user.email}</div>
        ) : null}
      </td>
      <td className="px-4 py-3">
        <span
          className={cn(
            "inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[10px] font-bold",
            user.role === "admin"
              ? "bg-primary/10 text-primary"
              : "bg-secondary text-secondary-foreground",
          )}
        >
          {user.role === "admin" ? (
            <Shield className="h-3 w-3" />
          ) : (
            <User className="h-3 w-3" />
          )}
          {user.role}
        </span>
      </td>
      <td className="px-4 py-3 text-xs">
        {user.active ? (
          <span className="text-emerald-600">Actif</span>
        ) : (
          <span className="text-muted-foreground">Désactivé</span>
        )}
        {user.must_change_password ? (
          <div className="text-[10px] text-amber-700">⚠ mot de passe à changer</div>
        ) : null}
      </td>
      <td className="px-4 py-3 text-xs text-muted-foreground">{lastLogin}</td>
      <td className="px-4 py-3">
        <div className="flex justify-end gap-1">
          <button
            onClick={onReset}
            className="rounded-md border border-border bg-background px-2 py-1 text-[11px] font-medium hover:bg-secondary"
            title="Réinitialiser le mot de passe"
          >
            <Key className="h-3 w-3" />
          </button>
          <button
            onClick={toggleRole}
            className="rounded-md border border-border bg-background px-2 py-1 text-[11px] font-medium hover:bg-secondary"
            title="Changer le rôle"
          >
            {user.role === "admin" ? "→ member" : "→ admin"}
          </button>
          <button
            onClick={toggleActive}
            className="rounded-md border border-border bg-background px-2 py-1 text-[11px] font-medium hover:bg-secondary"
          >
            {user.active ? "Désactiver" : "Réactiver"}
          </button>
        </div>
      </td>
    </tr>
  );
}

function CreateUserModal({
  onClose,
  onCreated,
}: {
  onClose: () => void;
  onCreated: () => void;
}) {
  const [email, setEmail] = useState("");
  const [name, setName] = useState("");
  const [password, setPassword] = useState("");
  const [role, setRole] = useState<"admin" | "member">("member");
  const [loading, setLoading] = useState(false);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    try {
      const r = await fetch("/api/admin/users", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ email, password, name, role }),
      });
      const j = await r.json();
      if (r.ok) {
        toast.success("Collaborateur créé. Il devra changer son mot de passe à la 1re connexion.");
        onCreated();
      } else toast.error(j.error ?? "Erreur");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4">
      <form
        onSubmit={submit}
        className="w-full max-w-md space-y-3 rounded-xl border border-border bg-card p-5 shadow-xl"
      >
        <div className="flex items-center justify-between">
          <h2 className="text-sm font-bold">Nouveau collaborateur</h2>
          <button type="button" onClick={onClose} className="rounded-md p-1 hover:bg-secondary">
            <X className="h-4 w-4" />
          </button>
        </div>
        <Input
          type="email"
          placeholder="email@citeen.fr"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          required
          autoFocus
        />
        <Input
          placeholder="Nom complet (optionnel)"
          value={name}
          onChange={(e) => setName(e.target.value)}
        />
        <Input
          type="text"
          placeholder="Mot de passe initial (min 8)"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          minLength={8}
          required
        />
        <select
          value={role}
          onChange={(e) => setRole(e.target.value as "admin" | "member")}
          className="h-10 w-full rounded-lg border border-input bg-background px-3 text-sm"
        >
          <option value="member">Membre (collaborateur)</option>
          <option value="admin">Admin (gestion équipe + tout voir)</option>
        </select>
        <Button type="submit" disabled={loading} className="w-full">
          {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Plus className="h-4 w-4" />}
          Créer
        </Button>
      </form>
    </div>
  );
}

function ResetPasswordModal({
  user,
  onClose,
  onDone,
}: {
  user: UserRow;
  onClose: () => void;
  onDone: () => void;
}) {
  const [pwd, setPwd] = useState("");
  const [loading, setLoading] = useState(false);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    try {
      const r = await fetch(`/api/admin/users/${user.id}`, {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ resetPassword: pwd }),
      });
      if (r.ok) {
        toast.success("Mot de passe réinitialisé. Le user devra le changer à la prochaine connexion.");
        onDone();
      } else toast.error("Erreur");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4">
      <form
        onSubmit={submit}
        className="w-full max-w-md space-y-3 rounded-xl border border-border bg-card p-5 shadow-xl"
      >
        <div className="flex items-center justify-between">
          <h2 className="text-sm font-bold">Réinitialiser le mot de passe</h2>
          <button type="button" onClick={onClose} className="rounded-md p-1 hover:bg-secondary">
            <X className="h-4 w-4" />
          </button>
        </div>
        <p className="text-xs text-muted-foreground">
          Pour <strong>{user.email}</strong>. Le user devra changer ce mot de passe à sa prochaine connexion.
        </p>
        <Input
          type="text"
          placeholder="Nouveau mot de passe (min 8)"
          value={pwd}
          onChange={(e) => setPwd(e.target.value)}
          minLength={8}
          required
          autoFocus
        />
        <Button type="submit" disabled={loading} className="w-full">
          {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Key className="h-4 w-4" />}
          Définir
        </Button>
      </form>
    </div>
  );
}
