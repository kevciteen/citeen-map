"use client";
import { useState } from "react";
import { useRouter } from "next/navigation";
import { LogIn, Loader2, AlertCircle } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { CiteenLogoWordmark } from "@/components/layout/citeen-logo";

export default function LoginPage() {
  const router = useRouter();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError(null);
    try {
      const r = await fetch("/api/auth/login", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ email, password }),
      });
      const j = await r.json();
      if (!r.ok) {
        setError(j.error ?? "Erreur de connexion");
        return;
      }
      // Si mot de passe initial à changer → redirige vers la page settings password
      if (j.mustChangePassword) {
        router.push("/settings/password?first=1");
      } else {
        router.push("/dashboard");
      }
      router.refresh();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="flex min-h-screen items-center justify-center bg-gradient-to-br from-primary/5 via-background to-secondary/30 p-6">
      <div className="w-full max-w-md">
        <div className="mb-6 flex flex-col items-center gap-2">
          <CiteenLogoWordmark height={56} />
          <p className="text-xs text-muted-foreground">CRM Prospection — Connexion</p>
        </div>

        <form
          onSubmit={submit}
          className="space-y-4 rounded-2xl border border-border bg-card p-6 shadow-lg"
        >
          <div>
            <label className="mb-1 block text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
              Email
            </label>
            <Input
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              placeholder="vous@citeen.fr"
              autoComplete="email"
              required
              autoFocus
            />
          </div>
          <div>
            <label className="mb-1 block text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
              Mot de passe
            </label>
            <Input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="••••••••"
              autoComplete="current-password"
              required
            />
          </div>

          {error ? (
            <div className="flex items-start gap-2 rounded-lg border border-red-200 bg-red-50 p-3 text-xs text-red-900">
              <AlertCircle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
              {error}
            </div>
          ) : null}

          <Button type="submit" disabled={loading} className="w-full" size="lg">
            {loading ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <LogIn className="h-4 w-4" />
            )}
            Se connecter
          </Button>

          <p className="text-center text-[11px] text-muted-foreground">
            Accès réservé. Contacte ton administrateur pour obtenir un compte.
          </p>
        </form>
      </div>
    </div>
  );
}
