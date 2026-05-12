"use client";
import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { KeyRound, LogOut, Settings, Shield, User2 } from "lucide-react";

type Me = {
  id: number;
  email: string;
  role: "admin" | "member";
  name: string | null;
};

export function UserMenu() {
  const router = useRouter();
  const [me, setMe] = useState<Me | null>(null);
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    fetch("/api/auth/me")
      .then((r) => r.json())
      .then((j) => setMe(j.user ?? null))
      .catch(() => setMe(null));
  }, []);

  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  const logout = async () => {
    await fetch("/api/auth/logout", { method: "POST" });
    router.push("/login");
    router.refresh();
  };

  const initials = (me?.name ?? me?.email ?? "?")
    .split(/[\s.@]+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((s) => s[0]?.toUpperCase() ?? "")
    .join("");

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen((v) => !v)}
        className="flex h-9 w-9 items-center justify-center rounded-full bg-primary text-primary-foreground font-semibold transition-transform hover:scale-105"
        title={me?.name ?? me?.email ?? "Mon compte"}
      >
        {me ? (
          <span className="text-[11px]">{initials}</span>
        ) : (
          <User2 className="h-4 w-4" />
        )}
      </button>

      {open ? (
        <div className="absolute right-0 top-full z-50 mt-1 w-64 overflow-hidden rounded-xl border border-border bg-popover shadow-xl">
          {me ? (
            <div className="border-b border-border bg-secondary/30 p-3">
              <p className="truncate text-sm font-semibold">{me.name ?? me.email}</p>
              <p className="truncate text-[11px] text-muted-foreground">{me.email}</p>
              <span className="mt-1 inline-block rounded-full bg-primary/10 px-2 py-0.5 text-[9px] font-bold uppercase tracking-wider text-primary">
                {me.role}
              </span>
            </div>
          ) : null}
          <div className="py-1">
            <MenuItem href="/settings/password" icon={<KeyRound className="h-3.5 w-3.5" />}>
              Changer le mot de passe
            </MenuItem>
            <MenuItem href="/settings" icon={<Settings className="h-3.5 w-3.5" />}>
              Paramètres
            </MenuItem>
            {me?.role === "admin" ? (
              <MenuItem href="/admin/users" icon={<Shield className="h-3.5 w-3.5" />}>
                Gestion équipe
              </MenuItem>
            ) : null}
            <button
              onClick={logout}
              className="flex w-full items-center gap-2 px-3 py-2 text-left text-xs font-medium text-red-700 hover:bg-red-50"
            >
              <LogOut className="h-3.5 w-3.5" />
              Déconnexion
            </button>
          </div>
        </div>
      ) : null}
    </div>
  );
}

function MenuItem({
  href,
  icon,
  children,
}: {
  href: string;
  icon: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <Link
      href={href}
      className="flex items-center gap-2 px-3 py-2 text-xs font-medium hover:bg-secondary"
    >
      {icon}
      {children}
    </Link>
  );
}
