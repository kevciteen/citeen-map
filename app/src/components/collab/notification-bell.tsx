"use client";
import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import { Bell, Check, CheckCheck, Loader2 } from "lucide-react";

type Notif = {
  id: number;
  kind: string;
  title: string;
  body: string | null;
  link: string | null;
  from_user_name: string | null;
  entity_type: string | null;
  entity_id: string | null;
  read_at: number | null;
  created_at: number;
};

const POLL_INTERVAL_MS = 30_000;

export function NotificationBell() {
  const [open, setOpen] = useState(false);
  const [items, setItems] = useState<Notif[]>([]);
  const [unread, setUnread] = useState(0);
  const [loading, setLoading] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  const load = async () => {
    try {
      const r = await fetch("/api/notifications?limit=30");
      const j = await r.json();
      if (r.ok) {
        setItems(j.items as Notif[]);
        setUnread(j.unreadCount as number);
      }
    } catch {
      // ignore
    }
  };

  // Initial + polling (léger)
  useEffect(() => {
    void load();
    const t = setInterval(() => void load(), POLL_INTERVAL_MS);
    return () => clearInterval(t);
  }, []);

  // Close on outside click
  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  const markAllRead = async () => {
    setLoading(true);
    try {
      await fetch("/api/notifications/mark-all-read", { method: "POST" });
      await load();
    } finally {
      setLoading(false);
    }
  };

  const markOneRead = async (id: number) => {
    await fetch(`/api/notifications/${id}`, { method: "POST" });
    await load();
  };

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen((v) => !v)}
        className="relative flex h-9 w-9 items-center justify-center rounded-lg hover:bg-secondary"
        title="Notifications"
      >
        <Bell className="h-4 w-4" />
        {unread > 0 ? (
          <span className="absolute -right-1 -top-1 flex h-4 min-w-[16px] items-center justify-center rounded-full bg-red-600 px-1 text-[9px] font-bold text-white">
            {unread > 99 ? "99+" : unread}
          </span>
        ) : null}
      </button>

      {open ? (
        <div className="absolute right-0 top-full z-50 mt-1 w-[360px] max-w-[calc(100vw-2rem)] overflow-hidden rounded-xl border border-border bg-popover shadow-xl">
          <div className="flex items-center justify-between border-b border-border bg-card p-3">
            <p className="text-sm font-bold">Notifications</p>
            {unread > 0 ? (
              <button
                onClick={markAllRead}
                disabled={loading}
                className="flex items-center gap-1 text-[10px] font-medium text-muted-foreground hover:text-primary"
              >
                {loading ? (
                  <Loader2 className="h-3 w-3 animate-spin" />
                ) : (
                  <CheckCheck className="h-3 w-3" />
                )}
                Tout marquer lu
              </button>
            ) : null}
          </div>

          <div className="max-h-[420px] overflow-y-auto">
            {items.length === 0 ? (
              <p className="p-6 text-center text-xs text-muted-foreground">
                Pas de notification — tes collègues te tagueront avec
                <strong> @ </strong>dans un commentaire.
              </p>
            ) : (
              items.map((n) => (
                <NotifRow key={n.id} notif={n} onRead={markOneRead} onNavigate={() => setOpen(false)} />
              ))
            )}
          </div>
        </div>
      ) : null}
    </div>
  );
}

function NotifRow({
  notif: n,
  onRead,
  onNavigate,
}: {
  notif: Notif;
  onRead: (id: number) => void;
  onNavigate: () => void;
}) {
  const isUnread = n.read_at == null;
  const date = new Date(n.created_at * 1000);
  const dateStr = date.toLocaleDateString("fr-FR", {
    day: "2-digit",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
  });
  const content = (
    <div
      className={`flex gap-2 border-b border-border/40 p-3 transition-colors last:border-0 ${
        isUnread ? "bg-blue-50/40" : "hover:bg-secondary/50"
      }`}
    >
      <span
        className={`mt-1 inline-block h-2 w-2 shrink-0 rounded-full ${
          isUnread ? "bg-blue-500" : "bg-transparent"
        }`}
      />
      <div className="min-w-0 flex-1">
        <p className="truncate text-xs font-semibold">{n.title}</p>
        {n.body ? (
          <p className="line-clamp-2 text-[11px] text-muted-foreground">{n.body}</p>
        ) : null}
        <p className="mt-0.5 text-[10px] text-muted-foreground">{dateStr}</p>
      </div>
      {isUnread ? (
        <button
          onClick={(e) => {
            e.preventDefault();
            e.stopPropagation();
            onRead(n.id);
          }}
          className="shrink-0 rounded-md p-1 text-muted-foreground hover:bg-secondary"
          title="Marquer comme lu"
        >
          <Check className="h-3 w-3" />
        </button>
      ) : null}
    </div>
  );

  if (n.link) {
    return (
      <Link
        href={n.link}
        onClick={() => {
          if (isUnread) onRead(n.id);
          onNavigate();
        }}
        className="block"
      >
        {content}
      </Link>
    );
  }
  return content;
}
