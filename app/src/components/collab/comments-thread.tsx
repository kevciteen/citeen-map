"use client";
import { useEffect, useRef, useState, useCallback } from "react";
import { Loader2, Send, MessageSquare } from "lucide-react";
import { Button } from "@/components/ui/button";
import { MentionInput } from "./mention-input";
import { toast } from "sonner";

type Comment = {
  id: number;
  entity_type: string;
  entity_id: string;
  author_id: number;
  author_email: string | null;
  author_name: string | null;
  body: string;
  parent_id: number | null;
  created_at: number;
};

/**
 * Affiche un fil de commentaires sur n'importe quelle entité (prospect,
 * copro, syndic, maison). Supporte le @mention. À chaque nouveau commentaire
 * contenant une mention, une notification est créée côté serveur pour le
 * destinataire.
 */
export function CommentsThread({
  entityType,
  entityId,
  link,
}: {
  entityType: "prospect" | "copro" | "maison" | "syndic";
  entityId: string;
  link?: string; // override du lien dans la notif (sinon dérivé par défaut)
}) {
  const [items, setItems] = useState<Comment[]>([]);
  const [loading, setLoading] = useState(true);
  const [draft, setDraft] = useState("");
  const [sending, setSending] = useState(false);
  const bottomRef = useRef<HTMLDivElement>(null);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const r = await fetch(
        `/api/comments?entity_type=${entityType}&entity_id=${encodeURIComponent(entityId)}`,
      );
      const j = await r.json();
      if (r.ok) setItems(j.items as Comment[]);
    } finally {
      setLoading(false);
    }
  }, [entityType, entityId]);

  useEffect(() => {
    void load();
  }, [load]);

  const submit = async () => {
    if (!draft.trim() || sending) return;
    setSending(true);
    try {
      const r = await fetch("/api/comments", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          entity_type: entityType,
          entity_id: entityId,
          body: draft,
          link,
        }),
      });
      const j = await r.json();
      if (r.ok) {
        setDraft("");
        await load();
        setTimeout(() => bottomRef.current?.scrollIntoView({ behavior: "smooth" }), 50);
        if (j.mentionsCreated > 0) {
          toast.success(
            `${j.mentionsCreated} personne(s) notifiée(s)`,
          );
        }
      } else {
        toast.error(j.error ?? "Erreur");
      }
    } finally {
      setSending(false);
    }
  };

  return (
    <div className="flex flex-col rounded-xl border border-border bg-card">
      <div className="flex items-center gap-2 border-b border-border p-3">
        <MessageSquare className="h-4 w-4 text-primary" />
        <h3 className="text-sm font-bold">
          Commentaires{" "}
          <span className="text-muted-foreground">({items.length})</span>
        </h3>
      </div>

      <div className="max-h-[420px] overflow-y-auto p-3">
        {loading ? (
          <div className="flex h-16 items-center justify-center">
            <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
          </div>
        ) : items.length === 0 ? (
          <p className="text-center text-xs text-muted-foreground">
            Aucun commentaire. Tape <strong>@</strong> pour mentionner un collaborateur.
          </p>
        ) : (
          <div className="space-y-2">
            {items.map((c) => (
              <CommentItem key={c.id} comment={c} />
            ))}
            <div ref={bottomRef} />
          </div>
        )}
      </div>

      <div className="space-y-2 border-t border-border bg-secondary/30 p-3">
        <MentionInput
          value={draft}
          onChange={setDraft}
          onSubmit={submit}
          disabled={sending}
        />
        <div className="flex items-center justify-between gap-2">
          <p className="text-[10px] text-muted-foreground">
            <strong>Entrée</strong> pour envoyer · <strong>Shift+Entrée</strong> retour ligne · <strong>@</strong> mention
          </p>
          <Button size="sm" onClick={submit} disabled={sending || !draft.trim()}>
            {sending ? (
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
            ) : (
              <Send className="h-3.5 w-3.5" />
            )}
            Envoyer
          </Button>
        </div>
      </div>
    </div>
  );
}

function CommentItem({ comment: c }: { comment: Comment }) {
  const date = new Date(c.created_at * 1000);
  const dateStr = date.toLocaleDateString("fr-FR", {
    day: "2-digit",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
  });
  const authorLabel = c.author_name ?? c.author_email ?? "Utilisateur";
  return (
    <div className="rounded-lg bg-background p-2.5">
      <div className="mb-1 flex items-center gap-2 text-[11px]">
        <Avatar name={authorLabel} />
        <span className="font-semibold">{authorLabel}</span>
        <span className="text-muted-foreground">·</span>
        <span className="text-muted-foreground">{dateStr}</span>
      </div>
      <CommentBody body={c.body} />
    </div>
  );
}

function CommentBody({ body }: { body: string }) {
  // Parse @[Nom](user:42) → spans bleus
  const parts: React.ReactNode[] = [];
  const re = /@\[([^\]]+)\]\(user:(\d+)\)/g;
  let last = 0;
  let m: RegExpExecArray | null;
  while ((m = re.exec(body)) !== null) {
    if (m.index > last) parts.push(body.slice(last, m.index));
    parts.push(
      <span
        key={`${m.index}-${m[2]}`}
        className="rounded bg-primary/15 px-1 py-0.5 text-[12px] font-semibold text-primary"
      >
        @{m[1]}
      </span>,
    );
    last = m.index + m[0].length;
  }
  if (last < body.length) parts.push(body.slice(last));
  return <div className="whitespace-pre-wrap text-sm leading-snug">{parts}</div>;
}

function Avatar({ name }: { name: string }) {
  const initials = name
    .split(/[\s.@]+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((s) => s[0]?.toUpperCase() ?? "")
    .join("");
  return (
    <span
      className="flex h-5 w-5 items-center justify-center rounded-full bg-primary/10 text-[9px] font-bold text-primary"
      title={name}
    >
      {initials}
    </span>
  );
}
