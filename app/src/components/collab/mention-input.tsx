"use client";
import { useEffect, useRef, useState } from "react";
import { AtSign } from "lucide-react";

type UserSug = { id: number; email: string; name: string | null };

/**
 * Textarea avec autocomplete @mention. Insère `@[Nom](user:42)` quand
 * un utilisateur est choisi (format parseable côté serveur).
 *
 * Trigger : on détecte `@` suivi de texte jusqu'au curseur, on requête
 * /api/users/mention-search?q=<query>, on affiche un popover.
 *
 * Validation Enter (sans Shift) → submit. Shift+Enter = retour ligne.
 */
export function MentionInput({
  value,
  onChange,
  onSubmit,
  placeholder = "Écris un commentaire… @ pour mentionner un collaborateur",
  disabled,
  rows = 3,
}: {
  value: string;
  onChange: (v: string) => void;
  onSubmit?: () => void;
  placeholder?: string;
  disabled?: boolean;
  rows?: number;
}) {
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [users, setUsers] = useState<UserSug[]>([]);
  const [activeIdx, setActiveIdx] = useState(0);
  const [triggerPos, setTriggerPos] = useState(-1);

  // Lance la recherche dès qu'on ouvre le popover
  useEffect(() => {
    if (!open) return;
    const t = setTimeout(() => {
      void fetch(`/api/users/mention-search?q=${encodeURIComponent(query)}`)
        .then((r) => r.json())
        .then((j) => {
          setUsers(j.items as UserSug[]);
          setActiveIdx(0);
        });
    }, 120);
    return () => clearTimeout(t);
  }, [open, query]);

  const handleChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    const v = e.target.value;
    onChange(v);
    // Détecte si on est en train de taper une mention
    const caret = e.target.selectionStart ?? v.length;
    const before = v.slice(0, caret);
    // Trouver le dernier @ après un espace ou début de chaîne, sans espace après
    const m = before.match(/(?:^|\s)(@)([^\s@]{0,40})$/);
    if (m) {
      setTriggerPos(caret - m[2].length - 1); // position du @
      setQuery(m[2]);
      setOpen(true);
    } else {
      setOpen(false);
      setTriggerPos(-1);
    }
  };

  const insertMention = (u: UserSug) => {
    if (triggerPos < 0) return;
    const ta = textareaRef.current;
    const caret = ta?.selectionStart ?? value.length;
    const before = value.slice(0, triggerPos);
    const after = value.slice(caret);
    const displayName = u.name ?? u.email.split("@")[0];
    const mention = `@[${displayName}](user:${u.id}) `;
    const next = before + mention + after;
    onChange(next);
    setOpen(false);
    setTriggerPos(-1);
    setQuery("");
    // Place cursor after the inserted mention
    setTimeout(() => {
      if (!ta) return;
      const pos = before.length + mention.length;
      ta.focus();
      ta.setSelectionRange(pos, pos);
    }, 0);
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (open && users.length > 0) {
      if (e.key === "ArrowDown") {
        e.preventDefault();
        setActiveIdx((i) => (i + 1) % users.length);
        return;
      }
      if (e.key === "ArrowUp") {
        e.preventDefault();
        setActiveIdx((i) => (i - 1 + users.length) % users.length);
        return;
      }
      if (e.key === "Enter" || e.key === "Tab") {
        e.preventDefault();
        insertMention(users[activeIdx]);
        return;
      }
      if (e.key === "Escape") {
        e.preventDefault();
        setOpen(false);
        return;
      }
    }
    // Submit on plain Enter (no Shift, no popover open)
    if (e.key === "Enter" && !e.shiftKey && !open) {
      e.preventDefault();
      onSubmit?.();
    }
  };

  return (
    <div className="relative">
      <textarea
        ref={textareaRef}
        value={value}
        onChange={handleChange}
        onKeyDown={handleKeyDown}
        placeholder={placeholder}
        disabled={disabled}
        rows={rows}
        className="w-full resize-none rounded-lg border border-input bg-background px-3 py-2 text-sm focus:border-primary focus:outline-none disabled:opacity-50"
      />
      {open && users.length > 0 ? (
        <div className="absolute left-2 right-2 top-full z-30 mt-1 max-h-56 overflow-y-auto rounded-lg border border-border bg-popover shadow-lg">
          <p className="border-b border-border px-2 py-1 text-[10px] uppercase tracking-wider text-muted-foreground">
            <AtSign className="mr-1 inline h-3 w-3" /> Mentionner
          </p>
          {users.map((u, idx) => (
            <button
              key={u.id}
              type="button"
              onMouseDown={(e) => {
                e.preventDefault();
                insertMention(u);
              }}
              className={`flex w-full items-center justify-between gap-2 px-3 py-1.5 text-left text-sm ${
                idx === activeIdx ? "bg-secondary" : "hover:bg-secondary/50"
              }`}
            >
              <span className="truncate font-medium">
                {u.name ?? u.email.split("@")[0]}
              </span>
              <span className="truncate text-[10px] text-muted-foreground">
                {u.email}
              </span>
            </button>
          ))}
        </div>
      ) : null}
    </div>
  );
}
