"use client";
/**
 * Panneau d'édition CRM-style réutilisable pour n'importe quelle entité.
 *
 * 3 sections :
 *  - OVERRIDES : champs surchargeables (libellés prédéfinis ou custom).
 *    L'utilisateur peut renommer une copro, corriger un téléphone Sirene,
 *    etc. La data source publique reste intacte ; on stocke un override
 *    en DB qui prend la priorité côté lecture.
 *  - NOTES : notes libres horodatées, append-only.
 *  - TAGS : tags personnalisés filtrables (mots-clés normalisés).
 *
 * Utilisation type :
 *   <EntityEditPanel
 *     entityType="copro"
 *     entityRef={String(copro.id)}
 *     suggestedFields={[
 *       { key: "nom", label: "Nom" },
 *       { key: "telephone", label: "Téléphone" },
 *       { key: "email", label: "Email" },
 *     ]}
 *   />
 */
import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  Pencil, Plus, X, Loader2, Check, Tag as TagIcon, StickyNote,
  Sparkles, Trash2, Clock,
} from "lucide-react";
import { jsonFetcher } from "@/lib/fetcher";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { toast } from "sonner";

type Override = {
  field_name: string;
  value: string | null;
  author_id: number | null;
  updated_at: number;
};
type EntityNote = {
  id: number;
  body: string;
  author_id: number | null;
  created_at: number;
};
type EntityTag = {
  id: number;
  tag: string;
  author_id: number | null;
  created_at: number;
};
type Overlay = {
  overrides: Override[];
  notes: EntityNote[];
  tags: EntityTag[];
};

export type SuggestedField = {
  key: string;
  label: string;
  type?: "text" | "tel" | "email" | "url" | "textarea";
};

export function EntityEditPanel({
  entityType,
  entityRef,
  title = "Édition CRM (overrides + notes + tags)",
  suggestedFields = DEFAULT_SUGGESTED,
}: {
  entityType: string;
  entityRef: string;
  title?: string;
  suggestedFields?: SuggestedField[];
}) {
  const qc = useQueryClient();
  const queryKey = ["entity-overlay", entityType, entityRef] as const;

  const { data, isPending } = useQuery({
    queryKey,
    queryFn: ({ signal }) =>
      jsonFetcher<Overlay>(
        `/api/overrides?type=${entityType}&ref=${encodeURIComponent(entityRef)}`,
        signal,
      ),
    staleTime: 30 * 1000,
  });

  const overrides = data?.overrides ?? [];
  const notes = data?.notes ?? [];
  const tags = data?.tags ?? [];

  // Map override par fieldName
  const overrideMap = new Map(overrides.map((o) => [o.field_name, o]));

  const setOverrideMut = useMutation({
    mutationFn: async ({ field, value }: { field: string; value: string | null }) => {
      const r = await fetch(
        `/api/overrides?type=${entityType}&ref=${encodeURIComponent(entityRef)}`,
        {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ field, value }),
        },
      );
      if (!r.ok) throw new Error("Erreur");
    },
    onSuccess: () => qc.invalidateQueries({ queryKey }),
    onError: (e) => toast.error((e as Error).message),
  });

  const addNoteMut = useMutation({
    mutationFn: async (body: string) => {
      const r = await fetch(
        `/api/overrides/notes?type=${entityType}&ref=${encodeURIComponent(entityRef)}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ body }),
        },
      );
      if (!r.ok) throw new Error("Erreur");
    },
    onSuccess: () => qc.invalidateQueries({ queryKey }),
    onError: (e) => toast.error((e as Error).message),
  });

  const deleteNoteMut = useMutation({
    mutationFn: async (noteId: number) => {
      const r = await fetch(`/api/overrides/notes/${noteId}`, { method: "DELETE" });
      if (!r.ok) throw new Error("Erreur");
    },
    onSuccess: () => qc.invalidateQueries({ queryKey }),
  });

  const addTagMut = useMutation({
    mutationFn: async (tag: string) => {
      const r = await fetch(
        `/api/overrides/tags?type=${entityType}&ref=${encodeURIComponent(entityRef)}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ tag }),
        },
      );
      if (!r.ok) throw new Error("Erreur");
    },
    onSuccess: () => qc.invalidateQueries({ queryKey }),
  });

  const removeTagMut = useMutation({
    mutationFn: async (tag: string) => {
      const r = await fetch(
        `/api/overrides/tags?type=${entityType}&ref=${encodeURIComponent(entityRef)}&tag=${encodeURIComponent(tag)}`,
        { method: "DELETE" },
      );
      if (!r.ok) throw new Error("Erreur");
    },
    onSuccess: () => qc.invalidateQueries({ queryKey }),
  });

  return (
    <section className="rounded-xl border border-border bg-card p-4 shadow-sm print:hidden">
      <h2 className="mb-3 flex items-center gap-2 text-sm font-bold">
        <Pencil className="h-4 w-4 text-primary" />
        {title}
        {isPending ? (
          <Loader2 className="ml-2 h-3 w-3 animate-spin text-muted-foreground" />
        ) : null}
      </h2>

      {isPending ? (
        <Skeleton className="h-32 w-full" />
      ) : (
        <div className="space-y-4">
          {/* TAGS */}
          <TagsBlock
            tags={tags}
            onAdd={(t) => addTagMut.mutate(t)}
            onRemove={(t) => removeTagMut.mutate(t)}
          />

          {/* OVERRIDES */}
          <OverridesBlock
            overrideMap={overrideMap}
            suggestedFields={suggestedFields}
            onSet={(field, value) => setOverrideMut.mutate({ field, value })}
            pending={setOverrideMut.isPending}
          />

          {/* NOTES */}
          <NotesBlock
            notes={notes}
            onAdd={(b) => addNoteMut.mutate(b)}
            onDelete={(id) => deleteNoteMut.mutate(id)}
            pending={addNoteMut.isPending}
          />
        </div>
      )}
    </section>
  );
}

/* ============================== BLOCKS ============================== */

function TagsBlock({
  tags, onAdd, onRemove,
}: {
  tags: EntityTag[];
  onAdd: (tag: string) => void;
  onRemove: (tag: string) => void;
}) {
  const [input, setInput] = useState("");
  return (
    <div>
      <p className="mb-1.5 flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
        <TagIcon className="h-3 w-3" /> Tags
      </p>
      <div className="flex flex-wrap items-center gap-1.5">
        {tags.map((t) => (
          <span
            key={t.id}
            className="inline-flex items-center gap-1 rounded-full border border-primary/30 bg-primary/10 px-2.5 py-0.5 text-[11px] font-medium text-primary"
          >
            {t.tag}
            <button
              onClick={() => onRemove(t.tag)}
              className="rounded hover:bg-primary/20"
            >
              <X className="h-2.5 w-2.5" />
            </button>
          </span>
        ))}
        <form
          onSubmit={(e) => {
            e.preventDefault();
            if (!input.trim()) return;
            onAdd(input);
            setInput("");
          }}
          className="flex items-center gap-1"
        >
          <Input
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="ajouter un tag…"
            className="h-7 w-32 text-[11px]"
          />
          <button
            type="submit"
            disabled={!input.trim()}
            className="rounded-full bg-primary p-1 text-primary-foreground disabled:opacity-40"
          >
            <Plus className="h-3 w-3" />
          </button>
        </form>
      </div>
    </div>
  );
}

function OverridesBlock({
  overrideMap, suggestedFields, onSet, pending,
}: {
  overrideMap: Map<string, Override>;
  suggestedFields: SuggestedField[];
  onSet: (field: string, value: string | null) => void;
  pending: boolean;
}) {
  // Champs déjà overridés mais pas dans suggested → on les affiche aussi
  const customFields = [...overrideMap.values()]
    .filter((o) => !suggestedFields.some((s) => s.key === o.field_name))
    .map((o) => ({
      key: o.field_name,
      label: o.field_name,
      type: "text" as const,
    }));
  const allFields = [...suggestedFields, ...customFields];

  return (
    <div>
      <p className="mb-1.5 flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
        <Sparkles className="h-3 w-3" /> Surcharges
        <span className="font-normal opacity-70">
          (prennent la priorité sur les données source)
        </span>
      </p>
      <div className="grid gap-2 sm:grid-cols-2">
        {allFields.map((f) => (
          <FieldEditor
            key={f.key}
            field={f}
            current={overrideMap.get(f.key)}
            onSet={onSet}
            pending={pending}
          />
        ))}
      </div>
      <AddCustomField onAdd={(k) => onSet(k, "")} />
    </div>
  );
}

function FieldEditor({
  field, current, onSet, pending,
}: {
  field: SuggestedField;
  current: Override | undefined;
  onSet: (field: string, value: string | null) => void;
  pending: boolean;
}) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(current?.value ?? "");
  const isOverridden = current && current.value !== null;

  if (!editing) {
    return (
      <div className="group rounded-md border border-border bg-secondary/30 p-2">
        <div className="flex items-baseline justify-between gap-2 text-xs">
          <span className="text-muted-foreground">{field.label}</span>
          {isOverridden ? (
            <span className="rounded bg-emerald-100 px-1 py-0 text-[9px] font-semibold text-emerald-900">
              édité
            </span>
          ) : null}
        </div>
        <div className="mt-0.5 flex items-center justify-between gap-2">
          <p className="truncate text-sm">
            {current?.value || <span className="text-muted-foreground/60">—</span>}
          </p>
          <button
            onClick={() => {
              setDraft(current?.value ?? "");
              setEditing(true);
            }}
            className="opacity-0 transition-opacity group-hover:opacity-100"
          >
            <Pencil className="h-3 w-3 text-muted-foreground hover:text-primary" />
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="rounded-md border border-primary/40 bg-primary/5 p-2">
      <p className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
        {field.label}
      </p>
      <div className="flex items-center gap-1">
        {field.type === "textarea" ? (
          <textarea
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            rows={2}
            className="flex-1 rounded border border-input bg-background px-2 py-1 text-xs"
          />
        ) : (
          <Input
            type={field.type ?? "text"}
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            className="h-7 text-xs"
            autoFocus
          />
        )}
        <button
          onClick={() => {
            onSet(field.key, draft.trim() ? draft : null);
            setEditing(false);
          }}
          disabled={pending}
          className="rounded bg-primary p-1 text-primary-foreground"
        >
          <Check className="h-3 w-3" />
        </button>
        <button
          onClick={() => setEditing(false)}
          className="rounded bg-secondary p-1"
        >
          <X className="h-3 w-3" />
        </button>
      </div>
    </div>
  );
}

function AddCustomField({ onAdd }: { onAdd: (key: string) => void }) {
  const [open, setOpen] = useState(false);
  const [key, setKey] = useState("");
  if (!open) {
    return (
      <button
        onClick={() => setOpen(true)}
        className="mt-2 inline-flex items-center gap-1 rounded-md border border-dashed border-border bg-background px-2 py-1 text-[11px] text-muted-foreground hover:border-primary/40 hover:text-primary"
      >
        <Plus className="h-3 w-3" /> Ajouter un champ personnalisé
      </button>
    );
  }
  return (
    <form
      onSubmit={(e) => {
        e.preventDefault();
        const k = key.trim().toLowerCase().replace(/[^a-z0-9_]+/g, "_");
        if (k) onAdd(k);
        setKey("");
        setOpen(false);
      }}
      className="mt-2 flex items-center gap-1"
    >
      <Input
        value={key}
        onChange={(e) => setKey(e.target.value)}
        placeholder="nom_du_champ (ex: contact_president)"
        className="h-7 max-w-[280px] text-[11px]"
        autoFocus
      />
      <button type="submit" disabled={!key.trim()} className="rounded bg-primary p-1 text-primary-foreground disabled:opacity-40">
        <Check className="h-3 w-3" />
      </button>
      <button type="button" onClick={() => setOpen(false)} className="rounded bg-secondary p-1">
        <X className="h-3 w-3" />
      </button>
    </form>
  );
}

function NotesBlock({
  notes, onAdd, onDelete, pending,
}: {
  notes: EntityNote[];
  onAdd: (body: string) => void;
  onDelete: (id: number) => void;
  pending: boolean;
}) {
  const [draft, setDraft] = useState("");
  return (
    <div>
      <p className="mb-1.5 flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
        <StickyNote className="h-3 w-3" /> Notes ({notes.length})
      </p>
      <form
        onSubmit={(e) => {
          e.preventDefault();
          if (!draft.trim()) return;
          onAdd(draft);
          setDraft("");
        }}
        className="mb-2 flex items-start gap-2"
      >
        <textarea
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          placeholder="Note libre (ex: 'Rappelé syndic le 25/03 — chantier à venir')"
          rows={2}
          className="flex-1 rounded-md border border-input bg-background px-2 py-1.5 text-xs"
        />
        <Button type="submit" size="sm" disabled={pending || !draft.trim()}>
          {pending ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Plus className="h-3.5 w-3.5" />}
          Ajouter
        </Button>
      </form>
      {notes.length > 0 ? (
        <ul className="space-y-1.5">
          {notes.map((n) => (
            <li key={n.id} className="group flex items-start gap-2 rounded-md border border-border bg-secondary/20 p-2">
              <div className="min-w-0 flex-1">
                <p className="whitespace-pre-wrap text-xs">{n.body}</p>
                <p className="mt-0.5 flex items-center gap-1 text-[10px] text-muted-foreground">
                  <Clock className="h-2.5 w-2.5" />
                  {new Date(n.created_at * 1000).toLocaleString("fr-FR")}
                </p>
              </div>
              <button
                onClick={() => onDelete(n.id)}
                className="opacity-0 transition-opacity group-hover:opacity-100"
                title="Supprimer"
              >
                <Trash2 className="h-3 w-3 text-muted-foreground hover:text-red-700" />
              </button>
            </li>
          ))}
        </ul>
      ) : (
        <p className="text-[11px] text-muted-foreground">Aucune note. Utilise le formulaire ci-dessus.</p>
      )}
    </div>
  );
}

/* ============================== Préréglages ============================== */

const DEFAULT_SUGGESTED: SuggestedField[] = [
  { key: "nom_personnalise", label: "Nom personnalisé" },
  { key: "telephone", label: "Téléphone", type: "tel" },
  { key: "email", label: "Email", type: "email" },
  { key: "contact_principal", label: "Contact principal" },
  { key: "site_web", label: "Site web", type: "url" },
  { key: "commentaire_court", label: "Commentaire", type: "textarea" },
];
