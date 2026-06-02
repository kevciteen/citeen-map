"use client";
/**
 * Gestionnaire de templates email — page /settings/email-templates.
 *
 * Liste + éditeur inline (split view) :
 *  - gauche : liste de tous les templates (partagés + privés du user)
 *  - droite : éditeur du template sélectionné (nom, scope, objet, corps)
 *
 * Pas de WYSIWYG : texte brut → directement exploitable en mailto:.
 */
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { Globe, Lock, Plus, Save, Trash2 } from "lucide-react";
import { jsonFetcher } from "@/lib/fetcher";
import { Skeleton } from "@/components/ui/skeleton";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from "@/components/ui/select";
import { toast } from "sonner";

type Template = {
  id: number;
  name: string;
  subject: string;
  body: string;
  scope: "prospect" | "syndic" | "copro" | "generic";
  is_shared: number;
  created_by: number | null;
};

const SCOPE_LABEL: Record<Template["scope"], string> = {
  prospect: "Prospect",
  syndic: "Syndic",
  copro: "Copropriété",
  generic: "Générique",
};

const VARS_AVAILABLE = [
  { key: "nom_copro", label: "Nom de la copro" },
  { key: "adresse", label: "Adresse" },
  { key: "commune", label: "Commune" },
  { key: "syndic", label: "Nom du syndic" },
  { key: "nb_lots", label: "Nombre de lots" },
  { key: "classe_dpe", label: "Classe DPE" },
  { key: "prenom_destinataire", label: "Prénom destinataire" },
  { key: "nom_destinataire", label: "Nom destinataire" },
  { key: "mon_prenom", label: "Mon prénom" },
  { key: "mon_nom", label: "Mon nom" },
  { key: "mon_email", label: "Mon email" },
  { key: "lien_fiche_copro", label: "Lien fiche copro" },
];

export function EmailTemplatesManager() {
  const qc = useQueryClient();
  const [selectedId, setSelectedId] = useState<number | null>(null);

  const { data, isPending } = useQuery({
    queryKey: ["email-templates"],
    queryFn: ({ signal }) =>
      jsonFetcher<{ items: Template[] }>("/api/email-templates", signal),
  });

  const createMut = useMutation({
    mutationFn: async () => {
      const r = await fetch("/api/email-templates", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: "Nouveau template",
          subject: "Objet à compléter",
          body: "Bonjour {{prenom_destinataire}},\n\nÉcrivez votre message…\n\nCordialement,\n{{mon_prenom}} {{mon_nom}}",
          scope: "generic",
          isShared: true,
        }),
      });
      if (!r.ok) throw new Error("Erreur création");
      const j = await r.json();
      return j.id as number;
    },
    onSuccess: (id) => {
      qc.invalidateQueries({ queryKey: ["email-templates"] });
      setSelectedId(id);
      toast.success("Template créé");
    },
    onError: (e) => toast.error((e as Error).message),
  });

  const selected = data?.items.find((t) => t.id === selectedId) ?? null;

  // Auto-select 1er template au chargement
  useEffect(() => {
    if (!selectedId && data?.items.length) {
      setSelectedId(data.items[0].id);
    }
  }, [data, selectedId]);

  return (
    <div className="grid gap-3 lg:grid-cols-[320px_1fr]">
      {/* Liste */}
      <div className="space-y-2">
        <div className="flex items-center justify-between">
          <h2 className="text-sm font-semibold">Modèles</h2>
          <Button
            size="sm"
            onClick={() => createMut.mutate()}
            disabled={createMut.isPending}
          >
            <Plus className="h-3.5 w-3.5" /> Nouveau
          </Button>
        </div>
        {isPending ? (
          <Skeleton className="h-48 w-full" />
        ) : data && data.items.length > 0 ? (
          <ul className="space-y-1">
            {data.items.map((t) => (
              <li key={t.id}>
                <button
                  onClick={() => setSelectedId(t.id)}
                  className={`flex w-full items-start gap-2 rounded-md border p-2 text-left text-xs ${
                    selectedId === t.id
                      ? "border-primary bg-primary/5"
                      : "border-border bg-card hover:bg-secondary/30"
                  }`}
                >
                  <span className="mt-0.5 rounded bg-secondary px-1.5 py-0.5 text-[9px] uppercase tracking-wider">
                    {SCOPE_LABEL[t.scope]}
                  </span>
                  <div className="min-w-0 flex-1">
                    <div className="truncate font-medium">{t.name}</div>
                    <div className="truncate text-[10px] text-muted-foreground">
                      {t.subject}
                    </div>
                  </div>
                  {t.is_shared ? (
                    <Globe className="h-3 w-3 text-emerald-700" />
                  ) : (
                    <Lock className="h-3 w-3 text-muted-foreground" />
                  )}
                </button>
              </li>
            ))}
          </ul>
        ) : (
          <p className="rounded-md border border-border bg-card p-4 text-center text-xs text-muted-foreground">
            Aucun modèle — crée le premier.
          </p>
        )}
      </div>

      {/* Éditeur */}
      <div>
        {selected ? (
          <TemplateEditor key={selected.id} template={selected} />
        ) : (
          <div className="rounded-lg border border-border bg-card p-6 text-center text-xs text-muted-foreground">
            Sélectionne un modèle ou crée-en un nouveau.
          </div>
        )}
      </div>
    </div>
  );
}

function TemplateEditor({ template }: { template: Template }) {
  const qc = useQueryClient();
  const [form, setForm] = useState({
    name: template.name,
    subject: template.subject,
    body: template.body,
    scope: template.scope,
    isShared: !!template.is_shared,
  });

  const saveMut = useMutation({
    mutationFn: async () => {
      const r = await fetch(`/api/email-templates/${template.id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(form),
      });
      if (!r.ok) throw new Error("Erreur enregistrement");
    },
    onSuccess: () => {
      toast.success("Modèle enregistré");
      qc.invalidateQueries({ queryKey: ["email-templates"] });
    },
    onError: (e) => toast.error((e as Error).message),
  });

  const deleteMut = useMutation({
    mutationFn: async () => {
      const r = await fetch(`/api/email-templates/${template.id}`, {
        method: "DELETE",
      });
      if (!r.ok) throw new Error("Erreur suppression");
    },
    onSuccess: () => {
      toast.success("Modèle supprimé");
      qc.invalidateQueries({ queryKey: ["email-templates"] });
    },
    onError: (e) => toast.error((e as Error).message),
  });

  const insertVar = (key: string) => {
    setForm((f) => ({ ...f, body: `${f.body}{{${key}}}` }));
  };

  return (
    <div className="space-y-3 rounded-lg border border-border bg-card p-4">
      <div className="grid gap-3 sm:grid-cols-2">
        <div>
          <label className="text-[10px] uppercase tracking-wider text-muted-foreground">
            Nom
          </label>
          <Input
            value={form.name}
            onChange={(e) => setForm((f) => ({ ...f, name: e.target.value }))}
          />
        </div>
        <div>
          <label className="text-[10px] uppercase tracking-wider text-muted-foreground">
            Contexte
          </label>
          <Select
            value={form.scope}
            onValueChange={(v) =>
              setForm((f) => ({ ...f, scope: v as Template["scope"] }))
            }
          >
            <SelectTrigger>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {Object.entries(SCOPE_LABEL).map(([k, l]) => (
                <SelectItem key={k} value={k}>{l}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>

      <div>
        <label className="text-[10px] uppercase tracking-wider text-muted-foreground">
          Objet
        </label>
        <Input
          value={form.subject}
          onChange={(e) => setForm((f) => ({ ...f, subject: e.target.value }))}
        />
      </div>

      <div>
        <label className="text-[10px] uppercase tracking-wider text-muted-foreground">
          Corps du message
        </label>
        <Textarea
          rows={12}
          value={form.body}
          onChange={(e) => setForm((f) => ({ ...f, body: e.target.value }))}
          className="font-mono text-xs"
        />
      </div>

      <div>
        <p className="mb-1 text-[10px] uppercase tracking-wider text-muted-foreground">
          Variables disponibles (clique pour insérer)
        </p>
        <div className="flex flex-wrap gap-1">
          {VARS_AVAILABLE.map((v) => (
            <button
              key={v.key}
              onClick={() => insertVar(v.key)}
              className="rounded border border-border bg-secondary/30 px-2 py-0.5 text-[10px] hover:bg-secondary"
              title={v.label}
            >
              {`{{${v.key}}}`}
            </button>
          ))}
        </div>
      </div>

      <div className="flex items-center justify-between border-t border-border pt-3">
        <label className="flex items-center gap-2 text-xs">
          <input
            type="checkbox"
            checked={form.isShared}
            onChange={(e) => setForm((f) => ({ ...f, isShared: e.target.checked }))}
          />
          Partagé avec l&apos;équipe
        </label>
        <div className="flex gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => {
              if (confirm("Supprimer ce modèle ?")) deleteMut.mutate();
            }}
            disabled={deleteMut.isPending}
          >
            <Trash2 className="h-3.5 w-3.5" /> Supprimer
          </Button>
          <Button onClick={() => saveMut.mutate()} disabled={saveMut.isPending}>
            <Save className="h-3.5 w-3.5" /> Enregistrer
          </Button>
        </div>
      </div>
    </div>
  );
}
