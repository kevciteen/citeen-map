"use client";
/**
 * Bouton "Envoyer un email depuis un modèle".
 * Ouvre un modal qui :
 *  1. liste les templates pertinents (par scope)
 *  2. permet de sélectionner un destinataire parmi les contacts (si prospect)
 *  3. rend le template via /api/email-templates/render
 *  4. propose édition libre du sujet/corps avant envoi
 *  5. ouvre mailto: (l'envoi se fait dans le client mail du user)
 *
 * Pas de tracking d'envoi pour l'instant — on note juste une activité dans
 * le prospect quand le user a cliqué sur "Ouvrir dans le client mail".
 */
import { useMutation, useQuery } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import * as Dialog from "@radix-ui/react-dialog";
import { Mail, Loader2, ExternalLink, X } from "lucide-react";
import { jsonFetcher } from "@/lib/fetcher";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from "@/components/ui/select";
import Link from "next/link";
import { toast } from "sonner";

type Template = {
  id: number;
  name: string;
  subject: string;
  body: string;
  scope: "prospect" | "syndic" | "copro" | "generic";
};

type Contact = {
  id: number;
  full_name: string | null;
  email: string | null;
  role: string | null;
};

export function SendFromTemplate({
  scope,
  coproId,
  prospectId,
  contacts = [],
  label = "Email depuis modèle",
  size = "sm",
}: {
  scope: Template["scope"];
  coproId?: number;
  prospectId?: number;
  contacts?: Contact[];
  label?: string;
  size?: "sm" | "default";
}) {
  const [open, setOpen] = useState(false);
  const [templateId, setTemplateId] = useState<number | null>(null);
  const [contactId, setContactId] = useState<number | null>(null);
  const [subject, setSubject] = useState("");
  const [body, setBody] = useState("");
  const [to, setTo] = useState("");

  const { data: templatesData } = useQuery({
    queryKey: ["email-templates", scope],
    queryFn: ({ signal }) =>
      jsonFetcher<{ items: Template[] }>(`/api/email-templates?scope=${scope}`, signal),
    enabled: open,
  });

  const renderMut = useMutation({
    mutationFn: async (id: number) => {
      const r = await fetch("/api/email-templates/render", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          templateId: id,
          coproId,
          prospectId,
          contactId: contactId ?? undefined,
        }),
      });
      if (!r.ok) throw new Error("Erreur de rendu");
      return r.json() as Promise<{ subject: string; body: string; to: string | null }>;
    },
    onSuccess: (d) => {
      setSubject(d.subject);
      setBody(d.body);
      if (d.to) setTo(d.to);
    },
    onError: (e) => toast.error((e as Error).message),
  });

  // Auto-render quand template ou contact change
  useEffect(() => {
    if (open && templateId) renderMut.mutate(templateId);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [templateId, contactId, open]);

  const mailtoUrl = (() => {
    const params = new URLSearchParams();
    if (subject) params.set("subject", subject);
    if (body) params.set("body", body);
    return `mailto:${encodeURIComponent(to)}?${params.toString()}`;
  })();

  return (
    <Dialog.Root open={open} onOpenChange={setOpen}>
      <Dialog.Trigger asChild>
        <Button size={size} variant="outline">
          <Mail className="h-3.5 w-3.5" /> {label}
        </Button>
      </Dialog.Trigger>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-50 bg-black/60 backdrop-blur-sm" />
        <Dialog.Content className="fixed left-1/2 top-1/2 z-50 max-h-[90vh] w-[680px] max-w-[calc(100vw-2rem)] -translate-x-1/2 -translate-y-1/2 overflow-y-auto rounded-xl border border-border bg-card p-6 shadow-2xl space-y-3">
          <div className="flex items-start justify-between">
            <Dialog.Title className="text-sm font-bold">
              Envoyer un email depuis un modèle
            </Dialog.Title>
            <Dialog.Close asChild>
              <button className="flex h-7 w-7 items-center justify-center rounded-md text-muted-foreground hover:bg-secondary">
                <X className="h-4 w-4" />
              </button>
            </Dialog.Close>
          </div>

        {templatesData && templatesData.items.length === 0 ? (
          <div className="rounded-md border border-amber-200 bg-amber-50 p-3 text-xs text-amber-900">
            Aucun modèle disponible.{" "}
            <Link href="/settings/email-templates" className="underline">
              Créer un modèle
            </Link>
          </div>
        ) : null}

        <div className="grid gap-3 sm:grid-cols-2">
          <div>
            <label className="text-[10px] uppercase tracking-wider text-muted-foreground">
              Modèle
            </label>
            <Select
              value={templateId ? String(templateId) : ""}
              onValueChange={(v) => setTemplateId(Number(v))}
            >
              <SelectTrigger>
                <SelectValue placeholder="Choisir un modèle…" />
              </SelectTrigger>
              <SelectContent>
                {templatesData?.items.map((t) => (
                  <SelectItem key={t.id} value={String(t.id)}>
                    {t.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          {contacts.length > 0 ? (
            <div>
              <label className="text-[10px] uppercase tracking-wider text-muted-foreground">
                Destinataire (contact)
              </label>
              <Select
                value={contactId ? String(contactId) : ""}
                onValueChange={(v) => setContactId(Number(v))}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Choisir un contact…" />
                </SelectTrigger>
                <SelectContent>
                  {contacts
                    .filter((c) => c.email)
                    .map((c) => (
                      <SelectItem key={c.id} value={String(c.id)}>
                        {c.full_name ?? c.email} {c.role ? `· ${c.role}` : ""}
                      </SelectItem>
                    ))}
                </SelectContent>
              </Select>
            </div>
          ) : null}
        </div>

        <div>
          <label className="text-[10px] uppercase tracking-wider text-muted-foreground">
            À (email)
          </label>
          <Input
            value={to}
            onChange={(e) => setTo(e.target.value)}
            placeholder="destinataire@exemple.fr"
          />
        </div>

        <div>
          <label className="text-[10px] uppercase tracking-wider text-muted-foreground">
            Objet
          </label>
          <Input value={subject} onChange={(e) => setSubject(e.target.value)} />
        </div>

        <div>
          <label className="text-[10px] uppercase tracking-wider text-muted-foreground">
            Corps
          </label>
          <Textarea
            rows={10}
            value={body}
            onChange={(e) => setBody(e.target.value)}
            className="text-xs"
          />
          {body.includes("{{") ? (
            <p className="mt-1 text-[10px] text-amber-700">
              ⚠️ Certaines variables n&apos;ont pas pu être remplies — complète-les manuellement.
            </p>
          ) : null}
        </div>

        <div className="flex items-center justify-end gap-2 border-t border-border pt-3">
          {renderMut.isPending ? (
            <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />
          ) : null}
          <Button
            variant="outline"
            onClick={() => {
              navigator.clipboard.writeText(`Objet : ${subject}\n\n${body}`);
              toast.success("Contenu copié dans le presse-papiers");
            }}
            disabled={!subject && !body}
          >
            Copier
          </Button>
          <Button
            asChild
            disabled={!subject && !body}
          >
            <a href={mailtoUrl}>
              <ExternalLink className="h-3.5 w-3.5" /> Ouvrir dans le client mail
            </a>
          </Button>
        </div>
      </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
