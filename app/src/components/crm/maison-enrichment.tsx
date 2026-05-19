"use client";
import { useEffect, useState } from "react";
import {
  UserPlus,
  Phone,
  Mail,
  Building,
  Trash2,
  Plus,
  StickyNote,
  Paperclip,
  ExternalLink,
  Loader2,
  Pencil,
} from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";
import { toast } from "sonner";

type Contact = {
  id: number;
  prospect_id: number;
  role: string | null;
  full_name: string | null;
  email: string | null;
  phone: string | null;
  company: string | null;
  notes: string | null;
};

type Note = {
  id: number;
  prospect_id: number;
  body: string;
  author: string | null;
  created_at: number | null;
};

type Doc = {
  id: number;
  prospect_id: number;
  name: string;
  url: string;
  kind: string | null;
  description: string | null;
  created_at: number;
  created_by: string | null;
};

/**
 * Sections enrichissement (contacts, notes, documents) sur la fiche
 * maison/appartement détaillée. Toutes branchées sur le prospect lié.
 */
export function MaisonEnrichment({ prospectId }: { prospectId: number }) {
  return (
    <div className="space-y-4">
      <ContactsSection prospectId={prospectId} />
      <NotesSection prospectId={prospectId} />
      <DocumentsSection prospectId={prospectId} />
    </div>
  );
}

// === Contacts ============================================================

function ContactsSection({ prospectId }: { prospectId: number }) {
  const [contacts, setContacts] = useState<Contact[]>([]);
  const [loading, setLoading] = useState(true);
  const [adding, setAdding] = useState(false);
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState({
    role: "proprietaire" as "proprietaire" | "occupant" | "gardien" | "autre",
    fullName: "",
    email: "",
    phone: "",
    company: "",
    notes: "",
  });

  const reload = async () => {
    setLoading(true);
    try {
      const r = await fetch(`/api/prospects/${prospectId}/contacts`);
      const j = await r.json();
      setContacts(j.contacts ?? []);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    reload();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [prospectId]);

  const submit = async () => {
    if (!form.fullName.trim() && !form.phone.trim() && !form.email.trim()) {
      toast.error("Renseigne au moins un nom, email ou téléphone");
      return;
    }
    setAdding(true);
    try {
      const r = await fetch(`/api/prospects/${prospectId}/contacts`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(form),
      });
      if (!r.ok) {
        toast.error("Erreur ajout contact");
        return;
      }
      setForm({
        role: "proprietaire",
        fullName: "",
        email: "",
        phone: "",
        company: "",
        notes: "",
      });
      setShowForm(false);
      reload();
    } finally {
      setAdding(false);
    }
  };

  const remove = async (id: number) => {
    if (!confirm("Supprimer ce contact ?")) return;
    const r = await fetch(`/api/prospects/${prospectId}/contacts/${id}`, {
      method: "DELETE",
    });
    if (r.ok) reload();
  };

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between">
        <div className="flex items-center gap-2">
          <UserPlus className="h-4 w-4 text-primary" />
          <CardTitle className="text-sm">Contacts</CardTitle>
          {contacts.length > 0 ? (
            <span className="rounded-full bg-secondary px-2 py-0.5 text-[10px] font-bold">
              {contacts.length}
            </span>
          ) : null}
        </div>
        <Button
          size="sm"
          variant="outline"
          onClick={() => setShowForm(!showForm)}
        >
          <Plus className="h-3 w-3" /> {showForm ? "Annuler" : "Ajouter"}
        </Button>
      </CardHeader>
      <CardContent className="space-y-2">
        {loading ? (
          <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
        ) : contacts.length === 0 && !showForm ? (
          <p className="text-[11px] italic text-muted-foreground">
            Aucun contact — clique sur "Ajouter" pour saisir un propriétaire,
            occupant ou gardien.
          </p>
        ) : null}

        {showForm ? (
          <div className="rounded-lg border border-border bg-secondary/30 p-3">
            <div className="grid grid-cols-2 gap-2">
              <div>
                <label className="text-[10px] font-semibold text-muted-foreground">
                  Rôle
                </label>
                <select
                  value={form.role}
                  onChange={(e) =>
                    setForm({ ...form, role: e.target.value as typeof form.role })
                  }
                  className="h-8 w-full rounded-md border border-border bg-background px-2 text-xs"
                >
                  <option value="proprietaire">Propriétaire</option>
                  <option value="occupant">Occupant (locataire)</option>
                  <option value="gardien">Gardien / Concierge</option>
                  <option value="autre">Autre</option>
                </select>
              </div>
              <div>
                <label className="text-[10px] font-semibold text-muted-foreground">
                  Nom complet
                </label>
                <Input
                  value={form.fullName}
                  onChange={(e) => setForm({ ...form, fullName: e.target.value })}
                  placeholder="Jean Dupont"
                  className="h-8 text-xs"
                />
              </div>
              <div>
                <label className="text-[10px] font-semibold text-muted-foreground">
                  Email
                </label>
                <Input
                  value={form.email}
                  onChange={(e) => setForm({ ...form, email: e.target.value })}
                  type="email"
                  placeholder="jean@example.fr"
                  className="h-8 text-xs"
                />
              </div>
              <div>
                <label className="text-[10px] font-semibold text-muted-foreground">
                  Téléphone
                </label>
                <Input
                  value={form.phone}
                  onChange={(e) => setForm({ ...form, phone: e.target.value })}
                  placeholder="06 12 34 56 78"
                  className="h-8 text-xs"
                />
              </div>
              <div className="col-span-2">
                <label className="text-[10px] font-semibold text-muted-foreground">
                  Société (optionnel)
                </label>
                <Input
                  value={form.company}
                  onChange={(e) => setForm({ ...form, company: e.target.value })}
                  className="h-8 text-xs"
                />
              </div>
              <div className="col-span-2">
                <label className="text-[10px] font-semibold text-muted-foreground">
                  Notes
                </label>
                <Input
                  value={form.notes}
                  onChange={(e) => setForm({ ...form, notes: e.target.value })}
                  placeholder="Indispo le mercredi, etc."
                  className="h-8 text-xs"
                />
              </div>
            </div>
            <div className="mt-2 flex gap-2">
              <Button size="sm" onClick={submit} disabled={adding}>
                {adding ? <Loader2 className="h-3 w-3 animate-spin" /> : null}
                Enregistrer
              </Button>
              <Button size="sm" variant="outline" onClick={() => setShowForm(false)}>
                Annuler
              </Button>
            </div>
          </div>
        ) : null}

        {contacts.map((c) => (
          <div
            key={c.id}
            className="flex items-start justify-between gap-2 rounded-lg border border-border bg-card p-2"
          >
            <div className="flex-1">
              <div className="flex items-center gap-2">
                <RoleBadge role={c.role} />
                <span className="text-sm font-semibold">
                  {c.full_name ?? "(sans nom)"}
                </span>
              </div>
              <div className="mt-1 flex flex-wrap items-center gap-3 text-[11px] text-muted-foreground">
                {c.phone ? (
                  <a
                    href={`tel:${c.phone}`}
                    className="flex items-center gap-1 hover:text-foreground"
                  >
                    <Phone className="h-3 w-3" />
                    {c.phone}
                  </a>
                ) : null}
                {c.email ? (
                  <a
                    href={`mailto:${c.email}`}
                    className="flex items-center gap-1 hover:text-foreground"
                  >
                    <Mail className="h-3 w-3" />
                    {c.email}
                  </a>
                ) : null}
                {c.company ? (
                  <span className="flex items-center gap-1">
                    <Building className="h-3 w-3" />
                    {c.company}
                  </span>
                ) : null}
              </div>
              {c.notes ? (
                <p className="mt-1 text-[11px] italic text-muted-foreground">
                  {c.notes}
                </p>
              ) : null}
            </div>
            <button
              onClick={() => remove(c.id)}
              className="rounded-md p-1 text-muted-foreground hover:bg-destructive/10 hover:text-destructive"
              title="Supprimer"
            >
              <Trash2 className="h-3.5 w-3.5" />
            </button>
          </div>
        ))}
      </CardContent>
    </Card>
  );
}

function RoleBadge({ role }: { role: string | null }) {
  const cls =
    role === "proprietaire"
      ? "bg-emerald-100 text-emerald-800"
      : role === "occupant"
        ? "bg-blue-100 text-blue-800"
        : role === "gardien"
          ? "bg-amber-100 text-amber-800"
          : "bg-slate-100 text-slate-700";
  const label =
    role === "proprietaire"
      ? "Propriétaire"
      : role === "occupant"
        ? "Occupant"
        : role === "gardien"
          ? "Gardien"
          : role ?? "Contact";
  return (
    <span
      className={cn(
        "rounded-full px-2 py-0.5 text-[9px] font-bold uppercase tracking-wider",
        cls,
      )}
    >
      {label}
    </span>
  );
}

// === Notes ===============================================================

function NotesSection({ prospectId }: { prospectId: number }) {
  const [notes, setNotes] = useState<Note[]>([]);
  const [loading, setLoading] = useState(true);
  const [adding, setAdding] = useState(false);
  const [draft, setDraft] = useState("");

  const reload = async () => {
    setLoading(true);
    try {
      const r = await fetch(`/api/prospects/${prospectId}/notes`);
      const j = await r.json();
      setNotes(j.notes ?? []);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    reload();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [prospectId]);

  const submit = async () => {
    const body = draft.trim();
    if (!body) return;
    setAdding(true);
    try {
      const r = await fetch(`/api/prospects/${prospectId}/notes`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ body }),
      });
      if (!r.ok) {
        toast.error("Erreur");
        return;
      }
      setDraft("");
      reload();
    } finally {
      setAdding(false);
    }
  };

  const remove = async (id: number) => {
    if (!confirm("Supprimer cette note ?")) return;
    const r = await fetch(`/api/prospects/${prospectId}/notes/${id}`, {
      method: "DELETE",
    });
    if (r.ok) reload();
  };

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center gap-2">
          <StickyNote className="h-4 w-4 text-primary" />
          <CardTitle className="text-sm">Notes libres</CardTitle>
          {notes.length > 0 ? (
            <span className="rounded-full bg-secondary px-2 py-0.5 text-[10px] font-bold">
              {notes.length}
            </span>
          ) : null}
        </div>
      </CardHeader>
      <CardContent className="space-y-2">
        <div className="flex gap-2">
          <textarea
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            placeholder="Ajouter une note (interphone, visite, infos terrain…)"
            className="flex-1 rounded-md border border-border bg-background p-2 text-xs"
            rows={2}
          />
          <Button size="sm" onClick={submit} disabled={adding || !draft.trim()}>
            {adding ? <Loader2 className="h-3 w-3 animate-spin" /> : <Plus className="h-3 w-3" />}
          </Button>
        </div>

        {loading ? (
          <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
        ) : notes.length === 0 ? (
          <p className="text-[11px] italic text-muted-foreground">
            Aucune note pour le moment.
          </p>
        ) : null}

        {notes.map((n) => (
          <div
            key={n.id}
            className="flex items-start justify-between gap-2 rounded-lg border border-border bg-card p-2 text-xs"
          >
            <div className="flex-1">
              <p className="whitespace-pre-wrap">{n.body}</p>
              <p className="mt-1 text-[10px] text-muted-foreground">
                {n.author ? `${n.author} · ` : ""}
                {n.created_at ? fmtDateTime(n.created_at) : ""}
              </p>
            </div>
            <button
              onClick={() => remove(n.id)}
              className="rounded-md p-1 text-muted-foreground hover:bg-destructive/10 hover:text-destructive"
              title="Supprimer"
            >
              <Trash2 className="h-3.5 w-3.5" />
            </button>
          </div>
        ))}
      </CardContent>
    </Card>
  );
}

// === Documents ===========================================================

function DocumentsSection({ prospectId }: { prospectId: number }) {
  const [docs, setDocs] = useState<Doc[]>([]);
  const [loading, setLoading] = useState(true);
  const [adding, setAdding] = useState(false);
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState({
    name: "",
    url: "",
    kind: "devis" as "devis" | "audit" | "photo" | "plan" | "courrier" | "autre",
    description: "",
  });

  const reload = async () => {
    setLoading(true);
    try {
      const r = await fetch(`/api/prospects/${prospectId}/documents`);
      const j = await r.json();
      setDocs(j.documents ?? []);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    reload();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [prospectId]);

  const submit = async () => {
    if (!form.name.trim() || !form.url.trim()) {
      toast.error("Nom et URL obligatoires");
      return;
    }
    setAdding(true);
    try {
      const r = await fetch(`/api/prospects/${prospectId}/documents`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(form),
      });
      if (!r.ok) {
        const j = await r.json().catch(() => null);
        toast.error(j?.error ? "URL invalide" : "Erreur ajout");
        return;
      }
      setForm({ name: "", url: "", kind: "devis", description: "" });
      setShowForm(false);
      reload();
    } finally {
      setAdding(false);
    }
  };

  const remove = async (id: number) => {
    if (!confirm("Supprimer ce document ?")) return;
    const r = await fetch(`/api/prospects/${prospectId}/documents/${id}`, {
      method: "DELETE",
    });
    if (r.ok) reload();
  };

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between">
        <div className="flex items-center gap-2">
          <Paperclip className="h-4 w-4 text-primary" />
          <CardTitle className="text-sm">Documents joints</CardTitle>
          {docs.length > 0 ? (
            <span className="rounded-full bg-secondary px-2 py-0.5 text-[10px] font-bold">
              {docs.length}
            </span>
          ) : null}
        </div>
        <Button
          size="sm"
          variant="outline"
          onClick={() => setShowForm(!showForm)}
        >
          <Plus className="h-3 w-3" /> {showForm ? "Annuler" : "Ajouter lien"}
        </Button>
      </CardHeader>
      <CardContent className="space-y-2">
        <p className="text-[10px] italic text-muted-foreground">
          Stocke un lien externe (Google Drive, Dropbox, OneDrive…). L'upload
          direct n'est pas encore disponible — partage le lien après upload
          ailleurs.
        </p>

        {showForm ? (
          <div className="rounded-lg border border-border bg-secondary/30 p-3">
            <div className="grid grid-cols-2 gap-2">
              <div>
                <label className="text-[10px] font-semibold text-muted-foreground">
                  Type
                </label>
                <select
                  value={form.kind}
                  onChange={(e) =>
                    setForm({ ...form, kind: e.target.value as typeof form.kind })
                  }
                  className="h-8 w-full rounded-md border border-border bg-background px-2 text-xs"
                >
                  <option value="devis">Devis</option>
                  <option value="audit">Audit énergétique</option>
                  <option value="photo">Photo</option>
                  <option value="plan">Plan</option>
                  <option value="courrier">Courrier / Email</option>
                  <option value="autre">Autre</option>
                </select>
              </div>
              <div>
                <label className="text-[10px] font-semibold text-muted-foreground">
                  Nom
                </label>
                <Input
                  value={form.name}
                  onChange={(e) => setForm({ ...form, name: e.target.value })}
                  placeholder="Devis chauffage 2026-05-15"
                  className="h-8 text-xs"
                />
              </div>
              <div className="col-span-2">
                <label className="text-[10px] font-semibold text-muted-foreground">
                  URL
                </label>
                <Input
                  value={form.url}
                  onChange={(e) => setForm({ ...form, url: e.target.value })}
                  placeholder="https://drive.google.com/..."
                  className="h-8 text-xs"
                />
              </div>
              <div className="col-span-2">
                <label className="text-[10px] font-semibold text-muted-foreground">
                  Description (optionnel)
                </label>
                <Input
                  value={form.description}
                  onChange={(e) =>
                    setForm({ ...form, description: e.target.value })
                  }
                  className="h-8 text-xs"
                />
              </div>
            </div>
            <div className="mt-2 flex gap-2">
              <Button size="sm" onClick={submit} disabled={adding}>
                {adding ? <Loader2 className="h-3 w-3 animate-spin" /> : null}
                Enregistrer
              </Button>
              <Button size="sm" variant="outline" onClick={() => setShowForm(false)}>
                Annuler
              </Button>
            </div>
          </div>
        ) : null}

        {loading ? (
          <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
        ) : docs.length === 0 && !showForm ? (
          <p className="text-[11px] italic text-muted-foreground">
            Aucun document joint.
          </p>
        ) : null}

        {docs.map((d) => (
          <div
            key={d.id}
            className="flex items-start justify-between gap-2 rounded-lg border border-border bg-card p-2 text-xs"
          >
            <div className="flex-1">
              <div className="flex items-center gap-2">
                <KindBadge kind={d.kind} />
                <a
                  href={d.url}
                  target="_blank"
                  rel="noreferrer"
                  className="font-semibold text-primary hover:underline"
                >
                  {d.name}
                  <ExternalLink className="ml-1 inline h-3 w-3" />
                </a>
              </div>
              {d.description ? (
                <p className="mt-1 text-[11px] text-muted-foreground">
                  {d.description}
                </p>
              ) : null}
              <p className="mt-0.5 text-[10px] text-muted-foreground">
                {d.created_by ? `${d.created_by} · ` : ""}
                {d.created_at ? fmtDateTime(d.created_at) : ""}
              </p>
            </div>
            <button
              onClick={() => remove(d.id)}
              className="rounded-md p-1 text-muted-foreground hover:bg-destructive/10 hover:text-destructive"
              title="Supprimer"
            >
              <Trash2 className="h-3.5 w-3.5" />
            </button>
          </div>
        ))}
      </CardContent>
    </Card>
  );
}

function KindBadge({ kind }: { kind: string | null }) {
  const cls =
    kind === "devis"
      ? "bg-emerald-100 text-emerald-800"
      : kind === "audit"
        ? "bg-blue-100 text-blue-800"
        : kind === "photo"
          ? "bg-purple-100 text-purple-800"
          : kind === "plan"
            ? "bg-amber-100 text-amber-800"
            : kind === "courrier"
              ? "bg-slate-100 text-slate-700"
              : "bg-slate-100 text-slate-700";
  return (
    <span
      className={cn(
        "rounded-full px-2 py-0.5 text-[9px] font-bold uppercase tracking-wider",
        cls,
      )}
    >
      {kind ?? "doc"}
    </span>
  );
}

// === Utils ===============================================================

function fmtDateTime(secondsOrIso: number | string): string {
  const t =
    typeof secondsOrIso === "number"
      ? secondsOrIso * 1000
      : Date.parse(secondsOrIso);
  if (!Number.isFinite(t)) return "";
  return new Date(t).toLocaleString("fr-FR", {
    day: "2-digit",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

void Pencil;
