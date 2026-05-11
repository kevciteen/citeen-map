"use client";
import { useEffect, useState } from "react";
import {
  Building2,
  Calendar,
  CheckSquare,
  Edit3,
  Mail,
  MessageSquare,
  Phone,
  Plus,
  Square,
  Trash2,
  User,
} from "lucide-react";
import { DpeBadge } from "@/components/ui/dpe-badge";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  PIPELINE_ORDER,
  stageMeta,
  formatCurrency,
  formatRelative,
} from "@/lib/utils";
import { toast } from "sonner";
import { useRouter } from "next/navigation";

type Note = { id: number; body: string; author: string | null; created_at: number };
type Task = {
  id: number;
  title: string;
  kind: string | null;
  due_at: number | null;
  done_at: number | null;
};
type Activity = {
  id: number;
  type: string;
  payload: string | null;
  created_at: number;
};
type Contact = {
  id: number;
  role: string | null;
  full_name: string | null;
  email: string | null;
  phone: string | null;
  company: string | null;
};

export function ProspectDetail({
  initialProspect,
  id,
}: {
  initialProspect: Record<string, any>;
  id: number;
}) {
  const router = useRouter();
  const [prospect, setProspect] = useState<Record<string, any>>(initialProspect);
  const [notes, setNotes] = useState<Note[]>([]);
  const [tasks, setTasks] = useState<Task[]>([]);
  const [activities, setActivities] = useState<Activity[]>([]);
  const [contacts, setContacts] = useState<Contact[]>([]);
  const [newNote, setNewNote] = useState("");
  const [newTask, setNewTask] = useState("");
  const [estimatedValue, setEstimatedValue] = useState<string>(
    prospect.estimated_value ? String(prospect.estimated_value) : "",
  );

  const fetchAll = async () => {
    const r = await fetch(`/api/prospects/${id}`);
    if (!r.ok) return;
    const j = await r.json();
    setProspect(j.prospect);
    setNotes(j.notes);
    setTasks(j.tasks);
    setActivities(j.activities);
    setContacts(j.contacts);
  };

  useEffect(() => {
    fetchAll();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [id]);

  const updateField = async (patch: Record<string, unknown>) => {
    const r = await fetch(`/api/prospects/${id}`, {
      method: "PATCH",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(patch),
    });
    if (r.ok) {
      toast.success("Mise à jour");
      fetchAll();
    } else toast.error("Erreur");
  };

  const addNote = async () => {
    if (!newNote.trim()) return;
    const r = await fetch(`/api/prospects/${id}/notes`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ body: newNote }),
    });
    if (r.ok) {
      setNewNote("");
      fetchAll();
    }
  };

  const addTask = async () => {
    if (!newTask.trim()) return;
    const r = await fetch(`/api/prospects/${id}/tasks`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ title: newTask }),
    });
    if (r.ok) {
      setNewTask("");
      fetchAll();
    }
  };

  const toggleTask = async (taskId: number, done: boolean) => {
    await fetch(`/api/prospects/${id}/tasks`, {
      method: "PATCH",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ taskId, done }),
    });
    fetchAll();
  };

  const deleteProspect = async () => {
    if (!confirm("Supprimer ce prospect ?")) return;
    const r = await fetch(`/api/prospects/${id}`, { method: "DELETE" });
    if (r.ok) {
      toast.success("Prospect supprimé");
      router.push("/prospects");
    }
  };

  const title =
    prospect.nom_copro ||
    prospect.adresse ||
    prospect.custom_label ||
    prospect.custom_address ||
    `Prospect #${id}`;

  return (
    <div className="mx-auto grid max-w-7xl gap-4 lg:grid-cols-[1fr_360px]">
      {/* Main column */}
      <div className="space-y-4">
        <Card>
          <CardHeader>
            <div className="flex items-start justify-between gap-3">
              <div className="flex items-start gap-3">
                <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-primary/10 text-primary">
                  <Building2 className="h-6 w-6" />
                </div>
                <div>
                  <CardTitle className="text-xl">{title}</CardTitle>
                  <p className="mt-0.5 text-sm text-muted-foreground">
                    {prospect.adresse}
                    {prospect.adresse ? " · " : ""}
                    {prospect.code_postal} {prospect.commune}
                  </p>
                  <div className="mt-2 flex flex-wrap items-center gap-1.5">
                    {prospect.numero_immatriculation ? (
                      <Badge variant="outline" className="font-mono text-[10px]">
                        {prospect.numero_immatriculation}
                      </Badge>
                    ) : null}
                    <DpeBadge classe={prospect.classe_finale} size="sm" />
                    {prospect.nb_lots_habitation ? (
                      <Badge variant="secondary">{prospect.nb_lots_habitation} lots</Badge>
                    ) : null}
                  </div>
                </div>
              </div>
              <Button variant="ghost" size="icon" onClick={deleteProspect}>
                <Trash2 className="h-4 w-4 text-destructive" />
              </Button>
            </div>
          </CardHeader>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-sm">Notes</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="flex gap-2">
              <Textarea
                value={newNote}
                onChange={(e) => setNewNote(e.target.value)}
                placeholder="Compte-rendu d'appel, prochaine étape, contexte…"
                className="min-h-[60px] flex-1"
              />
              <Button onClick={addNote} disabled={!newNote.trim()}>
                <Plus className="h-4 w-4" />
                Ajouter
              </Button>
            </div>
            <div className="space-y-2">
              {notes.length === 0 ? (
                <p className="text-xs text-muted-foreground">Aucune note.</p>
              ) : (
                notes.map((n) => (
                  <div key={n.id} className="rounded-lg border border-border bg-secondary/40 p-3">
                    <p className="whitespace-pre-wrap text-sm">{n.body}</p>
                    <p className="mt-1.5 text-[10px] text-muted-foreground">
                      {n.author ?? "Anonyme"} · {formatRelative(n.created_at)}
                    </p>
                  </div>
                ))
              )}
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-sm">Tâches & relances</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="flex gap-2">
              <Input
                value={newTask}
                onChange={(e) => setNewTask(e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && addTask()}
                placeholder="Rappeler le syndic / Envoyer offre / Visite sur place…"
              />
              <Button onClick={addTask} disabled={!newTask.trim()}>
                <Plus className="h-4 w-4" />
              </Button>
            </div>
            <ul className="space-y-1.5">
              {tasks.length === 0 ? (
                <p className="text-xs text-muted-foreground">Aucune tâche.</p>
              ) : (
                tasks.map((t) => (
                  <li key={t.id} className="flex items-center gap-2">
                    <button onClick={() => toggleTask(t.id, !t.done_at)}>
                      {t.done_at ? (
                        <CheckSquare className="h-4 w-4 text-emerald-600" />
                      ) : (
                        <Square className="h-4 w-4 text-muted-foreground" />
                      )}
                    </button>
                    <span
                      className={
                        t.done_at ? "text-sm text-muted-foreground line-through" : "text-sm"
                      }
                    >
                      {t.title}
                    </span>
                    {t.due_at ? (
                      <Badge variant="outline" className="ml-auto text-[10px]">
                        <Calendar className="mr-1 h-3 w-3" />
                        {new Date(t.due_at * 1000).toLocaleDateString("fr-FR")}
                      </Badge>
                    ) : null}
                  </li>
                ))
              )}
            </ul>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-sm">Historique d'activité</CardTitle>
          </CardHeader>
          <CardContent>
            <ul className="space-y-2">
              {activities.length === 0 ? (
                <li className="text-xs text-muted-foreground">Pas d'activité.</li>
              ) : (
                activities.slice(0, 10).map((a) => (
                  <li key={a.id} className="flex items-start gap-2 text-xs">
                    <span className="mt-1 h-1.5 w-1.5 rounded-full bg-primary" />
                    <div className="flex-1">
                      <span className="font-medium">{labelForActivity(a.type)}</span>
                      <span className="ml-2 text-muted-foreground">
                        {formatRelative(a.created_at)}
                      </span>
                    </div>
                  </li>
                ))
              )}
            </ul>
          </CardContent>
        </Card>
      </div>

      {/* Side column */}
      <div className="space-y-4">
        <Card>
          <CardHeader>
            <CardTitle className="text-sm">Statut</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <div>
              <label className="text-[10px] uppercase tracking-wider text-muted-foreground">
                Étape
              </label>
              <Select
                value={prospect.stage}
                onValueChange={(v) => updateField({ stage: v })}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {PIPELINE_ORDER.map((s) => (
                    <SelectItem key={s} value={s}>
                      {stageMeta(s).label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div>
              <label className="text-[10px] uppercase tracking-wider text-muted-foreground">
                Priorité
              </label>
              <Select
                value={String(prospect.priority ?? 2)}
                onValueChange={(v) => updateField({ priority: Number(v) })}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="1">Faible</SelectItem>
                  <SelectItem value="2">Normale</SelectItem>
                  <SelectItem value="3">Haute</SelectItem>
                </SelectContent>
              </Select>
            </div>

            <div>
              <label className="text-[10px] uppercase tracking-wider text-muted-foreground">
                Valeur estimée (€)
              </label>
              <div className="flex gap-1">
                <Input
                  type="number"
                  value={estimatedValue}
                  onChange={(e) => setEstimatedValue(e.target.value)}
                  placeholder="0"
                />
                <Button
                  variant="outline"
                  size="icon"
                  onClick={() =>
                    updateField({
                      estimatedValue:
                        estimatedValue === "" ? null : Number(estimatedValue),
                    })
                  }
                >
                  <Edit3 className="h-3.5 w-3.5" />
                </Button>
              </div>
              <p className="mt-1 text-[10px] text-muted-foreground">
                Actuel : {formatCurrency(prospect.estimated_value)}
              </p>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-sm">Syndic / contact</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2">
            {prospect.syndic ? (
              <div className="rounded-lg border border-border bg-secondary/30 p-2.5">
                <p className="text-xs font-semibold">{prospect.syndic}</p>
                <p className="text-[10px] text-muted-foreground">Syndic référencé</p>
              </div>
            ) : null}
            {contacts.length === 0 ? (
              <p className="text-xs text-muted-foreground">
                Aucun contact direct. (Ajout multi-contact bientôt disponible)
              </p>
            ) : (
              <ul className="space-y-1.5">
                {contacts.map((c) => (
                  <li key={c.id} className="flex items-center gap-2 text-xs">
                    <User className="h-3 w-3 text-muted-foreground" />
                    <span className="font-medium">{c.full_name ?? "—"}</span>
                    {c.email ? (
                      <a
                        href={`mailto:${c.email}`}
                        className="text-muted-foreground hover:text-primary"
                      >
                        <Mail className="h-3 w-3" />
                      </a>
                    ) : null}
                    {c.phone ? (
                      <a
                        href={`tel:${c.phone}`}
                        className="text-muted-foreground hover:text-primary"
                      >
                        <Phone className="h-3 w-3" />
                      </a>
                    ) : null}
                  </li>
                ))}
              </ul>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-sm">DPE & caractéristiques</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2 text-xs">
            <Row label="Classe finale" value={<DpeBadge classe={prospect.classe_finale} />} />
            <Row label="Classe réelle" value={prospect.classe_reelle ?? "—"} />
            <Row label="Classe simulée" value={prospect.classe_simulee ?? "—"} />
            <Row label="Conso (kWh/m²/an)" value={prospect.conso_moyenne ?? "—"} />
            <Row label="DPE individuels" value={prospect.nb_dpe_individuels ?? "—"} />
            <Row label="Lots habitation" value={prospect.nb_lots_habitation ?? "—"} />
            <Row label="Construction" value={(prospect.periode_construction ?? "—").replace(/_/g, " ")} />
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

function Row({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex items-center justify-between gap-2">
      <span className="text-muted-foreground">{label}</span>
      <span className="font-medium">{value}</span>
    </div>
  );
}

function labelForActivity(type: string): string {
  switch (type) {
    case "created":
      return "Prospect créé";
    case "stage_change":
      return "Changement d'étape";
    case "note_added":
      return "Note ajoutée";
    case "task_done":
      return "Tâche complétée";
    case "contact_added":
      return "Contact ajouté";
    default:
      return type;
  }
}

void MessageSquare;
