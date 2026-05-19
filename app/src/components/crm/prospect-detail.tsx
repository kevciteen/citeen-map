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
import { CommentsThread } from "@/components/collab/comments-thread";
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

        {/* Commentaires + @mentions équipe */}
        <CommentsThread entityType="prospect" entityId={String(id)} />
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

            {/* Assignation à un user (Phase C) */}
            <AssignmentPicker
              currentUserId={prospect.assigned_user_id ?? null}
              currentName={prospect.assigned_user_name ?? prospect.assigned_user_email ?? null}
              onChange={(uid) => updateField({ assignedUserId: uid })}
            />

            {/* Raison gain/perte selon le stage */}
            {prospect.stage === "won" || prospect.stage === "lost" ? (
              <WinLossReason
                stage={prospect.stage}
                value={
                  prospect.stage === "won"
                    ? (prospect.won_reason ?? "")
                    : (prospect.lost_reason ?? "")
                }
                onChange={(v) =>
                  updateField(
                    prospect.stage === "won"
                      ? { wonReason: v }
                      : { lostReason: v },
                  )
                }
              />
            ) : null}

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

        {prospect.copro_id ? (
          <>
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
                    Aucun contact direct.
                  </p>
                ) : (
                  <ul className="space-y-1.5">
                    {contacts.map((c) => (
                      <li key={c.id} className="flex items-center gap-2 text-xs">
                        <User className="h-3 w-3 text-muted-foreground" />
                        <span className="font-medium">{c.full_name ?? "—"}</span>
                        {c.email ? (
                          <a href={`mailto:${c.email}`} className="text-muted-foreground hover:text-primary">
                            <Mail className="h-3 w-3" />
                          </a>
                        ) : null}
                        {c.phone ? (
                          <a href={`tel:${c.phone}`} className="text-muted-foreground hover:text-primary">
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
                <CardTitle className="text-sm">DPE & caractéristiques copro</CardTitle>
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
          </>
        ) : (
          <MaisonProspectCard prospect={prospect} contacts={contacts} />
        )}
      </div>
    </div>
  );
}

function MaisonProspectCard({
  prospect,
  contacts,
}: {
  prospect: Record<string, unknown>;
  contacts: Array<{ id: number; full_name: string | null; email: string | null; phone: string | null; role?: string | null }>;
}) {
  // Détection du type de bien depuis les tags du prospect
  let kind: "maison" | "appartement" = "maison";
  try {
    const tags = JSON.parse(String(prospect.tags ?? "[]"));
    if (Array.isArray(tags) && tags.includes("appartement")) kind = "appartement";
  } catch {}
  const address = (prospect.custom_address ?? prospect.custom_label ?? "") as string;
  const numeroDpe = (prospect.numero_dpe ?? null) as string | null;
  const lat = (prospect.custom_lat ?? null) as number | null;
  const lon = (prospect.custom_lon ?? null) as number | null;
  const segment = kind === "appartement" ? "appartements" : "maisons";
  const router = useRouter();
  const [loading, setLoading] = useState(false);

  const openDetail = async () => {
    // Cas 1 : numero_dpe stocké → redirection directe
    if (numeroDpe) {
      router.push(`/${segment}/${encodeURIComponent(numeroDpe)}`);
      return;
    }
    // Cas 2 : lookup à la volée depuis lat/lon
    if (lat != null && lon != null) {
      setLoading(true);
      try {
        const r = await fetch(
          `/api/maisons/find-dpe?lat=${lat}&lon=${lon}&type=${kind}`,
        );
        if (r.ok) {
          const j = await r.json();
          if (j.numero_dpe) {
            router.push(`/${segment}/${encodeURIComponent(j.numero_dpe)}`);
            return;
          }
        }
        toast.error(
          "Pas de DPE trouvé à ces coordonnées. Ouverture de la recherche.",
        );
        router.push(`/${segment}?q=${encodeURIComponent(address)}`);
      } finally {
        setLoading(false);
      }
      return;
    }
    // Cas 3 : pas de coords → fallback recherche
    router.push(`/${segment}?q=${encodeURIComponent(address)}`);
  };

  return (
    <>
      <Card>
        <CardHeader>
          <CardTitle className="text-sm">Bien immobilier</CardTitle>
        </CardHeader>
        <CardContent className="space-y-2 text-xs">
          <Row
            label="Type"
            value={
              <Badge variant="secondary" className="capitalize">
                {kind === "appartement" ? "Appartement" : "Maison individuelle"}
              </Badge>
            }
          />
          {address ? <Row label="Adresse" value={<span className="text-right">{address}</span>} /> : null}
          {prospect.custom_lat != null && prospect.custom_lon != null ? (
            <Row
              label="Coordonnées"
              value={
                <span className="font-mono text-[10px]">
                  {Number(prospect.custom_lat).toFixed(5)}, {Number(prospect.custom_lon).toFixed(5)}
                </span>
              }
            />
          ) : null}
          {numeroDpe ? (
            <Row
              label="N° DPE"
              value={<span className="font-mono text-[10px]">{numeroDpe}</span>}
            />
          ) : null}
          <button
            type="button"
            onClick={openDetail}
            disabled={loading}
            className="mt-2 flex w-full items-center justify-center gap-1.5 rounded-md bg-primary px-3 py-1.5 text-center text-xs font-semibold text-primary-foreground hover:bg-primary/90 disabled:opacity-60"
          >
            {loading ? "Recherche du DPE…" : "Ouvrir la fiche détaillée"}
          </button>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-sm">Contacts ({contacts.length})</CardTitle>
        </CardHeader>
        <CardContent className="space-y-2">
          {contacts.length === 0 ? (
            <p className="text-[11px] italic text-muted-foreground">
              Pas encore de contact (propriétaire, occupant…). Édite depuis la fiche détaillée.
            </p>
          ) : (
            <ul className="space-y-1.5">
              {contacts.map((c) => (
                <li key={c.id} className="rounded-md border border-border bg-card p-2 text-xs">
                  <div className="flex items-center gap-2">
                    <User className="h-3 w-3 text-muted-foreground" />
                    <span className="font-medium">{c.full_name ?? "—"}</span>
                    {c.role ? (
                      <Badge variant="outline" className="text-[9px]">
                        {c.role}
                      </Badge>
                    ) : null}
                  </div>
                  <div className="mt-1 flex flex-wrap gap-2 text-[11px] text-muted-foreground">
                    {c.email ? (
                      <a href={`mailto:${c.email}`} className="flex items-center gap-1 hover:text-foreground">
                        <Mail className="h-3 w-3" /> {c.email}
                      </a>
                    ) : null}
                    {c.phone ? (
                      <a href={`tel:${c.phone}`} className="flex items-center gap-1 hover:text-foreground">
                        <Phone className="h-3 w-3" /> {c.phone}
                      </a>
                    ) : null}
                  </div>
                </li>
              ))}
            </ul>
          )}
        </CardContent>
      </Card>
    </>
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

/* ===== Phase C : assignation à un user + raison gain/perte ===== */

function AssignmentPicker({
  currentUserId,
  currentName,
  onChange,
}: {
  currentUserId: number | null;
  currentName: string | null;
  onChange: (userId: number | null) => void;
}) {
  const [users, setUsers] = useState<{ id: number; email: string; name: string | null }[]>([]);
  useEffect(() => {
    fetch("/api/users/mention-search?limit=50")
      .then((r) => r.json())
      .then((j) => setUsers(j.items ?? []))
      .catch(() => setUsers([]));
  }, []);
  return (
    <div>
      <label className="text-[10px] uppercase tracking-wider text-muted-foreground">
        Assigné à
      </label>
      <Select
        value={currentUserId ? String(currentUserId) : "_unassigned"}
        onValueChange={(v) => onChange(v === "_unassigned" ? null : Number(v))}
      >
        <SelectTrigger>
          <SelectValue>
            {currentName ?? "Non assigné"}
          </SelectValue>
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="_unassigned">Non assigné</SelectItem>
          {users.map((u) => (
            <SelectItem key={u.id} value={String(u.id)}>
              {u.name ?? u.email}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  );
}

const LOST_REASONS = [
  "Trop cher",
  "Déjà engagé avec concurrent",
  "Pas le bon moment / report AG",
  "Pas de quorum / refus AG",
  "Pas de budget",
  "Autre",
];
const WON_REASONS = [
  "MaPrimeRénov' Copro déclencheur",
  "Audit préalable convaincant",
  "Recommandation tiers",
  "Levée passoire DPE F/G",
  "Autre",
];

function WinLossReason({
  stage,
  value,
  onChange,
}: {
  stage: "won" | "lost";
  value: string;
  onChange: (v: string) => void;
}) {
  const options = stage === "won" ? WON_REASONS : LOST_REASONS;
  const label = stage === "won" ? "Raison du gain" : "Raison de la perte";
  return (
    <div>
      <label className="text-[10px] uppercase tracking-wider text-muted-foreground">
        {label}
      </label>
      <Select
        value={options.includes(value) ? value : "_custom"}
        onValueChange={(v) => {
          if (v === "_custom") return;
          onChange(v);
        }}
      >
        <SelectTrigger>
          <SelectValue placeholder="Sélectionner…" />
        </SelectTrigger>
        <SelectContent>
          {options.map((o) => (
            <SelectItem key={o} value={o}>
              {o}
            </SelectItem>
          ))}
          <SelectItem value="_custom">Autre (saisie libre ↓)</SelectItem>
        </SelectContent>
      </Select>
      <Input
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={
          stage === "won"
            ? "Détaille la raison du gain pour ton équipe"
            : "Détaille la raison de la perte"
        }
        className="mt-1"
      />
    </div>
  );
}
