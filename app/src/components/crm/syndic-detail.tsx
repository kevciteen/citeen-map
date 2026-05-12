"use client";
import { useState } from "react";
import Link from "next/link";
import {
  Building2,
  Mail,
  MapPin,
  Phone,
  Save,
  ExternalLink,
  Globe,
  StickyNote,
  Users,
  Loader2,
  Copy,
  Search,
} from "lucide-react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { toast } from "sonner";

type SyndicAggregate = {
  nb_copros: number;
  lots_total: number;
  nb_communes: number;
  nb_departements: number;
  dpe_a: number;
  dpe_b: number;
  dpe_c: number;
  dpe_d: number;
  dpe_e: number;
  dpe_f: number;
  dpe_g: number;
  dpe_nc: number;
  in_pipeline: number;
  dept_list: string | null;
  commune_list: string | null;
};

type SyndicContact = {
  source: string;
  nomComplet: string;
  siren: string;
  adresse: string | null;
  codePostal: string | null;
  commune: string | null;
  departement: string | null;
  codeApe: string | null;
  libelleApe: string | null;
  dirigeant: string | null;
  trancheEffectif: string | null;
  matchScore: number;
};

type Editable = {
  email: string | null;
  phone: string | null;
  contact_person: string | null;
  website: string | null;
  address_override: string | null;
  notes: string | null;
};

type FullDetail = {
  slug: string;
  name: string;
  aggregate: SyndicAggregate | null;
  sirene: SyndicContact | null;
  editable: Editable;
};

const DPE_COLORS: Record<string, string> = {
  a: "#1f9d55",
  b: "#7cb342",
  c: "#cddc39",
  d: "#ffeb3b",
  e: "#ffb300",
  f: "#fb8c00",
  g: "#e53935",
};

export function SyndicDetail({ initial }: { initial: FullDetail }) {
  const { slug, name, aggregate, sirene } = initial;
  const [edit, setEdit] = useState<Editable>(initial.editable);
  const [saving, setSaving] = useState(false);
  const [dirty, setDirty] = useState(false);

  const setField = (k: keyof Editable, v: string) => {
    setEdit((e) => ({ ...e, [k]: v || null }));
    setDirty(true);
  };

  const save = async () => {
    setSaving(true);
    try {
      const r = await fetch(`/api/syndics/${slug}`, {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          name,
          email: edit.email ?? "",
          phone: edit.phone ?? "",
          contact_person: edit.contact_person ?? "",
          website: edit.website ?? "",
          address_override: edit.address_override ?? "",
          notes: edit.notes ?? "",
        }),
      });
      if (r.ok) {
        toast.success("Fiche syndic enregistrée");
        setDirty(false);
      } else toast.error("Erreur d'enregistrement");
    } finally {
      setSaving(false);
    }
  };

  const copy = (label: string, text: string) => {
    void navigator.clipboard.writeText(text);
    toast.success(`${label} copié`);
  };

  const fullSireneAddr = sirene
    ? [sirene.adresse, sirene.codePostal, sirene.commune].filter(Boolean).join(" ")
    : "";

  const totalDpe = aggregate
    ? aggregate.dpe_a + aggregate.dpe_b + aggregate.dpe_c + aggregate.dpe_d + aggregate.dpe_e + aggregate.dpe_f + aggregate.dpe_g
    : 0;

  return (
    <div className="space-y-5">
      {/* HEADER */}
      <div className="overflow-hidden rounded-2xl border border-border bg-gradient-to-br from-primary/10 via-card to-secondary/30 shadow-sm">
        <div className="flex items-start justify-between gap-4 p-6">
          <div className="flex items-start gap-4">
            <div className="flex h-14 w-14 shrink-0 items-center justify-center rounded-xl bg-primary text-primary-foreground shadow">
              <Users className="h-7 w-7" />
            </div>
            <div className="min-w-0">
              <h1 className="truncate text-2xl font-black tracking-tight">{name}</h1>
              <div className="mt-1 flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
                {sirene?.siren ? (
                  <span className="rounded-md bg-background px-2 py-0.5 font-mono">
                    SIREN {sirene.siren}
                  </span>
                ) : null}
                {sirene?.libelleApe ? (
                  <span className="rounded-md bg-background px-2 py-0.5">
                    {sirene.libelleApe}
                  </span>
                ) : null}
                {aggregate?.dept_list ? (
                  <span className="rounded-md bg-background px-2 py-0.5">
                    Dpt {aggregate.dept_list}
                  </span>
                ) : null}
              </div>
            </div>
          </div>
          <Link
            href={`/copros?syndic=${encodeURIComponent(name)}`}
            className="flex shrink-0 items-center gap-1.5 rounded-lg border border-border bg-background px-3 py-2 text-xs font-semibold hover:bg-secondary"
          >
            <Building2 className="h-3.5 w-3.5" />
            Voir {aggregate?.nb_copros ?? 0} copros
          </Link>
        </div>
      </div>

      {/* KPIs */}
      {aggregate ? (
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
          <Kpi
            label="Copros gérées"
            value={aggregate.nb_copros.toLocaleString("fr-FR")}
            hint={`${aggregate.in_pipeline} déjà en pipeline`}
          />
          <Kpi
            label="Lots habitation"
            value={aggregate.lots_total.toLocaleString("fr-FR")}
            hint={`~${Math.round(aggregate.lots_total / Math.max(aggregate.nb_copros, 1))} lots/copro`}
          />
          <Kpi
            label="Communes"
            value={aggregate.nb_communes.toString()}
            hint={`${aggregate.nb_departements} département(s)`}
          />
          <Kpi
            label="Passoires F+G"
            value={(aggregate.dpe_f + aggregate.dpe_g).toString()}
            hint={`sur ${totalDpe} DPE estimés`}
          />
        </div>
      ) : null}

      {/* RÉPARTITION DPE */}
      {aggregate && totalDpe > 0 ? (
        <div className="rounded-xl border border-border bg-card p-4">
          <p className="mb-2 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
            Répartition DPE des copros gérées
          </p>
          <div className="flex h-4 w-full overflow-hidden rounded-full">
            {(["a", "b", "c", "d", "e", "f", "g"] as const).map((cls) => {
              const v = (aggregate as unknown as Record<string, number>)[`dpe_${cls}`];
              if (!v) return null;
              const pct = (v / totalDpe) * 100;
              return (
                <span
                  key={cls}
                  title={`Classe ${cls.toUpperCase()} : ${v} copros (${pct.toFixed(0)}%)`}
                  style={{ width: `${pct}%`, background: DPE_COLORS[cls] }}
                />
              );
            })}
          </div>
          <div className="mt-2 flex flex-wrap gap-2 text-[10px]">
            {(["a", "b", "c", "d", "e", "f", "g"] as const).map((cls) => {
              const v = (aggregate as unknown as Record<string, number>)[`dpe_${cls}`];
              return (
                <span key={cls} className="flex items-center gap-1">
                  <span
                    className="inline-block h-2 w-2 rounded-sm"
                    style={{ background: DPE_COLORS[cls] }}
                  />
                  {cls.toUpperCase()} {v}
                </span>
              );
            })}
          </div>
        </div>
      ) : null}

      <div className="grid gap-5 lg:grid-cols-2">
        {/* INFOS SIRENE (LECTURE SEULE) */}
        <div className="rounded-xl border border-emerald-200 bg-emerald-50/30 p-4">
          <h2 className="mb-3 flex items-center gap-2 text-sm font-bold text-emerald-900">
            <Globe className="h-4 w-4" />
            Données entreprise (Sirene)
          </h2>
          {sirene ? (
            <div className="space-y-2 text-sm">
              <RowKV label="Raison sociale" value={sirene.nomComplet} />
              <RowKV label="SIREN" value={sirene.siren} mono />
              <RowKV label="Code APE" value={sirene.codeApe ?? "—"} mono />
              <RowKV label="Libellé APE" value={sirene.libelleApe ?? "—"} />
              <RowKV
                label="Adresse siège"
                value={fullSireneAddr || "—"}
                actions={
                  fullSireneAddr ? (
                    <button
                      onClick={() => copy("Adresse", fullSireneAddr)}
                      className="rounded p-1 text-emerald-700 hover:bg-emerald-100"
                    >
                      <Copy className="h-3 w-3" />
                    </button>
                  ) : null
                }
              />
              <RowKV label="Dirigeant" value={sirene.dirigeant ?? "—"} />
              <RowKV
                label="Effectif"
                value={sirene.trancheEffectif ?? "—"}
              />
              <div className="pt-2">
                <a
                  href={`https://annuaire-entreprises.data.gouv.fr/entreprise/${sirene.siren}`}
                  target="_blank"
                  rel="noreferrer"
                  className="inline-flex items-center gap-1 text-xs font-medium text-emerald-700 hover:text-emerald-900"
                >
                  <ExternalLink className="h-3 w-3" />
                  Annuaire Entreprises officiel
                </a>
              </div>
              <p className="text-[10px] italic text-emerald-700/80">
                Score de match : {Math.round(sirene.matchScore * 100)}% · données
                publiques data.gouv.fr (cache 24h)
              </p>
            </div>
          ) : (
            <div className="text-sm text-amber-900">
              <p className="font-semibold">Pas de fiche entreprise trouvée</p>
              <p className="mt-1 text-xs">
                Le nom du syndic ne correspond à aucune entreprise active dans Sirene.
                Tu peux saisir manuellement les coordonnées ci-contre.
              </p>
              <a
                href={`https://www.google.com/search?q=${encodeURIComponent(name + " syndic")}`}
                target="_blank"
                rel="noreferrer"
                className="mt-2 inline-flex items-center gap-1 text-xs font-medium underline"
              >
                <Search className="h-3 w-3" /> Chercher sur Google
              </a>
            </div>
          )}
        </div>

        {/* COORDONNÉES ÉDITABLES */}
        <div className="rounded-xl border border-border bg-card p-4">
          <h2 className="mb-3 flex items-center gap-2 text-sm font-bold">
            <Mail className="h-4 w-4 text-primary" />
            Coordonnées prospection (édition manuelle)
          </h2>
          <div className="space-y-3">
            <Field label="Email" icon={<Mail className="h-3.5 w-3.5" />}>
              <Input
                type="email"
                value={edit.email ?? ""}
                onChange={(e) => setField("email", e.target.value)}
                placeholder="contact@syndic.fr"
              />
            </Field>
            <Field label="Téléphone" icon={<Phone className="h-3.5 w-3.5" />}>
              <Input
                type="tel"
                value={edit.phone ?? ""}
                onChange={(e) => setField("phone", e.target.value)}
                placeholder="01 23 45 67 89"
              />
            </Field>
            <Field label="Personne à contacter" icon={<Users className="h-3.5 w-3.5" />}>
              <Input
                value={edit.contact_person ?? ""}
                onChange={(e) => setField("contact_person", e.target.value)}
                placeholder="Mme Dupont, gestionnaire copro"
              />
            </Field>
            <Field label="Site web" icon={<Globe className="h-3.5 w-3.5" />}>
              <Input
                type="url"
                value={edit.website ?? ""}
                onChange={(e) => setField("website", e.target.value)}
                placeholder="https://syndic.fr"
              />
            </Field>
            <Field
              label="Adresse override (si Sirene faux)"
              icon={<MapPin className="h-3.5 w-3.5" />}
            >
              <Input
                value={edit.address_override ?? ""}
                onChange={(e) => setField("address_override", e.target.value)}
                placeholder="123 rue de la Paix 75002 Paris"
              />
            </Field>
            <Field
              label="Notes internes"
              icon={<StickyNote className="h-3.5 w-3.5" />}
            >
              <textarea
                value={edit.notes ?? ""}
                onChange={(e) => setField("notes", e.target.value)}
                placeholder="Historique d'échanges, points clés…"
                rows={4}
                className="w-full rounded-lg border border-input bg-background px-3 py-2 text-sm"
              />
            </Field>
            <div className="flex items-center justify-between gap-2 pt-2">
              <p className="text-[10px] text-muted-foreground">
                {dirty ? "Modifications non enregistrées" : "Synchronisé"}
              </p>
              <Button onClick={save} disabled={saving || !dirty}>
                {saving ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <Save className="h-4 w-4" />
                )}
                Enregistrer
              </Button>
            </div>
          </div>
        </div>
      </div>

      {/* COMMUNES */}
      {aggregate?.commune_list ? (
        <div className="rounded-xl border border-border bg-card p-4">
          <p className="mb-2 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
            Communes d&apos;intervention ({aggregate.nb_communes})
          </p>
          <div className="flex flex-wrap gap-1.5">
            {aggregate.commune_list
              .split(",")
              .filter(Boolean)
              .slice(0, 60)
              .map((c) => (
                <span
                  key={c}
                  className="rounded-full bg-secondary px-2 py-0.5 text-[11px] font-medium"
                >
                  {c.trim()}
                </span>
              ))}
          </div>
        </div>
      ) : null}
    </div>
  );
}

function Kpi({ label, value, hint }: { label: string; value: string; hint?: string }) {
  return (
    <div className="rounded-xl border border-border bg-card p-4">
      <p className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</p>
      <p className="mt-0.5 text-2xl font-black text-foreground">{value}</p>
      {hint ? <p className="mt-0.5 text-[10px] text-muted-foreground">{hint}</p> : null}
    </div>
  );
}

function Field({
  label,
  icon,
  children,
}: {
  label: string;
  icon: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <div>
      <label className="mb-1 flex items-center gap-1 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
        {icon}
        {label}
      </label>
      {children}
    </div>
  );
}

function RowKV({
  label,
  value,
  mono,
  actions,
}: {
  label: string;
  value: string;
  mono?: boolean;
  actions?: React.ReactNode;
}) {
  return (
    <div className="flex items-start justify-between gap-3 border-b border-emerald-200/50 py-1.5 last:border-0">
      <span className="shrink-0 text-xs text-emerald-700">{label}</span>
      <span className={`text-right text-xs font-medium text-emerald-950 ${mono ? "font-mono" : ""}`}>
        {value}
      </span>
      {actions}
    </div>
  );
}
