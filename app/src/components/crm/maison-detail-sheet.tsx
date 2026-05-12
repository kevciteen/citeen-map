"use client";
import { useState } from "react";
import * as Dialog from "@radix-ui/react-dialog";
import {
  Home, ExternalLink, Plus, X, Calendar, Ruler, Zap, Flame,
  Snowflake, Wind, Wallet, Building, Shield, FileText, Loader2,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { DpeBadge, DpeScaleBar } from "@/components/ui/dpe-badge";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { toast } from "sonner";

type MaisonDpe = {
  numero_dpe: string;
  classe: string;
  ges: string;
  conso: number | null;
  emission_ges: number | null;
  surface: number | null;
  date: string | null;
  date_visite: string | null;
  date_fin_validite: string | null;
  annee_construction: number | null;
  nb_niveaux: number | null;
  nb_pieces: number | null;
  hauteur_sous_plafond: number | null;
  type_batiment: string | null;
  methode_dpe: string | null;
  energie_principale_chauffage: string | null;
  energie_n2: string | null;
  energie_n3: string | null;
  installation_chauffage: string | null;
  type_ventilation: string | null;
  cout_total: number | null;
  cout_chauffage: number | null;
  cout_ecs: number | null;
  cout_eclairage: number | null;
  cout_refroidissement: number | null;
  isolation_enveloppe: string | null;
  isolation_murs: string | null;
  isolation_toiture: string | null;
  isolation_plancher_bas: string | null;
  isolation_menuiseries: string | null;
  address: {
    housenumber: string | null;
    street: string | null;
    postcode: string | null;
    city: string | null;
    label: string;
  };
  lat: number | null;
  lon: number | null;
  ademe_url: string;
};

const DPE_GRADIENT: Record<string, string> = {
  A: "linear-gradient(135deg, #1f9d55 0%, #15803d 100%)",
  B: "linear-gradient(135deg, #7cb342 0%, #4d7c0f 100%)",
  C: "linear-gradient(135deg, #cddc39 0%, #84cc16 100%)",
  D: "linear-gradient(135deg, #ffeb3b 0%, #eab308 100%)",
  E: "linear-gradient(135deg, #ffb300 0%, #d97706 100%)",
  F: "linear-gradient(135deg, #fb8c00 0%, #ea580c 100%)",
  G: "linear-gradient(135deg, #e53935 0%, #b91c1c 100%)",
  NC: "linear-gradient(135deg, #94a3b8 0%, #475569 100%)",
};

const ISO_COLOR: Record<string, string> = {
  insuffisante: "#e53935",
  "tres mauvaise": "#e53935",
  mauvaise: "#fb8c00",
  moyenne: "#ffeb3b",
  bonne: "#7cb342",
  "tres bonne": "#1f9d55",
  excellent: "#1f9d55",
};

function isoBadge(q: string | null): { bg: string; label: string } | null {
  if (!q) return null;
  const norm = q
    .toLowerCase()
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "");
  for (const k of Object.keys(ISO_COLOR)) {
    if (norm.includes(k)) return { bg: ISO_COLOR[k], label: q };
  }
  return { bg: "#94a3b8", label: q };
}

function isExpired(dateFinValidite: string | null): boolean {
  if (!dateFinValidite) return false;
  const t = Date.parse(dateFinValidite);
  return Number.isFinite(t) && t < Date.now();
}

export function MaisonDetailSheet({
  maison,
  trigger,
  onAdded,
}: {
  maison: MaisonDpe;
  trigger: React.ReactNode;
  onAdded?: () => void;
}) {
  const [open, setOpen] = useState(false);
  const [adding, setAdding] = useState(false);
  const expired = isExpired(maison.date_fin_validite);

  const addToPipeline = async () => {
    setAdding(true);
    try {
      const r = await fetch("/api/prospects", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          customLabel: `Maison · ${maison.address.label}`,
          customAddress: maison.address.label,
          customLat: maison.lat,
          customLon: maison.lon,
          stage: "to_contact",
          priority: 2,
          tags: ["maison", `dpe-${maison.classe}`, `ges-${maison.ges}`],
        }),
      });
      if (r.ok || r.status === 409) {
        toast.success("Ajouté au pipeline");
        onAdded?.();
      } else toast.error("Erreur");
    } finally {
      setAdding(false);
    }
  };

  return (
    <Dialog.Root open={open} onOpenChange={setOpen}>
      <Dialog.Trigger asChild>{trigger}</Dialog.Trigger>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-50 bg-black/60 backdrop-blur-sm" />
        <Dialog.Content className="fixed left-1/2 top-1/2 z-50 max-h-[92vh] w-[820px] max-w-[calc(100vw-2rem)] -translate-x-1/2 -translate-y-1/2 overflow-hidden rounded-xl bg-card shadow-2xl">
          <Dialog.Title className="sr-only">Fiche maison détaillée</Dialog.Title>
          <Dialog.Description className="sr-only">
            Détails du DPE de la maison à l'adresse {maison.address.label}
          </Dialog.Description>

          {/* HEADER coloré DPE */}
          <div
            className="relative p-5 text-white"
            style={{ background: DPE_GRADIENT[maison.classe] ?? DPE_GRADIENT.NC }}
          >
            <Dialog.Close asChild>
              <button className="absolute right-3 top-3 flex h-8 w-8 items-center justify-center rounded-full bg-white/20 text-white hover:bg-white/30">
                <X className="h-4 w-4" />
              </button>
            </Dialog.Close>
            <div className="flex items-start gap-4">
              <div className="flex h-12 w-12 shrink-0 items-center justify-center rounded-xl bg-white/20 backdrop-blur">
                <Home className="h-6 w-6" />
              </div>
              <div className="flex-1">
                <h2 className="text-xl font-black tracking-tight">
                  {maison.address.housenumber} {maison.address.street}
                </h2>
                <p className="text-sm opacity-90">
                  {maison.address.postcode} {maison.address.city}
                </p>
                <div className="mt-1.5 flex flex-wrap items-center gap-1.5">
                  <span className="rounded-full bg-white/20 px-2 py-0.5 text-[10px] font-semibold backdrop-blur">
                    {maison.type_batiment ?? "maison"}
                  </span>
                  {maison.annee_construction ? (
                    <span className="rounded-full bg-white/20 px-2 py-0.5 text-[10px] font-semibold backdrop-blur">
                      Construit en {maison.annee_construction}
                    </span>
                  ) : null}
                  {expired ? (
                    <span className="rounded-full bg-red-700/80 px-2 py-0.5 text-[10px] font-bold backdrop-blur">
                      ⚠ DPE expiré
                    </span>
                  ) : null}
                </div>
              </div>
              <div
                className="flex h-20 w-20 shrink-0 items-center justify-center rounded-2xl bg-white text-3xl font-black shadow"
                style={{ color: DPE_GRADIENT[maison.classe]?.match(/#[0-9a-f]+/i)?.[0] }}
              >
                {maison.classe || "—"}
              </div>
            </div>
          </div>

          {/* SCROLLABLE CONTENT */}
          <div className="max-h-[calc(92vh-180px)] overflow-y-auto p-5">
            <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
              <Kpi icon={<Zap className="h-3.5 w-3.5" />} label="Conso" value={maison.conso ? `${maison.conso}` : "—"} unit="kWhep/m²/an" />
              <Kpi icon={<DpeBadge classe={maison.ges} size="sm" />} label="GES" value={maison.ges} unit={maison.emission_ges ? `${maison.emission_ges} kgCO₂/m²` : ""} />
              <Kpi icon={<Ruler className="h-3.5 w-3.5" />} label="Surface" value={maison.surface ? `${maison.surface}` : "—"} unit="m²" />
              <Kpi icon={<Building className="h-3.5 w-3.5" />} label="Niveaux" value={maison.nb_niveaux ? `${maison.nb_niveaux}` : "—"} unit={maison.nb_pieces ? `${maison.nb_pieces} pièces` : ""} />
            </div>

            <div className="mt-3">
              <DpeScaleBar active={maison.classe} />
            </div>

            {/* CHAUFFAGE */}
            <Section icon={<Flame className="h-4 w-4 text-orange-500" />} title="Chauffage & énergie">
              <Row label="Énergie principale" value={maison.energie_principale_chauffage} strong />
              {maison.energie_n2 ? <Row label="Énergie secondaire" value={maison.energie_n2} /> : null}
              {maison.energie_n3 ? <Row label="Énergie tertiaire" value={maison.energie_n3} /> : null}
              <Row label="Installation chauffage" value={maison.installation_chauffage} />
              <Row label="Ventilation" value={maison.type_ventilation} />
            </Section>

            {/* COÛTS */}
            <Section icon={<Wallet className="h-4 w-4 text-emerald-600" />} title="Coûts annuels estimés (€)">
              <Row label="Total 5 usages" value={fmtEuro(maison.cout_total)} strong />
              <Row label="Chauffage" value={fmtEuro(maison.cout_chauffage)} />
              <Row label="Eau chaude sanitaire" value={fmtEuro(maison.cout_ecs)} />
              <Row label="Éclairage" value={fmtEuro(maison.cout_eclairage)} />
              <Row label="Refroidissement" value={fmtEuro(maison.cout_refroidissement)} />
              {maison.cout_chauffage ? (
                <p className="mt-2 rounded-lg bg-emerald-50 p-2 text-[11px] text-emerald-900">
                  💡 <strong>Argumentaire commercial</strong> : ce ménage paye
                  <strong> {fmtEuro(maison.cout_chauffage)}</strong> de chauffage
                  par an. Une rénovation passant en classe C permettrait des
                  économies significatives (estimation 50-70%).
                </p>
              ) : null}
            </Section>

            {/* ISOLATION */}
            <Section icon={<Shield className="h-4 w-4 text-blue-600" />} title="Qualité d'isolation">
              <IsolationRow label="Enveloppe globale" value={maison.isolation_enveloppe} />
              <IsolationRow label="Murs" value={maison.isolation_murs} />
              <IsolationRow label="Toiture" value={maison.isolation_toiture} />
              <IsolationRow label="Plancher bas" value={maison.isolation_plancher_bas} />
              <IsolationRow label="Menuiseries" value={maison.isolation_menuiseries} />
            </Section>

            {/* DPE */}
            <Section icon={<FileText className="h-4 w-4 text-primary" />} title="DPE">
              <Row label="N° DPE" value={maison.numero_dpe} mono />
              <Row label="Méthode" value={maison.methode_dpe} />
              <Row label="Date d'établissement" value={fmtDate(maison.date)} />
              <Row label="Date visite diagnostiqueur" value={fmtDate(maison.date_visite)} />
              <Row
                label="Fin de validité"
                value={fmtDate(maison.date_fin_validite)}
                strong={expired}
                danger={expired}
              />
            </Section>

            {/* CARACTÉRISTIQUES */}
            <Section icon={<Building className="h-4 w-4 text-slate-600" />} title="Caractéristiques">
              <Row label="Année construction" value={maison.annee_construction?.toString() ?? null} />
              <Row label="Nombre de pièces" value={maison.nb_pieces?.toString() ?? null} />
              <Row label="Nombre de niveaux" value={maison.nb_niveaux?.toString() ?? null} />
              <Row label="Hauteur sous plafond" value={maison.hauteur_sous_plafond ? `${maison.hauteur_sous_plafond} m` : null} />
              {maison.lat && maison.lon ? (
                <Row
                  label="Coordonnées GPS"
                  value={`${maison.lat.toFixed(6)}, ${maison.lon.toFixed(6)}`}
                  mono
                />
              ) : null}
            </Section>
          </div>

          {/* FOOTER ACTIONS */}
          <div className="flex flex-wrap items-center gap-2 border-t border-border bg-secondary/40 p-3">
            <Button onClick={addToPipeline} disabled={adding}>
              {adding ? <Loader2 className="h-4 w-4 animate-spin" /> : <Plus className="h-4 w-4" />}
              Ajouter au pipeline
            </Button>
            <a
              href={maison.ademe_url}
              target="_blank"
              rel="noreferrer"
              className="flex items-center gap-1 rounded-md border border-border bg-background px-3 py-1.5 text-xs font-medium hover:bg-secondary"
            >
              <ExternalLink className="h-3 w-3" /> Fiche officielle ADEME
            </a>
            {maison.lat && maison.lon ? (
              <>
                <a
                  href={`https://www.google.com/maps/search/?api=1&query=${maison.lat},${maison.lon}`}
                  target="_blank"
                  rel="noreferrer"
                  className="flex items-center gap-1 rounded-md border border-border bg-background px-3 py-1.5 text-xs font-medium hover:bg-secondary"
                >
                  <ExternalLink className="h-3 w-3" /> Google Maps
                </a>
                <a
                  href={`https://www.google.com/maps/@?api=1&map_action=pano&viewpoint=${maison.lat},${maison.lon}`}
                  target="_blank"
                  rel="noreferrer"
                  className="flex items-center gap-1 rounded-md border border-border bg-background px-3 py-1.5 text-xs font-medium hover:bg-secondary"
                >
                  <ExternalLink className="h-3 w-3" /> Street View
                </a>
              </>
            ) : null}
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

function Section({
  icon,
  title,
  children,
}: {
  icon: React.ReactNode;
  title: string;
  children: React.ReactNode;
}) {
  return (
    <section className="mt-4 rounded-xl border border-border bg-secondary/20 p-3">
      <h3 className="mb-2 flex items-center gap-2 text-xs font-bold uppercase tracking-wider">
        {icon}
        {title}
      </h3>
      <div className="space-y-1">{children}</div>
    </section>
  );
}

function Row({
  label,
  value,
  mono,
  strong,
  danger,
}: {
  label: string;
  value: string | null;
  mono?: boolean;
  strong?: boolean;
  danger?: boolean;
}) {
  return (
    <div className="flex items-center justify-between gap-3 border-b border-border/30 py-1 text-xs last:border-0">
      <span className="shrink-0 text-muted-foreground">{label}</span>
      <span
        className={cn(
          mono ? "font-mono " : "",
          strong ? "font-bold " : "",
          danger ? "text-destructive" : "",
          "text-right",
        )}
      >
        {value ?? "—"}
      </span>
    </div>
  );
}

function IsolationRow({ label, value }: { label: string; value: string | null }) {
  const badge = isoBadge(value);
  return (
    <div className="flex items-center justify-between gap-3 border-b border-border/30 py-1 text-xs last:border-0">
      <span className="shrink-0 text-muted-foreground">{label}</span>
      {badge ? (
        <span
          className="inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-bold text-white"
          style={{ background: badge.bg }}
        >
          {badge.label}
        </span>
      ) : (
        <span className="text-muted-foreground">—</span>
      )}
    </div>
  );
}

function Kpi({
  icon, label, value, unit,
}: {
  icon: React.ReactNode;
  label: string;
  value: string;
  unit: string;
}) {
  return (
    <div className="rounded-lg border border-border bg-card p-2.5">
      <div className="flex items-center gap-1 text-[10px] uppercase tracking-wider text-muted-foreground">
        {icon}
        {label}
      </div>
      <div className="mt-0.5 text-base font-black">{value}</div>
      {unit ? <div className="text-[10px] text-muted-foreground">{unit}</div> : null}
    </div>
  );
}

function fmtEuro(n: number | null): string | null {
  if (n == null) return null;
  return new Intl.NumberFormat("fr-FR", { style: "currency", currency: "EUR", maximumFractionDigits: 0 }).format(n);
}
function fmtDate(s: string | null): string | null {
  if (!s) return null;
  const d = new Date(s);
  return Number.isNaN(d.getTime()) ? s : d.toLocaleDateString("fr-FR");
}

void Snowflake;
void Wind;
void Calendar;
void Badge;
