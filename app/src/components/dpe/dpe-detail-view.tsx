"use client";
/**
 * Fiche DPE premium style ADEME — vue détaillée d'un diagnostic.
 *
 * Reprend les sections de l'observatoire ADEME mais avec un design plus
 * moderne (cards arrondies, KPI premium, scale bars colorées).
 */
import {
  Flame, Building2, Home, Zap, Wind, Droplet, Lightbulb, Sun,
  Thermometer, MapPin, Calendar, FileText, ExternalLink,
  TrendingDown, TrendingUp, ChevronRight,
} from "lucide-react";
import type { AdemeRecord } from "@/lib/services/ademe";
import { DpeBadge } from "@/components/ui/dpe-badge";
import { DPE_COLORS } from "@/components/annuaire/annuaire-map";

function num(v: unknown): number | null {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function dateLabel(v: unknown): string {
  if (!v) return "—";
  const d = new Date(String(v));
  if (Number.isNaN(d.getTime())) return String(v);
  return d.toLocaleDateString("fr-FR", { day: "numeric", month: "long", year: "numeric" });
}

function getClasse(record: AdemeRecord): string {
  return String(record.etiquette_dpe ?? "NC").toUpperCase();
}

function getGes(record: AdemeRecord): string {
  return String(record.etiquette_ges ?? "NC").toUpperCase();
}

// Ordre + seuils ADEME (kWhEP/m²/an pour DPE 2021)
const DPE_SCALE = [
  { letter: "A", upper: 70 },
  { letter: "B", upper: 110 },
  { letter: "C", upper: 180 },
  { letter: "D", upper: 250 },
  { letter: "E", upper: 330 },
  { letter: "F", upper: 420 },
  { letter: "G", upper: 999 },
] as const;

const GES_SCALE = [
  { letter: "A", upper: 6 },
  { letter: "B", upper: 11 },
  { letter: "C", upper: 30 },
  { letter: "D", upper: 50 },
  { letter: "E", upper: 70 },
  { letter: "F", upper: 100 },
  { letter: "G", upper: 999 },
] as const;

export function DpeDetailView({ record }: { record: AdemeRecord }) {
  const dpe = getClasse(record);
  const ges = getGes(record);
  const consoEP = num(record.conso_5_usages_par_m2_ep);
  const consoEF = num(record["conso_5_usages_par_m2_ef"]);
  const emissionsGes = num(record["emission_ges_5_usages_par_m2"]);
  const surface = num(record.surface_habitable_logement) ?? num(record.surface_habitable_immeuble);
  const typeBat = String(record.type_batiment ?? "").toUpperCase();
  const methode = String(record.methode_application_dpe ?? "");
  const adresse =
    record["adresse_complete_brut"] ??
    record.adresse_ban ??
    `${record.numero_voie_ban ?? ""} ${record.nom_rue_ban ?? ""}`.trim();
  const cp = record.code_postal_ban ?? record.code_postal_brut ?? "";
  const commune = record.nom_commune_ban ?? record.nom_commune_brut ?? "";

  const isCollectif = methode.toLowerCase().includes("immeuble collectif");

  return (
    <div className="space-y-4">
      {/* ============= HERO ============= */}
      <section className="overflow-hidden rounded-2xl border border-border bg-gradient-to-br from-primary/5 via-card to-secondary/30 shadow-sm">
        <div className="p-6">
          <div className="mb-4 flex flex-wrap items-center gap-2 text-[10px] uppercase tracking-wider text-muted-foreground">
            <span className="rounded bg-secondary/60 px-2 py-0.5">
              DPE {String(record.modele_dpe ?? "—")}
            </span>
            <span className="rounded bg-secondary/60 px-2 py-0.5">
              {typeBat || "—"}
            </span>
            {isCollectif ? (
              <span className="rounded bg-emerald-100 px-2 py-0.5 font-bold text-emerald-900">
                ✅ DPE collectif d&apos;immeuble (RÉEL ADEME)
              </span>
            ) : null}
            <a
              href={`https://observatoire-dpe-audit.ademe.fr/afficher-dpe/${record.numero_dpe}`}
              target="_blank"
              rel="noreferrer"
              className="ml-auto inline-flex items-center gap-1 text-primary hover:underline"
            >
              Voir sur ADEME <ExternalLink className="h-2.5 w-2.5" />
            </a>
          </div>

          <h1 className="mb-1 flex items-center gap-2 text-2xl font-black tracking-tight">
            <MapPin className="h-5 w-5 text-primary" />
            {adresse}
          </h1>
          <p className="text-sm text-muted-foreground">
            {cp} {commune}
            {surface ? ` · ${surface.toFixed(0)} m²` : ""}
            {record.annee_construction ? ` · construit en ${record.annee_construction}` : ""}
            {record.periode_construction ? ` · ${record.periode_construction}` : ""}
          </p>

          {/* Big labels */}
          <div className="mt-5 grid gap-4 sm:grid-cols-2">
            <ScaleCard
              label="Performance énergétique"
              unit="kWhEP/m²/an"
              value={consoEP}
              activeClass={dpe}
              scale={DPE_SCALE}
            />
            <ScaleCard
              label="Émissions de gaz à effet de serre"
              unit="kg CO₂/m²/an"
              value={emissionsGes}
              activeClass={ges}
              scale={GES_SCALE}
            />
          </div>
        </div>
      </section>

      {/* ============= KPIs CONSOMMATIONS PAR POSTE ============= */}
      <section className="rounded-xl border border-border bg-card p-5 shadow-sm">
        <h2 className="mb-3 flex items-center gap-2 text-sm font-bold">
          <Flame className="h-4 w-4 text-primary" />
          Consommations annuelles par usage
        </h2>
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-5">
          <UsageKpi
            label="Chauffage"
            icon={Thermometer}
            valueEP={num(record["conso_chauffage_ep"])}
            valueEF={num(record["conso_chauffage_ef"])}
            cost={num(record["cout_chauffage"])}
          />
          <UsageKpi
            label="ECS"
            icon={Droplet}
            valueEP={num(record["conso_ecs_ep"])}
            valueEF={num(record["conso_ecs_ef"])}
            cost={num(record["cout_ecs"])}
          />
          <UsageKpi
            label="Refroidissement"
            icon={Wind}
            valueEP={num(record["conso_refroidissement_ep"])}
            valueEF={num(record["conso_refroidissement_ef"])}
            cost={num(record["cout_refroidissement"])}
          />
          <UsageKpi
            label="Éclairage"
            icon={Lightbulb}
            valueEP={num(record["conso_eclairage_ep"])}
            valueEF={num(record["conso_eclairage_ef"])}
            cost={num(record["cout_eclairage"])}
          />
          <UsageKpi
            label="Auxiliaires"
            icon={Zap}
            valueEP={num(record["conso_auxiliaires_ep"])}
            valueEF={num(record["conso_auxiliaires_ef"])}
            cost={num(record["cout_auxiliaires"])}
          />
        </div>
        <div className="mt-3 flex flex-wrap gap-3 border-t border-border/60 pt-3 text-[11px]">
          <Stat
            label="Total conso 5 usages EF"
            value={
              num(record["conso_5_usages_ef"]) != null
                ? `${num(record["conso_5_usages_ef"])!.toFixed(0)} kWh/an`
                : "—"
            }
          />
          <Stat
            label="Total conso 5 usages EP"
            value={
              num(record["conso_5_usages_ep"]) != null
                ? `${num(record["conso_5_usages_ep"])!.toFixed(0)} kWh/an`
                : "—"
            }
          />
          <Stat
            label="Coût total 5 usages"
            value={
              num(record["cout_total_5_usages"]) != null
                ? `${num(record["cout_total_5_usages"])!.toFixed(0)} €/an`
                : "—"
            }
          />
          <Stat
            label="Émissions GES totales"
            value={
              num(record["emission_ges_5_usages"]) != null
                ? `${num(record["emission_ges_5_usages"])!.toFixed(0)} kg CO₂/an`
                : "—"
            }
          />
        </div>
      </section>

      {/* ============= ENVELOPPE ============= */}
      <section className="rounded-xl border border-border bg-card p-5 shadow-sm">
        <h2 className="mb-3 flex items-center gap-2 text-sm font-bold">
          <Home className="h-4 w-4 text-primary" />
          Performance de l&apos;enveloppe
        </h2>
        <div className="grid gap-2 sm:grid-cols-2">
          <QualityRow label="Isolation murs" value={record["qualite_isolation_murs"]} />
          <QualityRow label="Isolation plancher bas" value={record["qualite_isolation_plancher_bas"]} />
          <QualityRow label="Isolation plancher haut / comble" value={record["qualite_isolation_plancher_haut_comble_perdu"]} />
          <QualityRow label="Isolation menuiseries" value={record["qualite_isolation_menuiseries"]} />
          <QualityRow label="Qualité globale enveloppe" value={record["qualite_isolation_enveloppe"]} />
          <Stat
            label="Ubat (W/m²·K)"
            value={num(record["ubat_w_par_m2_k"]) != null ? num(record["ubat_w_par_m2_k"])!.toFixed(2) : "—"}
            inline
          />
        </div>
        <div className="mt-3 grid grid-cols-2 gap-2 border-t border-border/60 pt-3 sm:grid-cols-4">
          <DeperditionKpi label="Murs" value={num(record["deperditions_murs"])} />
          <DeperditionKpi label="Planchers bas" value={num(record["deperditions_planchers_bas"])} />
          <DeperditionKpi label="Planchers hauts" value={num(record["deperditions_planchers_hauts"])} />
          <DeperditionKpi label="Baies vitrées" value={num(record["deperditions_baies_vitrees"])} />
          <DeperditionKpi label="Portes" value={num(record["deperditions_portes"])} />
          <DeperditionKpi label="Ponts thermiques" value={num(record["deperditions_ponts_thermiques"])} />
          <DeperditionKpi label="Renouvellement air" value={num(record["deperditions_renouvellement_air"])} />
          <DeperditionKpi label="Enveloppe totale" value={num(record["deperditions_enveloppe"])} strong />
        </div>
      </section>

      {/* ============= INSTALLATIONS ============= */}
      <section className="rounded-xl border border-border bg-card p-5 shadow-sm">
        <h2 className="mb-3 flex items-center gap-2 text-sm font-bold">
          <Flame className="h-4 w-4 text-primary" />
          Installations énergétiques
        </h2>
        <div className="grid gap-3 lg:grid-cols-3">
          <InstallationCard
            title="Chauffage"
            icon={Thermometer}
            rows={[
              { label: "Type installation", v: record["type_installation_chauffage"] },
              { label: "Énergie principale", v: record["type_energie_principale_chauffage"] },
              { label: "Générateur", v: record["type_generateur_chauffage_principal"] },
              { label: "Description", v: record["description_installation_chauffage_n1"] },
            ]}
          />
          <InstallationCard
            title="Eau chaude sanitaire (ECS)"
            icon={Droplet}
            rows={[
              { label: "Type installation", v: record["type_installation_ecs"] },
              { label: "Énergie principale", v: record["type_energie_principale_ecs"] },
              { label: "Générateur", v: record["type_generateur_chauffage_principal_ecs"] },
              { label: "Description", v: record["description_installation_ecs_n1"] },
            ]}
          />
          <InstallationCard
            title="Ventilation"
            icon={Wind}
            rows={[
              { label: "Type ventilation", v: record["type_ventilation"] ?? record["ventilation"] },
              {
                label: "Ventilation post-2012",
                v: record["ventilation_posterieure_2012"] === 1 ? "Oui" : record["ventilation_posterieure_2012"] === 0 ? "Non" : null,
              },
            ]}
          />
        </div>
        {num(record["production_electricite_pv_kwhep_par_an"]) != null && num(record["production_electricite_pv_kwhep_par_an"])! > 0 ? (
          <div className="mt-3 flex items-center gap-2 rounded-md border border-emerald-200 bg-emerald-50 p-2 text-xs text-emerald-900">
            <Sun className="h-3.5 w-3.5" />
            <span>
              Production photovoltaïque :{" "}
              <strong>{num(record["production_electricite_pv_kwhep_par_an"])!.toFixed(0)} kWhEP/an</strong>
            </span>
          </div>
        ) : null}
      </section>

      {/* ============= BÂTIMENT + DATES ============= */}
      <section className="grid gap-3 lg:grid-cols-2">
        <div className="rounded-xl border border-border bg-card p-5 shadow-sm">
          <h2 className="mb-3 flex items-center gap-2 text-sm font-bold">
            <Building2 className="h-4 w-4 text-primary" />
            Bâtiment
          </h2>
          <dl className="space-y-1.5 text-xs">
            <KvRow label="Type" v={record.type_batiment} />
            <KvRow label="Méthode application DPE" v={methode} />
            <KvRow label="Période construction" v={record.periode_construction} />
            <KvRow label="Année construction" v={record.annee_construction} />
            <KvRow label="Surface habitable" v={surface != null ? `${surface.toFixed(0)} m²` : null} />
            <KvRow label="Nombre niveaux immeuble" v={record["nombre_niveau_immeuble"]} />
            <KvRow label="Nombre logements" v={record["nombre_appartement"]} />
            <KvRow label="Étage appartement" v={record["numero_etage_appartement"]} />
            <KvRow label="Hauteur sous plafond" v={record["hauteur_sous_plafond"] != null ? `${record["hauteur_sous_plafond"]} m` : null} />
            <KvRow label="Inertie bâtiment" v={record["classe_inertie_batiment"]} />
            <KvRow label="Zone climatique" v={record["zone_climatique"]} />
          </dl>
        </div>

        <div className="rounded-xl border border-border bg-card p-5 shadow-sm">
          <h2 className="mb-3 flex items-center gap-2 text-sm font-bold">
            <Calendar className="h-4 w-4 text-primary" />
            Dates et identité
          </h2>
          <dl className="space-y-1.5 text-xs">
            <KvRow label="N° DPE" v={record.numero_dpe} mono />
            <KvRow label="N° DPE immeuble (si lié)" v={record["numero_dpe_immeuble"]} mono />
            <KvRow label="Date visite" v={dateLabel(record["date_visite_diagnostiqueur"])} />
            <KvRow label="Date établissement" v={dateLabel(record.date_etablissement_dpe)} />
            <KvRow label="Date réception ADEME" v={dateLabel(record["date_reception_dpe"])} />
            <KvRow label="Dernière modification" v={dateLabel(record.date_derniere_modification_dpe)} />
            <KvRow label="Fin de validité" v={dateLabel(record["date_fin_validite_dpe"])} />
            <KvRow label="Version DPE" v={record.version_dpe} />
            <KvRow label="Identifiant BAN" v={record["identifiant_ban"]} mono />
            <KvRow label="Statut géocodage" v={record["statut_geocodage"]} />
            <KvRow label="Score BAN" v={record["score_ban"] != null ? `${(Number(record["score_ban"]) * 100).toFixed(0)} %` : null} />
          </dl>
        </div>
      </section>
    </div>
  );
}

/* ============================== Sous-composants ============================== */

function ScaleCard({
  label, unit, value, activeClass, scale,
}: {
  label: string;
  unit: string;
  value: number | null;
  activeClass: string;
  scale: ReadonlyArray<{ letter: string; upper: number }>;
}) {
  return (
    <div className="rounded-xl bg-card p-4 ring-1 ring-border">
      <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
        {label}
      </p>
      <div className="mt-1 flex items-baseline gap-2">
        <p className="text-2xl font-black tracking-tight">
          {value != null ? value.toFixed(0) : "—"}
        </p>
        <span className="text-xs text-muted-foreground">{unit}</span>
        <DpeBadge classe={activeClass} className="ml-auto !h-9 !min-w-[36px] !text-base" />
      </div>
      <div className="mt-3 flex h-2.5 w-full overflow-hidden rounded-full">
        {scale.map((s) => (
          <span
            key={s.letter}
            title={`${s.letter} ≤ ${s.upper}`}
            className={s.letter === activeClass ? "ring-2 ring-foreground" : ""}
            style={{
              flex: 1,
              background: DPE_COLORS[s.letter] ?? "#94a3b8",
            }}
          />
        ))}
      </div>
      <div className="mt-1 flex justify-between text-[9px] font-bold text-muted-foreground">
        {scale.map((s) => (
          <span key={s.letter}>{s.letter}</span>
        ))}
      </div>
    </div>
  );
}

function UsageKpi({
  label, icon: Icon, valueEP, valueEF, cost,
}: {
  label: string;
  icon: React.ComponentType<{ className?: string }>;
  valueEP: number | null;
  valueEF: number | null;
  cost: number | null;
}) {
  return (
    <div className="rounded-lg border border-border bg-secondary/30 p-3">
      <p className="flex items-center gap-1 text-[10px] uppercase tracking-wider text-muted-foreground">
        <Icon className="h-3 w-3" /> {label}
      </p>
      <p className="mt-1 text-lg font-bold">
        {valueEP != null ? `${valueEP.toFixed(0)}` : "—"}
        <span className="ml-1 text-[10px] font-normal text-muted-foreground">
          kWhEP/an
        </span>
      </p>
      {valueEF != null ? (
        <p className="text-[10px] text-muted-foreground">
          EF : {valueEF.toFixed(0)} kWh
        </p>
      ) : null}
      {cost != null ? (
        <p className="text-[10px] text-primary">{cost.toFixed(0)} €/an</p>
      ) : null}
    </div>
  );
}

function Stat({
  label, value, inline,
}: {
  label: string;
  value: string;
  inline?: boolean;
}) {
  if (inline) {
    return (
      <div className="flex items-center justify-between rounded-md border border-border bg-secondary/30 px-2 py-1.5">
        <span className="text-muted-foreground">{label}</span>
        <strong>{value}</strong>
      </div>
    );
  }
  return (
    <div className="rounded-md border border-border bg-secondary/30 px-2 py-1 text-muted-foreground">
      {label} : <strong className="text-foreground">{value}</strong>
    </div>
  );
}

function QualityRow({ label, value }: { label: string; value: unknown }) {
  const v = String(value ?? "").toLowerCase();
  const color =
    v === "très bonne" ? "bg-emerald-500" :
    v === "bonne" ? "bg-lime-500" :
    v === "moyenne" ? "bg-yellow-500" :
    v === "insuffisante" || v === "mauvaise" ? "bg-orange-500" :
    v === "très insuffisante" ? "bg-red-500" :
    "bg-secondary";
  return (
    <div className="flex items-center justify-between rounded-md border border-border bg-secondary/30 px-3 py-1.5 text-xs">
      <span className="text-muted-foreground">{label}</span>
      <span className="flex items-center gap-1.5">
        <span className={`inline-block h-2 w-2 rounded-full ${color}`} />
        <span className="font-medium capitalize">{v || "—"}</span>
      </span>
    </div>
  );
}

function DeperditionKpi({ label, value, strong }: { label: string; value: number | null; strong?: boolean }) {
  return (
    <div className={`rounded-md border border-border ${strong ? "bg-primary/5 border-primary/30" : "bg-secondary/30"} p-2`}>
      <p className="text-[9px] uppercase tracking-wider text-muted-foreground">
        {label}
      </p>
      <p className={`text-xs ${strong ? "font-bold text-primary" : "font-semibold"}`}>
        {value != null ? `${value.toFixed(0)} W/K` : "—"}
      </p>
    </div>
  );
}

function InstallationCard({
  title, icon: Icon, rows,
}: {
  title: string;
  icon: React.ComponentType<{ className?: string }>;
  rows: Array<{ label: string; v: unknown }>;
}) {
  return (
    <div className="rounded-lg border border-border bg-secondary/30 p-3">
      <p className="mb-2 flex items-center gap-1.5 text-xs font-bold">
        <Icon className="h-3.5 w-3.5 text-primary" /> {title}
      </p>
      <dl className="space-y-1 text-[11px]">
        {rows.map((r) => (
          <div key={r.label}>
            <dt className="text-muted-foreground">{r.label}</dt>
            <dd className="font-medium text-foreground">
              {r.v != null && String(r.v) !== "" ? String(r.v) : "—"}
            </dd>
          </div>
        ))}
      </dl>
    </div>
  );
}

function KvRow({ label, v, mono }: { label: string; v: unknown; mono?: boolean }) {
  const display = v != null && v !== "" ? String(v) : "—";
  return (
    <div className="flex items-baseline justify-between gap-3 border-b border-border/40 py-0.5 last:border-b-0">
      <span className="text-muted-foreground">{label}</span>
      <span className={`text-right text-foreground ${mono ? "font-mono text-[10px]" : "font-medium"}`}>
        {display}
      </span>
    </div>
  );
}
