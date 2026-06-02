"use client";
/**
 * Fiche DPE tertiaire (dataset ADEME dpe-tertiaire, méthode 2007-2021).
 *
 * Structure différente du DPE résidentiel actuel : moins de champs détaillés
 * (pas de conso par usage, pas de qualité enveloppe détaillée), focus sur
 * l'étiquette DPE/GES, secteur d'activité, surfaces.
 */
import {
  MapPin, Briefcase, FileText, Calendar, ExternalLink, AlertTriangle,
  Building2,
} from "lucide-react";
import type { DpeTertiaireRecord } from "@/lib/services/dpe-tertiaire";
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

export function DpeTertiaireDetailView({ record }: { record: DpeTertiaireRecord }) {
  const dpe = String(record.classe_consommation_energie ?? "NC").toUpperCase();
  const ges = String(record.classe_estimation_ges ?? "NC").toUpperCase();
  const consoEnergie = num(record.consommation_energie);
  const estimationGes = num(record.estimation_ges);
  const surface = num(record.surface_utile) ?? num(record.surface_habitable) ?? num(record.shon);
  const typeBat = String(record.tr002_type_batiment_libelle ?? "").trim();
  const secteur = String(record.secteur_activite ?? "").trim();
  const adresse = record.geo_adresse ?? `${record.nom_rue ?? ""}`.trim();
  const cp = record.code_postal ?? "";
  const commune = record.commune ?? "";

  return (
    <div className="space-y-4">
      {/* ============= HERO ============= */}
      <section className="overflow-hidden rounded-2xl border border-border bg-gradient-to-br from-amber-50 via-card to-secondary/30 shadow-sm">
        <div className="p-6">
          <div className="mb-4 flex flex-wrap items-center gap-2 text-[10px] uppercase tracking-wider text-muted-foreground">
            <span className="rounded bg-amber-100 px-2 py-0.5 font-bold text-amber-900">
              DPE Tertiaire (méthode 2007-2021)
            </span>
            <span className="rounded bg-secondary/60 px-2 py-0.5">
              {typeBat || "—"}
            </span>
            {secteur ? (
              <span className="rounded bg-primary/15 px-2 py-0.5 text-primary">
                {secteur}
              </span>
            ) : null}
          </div>

          <h1 className="mb-1 flex items-center gap-2 text-2xl font-black tracking-tight">
            <MapPin className="h-5 w-5 text-primary" />
            {adresse || "Adresse non précisée"}
          </h1>
          <p className="text-sm text-muted-foreground">
            {cp} {commune}
            {surface ? ` · ${surface.toFixed(0)} m²` : ""}
            {record.annee_construction ? ` · construit en ${record.annee_construction}` : ""}
          </p>

          {/* Avertissement méthode ancienne */}
          <div className="mt-3 flex items-start gap-2 rounded-md border border-amber-200 bg-amber-50 p-2 text-[11px] text-amber-900">
            <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
            <span>
              Ce DPE utilise la <strong>méthode pré-2021</strong> (dataset
              ADEME dpe-tertiaire). Les champs détaillés (conso par usage,
              qualité enveloppe, installations) ne sont pas disponibles
              dans ce dataset. Pour un DPE post-juillet 2021 plus complet,
              consulter l&apos;observatoire ADEME.
            </span>
          </div>

          {/* Étiquettes DPE + GES */}
          <div className="mt-5 grid gap-4 sm:grid-cols-2">
            <ScaleCard
              label="Consommation énergie primaire"
              unit="kWhEP/m²/an"
              value={consoEnergie}
              activeClass={dpe}
              scale={DPE_SCALE}
            />
            <ScaleCard
              label="Émissions gaz à effet de serre"
              unit="kg CO₂/m²/an"
              value={estimationGes}
              activeClass={ges}
              scale={GES_SCALE}
            />
          </div>
        </div>
      </section>

      {/* ============= INFOS BÂTIMENT ============= */}
      <section className="grid gap-3 lg:grid-cols-2">
        <div className="rounded-xl border border-border bg-card p-5 shadow-sm">
          <h2 className="mb-3 flex items-center gap-2 text-sm font-bold">
            <Briefcase className="h-4 w-4 text-primary" />
            Activité et bâtiment
          </h2>
          <dl className="space-y-1.5 text-xs">
            <KvRow label="Type de bâtiment" v={typeBat || "—"} />
            <KvRow label="Secteur d'activité" v={secteur || "—"} />
            <KvRow label="Surface utile" v={num(record.surface_utile) ? `${num(record.surface_utile)!.toFixed(0)} m²` : null} />
            <KvRow label="Surface habitable" v={num(record.surface_habitable) ? `${num(record.surface_habitable)!.toFixed(0)} m²` : null} />
            <KvRow label="SHON" v={num(record.shon) ? `${num(record.shon)!.toFixed(0)} m²` : null} />
            <KvRow label="Année construction" v={record.annee_construction} />
          </dl>
        </div>

        <div className="rounded-xl border border-border bg-card p-5 shadow-sm">
          <h2 className="mb-3 flex items-center gap-2 text-sm font-bold">
            <FileText className="h-4 w-4 text-primary" />
            Identité ADEME
          </h2>
          <dl className="space-y-1.5 text-xs">
            <KvRow label="N° DPE" v={record.numero_dpe} mono />
            <KvRow label="Date établissement" v={dateLabel(record.date_etablissement_dpe)} />
            <KvRow label="Date réception ADEME" v={dateLabel(record.date_reception_dpe)} />
            <KvRow label="Adresse géocodée" v={record.geo_adresse} />
            <KvRow label="Nom de rue" v={record.nom_rue} />
            <KvRow label="Commune" v={record.commune} />
            <KvRow label="Code postal" v={record.code_postal} mono />
            <KvRow label="Code INSEE" v={record.code_insee_commune_actualise ?? record.code_insee_commune} mono />
            <KvRow label="Coordonnées" v={record.latitude && record.longitude ? `${Number(record.latitude).toFixed(5)}, ${Number(record.longitude).toFixed(5)}` : null} mono />
          </dl>
        </div>
      </section>

      {/* ============= LIEN ADEME ============= */}
      <section className="rounded-xl border border-primary/30 bg-primary/5 p-4 shadow-sm">
        <a
          href={`https://observatoire-dpe-audit.ademe.fr/afficher-dpe-tertiaire/${record.numero_dpe}`}
          target="_blank"
          rel="noreferrer"
          className="flex items-center justify-between gap-3 text-sm font-semibold text-primary hover:underline"
        >
          <span className="flex items-center gap-2">
            <Building2 className="h-4 w-4" />
            Voir la fiche complète sur l&apos;observatoire ADEME
          </span>
          <ExternalLink className="h-4 w-4" />
        </a>
      </section>
    </div>
  );
}

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
