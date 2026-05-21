"use client";
import { useMemo, useState } from "react";
import { Coins, Sparkles, Info, ArrowDownRight } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { evaluateAllSheets } from "@/lib/services/cee/engine";
import type { Project, Sector } from "@/lib/services/cee/types";

const SECTORS: Sector[] = [
  "Bureaux",
  "Commerces",
  "Hotellerie / Restauration",
  "Sante",
  "Enseignement",
  "Autres secteurs",
];

/**
 * Estimation CEE pour un bâtiment tertiaire.
 *
 * Branche sur le moteur existant (cee/engine.ts evaluateAllSheets) avec
 * `buildingType: "Tertiaire"` → ne sélectionne que les fiches BAT-*.
 *
 * Vue MVP : liste compacte des fiches éligibles + total estimé, sans la
 * marketplace complète résidentielle (à venir si besoin).
 */
export function CeeTertiairePostes({
  defaultSector,
  defaultPostalCode,
  defaultSurface,
  defaultYear,
}: {
  defaultSector?: string | null;
  defaultPostalCode?: string | null;
  defaultSurface?: number | null;
  defaultYear?: number | null;
}) {
  const [sector, setSector] = useState<string>(defaultSector ?? "Bureaux");
  const [postalCode, setPostalCode] = useState<string>(defaultPostalCode ?? "");
  const [surface, setSurface] = useState<string>(defaultSurface ? String(defaultSurface) : "");
  const [year, setYear] = useState<string>(defaultYear ? String(defaultYear) : "");
  const [priceStd, setPriceStd] = useState<string>("7");
  const [pricePrec, setPricePrec] = useState<string>("9");

  const project = useMemo<Project>(() => ({
    buildingType: "Tertiaire",
    sector: sector as Sector,
    postalCode,
    constructionYear: year || "",
    buildingSurface: surface || "",
    mwhCumacPrice: priceStd,
    mwhCumacPricePrecarious: pricePrec,
    projectSystemHeating: true, // assume tous les équipements potentiellement présents
    projectSystemCooling: true,
    projectSystemVentilation: true,
    projectSystemLighting: true,
    projectSystemDhw: true,
  }), [sector, postalCode, year, surface, priceStd, pricePrec]);

  const results = useMemo(() => {
    const all = evaluateAllSheets(project, { buildingTypes: ["Tertiaire"] });
    return all
      .filter((r) => r.evaluation.status !== "Non eligible" && r.evaluation.status !== undefined)
      .sort((a, b) => {
        const ea = a.evaluation.euroAmount ?? 0;
        const eb = b.evaluation.euroAmount ?? 0;
        return eb - ea;
      });
  }, [project]);

  const totals = useMemo(() => {
    let kwh = 0;
    let euros = 0;
    for (const r of results) {
      kwh += r.evaluation.kwhCumac ?? 0;
      euros += r.evaluation.euroAmount ?? 0;
    }
    return { kwh, euros, count: results.length };
  }, [results]);

  const fmtN = (n: number) => n.toLocaleString("fr-FR", { maximumFractionDigits: 0 });

  return (
    <div className="space-y-4">
      {/* Paramètres */}
      <section className="rounded-lg border border-border bg-card p-4">
        <div className="mb-3 flex items-center gap-2">
          <Sparkles className="h-4 w-4 text-primary" />
          <h3 className="text-sm font-semibold">Paramètres CEE tertiaire</h3>
        </div>
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
          <Field label="Secteur d'activité">
            <select
              value={sector}
              onChange={(e) => setSector(e.target.value)}
              className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
            >
              {SECTORS.map((s) => (
                <option key={s} value={s}>{s}</option>
              ))}
            </select>
          </Field>
          <Field label="Code postal">
            <Input value={postalCode} onChange={(e) => setPostalCode(e.target.value)} placeholder="75009" />
          </Field>
          <Field label="Surface (m²)">
            <Input value={surface} onChange={(e) => setSurface(e.target.value)} placeholder="1000" inputMode="numeric" />
          </Field>
          <Field label="Année construction">
            <Input value={year} onChange={(e) => setYear(e.target.value)} placeholder="1980" inputMode="numeric" />
          </Field>
          <Field label="Prix MWh cumac standard (€)">
            <Input value={priceStd} onChange={(e) => setPriceStd(e.target.value)} inputMode="decimal" />
          </Field>
          <Field label="Prix MWh cumac précarité (€)">
            <Input value={pricePrec} onChange={(e) => setPricePrec(e.target.value)} inputMode="decimal" />
          </Field>
        </div>
      </section>

      {/* Total */}
      <section className="grid grid-cols-1 gap-3 sm:grid-cols-3">
        <Kpi
          label="Fiches éligibles"
          value={String(totals.count)}
          icon={<Sparkles className="h-4 w-4" />}
        />
        <Kpi
          label="kWh cumac estimés"
          value={fmtN(totals.kwh)}
          icon={<ArrowDownRight className="h-4 w-4" />}
        />
        <Kpi
          label="Prime estimée"
          value={`${fmtN(totals.euros)} €`}
          icon={<Coins className="h-4 w-4" />}
          highlight
        />
      </section>

      {/* Liste fiches */}
      <section>
        <div className="mb-2 flex items-center justify-between">
          <h4 className="text-sm font-semibold">Fiches d'opérations standardisées éligibles</h4>
          <Badge variant="secondary" className="text-[10px]">
            BAT-* (tertiaire)
          </Badge>
        </div>
        {results.length === 0 ? (
          <div className="rounded-lg border border-border bg-secondary/30 p-6 text-center text-sm text-muted-foreground">
            Renseigne au moins le secteur, code postal et surface pour voir les fiches BAT applicables.
          </div>
        ) : (
          <ul className="grid grid-cols-1 gap-2 md:grid-cols-2">
            {results.slice(0, 30).map(({ evaluation, sheet }) => (
              <li
                key={sheet.code}
                className="rounded-lg border border-border bg-card p-3 transition-colors hover:border-primary/50"
              >
                <div className="flex items-start justify-between gap-2">
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-2">
                      <span className="text-[10px] font-mono font-semibold text-primary">{sheet.code}</span>
                      <Badge variant="outline" className="text-[9px]">{sheet.family}</Badge>
                      <StatusBadge status={evaluation.status} />
                    </div>
                    <p className="mt-1 line-clamp-2 text-xs font-medium">{sheet.title}</p>
                  </div>
                </div>
                <div className="mt-2 flex items-end justify-between border-t border-border/50 pt-2">
                  <div className="text-[10px] text-muted-foreground">
                    {evaluation.kwhCumac ? `${fmtN(evaluation.kwhCumac)} kWh cumac` : "—"}
                  </div>
                  <div className="text-sm font-semibold text-primary">
                    {evaluation.euroAmount ? `${fmtN(evaluation.euroAmount)} €` : "—"}
                  </div>
                </div>
                {evaluation.missing.length > 0 ? (
                  <p className="mt-1.5 flex items-start gap-1 text-[9px] italic text-muted-foreground">
                    <Info className="mt-0.5 h-2.5 w-2.5 shrink-0" />
                    Manque : {evaluation.missing.slice(0, 2).join(", ")}
                  </p>
                ) : null}
              </li>
            ))}
          </ul>
        )}
      </section>

      <Separator />
      <p className="text-[11px] italic text-muted-foreground">
        Estimations basées sur le moteur CEE interne (postes BAT-*). Les primes réelles dépendent du
        prix négocié avec l'obligé et des justificatifs fournis. Coup de pouce inclus quand applicable.
      </p>
    </div>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <label className="mb-1 block text-[11px] font-medium uppercase tracking-wider text-muted-foreground">
        {label}
      </label>
      {children}
    </div>
  );
}

function Kpi({ label, value, icon, highlight }: { label: string; value: string; icon: React.ReactNode; highlight?: boolean }) {
  return (
    <div className={`rounded-lg border p-3 ${highlight ? "border-primary bg-primary/5" : "border-border bg-card"}`}>
      <div className="flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
        {icon}
        {label}
      </div>
      <p className={`mt-1 text-xl font-bold ${highlight ? "text-primary" : ""}`}>{value}</p>
    </div>
  );
}

function StatusBadge({ status }: { status: string }) {
  const map: Record<string, { label: string; cls: string }> = {
    "Eligible": { label: "Éligible", cls: "bg-emerald-100 text-emerald-900" },
    "Eligibilite a confirmer": { label: "À confirmer", cls: "bg-amber-100 text-amber-900" },
    "Potentiellement eligible": { label: "Potentiel", cls: "bg-blue-100 text-blue-900" },
    "Non eligible": { label: "Non éligible", cls: "bg-stone-200 text-stone-700" },
  };
  const m = map[status] ?? { label: status, cls: "bg-stone-100 text-stone-700" };
  return <span className={`rounded px-1.5 py-0.5 text-[9px] font-semibold ${m.cls}`}>{m.label}</span>;
}
