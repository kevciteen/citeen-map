"use client";
import { useEffect, useState } from "react";
import {
  Building2,
  X,
  Plus,
  RefreshCw,
  Users,
  Zap,
  Calculator,
  Loader2,
  CheckCircle2,
  ExternalLink,
  Receipt,
  UserCheck,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import { Badge } from "@/components/ui/badge";
import { toast } from "sonner";

type Dirigeant = {
  nom: string | null;
  prenoms: string | null;
  qualite: string | null;
  typeDirigeant: "physique" | "morale" | null;
  denominationMorale: string | null;
  sirenMorale: string | null;
};

type DvfTx = {
  id_mutation: string;
  date_mutation: string;
  nature_mutation: string;
  valeur_fonciere: number | null;
  type_local: string;
  surface_reelle_bati: number | null;
  prix_m2: number | null;
};

type Building = {
  id: number;
  source: string;
  external_id: string | null;
  label: string | null;
  adresse: string | null;
  code_postal: string | null;
  commune: string | null;
  departement: string | null;
  lat: number | null;
  lon: number | null;
  secteur: string | null;
  type_usage: string | null;
  surface_m2: number | null;
  annee_construction: number | null;
};

type Dpe = {
  numero_dpe: string | null;
  etiquette_dpe: string | null;
  etiquette_ges: string | null;
  conso_energie_primaire: number | null;
  conso_energie_finale: number | null;
  emissions_ges: number | null;
  surface_utile: number | null;
  type_usage_dpe: string | null;
  date_etablissement: number | null;
};

type Occupant = {
  id: number;
  siret: string | null;
  siren: string | null;
  denomination: string | null;
  naf_code: string | null;
  naf_label: string | null;
  tranche_effectif: string | null;
  est_siege: number | null;
  dirigeants?: Dirigeant[];
};

// Code tranche INSEE → ordre numérique pour le tri (NN=null = -1)
const EFFECTIF_ORDER: Record<string, number> = {
  "53": 14, "52": 13, "51": 12, "42": 11, "41": 10, "32": 9, "31": 8,
  "22": 7, "21": 6, "12": 5, "11": 4, "03": 3, "02": 2, "01": 1, "00": 0,
};
function effectifRank(code: string | null): number {
  if (!code) return -1;
  return EFFECTIF_ORDER[code] ?? -1;
}
function effectifLabel(code: string | null): string {
  if (!code) return "Non renseigné";
  const map: Record<string, string> = {
    "00": "0 salarié", "01": "1-2", "02": "3-5", "03": "6-9",
    "11": "10-19", "12": "20-49", "21": "50-99", "22": "100-199",
    "31": "200-249", "32": "250-499", "41": "500-999", "42": "1000-1999",
    "51": "2000-4999", "52": "5000-9999", "53": "10000+",
    "NN": "Non renseigné",
  };
  return map[code] ?? code;
}
function isProbableDomiciliation(o: Occupant): boolean {
  // 0 salarié OU effectif non renseigné = souvent domiciliation/holding/SCI
  return !o.tranche_effectif || o.tranche_effectif === "NN" || o.tranche_effectif === "00";
}

type Detail = {
  building: Building;
  dpe: Dpe | null;
  occupants: Occupant[];
  occupantsCount: number;
  prospect: { id: number; stage: string } | null;
  ceeProject: {
    buildingType: "Tertiaire";
    sector: string;
    postalCode: string | number;
    constructionYear: string | number;
    buildingSurface: string | number;
  };
};

const DPE_COLORS: Record<string, string> = {
  A: "bg-emerald-500 text-white",
  B: "bg-lime-500 text-white",
  C: "bg-yellow-400 text-stone-900",
  D: "bg-amber-500 text-white",
  E: "bg-orange-500 text-white",
  F: "bg-red-500 text-white",
  G: "bg-rose-700 text-white",
};

function DpeBadge({ value }: { value?: string | null }) {
  if (!value) return <span className="text-xs text-muted-foreground">—</span>;
  const color = DPE_COLORS[value.toUpperCase()] ?? "bg-stone-300";
  return (
    <span className={`inline-flex h-8 w-8 items-center justify-center rounded-md text-sm font-bold ${color}`}>
      {value.toUpperCase()}
    </span>
  );
}

export function TertiairePanel({
  buildingId,
  onClose,
  onProspectCreated,
  onSimulerCee,
}: {
  buildingId: number;
  onClose: () => void;
  onProspectCreated?: (prospectId: number) => void;
  onSimulerCee?: (detail: Detail) => void;
}) {
  const [detail, setDetail] = useState<Detail | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [creating, setCreating] = useState(false);
  const [dvf, setDvf] = useState<{ transactions: DvfTx[]; stats: { count: number; last_sale_date: string | null; last_sale_price: number | null } } | null>(null);
  const [dvfLoading, setDvfLoading] = useState(false);

  const load = async (refresh = false) => {
    if (refresh) setRefreshing(true);
    else setLoading(true);
    try {
      const res = await fetch(`/api/tertiaire/${buildingId}${refresh ? "?refresh=1" : ""}`);
      if (!res.ok) throw new Error(`Erreur ${res.status}`);
      const json = (await res.json()) as Detail;
      setDetail(json);
    } catch (err) {
      toast.error((err as Error).message);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  const loadDvf = async () => {
    setDvfLoading(true);
    try {
      const res = await fetch(`/api/tertiaire/${buildingId}/dvf`);
      if (!res.ok) throw new Error(`DVF ${res.status}`);
      const json = await res.json() as { transactions: DvfTx[]; stats: { count: number; last_sale_date: string | null; last_sale_price: number | null } };
      setDvf(json);
    } catch (err) {
      // silencieux : DVF est best-effort
      setDvf({ transactions: [], stats: { count: 0, last_sale_date: null, last_sale_price: null } });
    } finally {
      setDvfLoading(false);
    }
  };

  useEffect(() => { load(); loadDvf(); }, [buildingId]); // eslint-disable-line react-hooks/exhaustive-deps

  const createProspect = async () => {
    setCreating(true);
    try {
      const res = await fetch(`/api/tertiaire/${buildingId}/create-prospect`, { method: "POST" });
      if (!res.ok) throw new Error(`Erreur ${res.status}`);
      const json = await res.json() as { prospect: { id: number; stage: string }; created: boolean };
      toast.success(json.created ? "Prospect créé" : "Prospect existant rouvert");
      onProspectCreated?.(json.prospect.id);
      await load();
    } catch (err) {
      toast.error((err as Error).message);
    } finally {
      setCreating(false);
    }
  };

  if (loading) {
    return (
      <aside className="flex h-full w-full flex-col items-center justify-center gap-2 text-sm text-muted-foreground">
        <Loader2 className="h-5 w-5 animate-spin" />
        Chargement de la fiche…
      </aside>
    );
  }
  if (!detail) {
    return (
      <aside className="flex h-full w-full flex-col items-center justify-center gap-2 p-4 text-sm text-muted-foreground">
        Fiche introuvable.
        <Button variant="outline" size="sm" onClick={onClose}>Fermer</Button>
      </aside>
    );
  }

  const { building, dpe, occupants, prospect } = detail;

  return (
    <aside className="flex h-full w-full flex-col overflow-hidden bg-card">
      {/* Header */}
      <div className="flex items-start justify-between gap-2 border-b border-border p-4">
        <div className="min-w-0 flex-1">
          <div className="mb-1 flex items-center gap-2">
            <Building2 className="h-4 w-4 text-primary" />
            <span className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
              Bâtiment tertiaire
            </span>
            {building.secteur ? (
              <Badge variant="secondary" className="text-[10px]">{building.secteur}</Badge>
            ) : null}
          </div>
          <h3 className="truncate text-base font-semibold">
            {building.label ?? building.adresse ?? `#${building.id}`}
          </h3>
          {building.adresse && building.label !== building.adresse ? (
            <p className="truncate text-xs text-muted-foreground">{building.adresse}</p>
          ) : null}
        </div>
        <Button variant="ghost" size="icon" onClick={onClose} aria-label="Fermer">
          <X className="h-4 w-4" />
        </Button>
      </div>

      <div className="flex-1 overflow-auto p-4">
        {/* Actions principales */}
        <div className="mb-4 flex flex-wrap gap-2">
          {prospect ? (
            <Badge className="gap-1.5 bg-emerald-100 text-emerald-900">
              <CheckCircle2 className="h-3 w-3" />
              Prospect ({prospect.stage})
            </Badge>
          ) : (
            <Button size="sm" onClick={createProspect} disabled={creating} className="gap-1.5">
              {creating ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Plus className="h-3.5 w-3.5" />}
              Créer prospect
            </Button>
          )}
          {onSimulerCee ? (
            <Button size="sm" variant="secondary" onClick={() => onSimulerCee(detail)} className="gap-1.5">
              <Calculator className="h-3.5 w-3.5" />
              Simuler CEE
            </Button>
          ) : null}
          <Button
            size="sm"
            variant="outline"
            onClick={() => load(true)}
            disabled={refreshing}
            className="gap-1.5"
          >
            {refreshing ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RefreshCw className="h-3.5 w-3.5" />}
            Rafraîchir
          </Button>
        </div>

        {/* DPE bloc */}
        <section className="mb-4 rounded-lg border border-border bg-secondary/30 p-3">
          <div className="mb-2 flex items-center gap-2">
            <Zap className="h-3.5 w-3.5 text-primary" />
            <p className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">DPE tertiaire</p>
          </div>
          {dpe?.etiquette_dpe || dpe?.etiquette_ges ? (
            <div className="space-y-2">
              <div className="flex items-center gap-3">
                <DpeBadge value={dpe.etiquette_dpe} />
                <span className="text-xs text-muted-foreground">DPE</span>
                <DpeBadge value={dpe.etiquette_ges} />
                <span className="text-xs text-muted-foreground">GES</span>
              </div>
              <dl className="space-y-0.5 text-xs">
                {dpe.conso_energie_primaire ? <Row label="Conso EP" value={`${dpe.conso_energie_primaire.toLocaleString("fr-FR")} kWhEP/m²/an`} /> : null}
                {dpe.emissions_ges ? <Row label="Émissions" value={`${dpe.emissions_ges.toLocaleString("fr-FR")} kgCO₂/m²/an`} /> : null}
                {dpe.surface_utile ? <Row label="Surface DPE" value={`${dpe.surface_utile.toLocaleString("fr-FR")} m²`} /> : null}
                {dpe.numero_dpe ? <Row label="N° DPE" value={dpe.numero_dpe} /> : null}
                {dpe.date_etablissement ? (
                  <Row
                    label="Date DPE"
                    value={new Date(dpe.date_etablissement * 1000).toLocaleDateString("fr-FR", { day: "2-digit", month: "short", year: "numeric" })}
                  />
                ) : null}
              </dl>
            </div>
          ) : (
            <p className="text-xs text-muted-foreground">Aucun DPE tertiaire enregistré pour ce bâtiment.</p>
          )}
        </section>

        {/* Caractéristiques bâtiment */}
        <section className="mb-4">
          <p className="mb-2 text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">Bâtiment</p>
          <dl className="space-y-0.5 text-xs">
            {building.surface_m2 ? <Row label="Surface" value={`${building.surface_m2.toLocaleString("fr-FR")} m²`} /> : null}
            {building.annee_construction ? <Row label="Année construction" value={String(building.annee_construction)} /> : null}
            {building.type_usage ? <Row label="Usage" value={building.type_usage} /> : null}
            {building.code_postal ? <Row label="Code postal" value={building.code_postal} /> : null}
            {building.commune ? <Row label="Commune" value={building.commune} /> : null}
            <Row label="Source" value={building.source} />
          </dl>
        </section>

        <Separator className="my-3" />

        {/* Occupants */}
        <section>
          <div className="mb-2 flex items-center gap-2">
            <Users className="h-3.5 w-3.5 text-primary" />
            <p className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
              Sociétés occupantes ({occupants.length})
            </p>
          </div>
          {occupants.length === 0 ? (
            <div className="space-y-1 rounded border border-border/50 bg-secondary/20 p-2">
              <p className="text-xs font-medium">Aucune société SIRENE à cette adresse exacte.</p>
              <p className="text-[10px] text-muted-foreground">
                Causes possibles : adresse résidentielle/industrielle non commerciale,
                adresse récente pas encore indexée SIRENE, ou rue avec un nom historique
                différent (ex: « ex Chemin Latéral »). Clique Rafraîchir pour re-essayer.
              </p>
            </div>
          ) : (
            <SortedOccupants occupants={occupants} />
          )}
        </section>

        <Separator className="my-3" />

        {/* Mutations DVF — placeholder ; le bloc réel suit */}
        <DvfBlock dvfLoading={dvfLoading} dvf={dvf} />
      </div>
    </aside>
  );
}

function SortedOccupants({ occupants }: { occupants: Occupant[] }) {
  const [hideDomic, setHideDomic] = useState(false);

  // Tri par effectif décroissant (vrais employeurs d'abord), puis siège, puis nom
  const sorted = [...occupants].sort((a, b) => {
    const r = effectifRank(b.tranche_effectif) - effectifRank(a.tranche_effectif);
    if (r !== 0) return r;
    const s = (b.est_siege ?? 0) - (a.est_siege ?? 0);
    if (s !== 0) return s;
    return (a.denomination ?? "").localeCompare(b.denomination ?? "");
  });

  const filtered = hideDomic ? sorted.filter((o) => !isProbableDomiciliation(o)) : sorted;
  const domicCount = sorted.filter(isProbableDomiciliation).length;
  const realCount = sorted.length - domicCount;

  return (
    <div>
      {domicCount > 0 ? (
        <div className="mb-2 flex items-center justify-between rounded border border-border/50 bg-amber-50 px-2 py-1.5 text-[10px]">
          <span className="text-amber-900">
            <strong>{realCount}</strong> employeur(s) · <strong>{domicCount}</strong> domiciliation(s) probable(s)
          </span>
          <button
            onClick={() => setHideDomic((v) => !v)}
            className="rounded bg-white px-2 py-0.5 text-[10px] font-semibold text-amber-900 hover:bg-amber-100"
          >
            {hideDomic ? "Tout afficher" : "Cacher domiciliations"}
          </button>
        </div>
      ) : null}
      <ul className="space-y-2">
        {filtered.slice(0, 30).map((o) => {
          const isDomic = isProbableDomiciliation(o);
          return (
            <li
              key={o.id}
              className={`rounded border p-2 ${
                isDomic ? "border-border/40 bg-secondary/10 opacity-70" : "border-border/50 bg-secondary/20"
              }`}
            >
              <div className="flex items-start justify-between gap-2">
                <div className="min-w-0 flex-1">
                  <p className="truncate text-xs font-semibold">
                    {o.denomination ?? "(sans dénomination)"}
                  </p>
                  <p className="truncate text-[10px] text-muted-foreground">
                    SIRET {o.siret}
                    {o.est_siege ? " · siège" : ""}
                    {" · "}
                    <span className={isDomic ? "" : "font-semibold text-emerald-700"}>
                      {effectifLabel(o.tranche_effectif)}
                    </span>
                    {isDomic ? <span className="text-amber-700"> · domic. probable</span> : null}
                  </p>
                  {o.naf_label ? (
                    <p className="truncate text-[10px] text-muted-foreground">
                      {o.naf_code} · {o.naf_label}
                    </p>
                  ) : null}
                  {o.dirigeants && o.dirigeants.length > 0 ? (
                    <div className="mt-1.5 border-t border-border/30 pt-1.5">
                      <p className="mb-1 flex items-center gap-1 text-[9px] font-semibold uppercase tracking-wider text-muted-foreground">
                        <UserCheck className="h-2.5 w-2.5" />
                        Dirigeants ({o.dirigeants.length})
                      </p>
                      <ul className="space-y-0.5">
                        {o.dirigeants.slice(0, 3).map((d, i) => (
                          <li key={i} className="text-[10px]">
                            {d.typeDirigeant === "morale" ? (
                              <span>
                                <strong>{d.denominationMorale ?? "(société)"}</strong>
                                {d.qualite ? <span className="text-muted-foreground"> · {d.qualite}</span> : null}
                              </span>
                            ) : (
                              <span>
                                {d.prenoms ?? ""} <strong>{d.nom ?? ""}</strong>
                                {d.qualite ? <span className="text-muted-foreground"> · {d.qualite}</span> : null}
                              </span>
                            )}
                          </li>
                        ))}
                        {o.dirigeants.length > 3 ? (
                          <li className="text-[10px] text-muted-foreground">+ {o.dirigeants.length - 3} autres…</li>
                        ) : null}
                      </ul>
                    </div>
                  ) : null}
                </div>
                {o.siren ? (
                  <a
                    href={`https://annuaire-entreprises.data.gouv.fr/entreprise/${o.siren}`}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-muted-foreground hover:text-primary"
                    title="Voir sur Annuaire Entreprises"
                  >
                    <ExternalLink className="h-3 w-3" />
                  </a>
                ) : null}
              </div>
            </li>
          );
        })}
        {filtered.length > 30 ? (
          <p className="text-[10px] text-muted-foreground">+ {filtered.length - 30} autres…</p>
        ) : null}
      </ul>
    </div>
  );
}

function DvfBlock({
  dvfLoading,
  dvf,
}: {
  dvfLoading: boolean;
  dvf: { transactions: DvfTx[]; stats: { count: number; last_sale_date: string | null; last_sale_price: number | null } } | null;
}) {
  return (
    <section>
      <div className="mb-2 flex items-center gap-2">
        <Receipt className="h-3.5 w-3.5 text-primary" />
        <p className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
          Mutations foncières (DVF, 5 ans)
          {dvfLoading ? <Loader2 className="ml-2 inline h-2.5 w-2.5 animate-spin" /> : null}
        </p>
      </div>
      {!dvf ? null : dvf.transactions.length === 0 ? (
        <p className="text-xs text-muted-foreground">
          Aucune mutation enregistrée dans un rayon de 40m sur les 5 dernières années.
        </p>
      ) : (
        <ul className="space-y-1">
          {dvf.transactions.slice(0, 6).map((t) => (
            <li key={t.id_mutation} className="rounded border border-border/50 bg-secondary/20 p-2 text-[10px]">
              <div className="flex items-center justify-between gap-2">
                <span className="font-semibold">
                  {new Date(t.date_mutation).toLocaleDateString("fr-FR", { month: "short", year: "numeric" })}
                </span>
                <span className="text-primary font-bold">
                  {t.valeur_fonciere ? `${t.valeur_fonciere.toLocaleString("fr-FR")} €` : "—"}
                </span>
              </div>
              <p className="text-muted-foreground">
                {t.nature_mutation} · {t.type_local}
                {t.surface_reelle_bati ? ` · ${t.surface_reelle_bati} m²` : ""}
                {t.prix_m2 ? ` · ${t.prix_m2.toLocaleString("fr-FR")} €/m²` : ""}
              </p>
            </li>
          ))}
          {dvf.transactions.length > 6 ? (
            <li className="text-[10px] text-muted-foreground">+ {dvf.transactions.length - 6} autres mutations…</li>
          ) : null}
        </ul>
      )}
      <p className="mt-2 text-[10px] italic text-muted-foreground">
        DVF (DGFiP, data.gouv) — acheteurs particuliers anonymisés (RGPD). Pour les SCI/foncières,
        consulter Annuaire Entreprises avec le SIREN éventuel sur la mutation.
      </p>
    </section>
  );
}

function Row({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex gap-2 py-0.5">
      <dt className="w-36 shrink-0 text-[11px] text-muted-foreground">{label}</dt>
      <dd className="min-w-0 flex-1 break-words text-[11px]">{value}</dd>
    </div>
  );
}
