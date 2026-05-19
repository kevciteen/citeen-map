"use client";
import { useEffect, useState } from "react";
import * as Dialog from "@radix-ui/react-dialog";
import {
  Home, Building, ExternalLink, Plus, X, Calendar, Ruler, Zap, Flame,
  Snowflake, Wind, Wallet, Shield, FileText, Loader2,
  Banknote, TrendingUp,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { DpeBadge, DpeScaleBar } from "@/components/ui/dpe-badge";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { toast } from "sonner";
import { CeeQuickEstimate } from "@/components/crm/cee-quick-estimate";

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
  generateur_chauffage: string | null;
  generateur_chauffage_desc: string | null;
  emetteur_chauffage: string | null;
  generateur_ecs: string | null;
  description_ecs: string | null;
  volume_ballon_ecs: number | null;
  energie_climatisation: string | null;
  surface_climatisee: number | null;
  type_ventilation: string | null;
  ventilation_recente: boolean | null;
  zone_climatique: string | null;
  deperdition_murs: number | null;
  deperdition_baies: number | null;
  deperdition_plancher_bas: number | null;
  deperdition_plancher_haut: number | null;
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

type DvfTx = {
  id_mutation: string;
  date_mutation: string;
  nature_mutation: string;
  valeur_fonciere: number | null;
  type_local: string;
  surface_reelle_bati: number | null;
  nombre_pieces_principales: number | null;
  surface_terrain: number | null;
  adresse_numero: string | null;
  adresse_nom_voie: string | null;
  prix_m2: number | null;
};

type DvfResponse = {
  transactions: DvfTx[];
  stats: {
    count: number;
    last_sale_date: string | null;
    last_sale_price: number | null;
    last_prix_m2: number | null;
    median_prix_m2: number | null;
  };
};

function monthsSince(dateStr: string | null): number | null {
  if (!dateStr) return null;
  const t = Date.parse(dateStr);
  if (!Number.isFinite(t)) return null;
  return Math.floor((Date.now() - t) / (30.4 * 24 * 3600 * 1000));
}

type BatimentType = "maison" | "appartement";

export function MaisonDetailSheet({
  maison,
  trigger,
  onAdded,
  typeBatiment = "maison",
}: {
  maison: MaisonDpe;
  trigger: React.ReactNode;
  onAdded?: () => void;
  typeBatiment?: BatimentType;
}) {
  const apiSegment = typeBatiment === "appartement" ? "appartements" : "maisons";
  const typeLabel = typeBatiment === "appartement" ? "Appartement" : "Maison";
  const [open, setOpen] = useState(false);
  const [adding, setAdding] = useState(false);
  const [dvf, setDvf] = useState<DvfResponse | null>(null);
  const [dvfLoading, setDvfLoading] = useState(false);
  const expired = isExpired(maison.date_fin_validite);

  useEffect(() => {
    if (!open) return;
    if (dvf) return; // déjà chargé
    if (maison.lat == null || maison.lon == null) return;
    let aborted = false;
    setDvfLoading(true);
    const params = new URLSearchParams();
    params.set("lat", String(maison.lat));
    params.set("lon", String(maison.lon));
    params.set("dist", "30");
    params.set("type", typeBatiment);
    if (maison.address.housenumber)
      params.set("housenumber", maison.address.housenumber);
    if (maison.address.street) params.set("street", maison.address.street);
    fetch(`/api/${apiSegment}/dvf?${params.toString()}`)
      .then((r) => (r.ok ? r.json() : Promise.reject(r)))
      .then((j: DvfResponse) => {
        if (!aborted) setDvf(j);
      })
      .catch(() => {
        if (!aborted) setDvf({ transactions: [], stats: { count: 0, last_sale_date: null, last_sale_price: null, last_prix_m2: null, median_prix_m2: null } });
      })
      .finally(() => {
        if (!aborted) setDvfLoading(false);
      });
    return () => {
      aborted = true;
    };
  }, [open, maison.lat, maison.lon, maison.address.housenumber, maison.address.street, dvf, apiSegment, typeBatiment]);

  const lastSaleMonths = monthsSince(dvf?.stats.last_sale_date ?? null);
  const isRecentSale = lastSaleMonths != null && lastSaleMonths <= 24;

  const addToPipeline = async () => {
    setAdding(true);
    try {
      const r = await fetch("/api/prospects", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          customLabel: `${typeLabel} · ${maison.address.label}`,
          customAddress: maison.address.label,
          customLat: maison.lat,
          customLon: maison.lon,
          stage: "to_contact",
          priority: 2,
          tags: [typeBatiment, `dpe-${maison.classe}`, `ges-${maison.ges}`],
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
          <Dialog.Title className="sr-only">
            Fiche {typeLabel.toLowerCase()} détaillée
          </Dialog.Title>
          <Dialog.Description className="sr-only">
            Détails du DPE de {typeBatiment === "appartement" ? "l'appartement" : "la maison"} à l'adresse {maison.address.label}
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
                {typeBatiment === "appartement" ? (
                  <Building className="h-6 w-6" />
                ) : (
                  <Home className="h-6 w-6" />
                )}
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
                  {isRecentSale ? (
                    <span className="inline-flex items-center gap-1 rounded-full bg-emerald-600/90 px-2 py-0.5 text-[10px] font-bold backdrop-blur">
                      <TrendingUp className="h-3 w-3" />
                      {typeBatiment === "appartement" ? "Vendu" : "Vendue"} il y a {lastSaleMonths}&nbsp;mois
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

            {/* CHAUFFAGE — Équipements détaillés */}
            <Section icon={<Flame className="h-4 w-4 text-orange-500" />} title="Chauffage — équipements">
              <Row label="Énergie principale" value={maison.energie_principale_chauffage} strong />
              {maison.energie_n2 ? <Row label="Énergie secondaire" value={maison.energie_n2} /> : null}
              {maison.energie_n3 ? <Row label="Énergie tertiaire" value={maison.energie_n3} /> : null}
              <Row label="Générateur" value={maison.generateur_chauffage} strong />
              <Row label="Émetteurs" value={maison.emetteur_chauffage} />
              <Row label="Installation" value={maison.installation_chauffage} />
              {maison.generateur_chauffage_desc ? (
                <div className="mt-2 rounded-lg bg-background/70 p-2 text-[11px] italic text-muted-foreground">
                  💬 <strong>Description complète :</strong> {maison.generateur_chauffage_desc}
                </div>
              ) : null}
            </Section>

            {/* ECS */}
            {maison.generateur_ecs || maison.description_ecs ? (
              <Section icon={<Zap className="h-4 w-4 text-cyan-600" />} title="Eau chaude sanitaire">
                <Row label="Générateur ECS" value={maison.generateur_ecs} strong />
                <Row label="Configuration" value={maison.description_ecs} />
                {maison.volume_ballon_ecs ? (
                  <Row label="Volume ballon" value={`${maison.volume_ballon_ecs} L`} />
                ) : null}
              </Section>
            ) : null}

            {/* VENTILATION + CLIMATISATION */}
            {(maison.type_ventilation || maison.energie_climatisation) ? (
              <Section icon={<Wind className="h-4 w-4 text-sky-600" />} title="Ventilation & climatisation">
                {maison.type_ventilation ? (
                  <Row
                    label="Type ventilation"
                    value={
                      maison.type_ventilation +
                      (maison.ventilation_recente ? " (post‑2012)" : "")
                    }
                  />
                ) : null}
                {maison.energie_climatisation ? (
                  <Row label="Climatisation" value={`${maison.energie_climatisation}${maison.surface_climatisee ? ` (${maison.surface_climatisee} m²)` : ""}`} />
                ) : null}
                {maison.zone_climatique ? (
                  <Row label="Zone climatique" value={maison.zone_climatique} mono />
                ) : null}
              </Section>
            ) : null}

            {/* PERTES THERMIQUES */}
            {maison.deperdition_murs ||
            maison.deperdition_baies ||
            maison.deperdition_plancher_bas ||
            maison.deperdition_plancher_haut ? (
              <Section
                icon={<Snowflake className="h-4 w-4 text-blue-500" />}
                title="Pertes thermiques par poste (W/K)"
              >
                <DeperditionRow label="Murs" value={maison.deperdition_murs} />
                <DeperditionRow label="Toiture / plafond" value={maison.deperdition_plancher_haut} />
                <DeperditionRow label="Plancher bas" value={maison.deperdition_plancher_bas} />
                <DeperditionRow label="Baies vitrées" value={maison.deperdition_baies} />
                <p className="mt-2 rounded-lg bg-blue-50 p-2 text-[11px] text-blue-900">
                  💡 Plus la valeur W/K est élevée, plus la perte est forte — ces postes sont les
                  cibles prioritaires de rénovation (isolation, vitrage…).
                </p>
              </Section>
            ) : null}

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

            {/* POSTES DE TRAVAUX CEE ÉLIGIBLES */}
            <Section
              icon={<Banknote className="h-4 w-4 text-emerald-700" />}
              title="Travaux CEE éligibles"
            >
              <CeeQuickEstimate typeBatiment={typeBatiment} maison={maison} />
            </Section>

            {/* TRANSACTIONS DVF */}
            <Section
              icon={<Banknote className="h-4 w-4 text-emerald-600" />}
              title="Transactions immobilières (DVF)"
            >
              {dvfLoading ? (
                <div className="flex items-center gap-2 py-2 text-xs text-muted-foreground">
                  <Loader2 className="h-3.5 w-3.5 animate-spin" />
                  Chargement des transactions DGFiP…
                </div>
              ) : null}
              {!dvfLoading && dvf && dvf.transactions.length === 0 ? (
                <p className="py-2 text-xs italic text-muted-foreground">
                  Aucune vente trouvée pour cette adresse dans l'open data DVF.
                  Soit la maison n'a pas changé de mains depuis 2014, soit la
                  vente n'a pas encore été publiée par la DGFiP (délai ~6&nbsp;mois).
                </p>
              ) : null}
              {!dvfLoading && dvf && dvf.transactions.length > 0 ? (
                <>
                  {/* Indicateur marché */}
                  {dvf.stats.median_prix_m2 ? (
                    <div className="mb-3 grid grid-cols-2 gap-2">
                      <div className="rounded-lg bg-emerald-50 p-2 text-[11px]">
                        <div className="font-semibold uppercase tracking-wider text-emerald-700">
                          Dernière vente
                        </div>
                        <div className="mt-0.5 text-base font-black text-emerald-900">
                          {fmtEuro(dvf.stats.last_sale_price) ?? "—"}
                        </div>
                        <div className="text-[10px] text-emerald-800/80">
                          {fmtDate(dvf.stats.last_sale_date)} ·{" "}
                          {dvf.stats.last_prix_m2
                            ? `${dvf.stats.last_prix_m2.toLocaleString("fr-FR")} €/m²`
                            : "prix/m² n/c"}
                        </div>
                      </div>
                      <div className="rounded-lg bg-slate-100 p-2 text-[11px]">
                        <div className="font-semibold uppercase tracking-wider text-slate-700">
                          Prix médian zone (maisons)
                        </div>
                        <div className="mt-0.5 text-base font-black text-slate-900">
                          {dvf.stats.median_prix_m2.toLocaleString("fr-FR")} €/m²
                        </div>
                        <div className="text-[10px] text-slate-600">
                          sur {dvf.transactions.filter((t) => /maison/i.test(t.type_local)).length} transactions
                        </div>
                      </div>
                    </div>
                  ) : null}

                  {/* Historique */}
                  <div className="space-y-1.5">
                    {dvf.transactions.slice(0, 8).map((tx) => (
                      <DvfRow key={tx.id_mutation} tx={tx} />
                    ))}
                    {dvf.transactions.length > 8 ? (
                      <p className="pt-1 text-[10px] italic text-muted-foreground">
                        + {dvf.transactions.length - 8} transactions plus anciennes (non affichées)
                      </p>
                    ) : null}
                  </div>

                  <p className="mt-2 rounded-lg bg-blue-50 p-2 text-[11px] text-blue-900">
                    💡 Données DGFiP publiées via data.gouv.fr — anonymisées
                    (pas de nom d'acheteur/vendeur). Une vente récente est un
                    signal fort pour la prospection rénovation.
                  </p>
                </>
              ) : null}
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

function DeperditionRow({ label, value }: { label: string; value: number | null }) {
  if (value == null) {
    return (
      <div className="flex items-center justify-between gap-3 border-b border-border/30 py-1 text-xs last:border-0">
        <span className="shrink-0 text-muted-foreground">{label}</span>
        <span className="text-muted-foreground">—</span>
      </div>
    );
  }
  // Color scale: green < 50, yellow 50-150, orange 150-300, red > 300
  const color =
    value < 50 ? "#1f9d55"
    : value < 150 ? "#cddc39"
    : value < 300 ? "#fb8c00"
    : "#e53935";
  const intensity =
    value < 50 ? "Faible"
    : value < 150 ? "Modérée"
    : value < 300 ? "Élevée"
    : "Très élevée";
  return (
    <div className="flex items-center justify-between gap-3 border-b border-border/30 py-1 text-xs last:border-0">
      <span className="shrink-0 text-muted-foreground">{label}</span>
      <span className="flex items-center gap-2">
        <span className="font-mono font-bold">{Math.round(value)} W/K</span>
        <span
          className="rounded-full px-2 py-0.5 text-[10px] font-bold text-white"
          style={{ background: color }}
        >
          {intensity}
        </span>
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

function DvfRow({ tx }: { tx: DvfTx }) {
  const isMaison = /maison/i.test(tx.type_local);
  return (
    <div className="flex items-start justify-between gap-3 rounded-lg border border-border/40 bg-card px-3 py-2 text-xs">
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-2">
          <span className="font-bold tabular-nums">
            {fmtDate(tx.date_mutation) ?? "—"}
          </span>
          <span
            className={cn(
              "rounded-full px-1.5 py-0.5 text-[10px] font-semibold",
              isMaison ? "bg-emerald-100 text-emerald-800" : "bg-slate-100 text-slate-700",
            )}
          >
            {tx.type_local || "—"}
          </span>
          <span className="text-[10px] text-muted-foreground">
            {tx.nature_mutation}
          </span>
        </div>
        <div className="mt-0.5 truncate text-[10px] text-muted-foreground">
          {tx.adresse_numero ?? ""} {tx.adresse_nom_voie ?? ""}
          {tx.surface_reelle_bati ? ` · ${tx.surface_reelle_bati} m²` : ""}
          {tx.nombre_pieces_principales ? ` · ${tx.nombre_pieces_principales} pièces` : ""}
          {tx.surface_terrain ? ` · terrain ${tx.surface_terrain} m²` : ""}
        </div>
      </div>
      <div className="shrink-0 text-right">
        <div className="font-black tabular-nums">
          {fmtEuro(tx.valeur_fonciere) ?? "—"}
        </div>
        {tx.prix_m2 ? (
          <div className="text-[10px] text-muted-foreground tabular-nums">
            {tx.prix_m2.toLocaleString("fr-FR")} €/m²
          </div>
        ) : null}
      </div>
    </div>
  );
}

void Calendar;
void Badge;
