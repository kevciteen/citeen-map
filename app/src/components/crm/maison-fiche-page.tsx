"use client";
import { useEffect, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import {
  Home,
  Building,
  ArrowLeft,
  Plus,
  ExternalLink,
  Loader2,
  Banknote,
  Flame,
  Snowflake,
  Wind,
  Wallet,
  Shield,
  FileText,
  Tag as TagIcon,
  X,
  Calendar,
  Ruler,
  Zap,
} from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { DpeBadge, DpeScaleBar } from "@/components/ui/dpe-badge";
import { cn } from "@/lib/utils";
import { toast } from "sonner";
import { CeeQuickEstimate } from "@/components/crm/cee-quick-estimate";
import { MaisonEnrichment } from "@/components/crm/maison-enrichment";

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

type ProspectLite = {
  id: number;
  stage: string;
  custom_label: string | null;
  tags: string | null;
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

type DvfTx = {
  id_mutation: string;
  date_mutation: string;
  nature_mutation: string;
  valeur_fonciere: number | null;
  type_local: string;
  surface_reelle_bati: number | null;
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

export function MaisonFichePage({
  typeBatiment,
  numeroDpe,
}: {
  typeBatiment: "maison" | "appartement";
  numeroDpe: string;
}) {
  const router = useRouter();
  const apiSegment = typeBatiment === "appartement" ? "appartements" : "maisons";
  const [maison, setMaison] = useState<MaisonDpe | null>(null);
  const [prospect, setProspect] = useState<ProspectLite | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [dvf, setDvf] = useState<DvfResponse | null>(null);
  const [adding, setAdding] = useState(false);
  const [tagsEdit, setTagsEdit] = useState<string[]>([]);
  const [newTag, setNewTag] = useState("");
  const [savingTags, setSavingTags] = useState(false);

  // Charge le DPE + prospect lié
  useEffect(() => {
    let aborted = false;
    setLoading(true);
    fetch(`/api/${apiSegment}/by-dpe/${encodeURIComponent(numeroDpe)}`)
      .then((r) => (r.ok ? r.json() : Promise.reject(r)))
      .then((j) => {
        if (aborted) return;
        setMaison(j.maison);
        setProspect(j.prospect ?? null);
        if (j.prospect?.tags) {
          try {
            const parsed = JSON.parse(j.prospect.tags);
            if (Array.isArray(parsed)) setTagsEdit(parsed.map(String));
          } catch {}
        }
      })
      .catch(async (r) => {
        if (aborted) return;
        const msg = (await r?.json?.().catch(() => null))?.error;
        setError(msg ?? "Impossible de charger ce DPE");
      })
      .finally(() => {
        if (!aborted) setLoading(false);
      });
    return () => {
      aborted = true;
    };
  }, [apiSegment, numeroDpe]);

  // Charge le DVF
  useEffect(() => {
    if (!maison?.lat || !maison.lon) return;
    let aborted = false;
    const p = new URLSearchParams();
    p.set("lat", String(maison.lat));
    p.set("lon", String(maison.lon));
    p.set("dist", "30");
    p.set("type", typeBatiment);
    if (maison.address.housenumber)
      p.set("housenumber", maison.address.housenumber);
    if (maison.address.street) p.set("street", maison.address.street);
    fetch(`/api/${apiSegment}/dvf?${p.toString()}`)
      .then((r) => (r.ok ? r.json() : null))
      .then((j) => {
        if (!aborted && j) setDvf(j);
      })
      .catch(() => {});
    return () => {
      aborted = true;
    };
  }, [apiSegment, typeBatiment, maison?.lat, maison?.lon, maison?.address.housenumber, maison?.address.street]);

  const addToPipeline = async () => {
    if (!maison) return;
    setAdding(true);
    try {
      const r = await fetch("/api/prospects", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          customLabel: `${typeBatiment === "appartement" ? "Appartement" : "Maison"} · ${maison.address.label}`,
          customAddress: maison.address.label,
          customLat: maison.lat,
          customLon: maison.lon,
          stage: "to_contact",
          priority: 2,
          tags: [typeBatiment, `dpe-${maison.classe}`],
        }),
      });
      const j = await r.json();
      if (r.ok) {
        toast.success("Ajouté au pipeline");
        setProspect({ id: j.id, stage: "to_contact", custom_label: null, tags: null });
      } else if (r.status === 409) {
        setProspect({
          id: j.prospectId,
          stage: "to_contact",
          custom_label: null,
          tags: null,
        });
        toast.info("Déjà dans le pipeline");
      } else toast.error("Erreur");
    } finally {
      setAdding(false);
    }
  };

  const saveTags = async (nextTags: string[]) => {
    if (!prospect) return;
    setSavingTags(true);
    setTagsEdit(nextTags);
    try {
      const r = await fetch(`/api/prospects/${prospect.id}`, {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ tags: nextTags }),
      });
      if (!r.ok) toast.error("Impossible de sauvegarder les tags");
    } finally {
      setSavingTags(false);
    }
  };

  const addTag = () => {
    const t = newTag.trim();
    if (!t) return;
    if (tagsEdit.includes(t)) {
      setNewTag("");
      return;
    }
    saveTags([...tagsEdit, t]);
    setNewTag("");
  };

  const removeTag = (t: string) => {
    saveTags(tagsEdit.filter((x) => x !== t));
  };

  if (loading) {
    return (
      <div className="flex h-full items-center justify-center">
        <Loader2 className="h-6 w-6 animate-spin text-primary" />
      </div>
    );
  }

  if (error || !maison) {
    return (
      <div className="mx-auto max-w-2xl p-6">
        <Button variant="outline" onClick={() => router.back()} className="mb-4">
          <ArrowLeft className="mr-1 h-3 w-3" /> Retour
        </Button>
        <Card>
          <CardContent className="p-6 text-center text-sm text-muted-foreground">
            {error ?? "DPE introuvable"}
          </CardContent>
        </Card>
      </div>
    );
  }

  const expired = maison.date_fin_validite
    ? Date.parse(maison.date_fin_validite) < Date.now()
    : false;
  const Icon = typeBatiment === "appartement" ? Building : Home;
  const typeLabel = typeBatiment === "appartement" ? "Appartement" : "Maison";

  return (
    <div className="mx-auto max-w-6xl space-y-4 p-4 lg:p-6">
      {/* Header coloré DPE */}
      <Card
        className="overflow-hidden border-0 text-white shadow-lg"
        style={{ background: DPE_GRADIENT[maison.classe] ?? DPE_GRADIENT.NC }}
      >
        <CardContent className="p-6">
          <div className="flex items-start gap-4">
            <div className="flex h-14 w-14 shrink-0 items-center justify-center rounded-xl bg-white/20 backdrop-blur">
              <Icon className="h-7 w-7" />
            </div>
            <div className="min-w-0 flex-1">
              <p className="text-[10px] font-semibold uppercase tracking-wider opacity-80">
                {typeLabel} · {maison.address.postcode} {maison.address.city}
              </p>
              <h1 className="text-2xl font-black tracking-tight">
                {maison.address.housenumber} {maison.address.street}
              </h1>
              <div className="mt-2 flex flex-wrap items-center gap-1.5">
                <Badge variant="secondary" className="bg-white/20 text-white">
                  {maison.type_batiment ?? typeBatiment}
                </Badge>
                {maison.annee_construction ? (
                  <Badge variant="secondary" className="bg-white/20 text-white">
                    Construit en {maison.annee_construction}
                  </Badge>
                ) : null}
                {maison.surface ? (
                  <Badge variant="secondary" className="bg-white/20 text-white">
                    {maison.surface} m²
                  </Badge>
                ) : null}
                {expired ? (
                  <Badge variant="secondary" className="bg-red-700/80 text-white">
                    ⚠ DPE expiré
                  </Badge>
                ) : null}
              </div>
            </div>
            <div
              className="flex h-24 w-24 shrink-0 items-center justify-center rounded-2xl bg-white text-4xl font-black shadow"
              style={{
                color: DPE_GRADIENT[maison.classe]?.match(/#[0-9a-f]+/i)?.[0],
              }}
            >
              {maison.classe || "—"}
            </div>
          </div>
          <div className="mt-4">
            <DpeScaleBar active={maison.classe} />
          </div>
        </CardContent>
      </Card>

      {/* Actions */}
      <Card>
        <CardContent className="flex flex-wrap items-center gap-2 p-4">
          {prospect ? (
            <Link
              href={`/prospects/${prospect.id}`}
              className="inline-flex items-center gap-1 rounded-md bg-primary px-3 py-1.5 text-xs font-semibold text-primary-foreground hover:bg-primary/90"
            >
              Voir dans le pipeline (étape : {prospect.stage})
            </Link>
          ) : (
            <Button onClick={addToPipeline} disabled={adding} size="sm">
              {adding ? (
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
              ) : (
                <Plus className="h-3.5 w-3.5" />
              )}
              Ajouter au pipeline
            </Button>
          )}
          <a
            href={maison.ademe_url}
            target="_blank"
            rel="noreferrer"
            className="inline-flex items-center gap-1 rounded-md border border-border bg-background px-3 py-1.5 text-xs font-medium hover:bg-secondary"
          >
            <ExternalLink className="h-3 w-3" /> Fiche ADEME
          </a>
          {maison.lat && maison.lon ? (
            <>
              <a
                href={`https://www.google.com/maps/search/?api=1&query=${maison.lat},${maison.lon}`}
                target="_blank"
                rel="noreferrer"
                className="inline-flex items-center gap-1 rounded-md border border-border bg-background px-3 py-1.5 text-xs font-medium hover:bg-secondary"
              >
                <ExternalLink className="h-3 w-3" /> Google Maps
              </a>
              <a
                href={`https://www.google.com/maps/@?api=1&map_action=pano&viewpoint=${maison.lat},${maison.lon}`}
                target="_blank"
                rel="noreferrer"
                className="inline-flex items-center gap-1 rounded-md border border-border bg-background px-3 py-1.5 text-xs font-medium hover:bg-secondary"
              >
                <ExternalLink className="h-3 w-3" /> Street View
              </a>
            </>
          ) : null}
          <span className="ml-auto font-mono text-[10px] text-muted-foreground">
            N° DPE : {maison.numero_dpe}
          </span>
        </CardContent>
      </Card>

      {/* Tags (si prospect lié) */}
      {prospect ? (
        <Card>
          <CardHeader>
            <div className="flex items-center gap-2">
              <TagIcon className="h-4 w-4 text-primary" />
              <CardTitle className="text-sm">Tags & étiquettes</CardTitle>
            </div>
          </CardHeader>
          <CardContent className="space-y-2">
            <div className="flex flex-wrap items-center gap-1.5">
              {tagsEdit.map((t) => (
                <span
                  key={t}
                  className="inline-flex items-center gap-1 rounded-full bg-secondary px-2 py-0.5 text-[11px]"
                >
                  {t}
                  <button
                    type="button"
                    onClick={() => removeTag(t)}
                    className="hover:text-destructive"
                  >
                    <X className="h-3 w-3" />
                  </button>
                </span>
              ))}
              {tagsEdit.length === 0 ? (
                <span className="text-[11px] italic text-muted-foreground">
                  Aucun tag — ajoute-en pour catégoriser ce bien
                </span>
              ) : null}
            </div>
            <div className="flex gap-2">
              <Input
                value={newTag}
                onChange={(e) => setNewTag(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter") {
                    e.preventDefault();
                    addTag();
                  }
                }}
                placeholder="Nouveau tag (entrée pour valider)"
                className="h-8 max-w-xs text-xs"
              />
              <Button size="sm" onClick={addTag} disabled={savingTags}>
                {savingTags ? <Loader2 className="h-3 w-3 animate-spin" /> : "+"}
              </Button>
            </div>
          </CardContent>
        </Card>
      ) : null}

      {/* Section CEE */}
      <Card>
        <CardHeader>
          <div className="flex items-center gap-2">
            <Banknote className="h-4 w-4 text-emerald-700" />
            <CardTitle className="text-sm">Travaux CEE éligibles</CardTitle>
          </div>
        </CardHeader>
        <CardContent>
          <CeeQuickEstimate typeBatiment={typeBatiment} maison={maison} />
        </CardContent>
      </Card>

      {/* Section DVF */}
      {dvf && dvf.transactions.length > 0 ? (
        <Card>
          <CardHeader>
            <div className="flex items-center gap-2">
              <Banknote className="h-4 w-4 text-emerald-600" />
              <CardTitle className="text-sm">Transactions immobilières (DVF)</CardTitle>
            </div>
          </CardHeader>
          <CardContent className="space-y-2">
            {dvf.stats.median_prix_m2 ? (
              <div className="grid grid-cols-2 gap-2">
                <div className="rounded-lg bg-emerald-50 p-2 text-xs">
                  <div className="text-[10px] font-semibold uppercase tracking-wider text-emerald-700">
                    Dernière vente
                  </div>
                  <div className="text-base font-black text-emerald-900">
                    {fmtEuros(dvf.stats.last_sale_price)}
                  </div>
                  <div className="text-[10px] text-emerald-800/80">
                    {fmtDate(dvf.stats.last_sale_date)} ·{" "}
                    {dvf.stats.last_prix_m2
                      ? `${dvf.stats.last_prix_m2.toLocaleString("fr-FR")} €/m²`
                      : "prix/m² n/c"}
                  </div>
                </div>
                <div className="rounded-lg bg-slate-100 p-2 text-xs">
                  <div className="text-[10px] font-semibold uppercase tracking-wider text-slate-700">
                    Médiane zone
                  </div>
                  <div className="text-base font-black text-slate-900">
                    {dvf.stats.median_prix_m2.toLocaleString("fr-FR")} €/m²
                  </div>
                </div>
              </div>
            ) : null}
            <div className="space-y-1">
              {dvf.transactions.slice(0, 6).map((tx) => (
                <div
                  key={tx.id_mutation}
                  className="flex items-center justify-between rounded-md border border-border bg-card px-2 py-1.5 text-xs"
                >
                  <div>
                    <span className="font-bold tabular-nums">
                      {fmtDate(tx.date_mutation)}
                    </span>
                    <span className="ml-2 text-[10px] text-muted-foreground">
                      {tx.type_local} · {tx.surface_reelle_bati ?? "—"} m²
                    </span>
                  </div>
                  <div className="text-right">
                    <div className="font-black tabular-nums">
                      {fmtEuros(tx.valeur_fonciere)}
                    </div>
                    {tx.prix_m2 ? (
                      <div className="text-[10px] text-muted-foreground tabular-nums">
                        {tx.prix_m2.toLocaleString("fr-FR")} €/m²
                      </div>
                    ) : null}
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      ) : null}

      {/* DPE détail */}
      <Card>
        <CardHeader>
          <div className="flex items-center gap-2">
            <FileText className="h-4 w-4 text-primary" />
            <CardTitle className="text-sm">DPE — détail technique</CardTitle>
          </div>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
            <Kpi icon={<Zap className="h-3.5 w-3.5" />} label="Conso" value={maison.conso ? `${maison.conso}` : "—"} unit="kWhep/m²/an" />
            <Kpi icon={<DpeBadge classe={maison.ges} size="sm" />} label="GES" value={maison.ges} unit={maison.emission_ges ? `${maison.emission_ges} kgCO₂/m²` : ""} />
            <Kpi icon={<Ruler className="h-3.5 w-3.5" />} label="Surface" value={maison.surface ? `${maison.surface}` : "—"} unit="m²" />
            <Kpi icon={<Calendar className="h-3.5 w-3.5" />} label="Établi" value={fmtDate(maison.date)} unit={maison.date_fin_validite ? `Valide jusqu'au ${fmtDate(maison.date_fin_validite)}` : ""} />
          </div>

          {/* Chauffage */}
          <Section icon={<Flame className="h-4 w-4 text-orange-500" />} title="Chauffage">
            <Row label="Énergie principale" value={maison.energie_principale_chauffage} strong />
            {maison.energie_n2 ? <Row label="Énergie secondaire" value={maison.energie_n2} /> : null}
            <Row label="Générateur" value={maison.generateur_chauffage} strong />
            <Row label="Émetteurs" value={maison.emetteur_chauffage} />
            <Row label="Installation" value={maison.installation_chauffage} />
            {maison.generateur_chauffage_desc ? (
              <p className="mt-1 rounded-md bg-background/70 p-2 text-[11px] italic text-muted-foreground">
                {maison.generateur_chauffage_desc}
              </p>
            ) : null}
          </Section>

          {/* ECS */}
          {maison.generateur_ecs || maison.description_ecs ? (
            <Section icon={<Zap className="h-4 w-4 text-cyan-600" />} title="Eau chaude sanitaire">
              <Row label="Générateur ECS" value={maison.generateur_ecs} strong />
              <Row label="Configuration" value={maison.description_ecs} />
              {maison.volume_ballon_ecs ? <Row label="Volume ballon" value={`${maison.volume_ballon_ecs} L`} /> : null}
            </Section>
          ) : null}

          {/* Ventilation / Climatisation */}
          {maison.type_ventilation || maison.energie_climatisation ? (
            <Section icon={<Wind className="h-4 w-4 text-sky-600" />} title="Ventilation & climatisation">
              {maison.type_ventilation ? (
                <Row label="Type ventilation" value={maison.type_ventilation + (maison.ventilation_recente ? " (post-2012)" : "")} />
              ) : null}
              {maison.energie_climatisation ? (
                <Row label="Climatisation" value={`${maison.energie_climatisation}${maison.surface_climatisee ? ` (${maison.surface_climatisee} m²)` : ""}`} />
              ) : null}
            </Section>
          ) : null}

          {/* Coûts */}
          <Section icon={<Wallet className="h-4 w-4 text-emerald-600" />} title="Coûts annuels estimés (€)">
            <Row label="Total 5 usages" value={fmtEuros(maison.cout_total)} strong />
            <Row label="Chauffage" value={fmtEuros(maison.cout_chauffage)} />
            <Row label="ECS" value={fmtEuros(maison.cout_ecs)} />
            <Row label="Éclairage" value={fmtEuros(maison.cout_eclairage)} />
            <Row label="Refroidissement" value={fmtEuros(maison.cout_refroidissement)} />
          </Section>

          {/* Déperditions */}
          {(maison.deperdition_murs || maison.deperdition_baies || maison.deperdition_plancher_bas || maison.deperdition_plancher_haut) ? (
            <Section icon={<Snowflake className="h-4 w-4 text-blue-500" />} title="Pertes thermiques (W/K)">
              <DeperditionRow label="Murs" value={maison.deperdition_murs} />
              <DeperditionRow label="Toiture / plafond" value={maison.deperdition_plancher_haut} />
              <DeperditionRow label="Plancher bas" value={maison.deperdition_plancher_bas} />
              <DeperditionRow label="Baies vitrées" value={maison.deperdition_baies} />
            </Section>
          ) : null}

          {/* Isolation */}
          <Section icon={<Shield className="h-4 w-4 text-blue-600" />} title="Qualité d'isolation">
            <IsolationRow label="Enveloppe" value={maison.isolation_enveloppe} />
            <IsolationRow label="Murs" value={maison.isolation_murs} />
            <IsolationRow label="Toiture" value={maison.isolation_toiture} />
            <IsolationRow label="Plancher bas" value={maison.isolation_plancher_bas} />
            <IsolationRow label="Menuiseries" value={maison.isolation_menuiseries} />
          </Section>
        </CardContent>
      </Card>

      {/* Enrichissement : contacts + notes + documents */}
      {prospect ? (
        <MaisonEnrichment prospectId={prospect.id} />
      ) : (
        <Card>
          <CardContent className="p-4 text-xs italic text-muted-foreground">
            💡 Ajoute d'abord ce {typeBatiment === "appartement" ? "logement" : "logement"} au
            pipeline (bouton plus haut) pour pouvoir saisir le propriétaire,
            l'occupant, des notes ou joindre des documents.
          </CardContent>
        </Card>
      )}
    </div>
  );
}

// === Sub-components ====================================================

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
    <section className="rounded-xl border border-border bg-secondary/20 p-3">
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
  strong,
}: {
  label: string;
  value: string | null;
  strong?: boolean;
}) {
  return (
    <div className="flex items-center justify-between gap-3 border-b border-border/30 py-1 text-xs last:border-0">
      <span className="shrink-0 text-muted-foreground">{label}</span>
      <span className={cn(strong ? "font-bold" : "", "text-right")}>{value ?? "—"}</span>
    </div>
  );
}

function DeperditionRow({ label, value }: { label: string; value: number | null }) {
  if (value == null) {
    return (
      <div className="flex items-center justify-between gap-3 border-b border-border/30 py-1 text-xs last:border-0">
        <span className="text-muted-foreground">{label}</span>
        <span className="text-muted-foreground">—</span>
      </div>
    );
  }
  const color = value < 50 ? "#1f9d55" : value < 150 ? "#cddc39" : value < 300 ? "#fb8c00" : "#e53935";
  const intensity = value < 50 ? "Faible" : value < 150 ? "Modérée" : value < 300 ? "Élevée" : "Très élevée";
  return (
    <div className="flex items-center justify-between gap-3 border-b border-border/30 py-1 text-xs last:border-0">
      <span className="text-muted-foreground">{label}</span>
      <span className="flex items-center gap-2">
        <span className="font-mono font-bold">{Math.round(value)} W/K</span>
        <span className="rounded-full px-2 py-0.5 text-[10px] font-bold text-white" style={{ background: color }}>
          {intensity}
        </span>
      </span>
    </div>
  );
}

function IsolationRow({ label, value }: { label: string; value: string | null }) {
  if (!value) {
    return (
      <div className="flex items-center justify-between gap-3 border-b border-border/30 py-1 text-xs last:border-0">
        <span className="text-muted-foreground">{label}</span>
        <span className="text-muted-foreground">—</span>
      </div>
    );
  }
  const norm = value.toLowerCase().normalize("NFD").replace(/[̀-ͯ]/g, "");
  let bg = "#94a3b8";
  for (const k of Object.keys(ISO_COLOR)) {
    if (norm.includes(k)) {
      bg = ISO_COLOR[k];
      break;
    }
  }
  return (
    <div className="flex items-center justify-between gap-3 border-b border-border/30 py-1 text-xs last:border-0">
      <span className="text-muted-foreground">{label}</span>
      <span
        className="inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-bold text-white"
        style={{ background: bg }}
      >
        {value}
      </span>
    </div>
  );
}

function Kpi({
  icon,
  label,
  value,
  unit,
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

function fmtEuros(v: number | null): string {
  if (v == null) return "—";
  return new Intl.NumberFormat("fr-FR", {
    style: "currency",
    currency: "EUR",
    maximumFractionDigits: 0,
  }).format(v);
}

function fmtDate(s: string | null): string {
  if (!s) return "—";
  const d = new Date(s);
  return Number.isNaN(d.getTime()) ? s : d.toLocaleDateString("fr-FR");
}
