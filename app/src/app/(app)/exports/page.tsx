"use client";
import { useState } from "react";
import { Topbar } from "@/components/layout/topbar";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { DpeBadge } from "@/components/ui/dpe-badge";
import {
  Download,
  FileText,
  Loader2,
  Mail,
  Building2,
  Home,
  Info,
} from "lucide-react";
import { cn } from "@/lib/utils";

type Cible = "copros" | "maisons" | "both";

const STATIC_EXPORTS = [
  {
    title: "Prospects pipeline (CSV)",
    description:
      "Tous les prospects avec leurs informations copropriété, DPE, syndic, étape pipeline et valeur estimée.",
    href: "/api/export/prospects.csv",
    filename: "prospects-pipeline.csv",
  },
  {
    title: "Copros filtrées (XLSX)",
    description:
      "Export Excel premium des copropriétés correspondant à un filtre courant (avec mise en forme DPE).",
    href: "/api/export/copros-by-filter.xlsx",
    filename: "copros-filtrees.xlsx",
  },
];

const DPE_CLASSES = ["A", "B", "C", "D", "E", "F", "G"] as const;

export default function ExportsPage() {
  const [cible, setCible] = useState<Cible>("copros");
  const [cp, setCp] = useState("");
  const [commune, setCommune] = useState("");
  const [syndic, setSyndic] = useState("");
  const [dept, setDept] = useState("");
  const [dpeClasses, setDpeClasses] = useState<Set<string>>(new Set(["F", "G"]));
  const [resolveSyndic, setResolveSyndic] = useState(true);
  const [generating, setGenerating] = useState(false);

  const toggleClass = (c: string) => {
    const next = new Set(dpeClasses);
    if (next.has(c)) next.delete(c);
    else next.add(c);
    setDpeClasses(next);
  };

  const generate = async () => {
    setGenerating(true);
    try {
      const sp = new URLSearchParams();
      sp.set("cible", cible);
      if (cp.trim()) sp.set("cp", cp.trim());
      if (commune.trim()) sp.set("commune", commune.trim());
      if (syndic.trim()) sp.set("syndic", syndic.trim());
      if (dept.trim()) sp.set("dept", dept.trim());
      if (dpeClasses.size > 0) sp.set("dpe", [...dpeClasses].join(","));
      if (!resolveSyndic) sp.set("resolveSyndic", "0");

      const r = await fetch(`/api/export/campagne?${sp}`);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const blob = await r.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `campagne-mailing-${cible}-${new Date().toISOString().slice(0, 10)}.csv`;
      a.click();
      URL.revokeObjectURL(url);
    } finally {
      setGenerating(false);
    }
  };

  return (
    <div className="flex h-full flex-col">
      <Topbar
        title="Exports"
        subtitle="Données enrichies + campagnes de prospection prêtes à imprimer"
      />
      <div className="flex-1 overflow-y-auto bg-secondary/30 p-6">
        <div className="mx-auto max-w-5xl space-y-6">
          {/* ===== Campagne mailing ===== */}
          <Card>
            <CardHeader>
              <div className="flex items-center gap-3">
                <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-emerald-100 text-emerald-700">
                  <Mail className="h-5 w-5" />
                </div>
                <div>
                  <CardTitle className="text-base">Campagne mailing — générateur</CardTitle>
                  <CardDescription className="text-xs">
                    Génère un CSV publipostage (Word, Excel, La Poste) avec adresses
                    postales formatées prêtes à imprimer
                  </CardDescription>
                </div>
              </div>
            </CardHeader>
            <CardContent className="space-y-4">
              {/* Choix de la cible */}
              <div>
                <p className="mb-2 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                  Cible
                </p>
                <div className="grid grid-cols-3 gap-2">
                  <CibleBtn
                    active={cible === "copros"}
                    onClick={() => setCible("copros")}
                    icon={<Building2 className="h-4 w-4" />}
                    label="Copropriétés"
                    hint="adresse syndic"
                  />
                  <CibleBtn
                    active={cible === "maisons"}
                    onClick={() => setCible("maisons")}
                    icon={<Home className="h-4 w-4" />}
                    label="Maisons indiv."
                    hint="adresse logement"
                  />
                  <CibleBtn
                    active={cible === "both"}
                    onClick={() => setCible("both")}
                    icon={null}
                    label="Les deux"
                    hint="fichier unifié"
                  />
                </div>
              </div>

              {/* Filtres */}
              <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
                <Field label="Code postal">
                  <Input
                    value={cp}
                    onChange={(e) => setCp(e.target.value)}
                    placeholder="ex. 75011"
                    inputMode="numeric"
                  />
                </Field>
                {cible !== "copros" ? (
                  <Field label="Commune">
                    <Input
                      value={commune}
                      onChange={(e) => setCommune(e.target.value)}
                      placeholder="ex. Romainville"
                    />
                  </Field>
                ) : (
                  <Field label="Syndic (nom)">
                    <Input
                      value={syndic}
                      onChange={(e) => setSyndic(e.target.value)}
                      placeholder="ex. FONCIA"
                    />
                  </Field>
                )}
                <Field label="Département">
                  <Input
                    value={dept}
                    onChange={(e) => setDept(e.target.value)}
                    placeholder="ex. 75"
                    inputMode="numeric"
                  />
                </Field>
              </div>

              <div>
                <p className="mb-2 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                  Classes DPE cibles
                </p>
                <div className="flex flex-wrap gap-1.5">
                  {DPE_CLASSES.map((c) => (
                    <button
                      key={c}
                      onClick={() => toggleClass(c)}
                      className={cn(
                        "transition-transform",
                        dpeClasses.has(c)
                          ? "scale-110 ring-2 ring-foreground/70 ring-offset-1"
                          : "opacity-50 hover:opacity-100",
                      )}
                    >
                      <DpeBadge classe={c} size="sm" />
                    </button>
                  ))}
                </div>
              </div>

              {cible !== "maisons" ? (
                <div className="flex items-start gap-2 rounded-lg border border-blue-200 bg-blue-50 p-3">
                  <label className="flex cursor-pointer items-start gap-2 text-xs">
                    <input
                      type="checkbox"
                      checked={resolveSyndic}
                      onChange={(e) => setResolveSyndic(e.target.checked)}
                      className="mt-0.5 h-3.5 w-3.5"
                    />
                    <div>
                      <p className="font-semibold text-blue-900">
                        Résoudre l&apos;adresse du syndic via Sirene
                      </p>
                      <p className="mt-0.5 text-blue-800/80">
                        Récupère l&apos;adresse complète du siège (limite ~300 copros par
                        export, ~1 sec par syndic). Désactive si tu veux juste l&apos;adresse
                        de l&apos;immeuble.
                      </p>
                    </div>
                  </label>
                </div>
              ) : null}

              <div className="flex items-start gap-2 rounded-lg border border-amber-200 bg-amber-50 p-3 text-xs text-amber-900">
                <Info className="mt-0.5 h-3.5 w-3.5 shrink-0" />
                <p>
                  Pour les maisons, le destinataire est{" "}
                  <strong>&quot;Propriétaire occupant&quot;</strong> (le nom du propriétaire
                  n&apos;est pas accessible légalement via API publique). Pour les copros sans
                  syndic résolu, le mailing est adressé au{" "}
                  <strong>conseil syndical</strong> à l&apos;adresse de l&apos;immeuble.
                </p>
              </div>

              <Button
                onClick={generate}
                disabled={generating}
                size="lg"
                className="w-full"
              >
                {generating ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <Download className="h-4 w-4" />
                )}
                Générer le CSV de campagne
              </Button>
            </CardContent>
          </Card>

          {/* ===== Exports statiques ===== */}
          <div>
            <h2 className="mb-3 text-sm font-bold uppercase tracking-wider text-muted-foreground">
              Exports rapides
            </h2>
            <div className="grid gap-3 sm:grid-cols-2">
              {STATIC_EXPORTS.map((e) => (
                <Card key={e.href}>
                  <CardHeader>
                    <div className="flex items-center gap-3">
                      <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-primary/10 text-primary">
                        <FileText className="h-5 w-5" />
                      </div>
                      <div>
                        <CardTitle className="text-sm">{e.title}</CardTitle>
                        <CardDescription className="text-xs">{e.filename}</CardDescription>
                      </div>
                    </div>
                  </CardHeader>
                  <CardContent>
                    <p className="mb-3 text-xs text-muted-foreground">{e.description}</p>
                    <a
                      href={e.href}
                      className="inline-flex items-center gap-2 rounded-lg bg-primary px-3 py-1.5 text-xs font-semibold text-primary-foreground hover:bg-primary/90"
                    >
                      <Download className="h-3.5 w-3.5" />
                      Télécharger
                    </a>
                  </CardContent>
                </Card>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function CibleBtn({
  active,
  onClick,
  icon,
  label,
  hint,
}: {
  active: boolean;
  onClick: () => void;
  icon: React.ReactNode;
  label: string;
  hint: string;
}) {
  return (
    <button
      onClick={onClick}
      className={cn(
        "flex flex-col items-center gap-1 rounded-lg border p-3 text-sm font-medium transition-colors",
        active
          ? "border-primary bg-primary text-primary-foreground shadow-sm"
          : "border-border bg-background text-muted-foreground hover:bg-secondary",
      )}
    >
      <div className="flex items-center gap-1.5">
        {icon}
        {label}
      </div>
      <span className="text-[10px] opacity-80">{hint}</span>
    </button>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <label className="mb-1 block text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
        {label}
      </label>
      {children}
    </div>
  );
}
