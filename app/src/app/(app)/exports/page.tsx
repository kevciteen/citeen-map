"use client";
import { useState } from "react";
import { Topbar } from "@/components/layout/topbar";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { DpeBadge } from "@/components/ui/dpe-badge";
import {
  Download, FileText, Loader2, Sparkles, Info,
} from "lucide-react";
import { cn } from "@/lib/utils";

const DPE_CLASSES = ["A", "B", "C", "D", "E", "F", "G"] as const;
const PERIOD_OPTIONS = [
  "Avant 1949", "1949-1974", "1975-1989", "1990-2005", "Après 2005",
];

export default function ExportsPage() {
  const [cp, setCp] = useState("");
  const [commune, setCommune] = useState("");
  const [syndic, setSyndic] = useState("");
  const [dept, setDept] = useState("");
  const [minLots, setMinLots] = useState("");
  const [periode, setPeriode] = useState("");
  const [dpeClasses, setDpeClasses] = useState<Set<string>>(new Set(["F", "G"]));
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
      if (cp.trim()) sp.set("cp", cp.trim());
      if (commune.trim()) sp.set("commune", commune.trim());
      if (syndic.trim()) sp.set("syndic", syndic.trim());
      if (dept.trim()) sp.set("dept", dept.trim());
      if (minLots.trim()) sp.set("minLots", minLots.trim());
      if (periode) sp.set("periode", periode);
      if (dpeClasses.size > 0) sp.set("dpe", [...dpeClasses].join(","));

      const r = await fetch(`/api/export/copros-by-filter.xlsx?${sp}`);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const blob = await r.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `copros-${new Date().toISOString().slice(0, 10)}.xlsx`;
      a.click();
      URL.revokeObjectURL(url);
    } finally {
      setGenerating(false);
    }
  };

  return (
    <div className="flex h-full flex-col">
      <Topbar
        title="Exports data"
        subtitle="Exports quali — données enrichies et formatées pour analyse"
      />
      <div className="flex-1 overflow-y-auto bg-secondary/30 p-6">
        <div className="mx-auto max-w-5xl space-y-6">
          <div className="rounded-lg border border-primary/30 bg-primary/5 p-3 text-xs">
            <strong>Approche :</strong> chaque export est calibré pour un usage
            précis avec données enrichies (DPE estimés, syndic, lots, qualité du
            match). Pour exporter une seule fiche en PDF, utilise le bouton
            <strong> &quot;Imprimer&quot;</strong> directement sur la fiche.
          </div>

          {/* Export filtré XLSX quali */}
          <Card>
            <CardHeader>
              <div className="flex items-center gap-3">
                <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-emerald-100 text-emerald-700">
                  <Sparkles className="h-5 w-5" />
                </div>
                <div>
                  <CardTitle className="text-base">Copropriétés ciblées (Excel premium)</CardTitle>
                  <CardDescription className="text-xs">
                    Export XLSX avec coloriage DPE, qualité du match, syndic résolu,
                    lots + période + estimation. Limite 20 000 lignes.
                  </CardDescription>
                </div>
              </div>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
                <Field label="Code postal">
                  <Input value={cp} onChange={(e) => setCp(e.target.value)} placeholder="ex: 75011" inputMode="numeric" />
                </Field>
                <Field label="Commune (LIKE)">
                  <Input value={commune} onChange={(e) => setCommune(e.target.value)} placeholder="ex: Paris" />
                </Field>
                <Field label="Syndic (LIKE)">
                  <Input value={syndic} onChange={(e) => setSyndic(e.target.value)} placeholder="ex: FONCIA" />
                </Field>
                <Field label="Département">
                  <Input value={dept} onChange={(e) => setDept(e.target.value)} placeholder="ex: 93" />
                </Field>
                <Field label="Lots minimum">
                  <Input value={minLots} onChange={(e) => setMinLots(e.target.value)} placeholder="ex: 20" type="number" min="0" />
                </Field>
                <Field label="Période construction">
                  <select
                    value={periode}
                    onChange={(e) => setPeriode(e.target.value)}
                    className="h-9 w-full rounded-md border border-input bg-background px-2 text-sm"
                  >
                    <option value="">— toutes —</option>
                    {PERIOD_OPTIONS.map((p) => (
                      <option key={p} value={p}>{p}</option>
                    ))}
                  </select>
                </Field>
              </div>

              <div>
                <p className="mb-2 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                  Classes DPE
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
                  <button
                    onClick={() => setDpeClasses(new Set(["F", "G"]))}
                    className="ml-1 rounded-md border border-rose-300 bg-rose-50 px-2 py-1 text-xs font-semibold text-rose-900"
                  >
                    🎯 Passoires F+G
                  </button>
                </div>
              </div>

              <Button onClick={generate} disabled={generating} size="lg" className="w-full">
                {generating ? <Loader2 className="h-4 w-4 animate-spin" /> : <Download className="h-4 w-4" />}
                Générer l&apos;export XLSX
              </Button>
            </CardContent>
          </Card>

          {/* Exports complémentaires */}
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Exports complémentaires</CardTitle>
              <CardDescription className="text-xs">À utiliser ponctuellement</CardDescription>
            </CardHeader>
            <CardContent className="grid gap-3 sm:grid-cols-2">
              <ExportTile
                title="Prospects actifs (CSV)"
                description="Pipeline complet — copro/maison/tertiaire associés, étape, valeur estimée."
                href="/api/export/prospects.csv"
                filename="prospects-pipeline.csv"
              />
              <ExportTile
                title="Annuaire unifié (CSV)"
                description="Vue cross-entité — copros + sociétés + syndics avec contacts enrichis."
                href="/api/directory/export?limit=10000"
                filename="annuaire.csv"
              />
            </CardContent>
          </Card>

          <div className="flex items-start gap-2 rounded-lg border border-amber-200 bg-amber-50 p-3 text-xs text-amber-900">
            <Info className="mt-0.5 h-3.5 w-3.5 shrink-0" />
            <div>
              <p className="font-semibold">Tu cherches un export qui n&apos;est pas là ?</p>
              <p className="mt-1 opacity-90">
                Chaque fiche (copro, maison, appartement, tertiaire, DPE) a un bouton
                <strong> &quot;Imprimer&quot;</strong> en haut à droite qui génère un PDF
                propre à transmettre. C&apos;est souvent ce qu&apos;on veut pour une fiche
                unique. Les exports massifs ci-dessus sont pour les analyses en lot (Excel).
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
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

function ExportTile({
  title, description, href, filename,
}: {
  title: string;
  description: string;
  href: string;
  filename: string;
}) {
  return (
    <a
      href={href}
      download={filename}
      className="group flex items-start gap-3 rounded-lg border border-border bg-card p-3 transition-colors hover:border-primary/40 hover:bg-primary/5"
    >
      <div className="rounded-md bg-secondary/60 p-2 text-primary group-hover:bg-primary/20">
        <FileText className="h-4 w-4" />
      </div>
      <div className="min-w-0 flex-1">
        <p className="text-sm font-semibold">{title}</p>
        <p className="text-[11px] text-muted-foreground">{description}</p>
      </div>
      <Download className="h-3.5 w-3.5 text-muted-foreground" />
    </a>
  );
}
