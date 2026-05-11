"use client";
import { useEffect, useState } from "react";
import Link from "next/link";
import { Building2, Sparkles, Plus, Loader2 } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { DpeBadge } from "@/components/ui/dpe-badge";
import { Button } from "@/components/ui/button";
import { toast } from "sonner";

type Similar = {
  id: number;
  nom_copro: string | null;
  adresse: string | null;
  code_postal: string | null;
  commune: string | null;
  syndic: string | null;
  nb_lots_habitation: number | null;
  classe_finale: string | null;
  conso_moyenne: number | null;
  prospect_id: number | null;
};

export function SimilarsCard({ coproId }: { coproId: number }) {
  const [items, setItems] = useState<Similar[]>([]);
  const [loading, setLoading] = useState(true);
  const [adding, setAdding] = useState<number | null>(null);

  useEffect(() => {
    (async () => {
      setLoading(true);
      try {
        const r = await fetch(`/api/copros/${coproId}/similar?limit=6`);
        if (r.ok) {
          const j = await r.json();
          setItems(j.items ?? []);
        }
      } finally {
        setLoading(false);
      }
    })();
  }, [coproId]);

  const addToPipeline = async (id: number) => {
    setAdding(id);
    try {
      const r = await fetch("/api/prospects", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ coproId: id, stage: "to_contact" }),
      });
      if (r.ok || r.status === 409) {
        toast.success("Ajouté au pipeline");
        setItems((arr) =>
          arr.map((x) => (x.id === id ? { ...x, prospect_id: 1 } : x)),
        );
      } else toast.error("Erreur");
    } finally {
      setAdding(null);
    }
  };

  return (
    <Card className="print:hidden">
      <CardHeader>
        <div className="flex items-center gap-2">
          <Sparkles className="h-4 w-4 text-primary" />
          <CardTitle className="text-sm">
            Copros similaires à prospecter
          </CardTitle>
        </div>
        <p className="text-[11px] text-muted-foreground">
          Suggestion automatique : copros du même syndic, même période et/ou
          même classe DPE — bonnes candidates pour une campagne ciblée.
        </p>
      </CardHeader>
      <CardContent>
        {loading ? (
          <div className="flex h-20 items-center justify-center">
            <Loader2 className="h-5 w-5 animate-spin text-primary" />
          </div>
        ) : items.length === 0 ? (
          <p className="text-xs text-muted-foreground">
            Aucune copro similaire trouvée — soit le syndic ou la période ne
            permet pas de regrouper, soit toutes les similaires sont déjà dans
            votre pipeline.
          </p>
        ) : (
          <ul className="space-y-2">
            {items.map((s) => (
              <li
                key={s.id}
                className="flex items-center gap-3 rounded-lg border border-border p-2.5 hover:border-primary/40"
              >
                <Building2 className="h-4 w-4 shrink-0 text-primary" />
                <div className="min-w-0 flex-1">
                  <Link
                    href={`/copros/${s.id}`}
                    className="block truncate text-xs font-semibold hover:text-primary"
                  >
                    {s.nom_copro || s.adresse || `Copro #${s.id}`}
                  </Link>
                  <p className="truncate text-[10px] text-muted-foreground">
                    {s.code_postal} {s.commune} · {s.nb_lots_habitation ?? "?"} lots
                  </p>
                  {s.syndic ? (
                    <p className="truncate text-[10px] text-muted-foreground">
                      Syndic · {s.syndic}
                    </p>
                  ) : null}
                </div>
                <DpeBadge classe={s.classe_finale} size="sm" />
                {s.conso_moyenne ? (
                  <span className="text-[10px] text-muted-foreground">
                    {s.conso_moyenne}
                  </span>
                ) : null}
                {s.prospect_id ? (
                  <span className="text-[10px] font-semibold text-emerald-700">
                    ✓ pipeline
                  </span>
                ) : (
                  <Button
                    size="sm"
                    variant="ghost"
                    onClick={() => addToPipeline(s.id)}
                    disabled={adding === s.id}
                  >
                    {adding === s.id ? (
                      <Loader2 className="h-3 w-3 animate-spin" />
                    ) : (
                      <Plus className="h-3 w-3" />
                    )}
                  </Button>
                )}
              </li>
            ))}
          </ul>
        )}
      </CardContent>
    </Card>
  );
}
