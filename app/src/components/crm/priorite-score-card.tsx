"use client";
/**
 * Widget affichant le score de priorité d'une copro + le breakdown
 * (composants qui contribuent au score).
 *
 * Greffé sur la fiche copro (CoproFiche). Le score n'est PAS un dashboard :
 * c'est un outil d'action — il explicite POURQUOI relancer cette copro
 * maintenant pour qu'un commercial puisse l'argumenter au téléphone.
 */
import { useQuery } from "@tanstack/react-query";
import { Gauge, Info, Minus, Plus } from "lucide-react";
import { jsonFetcher } from "@/lib/fetcher";
import { Skeleton } from "@/components/ui/skeleton";

type ScoreData = {
  score: number;
  components: Array<{ label: string; value: number; reason: string }>;
  excluded: boolean;
  excludedReason?: string;
};

function scoreColor(score: number) {
  if (score >= 70) return { bg: "bg-rose-50", text: "text-rose-700", border: "border-rose-300", dot: "bg-rose-500" };
  if (score >= 40) return { bg: "bg-amber-50", text: "text-amber-700", border: "border-amber-300", dot: "bg-amber-500" };
  if (score >= 20) return { bg: "bg-sky-50", text: "text-sky-700", border: "border-sky-300", dot: "bg-sky-500" };
  return { bg: "bg-secondary/30", text: "text-muted-foreground", border: "border-border", dot: "bg-muted-foreground" };
}

function scoreLabel(score: number) {
  if (score >= 70) return "Priorité haute";
  if (score >= 40) return "Priorité moyenne";
  if (score >= 20) return "À surveiller";
  return "Faible priorité";
}

export function PrioriteScoreCard({ coproId }: { coproId: number }) {
  const { data, isPending } = useQuery({
    queryKey: ["copro-score", coproId],
    queryFn: ({ signal }) => jsonFetcher<ScoreData>(`/api/copros/${coproId}/score`, signal),
    staleTime: 2 * 60 * 1000,
  });

  if (isPending) return <Skeleton className="h-32 w-full" />;
  if (!data) return null;

  if (data.excluded) {
    return (
      <div className="rounded-lg border border-border bg-secondary/30 p-3 text-xs">
        <div className="mb-1 flex items-center gap-1.5 font-medium text-muted-foreground">
          <Gauge className="h-3.5 w-3.5" />
          Score de priorité
        </div>
        <p className="text-muted-foreground">{data.excludedReason}</p>
      </div>
    );
  }

  const color = scoreColor(data.score);

  return (
    <div className={`rounded-lg border ${color.border} ${color.bg} p-3 text-xs`}>
      <div className="mb-2 flex items-center justify-between gap-2">
        <div className="flex items-center gap-1.5 font-medium">
          <Gauge className={`h-3.5 w-3.5 ${color.text}`} />
          <span className={color.text}>Score de priorité</span>
        </div>
        <div className="flex items-baseline gap-1">
          <span className={`text-2xl font-semibold tabular-nums ${color.text}`}>
            {data.score}
          </span>
          <span className="text-[10px] text-muted-foreground">/ 100</span>
        </div>
      </div>

      <div className={`mb-2 text-[10px] font-medium uppercase tracking-wider ${color.text}`}>
        {scoreLabel(data.score)}
      </div>

      {data.components.length === 0 ? (
        <p className="text-[11px] text-muted-foreground">
          <Info className="mr-1 inline h-3 w-3" />
          Aucun critère d&apos;intérêt activé — copro standard.
        </p>
      ) : (
        <ul className="space-y-1">
          {data.components.map((c, i) => (
            <li key={i} className="flex items-start gap-2 text-[11px]">
              <span
                className={`mt-0.5 inline-flex h-4 min-w-[28px] items-center justify-center rounded px-1 font-mono text-[10px] font-semibold tabular-nums ${
                  c.value > 0
                    ? "bg-emerald-100 text-emerald-700"
                    : "bg-rose-100 text-rose-700"
                }`}
              >
                {c.value > 0 ? <Plus className="h-2.5 w-2.5" /> : <Minus className="h-2.5 w-2.5" />}
                {Math.abs(c.value)}
              </span>
              <div className="min-w-0 flex-1">
                <div className="font-medium text-foreground">{c.label}</div>
                <div className="text-muted-foreground">{c.reason}</div>
              </div>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
