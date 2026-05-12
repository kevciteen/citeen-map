import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

// Pipeline adapté au cycle rénovation copro :
// audit + ag_vote insérés entre proposal et won car c'est là que les deals
// copro stagnent 3-6 mois (audit énergétique → présentation en AG → vote).
const PIPELINE_STAGES = {
  lead: { label: "Lead", color: "#94a3b8", order: 0 },
  to_contact: { label: "À contacter", color: "#3b82f6", order: 1 },
  contacted: { label: "Contacté", color: "#8b5cf6", order: 2 },
  meeting: { label: "RDV", color: "#f59e0b", order: 3 },
  proposal: { label: "Proposition", color: "#06b6d4", order: 4 },
  audit: { label: "Audit énergétique", color: "#a855f7", order: 5 },
  ag_vote: { label: "AG vote", color: "#f97316", order: 6 },
  won: { label: "Signé", color: "#10b981", order: 7 },
  lost: { label: "Perdu", color: "#ef4444", order: 8 },
} as const;

export type PipelineStageKey = keyof typeof PIPELINE_STAGES;

export function stageMeta(key: string) {
  return PIPELINE_STAGES[key as PipelineStageKey] ?? PIPELINE_STAGES.lead;
}

export const PIPELINE_ORDER: PipelineStageKey[] = [
  "lead",
  "to_contact",
  "contacted",
  "meeting",
  "proposal",
  "audit",
  "ag_vote",
  "won",
  "lost",
];

export function formatCurrency(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "—";
  return new Intl.NumberFormat("fr-FR", {
    style: "currency",
    currency: "EUR",
    maximumFractionDigits: 0,
  }).format(value);
}

export function formatDate(d: Date | number | null | undefined): string {
  if (!d) return "—";
  const date = typeof d === "number" ? new Date(d * 1000) : d;
  return new Intl.DateTimeFormat("fr-FR", {
    day: "2-digit",
    month: "short",
    year: "numeric",
  }).format(date);
}

export function formatRelative(d: Date | number | null | undefined): string {
  if (!d) return "—";
  const date = typeof d === "number" ? new Date(d * 1000) : d;
  const diff = (date.getTime() - Date.now()) / 1000;
  const abs = Math.abs(diff);
  const rtf = new Intl.RelativeTimeFormat("fr-FR", { numeric: "auto" });
  if (abs < 60) return rtf.format(Math.round(diff), "second");
  if (abs < 3600) return rtf.format(Math.round(diff / 60), "minute");
  if (abs < 86400) return rtf.format(Math.round(diff / 3600), "hour");
  if (abs < 86400 * 30) return rtf.format(Math.round(diff / 86400), "day");
  if (abs < 86400 * 365)
    return rtf.format(Math.round(diff / (86400 * 30)), "month");
  return rtf.format(Math.round(diff / (86400 * 365)), "year");
}
