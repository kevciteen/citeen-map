import { cn } from "@/lib/utils";

const CLASSES = ["A", "B", "C", "D", "E", "F", "G"] as const;
type DpeClass = (typeof CLASSES)[number] | "NC" | "NA";

const DPE_BG: Record<string, string> = {
  A: "#1f9d55",
  B: "#7cb342",
  C: "#cddc39",
  D: "#ffeb3b",
  E: "#ffb300",
  F: "#fb8c00",
  G: "#e53935",
  NC: "#94a3b8",
  NA: "#94a3b8",
};
const DPE_FG: Record<string, string> = {
  A: "#ffffff",
  B: "#ffffff",
  C: "#1f2937",
  D: "#1f2937",
  E: "#ffffff",
  F: "#ffffff",
  G: "#ffffff",
  NC: "#ffffff",
  NA: "#ffffff",
};

export function DpeBadge({
  classe,
  size = "md",
  className,
}: {
  classe?: string | null;
  size?: "sm" | "md" | "lg";
  className?: string;
}) {
  const k = (String(classe ?? "NC").toUpperCase() || "NC") as DpeClass;
  const isKnown = (CLASSES as readonly string[]).includes(k);
  const cls = isKnown ? k : "NC";
  return (
    <span
      className={cn(
        "inline-flex items-center justify-center rounded font-bold",
        size === "sm" && "h-5 min-w-[20px] px-1 text-[10px]",
        size === "md" && "h-6 min-w-[24px] px-1.5 text-xs",
        size === "lg" && "h-9 min-w-[36px] px-2 text-base",
        className,
      )}
      style={{ background: DPE_BG[cls], color: DPE_FG[cls] }}
    >
      {cls === "NC" ? "—" : cls}
    </span>
  );
}

export function DpeScaleBar({ active }: { active?: string | null }) {
  const k = String(active ?? "").toUpperCase();
  return (
    <div className="flex items-center gap-0.5">
      {CLASSES.map((c) => {
        const on = k === c;
        return (
          <span
            key={c}
            className={cn(
              "flex h-7 flex-1 items-center justify-center text-[11px] font-bold transition-opacity",
              on ? "ring-2 ring-foreground/70 ring-offset-1" : "",
            )}
            style={{
              background: DPE_BG[c],
              color: DPE_FG[c],
              opacity: on ? 1 : 0.5,
            }}
          >
            {c}
          </span>
        );
      })}
    </div>
  );
}
