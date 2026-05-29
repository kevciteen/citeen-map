import { cn } from "@/lib/utils";

/**
 * Bloc gris animé qui marque visuellement "ça charge" sans casser la mise
 * en page. Évite le flash de contenu vide pendant que TanStack Query
 * rafraîchit en arrière-plan.
 */
export function Skeleton({
  className,
  ...props
}: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={cn(
        "animate-pulse rounded-md bg-secondary/70",
        className,
      )}
      {...props}
    />
  );
}
