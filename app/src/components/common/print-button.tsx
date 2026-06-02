"use client";
/**
 * Bouton "Imprimer / PDF" universel — utilise window.print() qui ouvre
 * la dialog d'impression native du navigateur. L'utilisateur peut alors
 * choisir "Enregistrer en PDF" comme destination.
 *
 * Les fiches doivent avoir des classes `print:shadow-none print:border-0`
 * sur les Cards pour rendre proprement à l'impression. La sidebar et la
 * Topbar peuvent être masquées via une règle globale `@media print`.
 */
import { Printer } from "lucide-react";

export function PrintButton({
  label = "Imprimer / PDF",
  className = "",
}: {
  label?: string;
  className?: string;
}) {
  return (
    <button
      onClick={() => window.print()}
      className={
        "inline-flex items-center gap-1.5 rounded-md border border-border bg-card px-3 py-1.5 text-xs font-semibold text-foreground transition-colors hover:bg-secondary print:hidden " +
        className
      }
      title="Ouvre la dialog d'impression — choisis 'Enregistrer en PDF' comme destination"
    >
      <Printer className="h-3.5 w-3.5" />
      {label}
    </button>
  );
}
