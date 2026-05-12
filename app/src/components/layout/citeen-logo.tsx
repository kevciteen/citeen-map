import Image from "next/image";

/**
 * Logo Citeen — utilise le fichier déposé dans /public/citeen-logo.png
 * Le PNG fait ~5484×1646 (logo + wordmark côte à côte, ratio ~3.33:1).
 *
 * Deux variantes :
 *   - <CiteenLogo size={32} /> → mini icône carrée (fallback SVG si besoin
 *     d'une version compacte sans le texte "Citeen")
 *   - <CiteenLogoWordmark height={36} /> → logo complet (chaînons + texte),
 *     à utiliser en header sidebar et page login. Pas de texte "Citeen"
 *     additionnel à côté, c'est déjà dans le PNG.
 */
export function CiteenLogoWordmark({
  height = 36,
  className,
}: {
  height?: number;
  className?: string;
}) {
  // Ratio du PNG : 5484 / 1646 ≈ 3.331
  const width = Math.round(height * 3.331);
  return (
    <Image
      src="/citeen-logo.png"
      alt="Citeen"
      width={width}
      height={height}
      priority
      className={className}
      style={{ width: "auto", height }}
    />
  );
}

/**
 * Variante icône seule (chaînons verts) — utilisée quand on a besoin d'un
 * marker carré, ex. avatar de bell sans place pour le wordmark.
 * SVG inline → reste crisp à toute taille.
 */
export function CiteenLogo({
  size = 32,
  className,
}: {
  size?: number;
  className?: string;
}) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 64 64"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      className={className}
      aria-label="Citeen"
    >
      <path
        d="M22 12 a 18 18 0 1 0 0 40 a 10 10 0 0 1 0 -20 a 10 10 0 0 1 0 -20 z"
        fill="#7CC576"
      />
      <path
        d="M42 52 a 18 18 0 1 0 0 -40 a 10 10 0 0 1 0 20 a 10 10 0 0 1 0 20 z"
        fill="#7CC576"
      />
      <circle cx="32" cy="32" r="3" fill="white" opacity="0.6" />
    </svg>
  );
}
