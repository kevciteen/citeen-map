/**
 * Logo Citeen — deux chaînons verts entrelacés.
 *
 * SVG inline pour rester crisp en toute taille et controlable via CSS.
 * Si tu veux remplacer par ton vrai fichier source : dépose-le dans
 * /public/citeen-logo.svg (ou .png) et utilise <Image> à la place ici.
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
      {/* Chaînon gauche (vert Citeen) */}
      <path
        d="M22 12 a 18 18 0 1 0 0 40 a 10 10 0 0 1 0 -20 a 10 10 0 0 1 0 -20 z"
        fill="#7CC576"
      />
      {/* Chaînon droit, en partie devant */}
      <path
        d="M42 52 a 18 18 0 1 0 0 -40 a 10 10 0 0 1 0 20 a 10 10 0 0 1 0 20 z"
        fill="#7CC576"
      />
      {/* Petit highlight pour le tressage */}
      <circle cx="32" cy="32" r="3" fill="white" opacity="0.6" />
    </svg>
  );
}

export function CiteenWordmark({ size = 26 }: { size?: number }) {
  return (
    <div className="flex items-center gap-2.5">
      <CiteenLogo size={size + 6} />
      <span
        className="font-black tracking-tight text-foreground"
        style={{ fontSize: size, lineHeight: 1 }}
      >
        Citeen
      </span>
    </div>
  );
}
