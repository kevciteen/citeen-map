/**
 * Substitution de variables {{xxx}} dans les modèles d'email.
 *
 * Variables supportées (toutes optionnelles — laissées tel quel si absentes,
 * pour que l'utilisateur voie le placeholder et puisse le compléter à la main) :
 *
 *  Contexte copro / syndic :
 *   {{nom_copro}}   {{adresse}}   {{commune}}   {{code_postal}}
 *   {{syndic}}      {{nb_lots}}   {{classe_dpe}}   {{numero_immatriculation}}
 *
 *  Contexte destinataire (si on a un contact identifié) :
 *   {{prenom_destinataire}}   {{nom_destinataire}}   {{email_destinataire}}
 *
 *  Contexte expéditeur (utilisateur connecté) :
 *   {{mon_prenom}}   {{mon_nom}}   {{mon_email}}   {{mon_telephone}}
 *
 *  Lien fiche :
 *   {{lien_fiche_copro}}   {{lien_fiche_prospect}}
 */

export type RenderContext = {
  copro?: {
    nom_copro?: string | null;
    adresse?: string | null;
    commune?: string | null;
    code_postal?: string | null;
    syndic?: string | null;
    nb_lots?: number | null;
    classe_dpe?: string | null;
    numero_immatriculation?: string | null;
    id?: number;
  };
  destinataire?: {
    prenom?: string | null;
    nom?: string | null;
    email?: string | null;
  };
  expediteur?: {
    prenom?: string | null;
    nom?: string | null;
    email?: string | null;
    telephone?: string | null;
  };
  prospect?: { id?: number };
  baseUrl?: string;
};

export function buildVarTable(ctx: RenderContext): Record<string, string> {
  const c = ctx.copro ?? {};
  const d = ctx.destinataire ?? {};
  const e = ctx.expediteur ?? {};
  const base = ctx.baseUrl ?? "";
  return {
    nom_copro: c.nom_copro ?? "",
    adresse: c.adresse ?? "",
    commune: c.commune ?? "",
    code_postal: c.code_postal ?? "",
    syndic: c.syndic ?? "",
    nb_lots: c.nb_lots != null ? String(c.nb_lots) : "",
    classe_dpe: c.classe_dpe ?? "",
    numero_immatriculation: c.numero_immatriculation ?? "",
    prenom_destinataire: d.prenom ?? "",
    nom_destinataire: d.nom ?? "",
    email_destinataire: d.email ?? "",
    mon_prenom: e.prenom ?? "",
    mon_nom: e.nom ?? "",
    mon_email: e.email ?? "",
    mon_telephone: e.telephone ?? "",
    lien_fiche_copro: c.id ? `${base}/copros/${c.id}` : "",
    lien_fiche_prospect: ctx.prospect?.id ? `${base}/prospects/${ctx.prospect.id}` : "",
  };
}

/**
 * Substitue les {{var}} par leurs valeurs.
 * Si la valeur est vide, laisse le placeholder {{var}} pour que l'utilisateur
 * voie qu'il doit le compléter manuellement avant d'envoyer.
 */
export function renderTemplate(
  template: string,
  vars: Record<string, string>,
): string {
  return template.replace(/\{\{(\w+)\}\}/g, (match, key: string) => {
    const val = vars[key];
    return val && val.trim() !== "" ? val : match;
  });
}

/**
 * Construit une URL mailto: complète à partir d'un template + contexte.
 * Le destinataire (`to`) peut être null — l'utilisateur le complète après.
 */
export function buildMailto(opts: {
  to?: string | null;
  subject: string;
  body: string;
}): string {
  const params = new URLSearchParams();
  params.set("subject", opts.subject);
  params.set("body", opts.body);
  const to = opts.to ?? "";
  return `mailto:${encodeURIComponent(to)}?${params.toString()}`;
}
