/**
 * Détection des "postes de travaux CEE éligibles" depuis les champs DPE ADEME.
 *
 * On ne calcule PAS de montant. On liste les fiches CEE applicables à un
 * logement donné en se basant sur les indicateurs DPE qu'on a déjà :
 *   - qualité d'isolation (murs, toiture, plancher bas, menuiseries)
 *   - énergie principale de chauffage (fioul/gaz/électricité…)
 *   - type de ventilation
 *   - classe DPE globale
 *   - année de construction (proxy pour combler les trous de qualité d'isolation)
 *
 * Cas d'usage : "pour cette maison G chauffée fioul 1975 avec isolation
 * murs mauvaise → 3 postes éligibles : remplacement chaudière fioul → PAC,
 * isolation des murs par l'extérieur, isolation des combles."
 *
 * Note : le statut "pertinent" est indicatif. Un audit énergétique reste
 * indispensable avant tout engagement de travaux.
 */

export type PosteStatus = "pertinent" | "à confirmer" | "exclu";

export type PosteFamille =
  | "Isolation"
  | "Chauffage"
  | "Eau chaude"
  | "Ventilation"
  | "Bouquet";

export interface Poste {
  code: string;            // ex: "BAR-EN-101"
  titre: string;           // ex: "Isolation des combles perdus"
  famille: PosteFamille;
  status: PosteStatus;
  /** Raisons détectées dans le DPE qui rendent ce poste pertinent. */
  motifs: string[];
  /** Lien vers la fiche officielle MTE. */
  sourceUrl?: string;
}

export interface PosteDetectionInput {
  typeBatiment: "maison" | "appartement";
  classeDpe: string | null;
  anneeConstruction: number | null;
  // Qualité d'isolation (chaînes ADEME : "insuffisante" / "moyenne" / "bonne" / "très bonne" / null)
  isolationMurs: string | null;
  isolationToiture: string | null;
  isolationPlancherBas: string | null;
  isolationMenuiseries: string | null;
  // Chauffage
  energiePrincipaleChauffage: string | null;
  generateurChauffage: string | null;
  // Eau chaude
  generateurEcs: string | null;
  // Ventilation
  typeVentilation: string | null;
}

// === Helpers de normalisation / matching =================================

function normalize(s: string | null | undefined): string {
  return (s ?? "")
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "")
    .toLowerCase()
    .trim();
}

function isolationIsWeak(value: string | null): boolean {
  const v = normalize(value);
  if (!v) return false;
  return (
    v.includes("insuffisante") ||
    v.includes("tres mauvaise") ||
    v.includes("mauvaise") ||
    v.includes("moyenne")
  );
}

function isolationIsUnknown(value: string | null): boolean {
  return !value || value.trim() === "";
}

function isFossilFuel(energy: string | null): boolean {
  const e = normalize(energy);
  if (!e) return false;
  return (
    e.includes("fioul") ||
    e.includes("fuel") ||
    e.includes("gaz") ||
    e.includes("charbon") ||
    e.includes("propane") ||
    e.includes("gpl")
  );
}

function isElectricDirect(energy: string | null, generateur: string | null): boolean {
  const e = normalize(energy);
  const g = normalize(generateur);
  if (!e.includes("electricite") && !e.includes("electrique")) return false;
  // Si générateur PAC ou solaire → pas "direct"
  if (g.includes("pompe a chaleur") || g.includes("pac") || g.includes("solaire")) return false;
  return true;
}

function ventilationIsAbsentOrBasic(typeVentilation: string | null): boolean {
  const v = normalize(typeVentilation);
  if (!v) return true;
  if (v.includes("vmc double") || v.includes("hygro b")) return false;
  // VMC simple flux autoréglable / hygro A / naturelle / aération haute et basse → travaux possibles
  return true;
}

function isOldBuilding(annee: number | null): boolean {
  return annee != null && annee < 1975;
}

// === Catalogue des postes (résidentiel, prospection rénovation) ==========

export function getPostesEligibles(input: PosteDetectionInput): Poste[] {
  const postes: Poste[] = [];
  const dpe = (input.classeDpe ?? "").toUpperCase();
  const isPassoire = dpe === "F" || dpe === "G";
  const isMauvais = dpe === "D" || dpe === "E";

  // --- BAR-EN-101 — Isolation combles perdus ----------------------------
  {
    const motifs: string[] = [];
    if (isolationIsWeak(input.isolationToiture)) {
      motifs.push(`Isolation toiture évaluée "${input.isolationToiture}"`);
    } else if (isolationIsUnknown(input.isolationToiture) && isOldBuilding(input.anneeConstruction)) {
      motifs.push("Construction antérieure à 1975 — isolation toiture probablement insuffisante");
    }
    if (motifs.length > 0) {
      postes.push({
        code: "BAR-EN-101",
        titre: "Isolation des combles perdus",
        famille: "Isolation",
        status: isolationIsWeak(input.isolationToiture) ? "pertinent" : "à confirmer",
        motifs,
        sourceUrl: "https://www.ecologie.gouv.fr/fiches-doperations-standardisees-pour-les-operations-deconomies-denergie",
      });
    }
  }

  // --- BAR-EN-103 — Isolation rampants / plancher bas -------------------
  {
    const motifs: string[] = [];
    if (isolationIsWeak(input.isolationPlancherBas)) {
      motifs.push(`Isolation plancher bas évaluée "${input.isolationPlancherBas}"`);
    } else if (isolationIsUnknown(input.isolationPlancherBas) && isOldBuilding(input.anneeConstruction)) {
      motifs.push("Construction antérieure à 1975 — plancher bas probablement non isolé");
    }
    if (motifs.length > 0) {
      postes.push({
        code: "BAR-EN-103",
        titre: "Isolation des planchers bas",
        famille: "Isolation",
        status: isolationIsWeak(input.isolationPlancherBas) ? "pertinent" : "à confirmer",
        motifs,
      });
    }
  }

  // --- BAR-EN-102 — Isolation murs (ITI ou ITE) -------------------------
  {
    const motifs: string[] = [];
    if (isolationIsWeak(input.isolationMurs)) {
      motifs.push(`Isolation murs évaluée "${input.isolationMurs}"`);
    } else if (isolationIsUnknown(input.isolationMurs) && isOldBuilding(input.anneeConstruction)) {
      motifs.push("Construction antérieure à 1975 — murs probablement non isolés");
    }
    if (motifs.length > 0) {
      postes.push({
        code: "BAR-EN-102",
        titre:
          input.typeBatiment === "maison"
            ? "Isolation des murs (ITE ou ITI)"
            : "Isolation des murs (ITI prioritaire en copro)",
        famille: "Isolation",
        status: isolationIsWeak(input.isolationMurs) ? "pertinent" : "à confirmer",
        motifs,
      });
    }
  }

  // --- BAR-EN-104 — Fenêtres double vitrage performant ------------------
  {
    const motifs: string[] = [];
    if (isolationIsWeak(input.isolationMenuiseries)) {
      motifs.push(`Menuiseries évaluées "${input.isolationMenuiseries}"`);
    } else if (isolationIsUnknown(input.isolationMenuiseries) && isOldBuilding(input.anneeConstruction)) {
      motifs.push("Construction antérieure à 1975 — menuiseries probablement simple vitrage");
    }
    if (motifs.length > 0) {
      postes.push({
        code: "BAR-EN-104",
        titre: "Remplacement fenêtres / vitrages performants",
        famille: "Isolation",
        status: isolationIsWeak(input.isolationMenuiseries) ? "pertinent" : "à confirmer",
        motifs,
      });
    }
  }

  // --- BAR-TH-129 — Pompe à chaleur air/eau -----------------------------
  if (isFossilFuel(input.energiePrincipaleChauffage)) {
    postes.push({
      code: "BAR-TH-129",
      titre: "Remplacement chaudière par pompe à chaleur air/eau",
      famille: "Chauffage",
      status: "pertinent",
      motifs: [
        `Chauffage actuel : ${input.energiePrincipaleChauffage} — substitution fortement valorisée en CEE`,
      ],
    });

    // --- BAR-TH-112 — Chaudière biomasse (alternative à la PAC)
    postes.push({
      code: "BAR-TH-112",
      titre: "Chaudière biomasse haute performance",
      famille: "Chauffage",
      status: "à confirmer",
      motifs: [
        "Alternative à la PAC pour les maisons sans terrain pour un groupe extérieur, ou en zone H1 froide.",
      ],
    });
  }

  // --- BAR-TH-148 — Chauffe-eau thermodynamique --------------------------
  {
    const energy = normalize(input.energiePrincipaleChauffage);
    const ecsGen = normalize(input.generateurEcs);
    // Pertinent si : ECS électrique direct, OU chauffage électrique direct
    const ecsElectricDirect =
      ecsGen.includes("ballon electrique") ||
      ecsGen.includes("cumulus") ||
      (ecsGen.includes("electrique") && !ecsGen.includes("thermo"));
    const chauffageElec =
      energy.includes("electricite") || energy.includes("electrique");
    const motifs: string[] = [];
    if (ecsElectricDirect) motifs.push(`ECS actuelle : ${input.generateurEcs}`);
    else if (chauffageElec && !ecsGen)
      motifs.push("Chauffage électrique direct — ECS souvent en ballon électrique classique");
    if (motifs.length > 0) {
      postes.push({
        code: "BAR-TH-148",
        titre: "Chauffe-eau thermodynamique (CET)",
        famille: "Eau chaude",
        status: ecsElectricDirect ? "pertinent" : "à confirmer",
        motifs,
      });
    }
  }

  // --- BAR-TH-127 / 125 — VMC double flux --------------------------------
  if (ventilationIsAbsentOrBasic(input.typeVentilation)) {
    postes.push({
      code: input.typeBatiment === "maison" ? "BAR-TH-125" : "BAR-TH-127",
      titre: "VMC double flux haute performance",
      famille: "Ventilation",
      status: input.typeVentilation ? "à confirmer" : "à confirmer",
      motifs: [
        input.typeVentilation
          ? `Ventilation actuelle : ${input.typeVentilation} — gain potentiel`
          : "Aucune VMC double flux détectée dans le DPE",
      ],
    });
  }

  // --- BAR-TH-174 / 175 — Rénovation d'ampleur (bouquet) -----------------
  if (isPassoire) {
    const code = input.typeBatiment === "maison" ? "BAR-TH-174" : "BAR-TH-175";
    postes.push({
      code,
      titre: `Rénovation d'ampleur ${input.typeBatiment === "maison" ? "maison individuelle" : "appartement"}`,
      famille: "Bouquet",
      status: "pertinent",
      motifs: [
        `Classe DPE actuelle : ${dpe} (passoire thermique)`,
        "Bouquet d'au moins 2 sauts de classe — valorisation CEE majeure + Coup de pouce ×2 pour ménages modestes",
      ],
      sourceUrl:
        input.typeBatiment === "maison"
          ? "https://www.ecologie.gouv.fr/sites/default/files/documents/BAR-TH-174%20vA80-3%20%C3%A0%20compter%20du%2017-01-2026.pdf"
          : "https://www.ecologie.gouv.fr/sites/default/files/documents/BAR-TH-175%20vA80-3%20%C3%A0%20compter%20du%2017-01-2026.pdf",
    });
  } else if (isMauvais) {
    const code = input.typeBatiment === "maison" ? "BAR-TH-174" : "BAR-TH-175";
    postes.push({
      code,
      titre: `Rénovation d'ampleur ${input.typeBatiment === "maison" ? "maison individuelle" : "appartement"}`,
      famille: "Bouquet",
      status: "à confirmer",
      motifs: [
        `Classe DPE actuelle : ${dpe} — 2 sauts de classe possibles mais à vérifier sur audit`,
      ],
    });
  }

  return postes;
}
