/**
 * Moteur CEE — API publique.
 *
 * Port partiel du simulateur interne (équipe Citeen). Pour le MVP de
 * prospection, on porte uniquement les **fiches Rénovation d'ampleur**
 * BAR-TH-174 (maison) et BAR-TH-175 (appartement) qui couvrent à elles
 * seules ~80% du cas d'usage : "DPE F/G → cibler une rénovation globale
 * pour passer en C/D, calculer la prime CEE potentielle".
 *
 * Les autres fiches (isolation seule, PAC seule, etc.) seront portées dans
 * des itérations suivantes.
 *
 * Référence : ` simulateur-cee-main/js/evaluators-residential.js`,
 *  fonctions `evaluateBarTh174` / `evaluateBarTh175` / `evaluateBarTh174Like`.
 */
import {
  applyCommonRules,
  baseEvaluation,
  buildCalculationLabel,
  finalizeStatus,
  getEstimatedPrimeEuros,
  getEffectiveMwhCumacPrice,
  getIncomeCategoryMeta,
  toNumber,
} from "./core";
import {
  COEFF_174_175,
  RESOURCE_CATEGORY_KEYS,
  STATUS,
} from "./config";
import type {
  Action,
  CoupDePouceInfo,
  Evaluation,
  Project,
} from "./types";

// === Évaluateur BAR-TH-174 / 175 (Rénovation d'ampleur) =================

interface Bar174LikeOptions {
  supportedHousingTypes: ("Maison individuelle" | "Appartement")[];
}

function getFirstStageAmount(classJumpCount: string | undefined): number | null {
  if (classJumpCount === "1") return 0;
  if (!classJumpCount) return null;
  return COEFF_174_175.classJump[classJumpCount] ?? null;
}

function applyCoupDePouceRenovationAmpleur(
  evaluation: Evaluation,
  project: Project,
  action: Action,
): void {
  const standardKwhCumac = evaluation.kwhCumac;
  if (standardKwhCumac == null) return;
  const standardEuroAmount = getEstimatedPrimeEuros(
    standardKwhCumac,
    getEffectiveMwhCumacPrice(project),
  );
  const incomeCategory = getIncomeCategoryMeta(project.incomeBracket)?.key;
  const coupDePouce: CoupDePouceInfo & {
    standardKwhCumac: number;
    standardEuroAmount: number | null;
    kwhCumac: number | null;
    euroAmount: number | null;
  } = {
    name: "Coup de pouce Renovation d'ampleur",
    status: "Eligibilite a confirmer",
    factor: 2,
    notes: [
      "Pour les operations engagees a compter du 17/01/2026, le Coup de pouce multiplie par 2 le volume total de CEE.",
      "Le logement ne doit pas etre une residence secondaire.",
      "Le projet ne doit pas relever d'un parcours Anah / MaPrimeRenov' renovation d'ampleur.",
      "L'offre d'un signataire doit etre acceptee avant signature du devis.",
    ],
    standardKwhCumac,
    standardEuroAmount,
    kwhCumac: null,
    euroAmount: null,
  };

  if (action.secondaryResidence === "Oui") {
    coupDePouce.status = "Non eligible";
    coupDePouce.notes.unshift(
      "Le Coup de pouce Renovation d'ampleur n'est pas ouvert aux residences secondaires.",
    );
    evaluation.coupDePouce = coupDePouce;
    return;
  }
  if (action.anahRenovationRoute === "Oui") {
    coupDePouce.status = "Non eligible";
    coupDePouce.notes.unshift(
      "Le projet releve d'un parcours Anah / MaPrimeRenov' qui valorise directement les CEE.",
    );
    evaluation.coupDePouce = coupDePouce;
    return;
  }

  if (
    incomeCategory &&
    incomeCategory !== RESOURCE_CATEGORY_KEYS.VERY_MODEST &&
    incomeCategory !== RESOURCE_CATEGORY_KEYS.MODEST
  ) {
    coupDePouce.status = "Non eligible";
    coupDePouce.notes.unshift(
      "Le Coup de pouce Renovation d'ampleur s'applique uniquement aux menages Tres modestes et Modestes.",
    );
    evaluation.coupDePouce = coupDePouce;
    return;
  }

  // Si éligible : on calcule le bonus = standardKwhCumac × factor
  if (
    incomeCategory === RESOURCE_CATEGORY_KEYS.VERY_MODEST ||
    incomeCategory === RESOURCE_CATEGORY_KEYS.MODEST
  ) {
    const bonusKwh = standardKwhCumac * coupDePouce.factor;
    const bonusEuros = getEstimatedPrimeEuros(
      bonusKwh,
      getEffectiveMwhCumacPrice(project),
    );
    coupDePouce.kwhCumac = bonusKwh;
    coupDePouce.euroAmount = bonusEuros;
    coupDePouce.status = "Eligible";
    // Le standard est remplacé par le bonus quand éligible
    evaluation.standardKwhCumac = standardKwhCumac;
    evaluation.standardEuroAmount = standardEuroAmount;
    evaluation.kwhCumac = bonusKwh;
    evaluation.euroAmount = bonusEuros;
    evaluation.standardCalculationLabel = evaluation.calculationLabel;
    evaluation.calculationLabel = `${evaluation.calculationLabel} → ×${coupDePouce.factor} Coup de pouce`;
  }
  evaluation.coupDePouce = coupDePouce;
}

function evaluateBarTh174Like(
  code: string,
  label: string,
  project: Project,
  action: Action,
  enabled: boolean,
  options: Bar174LikeOptions,
): Evaluation {
  const e = baseEvaluation(code, label, enabled);
  applyCommonRules(e, project, {
    supportedBuildingTypes: ["Habitation"],
    needsSector: false,
    needsHousingType: true,
    supportedHousingTypes: options.supportedHousingTypes,
  });
  const commonMissingCount = e.missing.length;

  e.notes.push(
    "Rénovation d'ampleur : minimum 2 sauts de classe énergétique requis sur le DPE.",
    "Bâtiment occupé depuis plus de 2 ans en résidence principale.",
    "Bouquet de travaux à valider par un Accompagnateur Rénov' / mandataire CEE.",
  );

  const shab = toNumber(project.buildingSurface);
  if (!shab) {
    e.missing.push(
      enabled
        ? "Renseigner la surface du logement dans les donnees projet."
        : "",
    );
  }
  const classJumpCount = action.classJumpCount as string | undefined;
  if (!classJumpCount) {
    e.missing.push(
      enabled ? "Preciser le nombre de sauts de classe energetique." : "",
    );
  }
  const totalAmount = classJumpCount
    ? COEFF_174_175.classJump[classJumpCount]
    : undefined;
  if (classJumpCount && totalAmount == null) {
    e.missing.push(
      enabled
        ? "La tranche de sauts de classe selectionnee n'est pas exploitable pour le calcul."
        : "",
    );
  }

  const priorStageDone = (action.priorWorkStageDone as string) || "Non";
  if (priorStageDone === "Oui" && !action.firstStageClassJumpCount) {
    e.missing.push(
      enabled
        ? "Preciser le nombre de sauts de classe de la premiere etape."
        : "",
    );
  }

  if (
    enabled &&
    !e.blockers.length &&
    !e.missing.filter(Boolean).length &&
    shab &&
    totalAmount != null
  ) {
    const surfaceFactor = COEFF_174_175.surfaceFactor(shab);

    if (priorStageDone === "Oui") {
      const firstStageAmount = getFirstStageAmount(
        action.firstStageClassJumpCount as string | undefined,
      );
      if (firstStageAmount == null) {
        e.missing.push(
          "Preciser le nombre de sauts de classe de la premiere etape.",
        );
      } else if (firstStageAmount >= totalAmount) {
        e.blockers.push(
          "Le nombre total de sauts de classe doit etre superieur a celui de la premiere etape.",
        );
      } else {
        e.kwhCumac = (totalAmount - firstStageAmount) * surfaceFactor;
        e.calculationLabel = buildCalculationLabel(
          [totalAmount - firstStageAmount, surfaceFactor],
          e.kwhCumac,
        );
      }
    } else {
      e.kwhCumac = totalAmount * surfaceFactor;
      e.calculationLabel = buildCalculationLabel(
        [totalAmount, surfaceFactor],
        e.kwhCumac,
      );
    }
  }

  if (!e.blockers.length && e.kwhCumac != null) {
    applyCoupDePouceRenovationAmpleur(e, project, action);
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluateBarTh174(
  project: Project,
  action: Action,
  enabled: boolean,
): Evaluation {
  return evaluateBarTh174Like(
    "BAR-TH-174",
    "Rénovation d'ampleur d'une maison individuelle",
    project,
    action,
    enabled,
    { supportedHousingTypes: ["Maison individuelle"] },
  );
}

export function evaluateBarTh175(
  project: Project,
  action: Action,
  enabled: boolean,
): Evaluation {
  return evaluateBarTh174Like(
    "BAR-TH-175",
    "Rénovation d'ampleur d'un appartement",
    project,
    action,
    enabled,
    { supportedHousingTypes: ["Appartement"] },
  );
}

// === API "Quick Estimate" pour la prospection ============================

/**
 * Entrée simplifiée pour l'estimation rapide depuis une fiche maison/appart.
 * On part du DPE ADEME et de quelques infos basiques pour produire une
 * fourchette de prime CEE attendue en rénovation d'ampleur.
 */
export interface QuickEstimateInput {
  housingType: "Maison individuelle" | "Appartement";
  surface: number; // m² habitable
  postalCode: string;
  constructionYear: number;
  classeDpe: "A" | "B" | "C" | "D" | "E" | "F" | "G" | "NC";
  /** Si fourni, on génère deux scénarios (modeste et standard). Sinon, standard seulement. */
  assumedIncomeCategory?: "veryModest" | "modest" | "intermediate" | "high";
  /** Prix CEE en €/MWh cumac (défaut : marché 2026 ~7€). */
  mwhCumacPrice?: number;
  mwhCumacPricePrecarious?: number;
}

export interface QuickEstimateScenario {
  /** Label : "Saut 2 classes (vers la classe X)" */
  label: string;
  classJumpCount: "2" | "3" | "4 ou plus";
  evaluationStandard: Evaluation; // sans Coup de pouce
  evaluationCoupDePouce: Evaluation; // avec Coup de pouce (revenus modestes)
}

export interface QuickEstimateResult {
  applicable: boolean;
  reason?: string;
  housingType: "Maison individuelle" | "Appartement";
  scenarios: QuickEstimateScenario[];
}

/** Mappe une classe DPE à la classe cible "réaliste" après 2-4 sauts. */
function targetClassAfterJumps(
  current: string,
  jumps: number,
): string | null {
  const order = ["A", "B", "C", "D", "E", "F", "G"];
  const idx = order.indexOf(current.toUpperCase());
  if (idx < 0) return null;
  const target = Math.max(0, idx - jumps);
  return order[target];
}

/**
 * Produit une estimation rapide en simulant un saut de 2 / 3 / 4+ classes.
 * Évalue chaque scénario standard et avec Coup de pouce.
 *
 * Bouchons :
 *   - On suppose toujours `secondaryResidence = "Non"` (résidence principale)
 *   - On suppose `anahRenovationRoute = "Non"` (pas de doublon avec MAR/MPR ampleur)
 *   - Le prix CEE est paramétrable, défaut conservateur : 7 €/MWh cumac
 *     (marché 2026) ; précaire : 9 €/MWh cumac.
 */
export function quickEstimate(input: QuickEstimateInput): QuickEstimateResult {
  const housingType = input.housingType;
  const dpe = (input.classeDpe || "NC").toUpperCase();

  // Une rénovation d'ampleur n'a de sens que sur des DPE D/E/F/G
  // (les A/B/C n'ont quasi rien à gagner, et la fiche exige 2 sauts).
  if (!["D", "E", "F", "G"].includes(dpe)) {
    return {
      applicable: false,
      reason:
        dpe === "NC"
          ? "Classe DPE non renseignée. Rénovation d'ampleur non simulable."
          : `Classe DPE ${dpe} : la rénovation d'ampleur cible surtout les passoires thermiques (D à G).`,
      housingType,
      scenarios: [],
    };
  }

  const project: Project = {
    buildingType: "Habitation",
    postalCode: input.postalCode,
    constructionYear: input.constructionYear,
    buildingSurface: input.surface,
    housingType,
    mwhCumacPrice: input.mwhCumacPrice ?? 7,
    mwhCumacPricePrecarious: input.mwhCumacPricePrecarious ?? 9,
  };

  const scenarios: QuickEstimateScenario[] = [];
  const jumpDefs: { count: "2" | "3" | "4 ou plus"; numericJumps: number }[] = [
    { count: "2", numericJumps: 2 },
    { count: "3", numericJumps: 3 },
    { count: "4 ou plus", numericJumps: 4 },
  ];

  for (const jd of jumpDefs) {
    const target = targetClassAfterJumps(dpe, jd.numericJumps);
    if (!target) continue;
    // Si la cible est meilleure que A, on saute (pas réaliste depuis D/E)
    if (target === "A" && jd.numericJumps > 4) continue;

    const action: Action = {
      classJumpCount: jd.count,
      priorWorkStageDone: "Non",
      secondaryResidence: "Non",
      anahRenovationRoute: "Non",
    };

    // Standard (sans Coup de pouce — revenus intermédiaires/supérieurs)
    const standardProject = { ...project, incomeBracket: "intermediate" as const };
    const evalStd =
      housingType === "Maison individuelle"
        ? evaluateBarTh174(standardProject, action, true)
        : evaluateBarTh175(standardProject, action, true);

    // Modeste (avec Coup de pouce — multiplie kWh cumac par 2)
    const modestProject = { ...project, incomeBracket: "modest" as const };
    const evalModest =
      housingType === "Maison individuelle"
        ? evaluateBarTh174(modestProject, action, true)
        : evaluateBarTh175(modestProject, action, true);

    scenarios.push({
      label: `${jd.count} saut${jd.count === "2" ? "s" : "s"} de classe (${dpe} → ${target})`,
      classJumpCount: jd.count,
      evaluationStandard: evalStd,
      evaluationCoupDePouce: evalModest,
    });
  }

  return {
    applicable: scenarios.length > 0,
    housingType,
    scenarios,
  };
}

// === API "Full Estimate" — évalue toutes les fiches du catalogue ========

import { SHEETS, type SheetDefinition } from "./sheets";
import { STATUS_PRIORITY } from "./config";
import type { BuildingType } from "./types";

export interface FullEvaluateOptions {
  /** Limite l'évaluation aux fiches de ce type. Si non précisé, on infère depuis project.buildingType. */
  buildingTypes?: BuildingType[];
  /** Actions par code de fiche. Si non fourni, on évalue en mode "discovery" (action = {}). */
  actions?: Record<string, Action>;
  /** Liste de codes à activer (enabled=true). Par défaut : tous activés. */
  enabledCodes?: string[];
}

export interface FullEvaluateResult {
  evaluation: Evaluation;
  sheet: SheetDefinition;
}

/**
 * Évalue toutes les fiches CEE pertinentes pour un projet donné.
 *
 * Mode "discovery" (par défaut) : action vide pour chaque fiche → on identifie
 * lesquelles sont applicables (status Eligible ou À confirmer) et lesquelles
 * sont bloquantes (Non éligible).
 *
 * Mode "estimation" : on peuple `actions[code]` avec des hypothèses raisonnables
 * (ex: surface, classJumpCount) → le moteur calcule kWh cumac + €.
 *
 * Résultats triés par pertinence (Eligible → Confirm → Potential → Ineligible)
 * puis par kWh cumac décroissant.
 */
export function evaluateAllSheets(
  project: Project,
  options: FullEvaluateOptions = {},
): FullEvaluateResult[] {
  const buildingTypes =
    options.buildingTypes ??
    (project.buildingType ? [project.buildingType as BuildingType] : ["Habitation", "Tertiaire"]);

  const allCodes = Object.keys(SHEETS).filter((code) => {
    const sheetTypes = SHEETS[code].buildingTypes;
    return sheetTypes.some((t) => buildingTypes.includes(t));
  });

  const enabledSet = options.enabledCodes
    ? new Set(options.enabledCodes)
    : null;

  const results: FullEvaluateResult[] = [];
  for (const code of allCodes) {
    const sheet = SHEETS[code];
    const action = options.actions?.[code] ?? {};
    const enabled = enabledSet ? enabledSet.has(code) : true;
    try {
      const evaluation = sheet.evaluate(project, action, enabled);
      results.push({ evaluation, sheet });
    } catch {
      // Une fiche qui crashe sur un input incomplet → on l'ignore silencieusement
      // (l'utilisateur n'a probablement pas rempli les bons champs pour celle-ci)
    }
  }

  // Tri : Eligible → Confirm → Potential → Ineligible, puis kWh cumac décroissant
  results.sort((a, b) => {
    const pa = STATUS_PRIORITY[a.evaluation.status] ?? 99;
    const pb = STATUS_PRIORITY[b.evaluation.status] ?? 99;
    if (pa !== pb) return pa - pb;
    const ka = a.evaluation.kwhCumac ?? 0;
    const kb = b.evaluation.kwhCumac ?? 0;
    return kb - ka;
  });

  return results;
}

/**
 * Helper : agrège les résultats par famille (Bouquet / Enveloppe / Thermique…).
 * Utile pour l'UI qui affiche par catégorie.
 */
export function groupByFamily(
  results: FullEvaluateResult[],
): Record<string, FullEvaluateResult[]> {
  const out: Record<string, FullEvaluateResult[]> = {};
  for (const r of results) {
    const family = r.sheet.family;
    if (!out[family]) out[family] = [];
    out[family].push(r);
  }
  return out;
}

/**
 * Totalise les montants estimés (kWh cumac + €) pour un sous-ensemble de
 * résultats. Utile pour afficher "Total potentiel" sur la fiche copro.
 */
export function sumEstimates(results: FullEvaluateResult[]): {
  kwhCumac: number;
  euroAmount: number;
} {
  let kwhCumac = 0;
  let euroAmount = 0;
  for (const r of results) {
    if (r.evaluation.kwhCumac != null) kwhCumac += r.evaluation.kwhCumac;
    if (r.evaluation.euroAmount != null) euroAmount += r.evaluation.euroAmount;
  }
  return { kwhCumac, euroAmount };
}

// Re-export utilitaires pour les consommateurs (UI, autres modules)
export { STATUS } from "./config";
export { SHEETS } from "./sheets";
export type { SheetDefinition } from "./sheets";
export type { Evaluation, Project, Action, CoupDePouceInfo } from "./types";
