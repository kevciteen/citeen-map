/**
 * Évaluateurs résidentiels (BAR-*).
 *
 * Port TypeScript de `simulateur-cee-main/js/evaluators-residential.js` et
 * des callbacks `compute(...)` du catalogue `RESIDENTIAL_SHEET_CONFIGS`
 * de `sheets.js` (lignes 841-2492).
 *
 * Les évaluateurs BAR-TH-174 / 175 (rénovation d'ampleur) restent dans
 * `engine.ts`. Les évaluateurs ci-dessous sont basés sur le pattern générique
 * `makeResidentialSheet({ requirements, compute, ... })` qui :
 *   1. construit une `Evaluation` vide,
 *   2. applique `applyCommonRules`,
 *   3. valide chaque "requirement" (champ obligatoire + blocker éventuel),
 *   4. exécute `compute({ project, action, derived })` pour obtenir kWh cumac,
 *   5. applique le Coup de pouce résidentiel sur les fiches éligibles,
 *   6. finalise le statut.
 */
import {
  applyCommonRules,
  baseEvaluation,
  buildCalculationLabel,
  finalizeStatus,
  getEffectiveMwhCumacPrice,
  getEstimatedPrimeEuros,
  getIncomeCategoryMeta,
  isModestHousehold,
  toNumber,
} from "../core";
import {
  COEFF_BAR_EN_101,
  COEFF_BAR_EN_102,
  COEFF_BAR_EN_103,
  COEFF_BAR_EN_104,
  COEFF_BAR_EN_105,
  COEFF_BAR_EN_108,
  COEFF_BAR_EN_110,
  COEFF_BAR_EQ_115,
  COEFF_BAR_SE_104,
  COEFF_BAR_SE_105,
  COEFF_BAR_SE_106,
  COEFF_BAR_SE_107,
  COEFF_BAR_SE_108,
  COEFF_BAR_TH_101,
  COEFF_BAR_TH_110,
  COEFF_BAR_TH_111,
  COEFF_BAR_TH_112,
  COEFF_BAR_TH_113,
  COEFF_BAR_TH_116,
  COEFF_BAR_TH_117,
  COEFF_BAR_TH_122,
  COEFF_BAR_TH_123,
  COEFF_BAR_TH_125,
  COEFF_BAR_TH_127,
  COEFF_BAR_TH_129,
  COEFF_BAR_TH_137,
  COEFF_BAR_TH_139,
  COEFF_BAR_TH_143,
  COEFF_BAR_TH_148,
  COEFF_BAR_TH_155,
  COEFF_BAR_TH_158,
  COEFF_BAR_TH_159,
  COEFF_BAR_TH_161,
  COEFF_BAR_TH_162,
  COEFF_BAR_TH_165,
  COEFF_BAR_TH_168,
  COEFF_BAR_TH_169,
  COEFF_BAR_TH_170,
  COEFF_BAR_TH_171,
  COEFF_BAR_TH_172,
  COEFF_BAR_TH_173,
  COEFF_BAR_TH_176,
  COEFF_BAR_TH_177,
  COEFF_BAR_TH_178,
  COEFF_BAR_TH_179,
  COEFF_BAR_TH_180,
  COLLECTIVE_HOUSING_TYPES,
  RESOURCE_CATEGORY_KEYS,
} from "../config";
import type {
  Action,
  CommonRulesOptions,
  CoupDePouceInfo,
  DerivedProjectData,
  Evaluation,
  HousingType,
  Project,
} from "../types";

// === Helpers ============================================================

interface Requirement {
  name: string;
  missing: string;
  blockerOnNo?: string;
  showWhen?: (action: Action, project: Project) => boolean;
}

interface ComputeContext {
  project: Project;
  action: Action;
  derived: DerivedProjectData;
}

interface ComputeResult {
  kwhCumac: number | null;
  calculationLabel?: string;
}

interface GenericSheetConfig {
  commonOptions?: CommonRulesOptions;
  requirements?: Requirement[];
  notes?: string[];
  compute?: (ctx: ComputeContext) => number | ComputeResult | undefined;
}

function evaluateGenericConfigSheet(
  code: string,
  label: string,
  project: Project,
  action: Action,
  enabled: boolean,
  config: GenericSheetConfig,
): Evaluation {
  const e = baseEvaluation(code, label, enabled);
  const baseCommonOptions: CommonRulesOptions = {
    supportedBuildingTypes: ["Habitation"],
    needsSector: false,
    needsHousingType: true,
    ...(config.commonOptions || {}),
  };
  const derived = applyCommonRules(e, project, baseCommonOptions);
  const commonMissingCount = e.missing.length;

  if (config.notes) e.notes.push(...config.notes);

  (config.requirements || []).forEach((requirement) => {
    if (typeof requirement.showWhen === "function" && !requirement.showWhen(action, project)) {
      return;
    }
    const value = (action as Record<string, unknown>)[requirement.name];
    const isMissing =
      value === "" ||
      value == null ||
      (typeof value === "number" && !Number.isFinite(value)) ||
      (typeof value !== "string" && !value);
    if (isMissing) {
      if (enabled) e.missing.push(requirement.missing);
      return;
    }
    if (value === "Non" && requirement.blockerOnNo) {
      e.blockers.push(requirement.blockerOnNo);
    }
  });

  if (enabled && !e.blockers.length && !e.missing.length) {
    if (typeof config.compute === "function") {
      const computeResult = config.compute({ project, action, derived });
      if (typeof computeResult === "number") {
        e.kwhCumac = computeResult;
      } else if (computeResult && typeof computeResult === "object") {
        e.kwhCumac = computeResult.kwhCumac ?? null;
        e.calculationLabel = computeResult.calculationLabel || "";
      }
    } else {
      e.calculationEngineMissing = true;
    }
  }

  if (!e.blockers.length && e.kwhCumac != null) {
    applyResidentialCoupDePouce(e, project, action);
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

// === Coup de pouce résidentiel ==========================================

function createCoupDePouceBase(
  name: string,
  standardKwhCumac: number,
  standardCalculationLabel: string,
  notes: string[],
  project: Project,
): CoupDePouceInfo & {
  reason: string;
  missing: string[];
  standardKwhCumac: number;
  standardEuroAmount: number | null;
  standardCalculationLabel: string;
  kwhCumac: number | null;
  euroAmount: number | null;
  calculationLabel: string;
} {
  return {
    name,
    status: "Eligibilite a confirmer",
    factor: 1,
    notes,
    reason: "",
    missing: [],
    standardKwhCumac,
    standardEuroAmount: getEstimatedPrimeEuros(standardKwhCumac, getEffectiveMwhCumacPrice(project)),
    standardCalculationLabel,
    kwhCumac: null,
    euroAmount: null,
    calculationLabel: "",
  };
}

function applyResidentialCoupDePouce(evaluation: Evaluation, project: Project, action: Action): void {
  if (!evaluation.kwhCumac || evaluation.blockers.length) return;

  if (["BAR-TH-112", "BAR-TH-113", "BAR-TH-143", "BAR-TH-171", "BAR-TH-172"].includes(evaluation.code)) {
    applyCoupDePouceChauffageResidential(evaluation, project, action);
    return;
  }

  if (evaluation.code === "BAR-TH-137") {
    if (project.housingType === "Maison individuelle") {
      applyCoupDePouceChauffageResidential(evaluation, project, action);
      return;
    }
    applyCoupDePouceBrctResidential(evaluation, project, action);
    return;
  }

  if (["BAR-TH-178", "BAR-TH-179", "BAR-TH-180"].includes(evaluation.code)) {
    applyCoupDePouceBrctResidential(evaluation, project, action);
  }
}

function applyCoupDePouceChauffageResidential(
  evaluation: Evaluation,
  project: Project,
  action: Action,
): void {
  const coupDePouce = createCoupDePouceBase(
    "Coup de pouce Chauffage",
    evaluation.kwhCumac as number,
    evaluation.calculationLabel,
    [
      "Une offre Coup de pouce d'un signataire doit etre acceptee avant signature du devis.",
      "Le Coup de pouce Chauffage s'applique seulement en residence principale.",
    ],
    project,
  );
  const standardKwhCumac = evaluation.kwhCumac as number;
  const incomeCategory = getIncomeCategoryMeta(project.incomeBracket)?.key;

  if (action.coupDePoucePrimaryResidence === "Non") {
    coupDePouce.status = "Non eligible";
    coupDePouce.reason =
      "Le Coup de pouce Chauffage s'applique seulement aux logements occupes a titre de residence principale.";
    evaluation.coupDePouce = coupDePouce;
    return;
  }

  let factor: number | null = null;

  if (evaluation.code === "BAR-TH-112") {
    if (action.coupDePouceReplacedIndependentCoalHeater === "Autre") {
      coupDePouce.status = "Non eligible";
      coupDePouce.reason =
        "Le Coup de pouce Chauffage sur BAR-TH-112 exige le remplacement d'un equipement independant de chauffage au charbon.";
      evaluation.coupDePouce = coupDePouce;
      return;
    }
    if (!incomeCategory) {
      coupDePouce.missing.push(
        "Renseigner les revenus clients pour determiner la bonification du Coup de pouce.",
      );
    } else {
      factor = isModestHousehold(project) ? 5 : 4;
    }
  } else if (evaluation.code === "BAR-TH-137") {
    if (action.coupDePouceReplacedEquipment === "Autre") {
      coupDePouce.status = "Non eligible";
      coupDePouce.reason =
        "Le Coup de pouce Chauffage sur BAR-TH-137 exige le remplacement d'une chaudiere au charbon, au fioul ou au gaz.";
      evaluation.coupDePouce = coupDePouce;
      return;
    }
    if (!incomeCategory) {
      coupDePouce.missing.push(
        "Renseigner les revenus clients pour determiner la bonification du Coup de pouce.",
      );
    } else {
      factor = isModestHousehold(project) ? 2 : 1.5;
    }
  } else {
    if (action.coupDePouceReplacedEquipment === "Autre") {
      coupDePouce.status = "Non eligible";
      coupDePouce.reason =
        "Le Coup de pouce Chauffage exige ici le remplacement d'une chaudiere au charbon, au fioul ou au gaz.";
      evaluation.coupDePouce = coupDePouce;
      return;
    }
    factor = evaluation.code === "BAR-TH-143" ? 2 : 5;
  }

  if (!action.coupDePoucePrimaryResidence) {
    coupDePouce.missing.push("Preciser si le logement est occupe a titre de residence principale.");
  }
  if (evaluation.code === "BAR-TH-112" && !action.coupDePouceReplacedIndependentCoalHeater) {
    coupDePouce.missing.push("Preciser l'equipement remplace pour verifier le Coup de pouce.");
  }
  if (evaluation.code !== "BAR-TH-112" && !action.coupDePouceReplacedEquipment) {
    coupDePouce.missing.push("Preciser l'equipement remplace pour verifier le Coup de pouce.");
  }

  if (coupDePouce.missing.length) {
    coupDePouce.reason =
      "Renseigner les conditions specifiques du dispositif pour confirmer l'application du Coup de pouce.";
    evaluation.coupDePouce = coupDePouce;
    return;
  }

  if (factor == null) {
    evaluation.coupDePouce = coupDePouce;
    return;
  }

  coupDePouce.status = "Eligible";
  coupDePouce.factor = factor;
  coupDePouce.reason =
    "Les conditions simples controlees dans le simulateur permettent d'appliquer la bonification Coup de pouce.";
  coupDePouce.kwhCumac = standardKwhCumac * factor;
  coupDePouce.euroAmount = getEstimatedPrimeEuros(
    coupDePouce.kwhCumac,
    getEffectiveMwhCumacPrice(project),
  );
  const calculationLabel = `${standardKwhCumac} x ${factor} = ${coupDePouce.kwhCumac} kWh cumac`;
  coupDePouce.calculationLabel = calculationLabel;

  evaluation.standardKwhCumac = standardKwhCumac;
  evaluation.standardEuroAmount = coupDePouce.standardEuroAmount;
  evaluation.standardCalculationLabel = evaluation.calculationLabel;
  evaluation.kwhCumac = coupDePouce.kwhCumac;
  evaluation.euroAmount = coupDePouce.euroAmount;
  evaluation.calculationLabel = coupDePouce.calculationLabel;
  evaluation.coupDePouce = coupDePouce;
}

function applyCoupDePouceBrctResidential(
  evaluation: Evaluation,
  project: Project,
  action: Action,
): void {
  const coupDePouce = createCoupDePouceBase(
    "Coup de pouce Chauffage des batiments residentiels collectifs et tertiaires",
    evaluation.kwhCumac as number,
    evaluation.calculationLabel,
    ["Une offre Coup de pouce d'un signataire doit etre acceptee avant signature du devis."],
    project,
  );
  const standardKwhCumac = evaluation.kwhCumac as number;
  let bonusKwhCumac: number | null = null;
  let calculationLabel = "";

  if (action.coupDePouceReplacedEquipment === "Autre") {
    coupDePouce.status = "Non eligible";
    coupDePouce.reason =
      "Le Coup de pouce exige ici le remplacement d'une chaudiere au charbon, au fioul ou au gaz.";
    evaluation.coupDePouce = coupDePouce;
    return;
  }

  if (!action.coupDePouceReplacedEquipment) {
    coupDePouce.missing.push("Preciser l'equipement remplace pour verifier le Coup de pouce.");
  }

  if (evaluation.code === "BAR-TH-137") {
    const dwellingCount = toNumber(action.apartmentCount);
    if (!dwellingCount) {
      coupDePouce.missing.push("Renseigner le nombre de logements raccordes pour le Coup de pouce.");
    } else if (dwellingCount <= 125) {
      bonusKwhCumac = 24000 * dwellingCount + 9000000;
      calculationLabel = `24000 x ${dwellingCount} + 9000000 = ${bonusKwhCumac} kWh cumac`;
    } else {
      bonusKwhCumac = 54000 * dwellingCount + 5200000;
      calculationLabel = `54000 x ${dwellingCount} + 5200000 = ${bonusKwhCumac} kWh cumac`;
    }
  } else {
    if (action.coupDePouceHeatNetworkImpossible === "Non") {
      coupDePouce.status = "Non eligible";
      coupDePouce.reason =
        "Le Coup de pouce ne s'applique sur cette operation que si l'impossibilite technique ou economique du raccordement au reseau de chaleur est justifiee.";
      evaluation.coupDePouce = coupDePouce;
      return;
    }
    if (!action.coupDePouceHeatNetworkImpossible) {
      coupDePouce.missing.push(
        "Preciser si l'impossibilite technique ou economique du raccordement au reseau de chaleur est justifiee.",
      );
    }

    const factor =
      evaluation.code === "BAR-TH-178" ? 5 : evaluation.code === "BAR-TH-179" ? 3 : 4;
    bonusKwhCumac = standardKwhCumac * factor;
    coupDePouce.factor = factor;
    calculationLabel = `${standardKwhCumac} x ${factor} = ${bonusKwhCumac} kWh cumac`;
  }

  if (coupDePouce.missing.length) {
    coupDePouce.reason =
      "Renseigner les conditions specifiques du dispositif pour confirmer l'application du Coup de pouce.";
    evaluation.coupDePouce = coupDePouce;
    return;
  }

  if (bonusKwhCumac == null) {
    evaluation.coupDePouce = coupDePouce;
    return;
  }

  coupDePouce.status = "Eligible";
  coupDePouce.reason =
    "Les conditions simples controlees dans le simulateur permettent d'appliquer la bonification Coup de pouce.";
  coupDePouce.kwhCumac = bonusKwhCumac;
  coupDePouce.euroAmount = getEstimatedPrimeEuros(bonusKwhCumac, getEffectiveMwhCumacPrice(project));
  coupDePouce.calculationLabel = calculationLabel;

  evaluation.standardKwhCumac = standardKwhCumac;
  evaluation.standardEuroAmount = coupDePouce.standardEuroAmount;
  evaluation.standardCalculationLabel = evaluation.calculationLabel;
  evaluation.kwhCumac = coupDePouce.kwhCumac;
  evaluation.euroAmount = coupDePouce.euroAmount;
  evaluation.calculationLabel = coupDePouce.calculationLabel;
  evaluation.coupDePouce = coupDePouce;
}

// === BAR-EN-* (Enveloppe) ===============================================

export function evaluateBarEn101(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet("BAR-EN-101", "Isolation de combles ou de toitures", project, action, enabled, {
    requirements: [{ name: "insulatedSurface", missing: "Renseigner la surface a isoler." }],
    compute: ({ action: a, derived }) => {
      if (!derived.climateZone) return undefined;
      const surface = toNumber(a.insulatedSurface);
      const coefficient = COEFF_BAR_EN_101[derived.climateZone];
      const kwhCumac = surface * coefficient;
      return { kwhCumac, calculationLabel: buildCalculationLabel([coefficient, surface], kwhCumac) };
    },
  });
}

export function evaluateBarEn102(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet("BAR-EN-102", "Isolation des murs", project, action, enabled, {
    requirements: [{ name: "insulatedWallSurface", missing: "Renseigner la surface de murs a isoler." }],
    compute: ({ action: a, derived }) => {
      if (!derived.climateZone) return undefined;
      const surface = toNumber(a.insulatedWallSurface);
      const coefficient = COEFF_BAR_EN_102[derived.climateZone];
      const kwhCumac = surface * coefficient;
      return { kwhCumac, calculationLabel: buildCalculationLabel([coefficient, surface], kwhCumac) };
    },
  });
}

export function evaluateBarEn103(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet("BAR-EN-103", "Isolation d'un plancher", project, action, enabled, {
    requirements: [{ name: "insulatedFloorSurface", missing: "Renseigner la surface de plancher a isoler." }],
    compute: ({ action: a, derived }) => {
      if (!derived.climateZone) return undefined;
      const surface = toNumber(a.insulatedFloorSurface);
      const coefficient = COEFF_BAR_EN_103[derived.climateZone];
      const kwhCumac = surface * coefficient;
      return { kwhCumac, calculationLabel: buildCalculationLabel([coefficient, surface], kwhCumac) };
    },
  });
}

export function evaluateBarEn104(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-EN-104",
    "Fenetre ou porte-fenetre complete avec vitrage isolant",
    project,
    action,
    enabled,
    {
      requirements: [{ name: "installedWindowSurface", missing: "Renseigner la surface de menuiseries installees." }],
      compute: ({ action: a, derived }) => {
        if (!derived.climateZone) return undefined;
        const surface = toNumber(a.installedWindowSurface);
        const coefficient = COEFF_BAR_EN_104[derived.climateZone];
        const kwhCumac = surface * coefficient;
        return { kwhCumac, calculationLabel: buildCalculationLabel([coefficient, surface], kwhCumac) };
      },
    },
  );
}

export function evaluateBarEn105(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet("BAR-EN-105", "Isolation des toitures terrasses", project, action, enabled, {
    requirements: [{ name: "terraceInsulatedSurface", missing: "Renseigner la surface de toiture-terrasse a isoler." }],
    compute: ({ action: a, derived }) => {
      if (!derived.climateZone) return undefined;
      const surface = toNumber(a.terraceInsulatedSurface);
      const coefficient = COEFF_BAR_EN_105[derived.climateZone];
      const kwhCumac = surface * coefficient;
      return { kwhCumac, calculationLabel: buildCalculationLabel([coefficient, surface], kwhCumac) };
    },
  });
}

export function evaluateBarEn108(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet("BAR-EN-108", "Fermeture isolante", project, action, enabled, {
    requirements: [{ name: "closingSurface", missing: "Renseigner la surface equipee." }],
    compute: ({ action: a, derived }) => {
      if (!derived.climateZone) return undefined;
      const surface = toNumber(a.closingSurface);
      const coefficient = COEFF_BAR_EN_108[derived.climateZone];
      const kwhCumac = surface * coefficient;
      return { kwhCumac, calculationLabel: buildCalculationLabel([coefficient, surface], kwhCumac) };
    },
  });
}

export function evaluateBarEn110(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-EN-110",
    "Fenetre ou porte-fenetre complete avec vitrage parietodynamique",
    project,
    action,
    enabled,
    {
      requirements: [
        { name: "parietodynamicCount", missing: "Renseigner le nombre de fenetres ou portes-fenetres posees." },
        {
          name: "compatibleVentilation",
          missing: "Preciser si le logement est equipe d'une ventilation compatible.",
          blockerOnNo: "La ventilation du logement doit etre compatible.",
        },
      ],
      compute: ({ action: a, derived }) => {
        if (!derived.climateZone) return undefined;
        const count = toNumber(a.parietodynamicCount);
        const coefficient = COEFF_BAR_EN_110[derived.climateZone];
        const kwhCumac = count * coefficient;
        return { kwhCumac, calculationLabel: buildCalculationLabel([coefficient, count], kwhCumac) };
      },
    },
  );
}

// === BAR-EQ-* (Équipement) ==============================================

export function evaluateBarEq115(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-EQ-115",
    "Dispositif d'affichage et d'interpretation des consommations d'energie",
    project,
    action,
    enabled,
    {
      commonOptions: { supportedHousingTypes: ["Maison individuelle", "Appartement"] },
      requirements: [
        {
          name: "displayProvisionCompliant",
          missing: "Preciser si le dispositif est achete ou loue pour au moins 24 mois.",
          blockerOnNo: "La fiche BAR-EQ-115 n'est pas applicable a une location inferieure a 24 mois.",
        },
        {
          name: "individualMetersIfCollectiveFuel",
          missing: "Preciser la situation du logement en cas de chauffage collectif par combustible.",
          blockerOnNo:
            "Le logement doit disposer de compteurs individuels d'energie ou de repartiteurs en cas de chauffage collectif par combustible.",
        },
        { name: "comfortMonitoringOption", missing: "Preciser si l'option suivi du confort est incluse." },
      ],
      compute: ({ project: p, action: a, derived }) => {
        if (!derived.climateZone) return undefined;
        const housingType = String(p.housingType || "");
        const surface = toNumber(p.buildingSurface);
        const base = COEFF_BAR_EQ_115.base[housingType]?.[derived.climateZone];
        const comfortFactor = COEFF_BAR_EQ_115.comfortFactor[String(a.comfortMonitoringOption)];
        const surfaceFactor = COEFF_BAR_EQ_115.surfaceFactor(housingType, surface);
        const fixedPart = COEFF_BAR_EQ_115.fixedPart[housingType];
        if (base == null || comfortFactor == null || fixedPart == null) return undefined;
        const kwhCumac = base * comfortFactor * surfaceFactor + fixedPart;
        return {
          kwhCumac,
          calculationLabel: buildCalculationLabel([base, comfortFactor, surfaceFactor, fixedPart], kwhCumac),
        };
      },
    },
  );
}

// === BAR-SE-* (Services) ================================================

export function evaluateBarSe104(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-SE-104",
    "Reglage des organes d'equilibrage d'une installation de chauffage a eau chaude",
    project,
    action,
    enabled,
    {
      commonOptions: { supportedHousingTypes: [...COLLECTIVE_HOUSING_TYPES] },
      requirements: [
        {
          name: "collectiveHotWaterHeating",
          missing: "Preciser si le batiment est equipe d'un chauffage collectif a eau chaude.",
          blockerOnNo: "La fiche BAR-SE-104 s'applique au chauffage collectif a eau chaude.",
        },
        { name: "apartmentCount", missing: "Renseigner le nombre d'appartements concernes." },
      ],
      compute: ({ action: a, derived }) => {
        if (!derived.climateZone) return undefined;
        const apartmentCount = toNumber(a.apartmentCount);
        const coefficient = COEFF_BAR_SE_104[derived.climateZone];
        const kwhCumac = coefficient * apartmentCount;
        return { kwhCumac, calculationLabel: buildCalculationLabel([coefficient, apartmentCount], kwhCumac) };
      },
    },
  );
}

export function evaluateBarSe105(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-SE-105",
    "Contrat de Performance Energetique Services (CPE Services)",
    project,
    action,
    enabled,
    {
      commonOptions: { supportedHousingTypes: [...COLLECTIVE_HOUSING_TYPES] },
      requirements: [
        {
          name: "cpeServicesContract",
          missing: "Preciser si le contrat releve bien d'un CPE Services.",
          blockerOnNo: "La fiche BAR-SE-105 s'applique a un CPE Services.",
        },
        { name: "apartmentCount", missing: "Renseigner le nombre d'appartements concernes." },
        { name: "guaranteeDurationYears", missing: "Renseigner la duree de garantie eligible." },
      ],
      compute: ({ action: a, derived }) => {
        if (!derived.climateZone) return undefined;
        const apartmentCount = toNumber(a.apartmentCount);
        const coefficient = COEFF_BAR_SE_105[String(a.guaranteeDurationYears)]?.[derived.climateZone];
        if (coefficient == null) return undefined;
        const kwhCumac = coefficient * apartmentCount;
        return { kwhCumac, calculationLabel: buildCalculationLabel([coefficient, apartmentCount], kwhCumac) };
      },
    },
  );
}

export function evaluateBarSe106(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-SE-106",
    "Service de suivi des consommations d'energie",
    project,
    action,
    enabled,
    {
      commonOptions: { supportedHousingTypes: ["Maison individuelle", "Appartement"] },
      requirements: [{ name: "followedEnergyScope", missing: "Preciser les energies couvertes par le service de suivi." }],
      compute: ({ project: p, action: a, derived }) => {
        if (!derived.climateZone) return undefined;
        const housingType = String(p.housingType || "");
        const coefficient =
          COEFF_BAR_SE_106[housingType]?.[String(a.followedEnergyScope)]?.[derived.climateZone];
        if (coefficient == null) return undefined;
        return { kwhCumac: coefficient, calculationLabel: buildCalculationLabel([coefficient], coefficient) };
      },
    },
  );
}

export function evaluateBarSe107(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-SE-107",
    "Abaissement de la temperature de retour vers un reseau de chaleur",
    project,
    action,
    enabled,
    {
      commonOptions: { supportedHousingTypes: [...COLLECTIVE_HOUSING_TYPES] },
      requirements: [
        {
          name: "connectedToHeatNetwork",
          missing: "Preciser si le batiment est raccorde a un reseau de chaleur.",
          blockerOnNo: "La fiche BAR-SE-107 s'applique aux batiments raccordes a un reseau de chaleur.",
        },
        { name: "apartmentCount", missing: "Renseigner le nombre d'appartements concernes." },
      ],
      compute: ({ action: a, derived }) => {
        if (!derived.climateZone) return undefined;
        const apartmentCount = toNumber(a.apartmentCount);
        const coefficient = COEFF_BAR_SE_107[derived.climateZone];
        const kwhCumac = coefficient * apartmentCount;
        return { kwhCumac, calculationLabel: buildCalculationLabel([coefficient, apartmentCount], kwhCumac) };
      },
    },
  );
}

export function evaluateBarSe108(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-SE-108",
    "Desembouage d'un reseau hydraulique individuel de chauffage",
    project,
    action,
    enabled,
    {
      commonOptions: { supportedHousingTypes: ["Maison individuelle", "Appartement"] },
      requirements: [
        {
          name: "individualHydraulicHeating",
          missing: "Preciser si le logement dispose d'un chauffage individuel hydraulique.",
          blockerOnNo: "La fiche BAR-SE-108 s'applique au chauffage individuel hydraulique.",
        },
        {
          name: "heatingPowerUnder70Kw",
          missing: "Preciser si la puissance thermique nominale est inferieure ou egale a 70 kW.",
          blockerOnNo:
            "La fiche BAR-SE-108 n'est applicable que si la puissance thermique nominale est inferieure ou egale a 70 kW.",
        },
        {
          name: "notHeatedByExcludedHeatPump",
          missing: "Preciser si la boucle d'eau est chauffee par une technologie exclue.",
          blockerOnNo:
            "La fiche BAR-SE-108 exclut les boucles d'eau chauffees, meme partiellement, par une pompe a chaleur air/eau, eau/eau, sol/eau ou hybride.",
        },
      ],
      compute: ({ project: p, derived }) => {
        if (!derived.climateZone) return undefined;
        const housingType = String(p.housingType || "");
        const coefficient = COEFF_BAR_SE_108[housingType]?.[derived.climateZone];
        if (coefficient == null) return undefined;
        return { kwhCumac: coefficient, calculationLabel: buildCalculationLabel([coefficient], coefficient) };
      },
    },
  );
}

// === BAR-TH-* (Thermique) ===============================================

export function evaluateBarTh101(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet("BAR-TH-101", "Chauffe-eau solaire individuel", project, action, enabled, {
    commonOptions: { supportedHousingTypes: ["Maison individuelle"] },
    requirements: [
      {
        name: "individualSolarWaterHeater",
        missing: "Preciser s'il s'agit d'un chauffe-eau solaire individuel.",
        blockerOnNo: "La fiche BAR-TH-101 s'applique au chauffe-eau solaire individuel.",
      },
      { name: "collectorAreaM2", missing: "Renseigner la surface de capteurs solaires." },
      {
        name: "coversAllDhwNeeds",
        missing: "Preciser si le chauffe-eau couvre la totalite du besoin d'eau chaude sanitaire.",
        blockerOnNo: "Le chauffe-eau solaire doit couvrir la totalite du besoin d'eau chaude sanitaire du logement.",
      },
      {
        name: "fluidTypeEligible",
        missing: "Preciser si les capteurs sont a circulation d'eau ou d'eau glycolee.",
        blockerOnNo: "Les capteurs doivent etre a circulation d'eau ou d'eau glycolee.",
      },
      { name: "backupEnergyType", missing: "Preciser l'energie de l'appoint." },
      { name: "drawOffProfile", missing: "Preciser le profil de soutirage declare." },
      {
        name: "waterHeatingEfficiencyCompliant",
        missing:
          "Preciser si l'efficacite energetique respecte le seuil officiel du profil et de l'appoint.",
        blockerOnNo:
          "L'efficacite energetique pour le chauffage de l'eau ne respecte pas le seuil minimal de la fiche.",
      },
    ],
    compute: ({ derived }) => {
      if (!derived.climateZone) return undefined;
      const kwhCumac = COEFF_BAR_TH_101[derived.climateZone];
      return { kwhCumac, calculationLabel: buildCalculationLabel([kwhCumac], kwhCumac) };
    },
  });
}

export function evaluateBarTh102(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet("BAR-TH-102", "Chauffe-eau solaire collectif", project, action, enabled, {
    commonOptions: { supportedHousingTypes: ["Appartement", ...COLLECTIVE_HOUSING_TYPES] },
    requirements: [
      {
        name: "collectiveSolarWaterHeater",
        missing: "Preciser s'il s'agit d'un chauffe-eau solaire collectif.",
        blockerOnNo: "La fiche BAR-TH-102 s'applique a un chauffe-eau solaire collectif.",
      },
      { name: "dhwSolarNeedKwh", missing: "Renseigner le besoin annuel en eau chaude sanitaire produit par l'energie solaire B." },
      { name: "solarCoverageRate", missing: "Renseigner le taux de couverture solaire T." },
    ],
    compute: ({ action: a }) => {
      const need = toNumber(a.dhwSolarNeedKwh);
      const rate = toNumber(a.solarCoverageRate);
      const kwhCumac = need * rate * 0.196;
      return { kwhCumac, calculationLabel: buildCalculationLabel([need, rate, 0.196], kwhCumac) };
    },
  });
}

function getBarTh110LikeBucket(project: Project, action: Action): string {
  const housingType = project.housingType;
  if (housingType === "Appartement") {
    return action.apartmentHeatingMode === "Collectif"
      ? "Appartement avec chauffage collectif"
      : "Appartement avec chauffage individuel";
  }
  if (
    housingType &&
    COLLECTIVE_HOUSING_TYPES.includes(housingType as (typeof COLLECTIVE_HOUSING_TYPES)[number])
  ) {
    return "Appartement avec chauffage collectif";
  }
  return String(housingType || "");
}

export function evaluateBarTh110(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-TH-110",
    "Radiateur basse temperature pour un chauffage central",
    project,
    action,
    enabled,
    {
      commonOptions: { supportedHousingTypes: ["Maison individuelle", "Appartement", ...COLLECTIVE_HOUSING_TYPES] },
      requirements: [
        {
          name: "lowTempRadiators",
          missing: "Preciser s'il s'agit de radiateurs basse temperature.",
          blockerOnNo: "La fiche BAR-TH-110 s'applique aux radiateurs basse temperature.",
        },
        { name: "radiatorCount", missing: "Renseigner le nombre de radiateurs installes." },
        {
          name: "apartmentHeatingMode",
          missing: "Preciser si l'appartement est en chauffage individuel ou collectif.",
          showWhen: (_a, p) => p.housingType === "Appartement",
        },
      ],
      compute: ({ project: p, action: a, derived }) => {
        if (!derived.climateZone) return undefined;
        const housingBucket = getBarTh110LikeBucket(p, a);
        const count = toNumber(a.radiatorCount);
        const coefficient = COEFF_BAR_TH_110[housingBucket]?.[derived.climateZone];
        if (coefficient == null) return undefined;
        const kwhCumac = coefficient * count;
        return { kwhCumac, calculationLabel: buildCalculationLabel([coefficient, count], kwhCumac) };
      },
    },
  );
}

export function evaluateBarTh111(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-TH-111",
    "Regulation par sonde de temperature exterieure",
    project,
    action,
    enabled,
    {
      commonOptions: { supportedHousingTypes: ["Maison individuelle"] },
      requirements: [
        {
          name: "outsideTemperatureProbe",
          missing: "Preciser si le projet met en place une regulation par sonde exterieure.",
          blockerOnNo: "La fiche BAR-TH-111 s'applique a une regulation par sonde de temperature exterieure.",
        },
        { name: "heatingEnergy", missing: "Preciser l'energie de chauffage." },
      ],
      compute: ({ project: p, action: a, derived }) => {
        if (!derived.climateZone) return undefined;
        const surface = toNumber(p.buildingSurface);
        const energyKey = String(a.heatingEnergy);
        const coefficient = (COEFF_BAR_TH_111 as Record<string, unknown>)[energyKey] as
          | { H1: number; H2: number; H3: number }
          | undefined;
        if (!coefficient) return undefined;
        const coeffVal = coefficient[derived.climateZone];
        const factor = COEFF_BAR_TH_111.surfaceFactor(surface);
        const kwhCumac = coeffVal * factor;
        return { kwhCumac, calculationLabel: buildCalculationLabel([coeffVal, factor], kwhCumac) };
      },
    },
  );
}

export function evaluateBarTh112(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-TH-112",
    "Appareil independant de chauffage au bois",
    project,
    action,
    enabled,
    {
      commonOptions: { supportedHousingTypes: ["Maison individuelle"] },
      requirements: [
        {
          name: "independentWoodHeater",
          missing: "Preciser s'il s'agit d'un appareil independant de chauffage au bois.",
          blockerOnNo: "La fiche BAR-TH-112 s'applique aux appareils independants de chauffage au bois.",
        },
        { name: "woodHeaterType", missing: "Preciser le type d'appareil installe." },
        {
          name: "performanceCompliant",
          missing: "Preciser si l'appareil respecte les performances officielles de la fiche.",
          blockerOnNo: "L'appareil ne respecte pas les performances minimales de la fiche BAR-TH-112.",
        },
      ],
      compute: ({ derived }) => {
        if (!derived.climateZone) return undefined;
        const kwhCumac = COEFF_BAR_TH_112[derived.climateZone];
        return { kwhCumac, calculationLabel: buildCalculationLabel([kwhCumac], kwhCumac) };
      },
    },
  );
}

export function evaluateBarTh113(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet("BAR-TH-113", "Chaudiere biomasse individuelle", project, action, enabled, {
    commonOptions: { supportedHousingTypes: ["Maison individuelle"] },
    requirements: [
      {
        name: "biomassBoiler",
        missing: "Preciser s'il s'agit d'une chaudiere biomasse individuelle.",
        blockerOnNo: "La fiche BAR-TH-113 s'applique aux chaudieres biomasse individuelles.",
      },
      {
        name: "class5OrFlammeVerte",
        missing: "Preciser si la chaudiere est de classe 5 ou labellisee Flamme verte.",
        blockerOnNo: "La chaudiere doit etre de classe 5 NF EN 303.5 ou beneficier du label Flamme verte.",
      },
    ],
    compute: ({ derived }) => {
      if (!derived.climateZone) return undefined;
      const kwhCumac = COEFF_BAR_TH_113[derived.climateZone];
      return { kwhCumac, calculationLabel: buildCalculationLabel([kwhCumac], kwhCumac) };
    },
  });
}

export function evaluateBarTh116(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-TH-116",
    "Plancher chauffant hydraulique a basse temperature",
    project,
    action,
    enabled,
    {
      commonOptions: { supportedHousingTypes: ["Maison individuelle", "Appartement", ...COLLECTIVE_HOUSING_TYPES] },
      requirements: [
        {
          name: "lowTempHydraulicFloor",
          missing: "Preciser s'il s'agit d'un plancher chauffant hydraulique basse temperature.",
          blockerOnNo: "La fiche BAR-TH-116 s'applique au plancher chauffant hydraulique basse temperature.",
        },
        { name: "heatedSurface", missing: "Renseigner la surface chauffee." },
        {
          name: "apartmentHeatingMode",
          missing: "Preciser si l'appartement est equipe d'un chauffage individuel.",
          showWhen: (_a, p) => p.housingType === "Appartement",
        },
      ],
      compute: ({ project: p, action: a, derived }) => {
        if (!derived.climateZone) return undefined;
        const surface = toNumber(a.heatedSurface);
        let housingBucket = String(p.housingType || "");
        if (p.housingType === "Appartement") {
          housingBucket =
            a.apartmentHeatingMode === "Oui"
              ? "Appartement avec chauffage individuel"
              : "Appartement avec chauffage collectif";
        } else if (
          p.housingType &&
          COLLECTIVE_HOUSING_TYPES.includes(p.housingType as (typeof COLLECTIVE_HOUSING_TYPES)[number])
        ) {
          housingBucket = "Appartement avec chauffage collectif";
        }
        const coefficient = COEFF_BAR_TH_116[housingBucket]?.[derived.climateZone];
        if (coefficient == null) return undefined;
        const kwhCumac = coefficient * surface;
        return { kwhCumac, calculationLabel: buildCalculationLabel([coefficient, surface], kwhCumac) };
      },
    },
  );
}

export function evaluateBarTh117(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet("BAR-TH-117", "Robinet thermostatique", project, action, enabled, {
    commonOptions: { supportedHousingTypes: ["Maison individuelle", "Appartement", ...COLLECTIVE_HOUSING_TYPES] },
    requirements: [
      {
        name: "thermostaticValves",
        missing: "Preciser si des robinets thermostatiques sont installes.",
        blockerOnNo: "La fiche BAR-TH-117 s'applique aux robinets thermostatiques.",
      },
      {
        name: "centralFuelBoiler",
        missing: "Preciser si les radiateurs sont raccordes a un chauffage central a combustible avec chaudiere existante.",
        blockerOnNo: "La fiche BAR-TH-117 n'est applicable que sur un chauffage central a combustible avec chaudiere existante.",
      },
      { name: "valveCount", missing: "Renseigner le nombre de robinets thermostatiques." },
      {
        name: "apartmentHeatingMode",
        missing: "Preciser si l'appartement est en chauffage individuel ou collectif.",
        showWhen: (_a, p) => p.housingType === "Appartement",
      },
    ],
    compute: ({ project: p, action: a, derived }) => {
      if (!derived.climateZone) return undefined;
      const housingBucket = getBarTh110LikeBucket(p, a);
      const count = toNumber(a.valveCount);
      const coefficient = COEFF_BAR_TH_117[housingBucket]?.[derived.climateZone];
      if (coefficient == null) return undefined;
      const kwhCumac = coefficient * count;
      return { kwhCumac, calculationLabel: buildCalculationLabel([coefficient, count], kwhCumac) };
    },
  });
}

export function evaluateBarTh122(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-TH-122",
    "Recuperateur de chaleur a condensation",
    project,
    action,
    enabled,
    {
      commonOptions: { supportedHousingTypes: ["Appartement", ...COLLECTIVE_HOUSING_TYPES] },
      requirements: [
        {
          name: "condensingRecovery",
          missing: "Preciser si le projet installe un recuperateur de chaleur a condensation.",
          blockerOnNo: "La fiche BAR-TH-122 s'applique a un recuperateur de chaleur a condensation.",
        },
        { name: "apartmentCount", missing: "Renseigner le nombre d'appartements concernes." },
        { name: "equippedBoilerPowerKw", missing: "Renseigner la puissance des chaudieres nouvellement equipees." },
        { name: "newPlantPowerKw", missing: "Renseigner la puissance totale de la nouvelle chaufferie." },
      ],
      compute: ({ action: a, derived }) => {
        if (!derived.climateZone) return undefined;
        const apartmentCount = toNumber(a.apartmentCount);
        const equippedPower = toNumber(a.equippedBoilerPowerKw);
        const plantPower = toNumber(a.newPlantPowerKw);
        const coefficient = COEFF_BAR_TH_122[derived.climateZone];
        const ratio = plantPower > 0 ? equippedPower / plantPower : 0;
        const factorR = ratio < 1 / 3 ? ratio : 1;
        const kwhCumac = coefficient * apartmentCount * factorR;
        return {
          kwhCumac,
          calculationLabel: buildCalculationLabel([coefficient, apartmentCount, factorR], kwhCumac),
        };
      },
    },
  );
}

export function evaluateBarTh123(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-TH-123",
    "Optimiseur de relance en chauffage collectif",
    project,
    action,
    enabled,
    {
      commonOptions: { supportedHousingTypes: [...COLLECTIVE_HOUSING_TYPES] },
      requirements: [
        {
          name: "collectiveHeating",
          missing: "Preciser si l'installation de chauffage est collective.",
          blockerOnNo: "La fiche BAR-TH-123 s'applique au chauffage collectif.",
        },
        {
          name: "autoAdaptive",
          missing: "Preciser si l'optimiseur comprend une fonction auto-adaptative.",
          blockerOnNo: "L'optimiseur doit comprendre une fonction auto-adaptative.",
        },
        {
          name: "summerWinterSwitch",
          missing: "Preciser si le dispositif integre une fonction commutateur ete/hiver.",
          blockerOnNo: "Le dispositif doit integrer une fonction commutateur ete/hiver.",
        },
        {
          name: "nightSetback",
          missing: "Preciser si le dispositif integre une fonction descente de temperature.",
          blockerOnNo: "Le dispositif doit integrer une fonction descente de temperature.",
        },
        { name: "apartmentCount", missing: "Renseigner le nombre d'appartements." },
      ],
      compute: ({ action: a, derived }) => {
        if (!derived.climateZone) return undefined;
        const apartmentCount = toNumber(a.apartmentCount);
        const coefficient = COEFF_BAR_TH_123[derived.climateZone];
        const kwhCumac = coefficient * apartmentCount;
        return { kwhCumac, calculationLabel: buildCalculationLabel([coefficient, apartmentCount], kwhCumac) };
      },
    },
  );
}

export function evaluateBarTh125(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-TH-125",
    "Ventilation mecanique double flux a haute performance",
    project,
    action,
    enabled,
    {
      requirements: [
        {
          name: "doubleFlowVentilation",
          missing: "Preciser si le logement recoit une ventilation double flux.",
          blockerOnNo: "La fiche BAR-TH-125 s'applique a une ventilation double flux.",
        },
        { name: "installationMode", missing: "Preciser le type d'installation." },
        {
          name: "dwellingCount",
          missing: "Renseigner le nombre de logements desservis.",
          showWhen: (a) => a.installationMode === "Collective",
        },
      ],
      compute: ({ project: p, action: a, derived }) => {
        if (!derived.climateZone) return undefined;
        if (a.installationMode === "Collective") {
          const dwellingCount = toNumber(a.dwellingCount);
          const coefficient = COEFF_BAR_TH_125.collective[derived.climateZone];
          const kwhCumac = coefficient * dwellingCount;
          return { kwhCumac, calculationLabel: buildCalculationLabel([coefficient, dwellingCount], kwhCumac) };
        }
        const surface = toNumber(p.buildingSurface);
        const kind = a.installationMode === "Individuelle modulee hygroreglable" ? "modulee" : "autoreglable";
        const coefficient = COEFF_BAR_TH_125.individual[kind][derived.climateZone];
        const surfaceFactor = COEFF_BAR_TH_125.surfaceFactor(surface);
        const kwhCumac = coefficient * surfaceFactor;
        return { kwhCumac, calculationLabel: buildCalculationLabel([coefficient, surfaceFactor], kwhCumac) };
      },
    },
  );
}

export function evaluateBarTh127(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-TH-127",
    "Ventilation mecanique simple flux hygroreglable",
    project,
    action,
    enabled,
    {
      requirements: [
        {
          name: "hygroVentilation",
          missing: "Preciser si le logement recoit une ventilation hygroreglable.",
          blockerOnNo: "La fiche BAR-TH-127 s'applique a une ventilation simple flux hygroreglable.",
        },
        { name: "installationMode", missing: "Preciser le type d'installation." },
        { name: "vmcType", missing: "Preciser le type de ventilation." },
        { name: "fanType", missing: "Preciser le type de caisson." },
        {
          name: "dwellingCount",
          missing: "Renseigner le nombre de logements desservis.",
          showWhen: (a) => a.installationMode === "Collective",
        },
      ],
      compute: ({ project: p, action: a, derived }) => {
        if (!derived.climateZone) return undefined;
        const key = `${String(a.vmcType)}|${String(a.fanType)}`;
        const rTable =
          a.installationMode === "Collective"
            ? COEFF_BAR_TH_127.rFactor.collective
            : COEFF_BAR_TH_127.rFactor.individual;
        const factorR = rTable[key];
        if (factorR == null) return undefined;
        if (a.installationMode === "Collective") {
          const dwellingCount = toNumber(a.dwellingCount);
          const coefficient = COEFF_BAR_TH_127.collective[derived.climateZone];
          const kwhCumac = coefficient * dwellingCount * factorR;
          return {
            kwhCumac,
            calculationLabel: buildCalculationLabel([coefficient, dwellingCount, factorR], kwhCumac),
          };
        }
        const surface = toNumber(p.buildingSurface);
        const coefficient = COEFF_BAR_TH_127.individual[derived.climateZone];
        const surfaceFactor = COEFF_BAR_TH_127.surfaceFactor(surface);
        const kwhCumac = coefficient * surfaceFactor * factorR;
        return {
          kwhCumac,
          calculationLabel: buildCalculationLabel([coefficient, surfaceFactor, factorR], kwhCumac),
        };
      },
    },
  );
}

export function evaluateBarTh129(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet("BAR-TH-129", "Pompe a chaleur de type air/air", project, action, enabled, {
    commonOptions: { supportedHousingTypes: ["Maison individuelle", "Appartement"] },
    requirements: [
      {
        name: "airAirPac",
        missing: "Preciser si la PAC installee est de type air/air.",
        blockerOnNo: "La fiche BAR-TH-129 s'applique aux PAC air/air.",
      },
      { name: "heatedSurface", missing: "Renseigner la surface traitee." },
      {
        name: "nominalPowerAtMost12",
        missing: "Preciser si la puissance nominale est inferieure ou egale a 12 kW.",
        blockerOnNo: "La puissance nominale de la PAC air/air doit etre inferieure ou egale a 12 kW.",
      },
      {
        name: "scopEligible",
        missing: "Preciser si le SCOP est superieur ou egal a 3,9.",
        blockerOnNo: "Le SCOP doit etre superieur ou egal a 3,9.",
      },
      {
        name: "houseScopBand",
        missing: "Preciser la tranche de SCOP.",
        showWhen: (_a, p) => p.housingType === "Maison individuelle",
      },
    ],
    compute: ({ project: p, action: a, derived }) => {
      if (!derived.climateZone) return undefined;
      const surface = toNumber(a.heatedSurface);
      if (p.housingType === "Appartement") {
        const coefficient = COEFF_BAR_TH_129.apartment[derived.climateZone];
        const surfaceFactor = COEFF_BAR_TH_129.apartmentSurfaceFactor(surface);
        const kwhCumac = coefficient * surfaceFactor;
        return { kwhCumac, calculationLabel: buildCalculationLabel([coefficient, surfaceFactor], kwhCumac) };
      }
      const coefficient = COEFF_BAR_TH_129.house[String(a.houseScopBand)]?.[derived.climateZone];
      if (coefficient == null) return undefined;
      const surfaceFactor = COEFF_BAR_TH_129.houseSurfaceFactor(surface);
      const kwhCumac = coefficient * surfaceFactor;
      return { kwhCumac, calculationLabel: buildCalculationLabel([coefficient, surfaceFactor], kwhCumac) };
    },
  });
}

export function evaluateBarTh130(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-TH-130",
    "Surperformance energetique pour un batiment neuf",
    project,
    action,
    enabled,
    {
      commonOptions: { requiresExistingBuilding: false },
      requirements: [
        {
          name: "newResidentialBuilding",
          missing: "Preciser si le projet concerne un batiment residentiel neuf.",
          blockerOnNo: "La fiche BAR-TH-130 s'applique a un batiment residentiel neuf.",
        },
        { name: "referenceSurface", missing: "Renseigner la surface de reference RE2020." },
        { name: "cefMax", missing: "Renseigner la consommation conventionnelle d'energie finale maximale Cefmax." },
        { name: "cef", missing: "Renseigner la consommation conventionnelle d'energie finale du batiment Cef." },
        {
          name: "bbioCompliant",
          missing: "Preciser si le projet verifie Bbio < 0,9 Bbiomax.",
          blockerOnNo: "Le projet doit verifier Bbio < 0,9 Bbiomax.",
        },
        {
          name: "icenergyCompliant",
          missing: "Preciser si le projet verifie Icenergie < Icenergie_max.",
          blockerOnNo: "Le projet doit verifier Icenergie < Icenergie_max.",
        },
      ],
      compute: ({ action: a }) => {
        const sref = toNumber(a.referenceSurface);
        const cefMax = toNumber(a.cefMax);
        const cef = toNumber(a.cef);
        const delta = cefMax - cef;
        const kwhCumac = delta * sref * 17.984;
        return { kwhCumac, calculationLabel: buildCalculationLabel([delta, sref, 17.984], kwhCumac) };
      },
    },
  );
}

export function evaluateBarTh137(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-TH-137",
    "Raccordement d'un batiment residentiel existant a un reseau de chaleur",
    project,
    action,
    enabled,
    {
      commonOptions: { supportedHousingTypes: ["Maison individuelle", "Appartement", ...COLLECTIVE_HOUSING_TYPES] },
      requirements: [
        {
          name: "heatNetworkConnection",
          missing: "Preciser si le projet concerne un raccordement a un reseau de chaleur.",
          blockerOnNo: "La fiche BAR-TH-137 s'applique au raccordement a un reseau de chaleur.",
        },
        {
          name: "neverConnectedLast5Years",
          missing: "Preciser si le batiment n'a pas ete raccorde a un reseau de chaleur dans les 5 dernieres annees.",
          blockerOnNo:
            "Le batiment ne doit pas avoir ete raccorde a un reseau de chaleur dans les 5 annees precedant l'operation.",
        },
        {
          name: "noPriorCeeOnConnection",
          missing: "Preciser si les raccordements precedents n'ont pas fait l'objet d'une demande de CEE.",
          blockerOnNo: "Les raccordements precedents ne doivent pas avoir fait l'objet d'une demande de CEE.",
        },
        {
          name: "apartmentCount",
          missing: "Renseigner le nombre d'appartements raccordes.",
          showWhen: (_a, p) => p.housingType !== "Maison individuelle",
        },
      ],
      compute: ({ project: p, action: a, derived }) => {
        if (!derived.climateZone) return undefined;
        if (p.housingType === "Maison individuelle") {
          const surface = toNumber(p.buildingSurface);
          const coefficient = COEFF_BAR_TH_137.house[derived.climateZone];
          const surfaceFactor = COEFF_BAR_TH_137.houseSurfaceFactor(surface);
          const kwhCumac = coefficient * surfaceFactor;
          return { kwhCumac, calculationLabel: buildCalculationLabel([coefficient, surfaceFactor], kwhCumac) };
        }
        const apartmentCount = toNumber(a.apartmentCount);
        const coefficient = COEFF_BAR_TH_137.collective[derived.climateZone];
        const kwhCumac = coefficient * apartmentCount;
        return { kwhCumac, calculationLabel: buildCalculationLabel([coefficient, apartmentCount], kwhCumac) };
      },
    },
  );
}

export function evaluateBarTh139(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-TH-139",
    "Systeme de variation electronique de vitesse sur une pompe",
    project,
    action,
    enabled,
    {
      commonOptions: { supportedHousingTypes: [...COLLECTIVE_HOUSING_TYPES] },
      requirements: [
        {
          name: "pumpVariation",
          missing: "Preciser si le projet installe une variation electronique de vitesse sur une pompe.",
          blockerOnNo: "La fiche BAR-TH-139 s'applique a une variation de vitesse sur pompe.",
        },
        { name: "pumpPowerKw", missing: "Renseigner la puissance de la pompe." },
      ],
      compute: ({ action: a }) => {
        const power = toNumber(a.pumpPowerKw);
        const kwhCumac = COEFF_BAR_TH_139 * power;
        return { kwhCumac, calculationLabel: buildCalculationLabel([COEFF_BAR_TH_139, power], kwhCumac) };
      },
    },
  );
}

export function evaluateBarTh143(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet("BAR-TH-143", "Systeme solaire combine", project, action, enabled, {
    commonOptions: { supportedHousingTypes: ["Maison individuelle"] },
    requirements: [
      {
        name: "solarCombinedSystem",
        missing: "Preciser si le projet concerne un systeme solaire combine.",
        blockerOnNo: "La fiche BAR-TH-143 s'applique a un systeme solaire combine.",
      },
      {
        name: "lowTempEmitters",
        missing: "Preciser si le systeme est couple a des emetteurs basse temperature.",
        blockerOnNo: "Le systeme doit etre couple a des emetteurs de chauffage central basse temperature.",
      },
      {
        name: "collectorProductivityEligible",
        missing: "Preciser si les capteurs respectent la productivite officielle et s'ils ne sont pas hybrides.",
        blockerOnNo: "Les capteurs doivent atteindre la productivite officielle et ne pas etre hybrides.",
      },
      {
        name: "collectorAreaEligible",
        missing: "Preciser si la surface de capteurs est superieure ou egale a 8 m2.",
        blockerOnNo: "La surface de capteurs doit etre superieure ou egale a 8 m2.",
      },
      {
        name: "storageEligible",
        missing: "Preciser si la capacite de stockage depasse 400 L.",
        blockerOnNo: "La capacite de stockage doit etre strictement superieure a 400 L.",
      },
    ],
    compute: ({ derived }) => {
      if (!derived.climateZone) return undefined;
      const kwhCumac = COEFF_BAR_TH_143[derived.climateZone];
      return { kwhCumac, calculationLabel: buildCalculationLabel([kwhCumac], kwhCumac) };
    },
  });
}

export function evaluateBarTh148(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-TH-148",
    "Chauffe-eau thermodynamique a accumulation",
    project,
    action,
    enabled,
    {
      commonOptions: { supportedHousingTypes: ["Maison individuelle", "Appartement"] },
      requirements: [
        {
          name: "thermodynamicWaterHeater",
          missing: "Preciser si le projet concerne un chauffe-eau thermodynamique a accumulation.",
          blockerOnNo: "La fiche BAR-TH-148 s'applique au chauffe-eau thermodynamique a accumulation.",
        },
        { name: "drawOffProfile", missing: "Preciser le profil de soutirage." },
        {
          name: "waterHeatingEfficiencyCompliant",
          missing: "Preciser si l'efficacite energetique respecte le seuil officiel du profil declare.",
          blockerOnNo:
            "L'efficacite energetique pour le chauffage de l'eau ne respecte pas le seuil minimal de la fiche.",
        },
      ],
      compute: ({ project: p }) => {
        const coefficient = COEFF_BAR_TH_148[String(p.housingType || "")];
        if (coefficient == null) return undefined;
        return { kwhCumac: coefficient, calculationLabel: buildCalculationLabel([coefficient], coefficient) };
      },
    },
  );
}

export function evaluateBarTh155(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet("BAR-TH-155", "Ventilation hybride hygroreglable", project, action, enabled, {
    commonOptions: { supportedHousingTypes: ["Appartement"] },
    requirements: [
      {
        name: "hybridVentilation",
        missing: "Preciser si le projet met en place une ventilation hybride hygroreglable.",
        blockerOnNo: "La fiche BAR-TH-155 s'applique a une ventilation hybride hygroreglable.",
      },
      {
        name: "naturalOrNoVentilation",
        missing: "Preciser si l'appartement etait equipe d'une ventilation naturelle ou sans systeme de ventilation.",
        blockerOnNo: "La fiche vise des appartements equipes d'une ventilation naturelle ou sans systeme de ventilation.",
      },
      { name: "apartmentCount", missing: "Renseigner le nombre d'appartements." },
      { name: "installationType", missing: "Preciser le type d'installation." },
      { name: "extractorType", missing: "Preciser le type d'extracteur." },
      {
        name: "technicalOpinionValid",
        missing: "Preciser si le systeme beneficie d'un avis technique en cours de validite.",
        blockerOnNo: "Le systeme doit beneficier d'un avis technique du CCFAT en cours de validite ou equivalent.",
      },
    ],
    compute: ({ action: a, derived }) => {
      if (!derived.climateZone) return undefined;
      const apartmentCount = toNumber(a.apartmentCount);
      const baseAmount = COEFF_BAR_TH_155.base[derived.climateZone];
      const factorR = COEFF_BAR_TH_155.factorR[String(a.installationType)]?.[String(a.extractorType)];
      if (factorR == null) return undefined;
      const kwhCumac = baseAmount * apartmentCount * factorR;
      return {
        kwhCumac,
        calculationLabel: buildCalculationLabel([baseAmount, apartmentCount, factorR], kwhCumac),
      };
    },
  });
}

export function evaluateBarTh158(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-TH-158",
    "Emetteur electrique a regulation electronique a fonctions avancees",
    project,
    action,
    enabled,
    {
      commonOptions: { supportedHousingTypes: ["Maison individuelle", "Appartement"] },
      requirements: [
        {
          name: "advancedElectricEmitter",
          missing: "Preciser si le projet installe des emetteurs electriques a regulation avancee.",
          blockerOnNo: "La fiche BAR-TH-158 s'applique a des emetteurs electriques a regulation avancee.",
        },
        { name: "emitterCount", missing: "Renseigner le nombre d'emetteurs remplaces." },
      ],
      compute: ({ project: p, action: a, derived }) => {
        if (!derived.climateZone) return undefined;
        const coefficient = COEFF_BAR_TH_158[String(p.housingType || "")]?.[derived.climateZone];
        if (coefficient == null) return undefined;
        const emitterCount = toNumber(a.emitterCount);
        const kwhCumac = coefficient * emitterCount;
        return { kwhCumac, calculationLabel: buildCalculationLabel([coefficient, emitterCount], kwhCumac) };
      },
    },
  );
}

export function evaluateBarTh159(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-TH-159",
    "Pompe a chaleur hybride individuelle",
    project,
    action,
    enabled,
    {
      commonOptions: { supportedHousingTypes: ["Maison individuelle", "Appartement"] },
      requirements: [
        {
          name: "hybridPac",
          missing: "Preciser si le projet concerne une PAC hybride individuelle.",
          blockerOnNo: "La fiche BAR-TH-159 s'applique a une PAC hybride individuelle.",
        },
        { name: "usage", missing: "Preciser l'usage couvert par la PAC." },
        { name: "etasBand", missing: "Preciser la tranche d'efficacite energetique saisonniere (Etas)." },
        {
          name: "coverRateAtLeast70",
          missing: "Preciser si le taux de couverture de la PAC hors appoint est superieur ou egal a 70 %.",
          blockerOnNo: "Le taux de couverture de la PAC hors appoint doit etre superieur ou egal a 70 %.",
        },
        {
          name: "regulatorEligible",
          missing: "Preciser si le regulateur releve d'une classe IV a VIII.",
          blockerOnNo: "Le regulateur doit relever d'une classe IV, V, VI, VII ou VIII.",
        },
        {
          name: "mediumHighTemperaturePac",
          missing: "Preciser si la PAC est de type moyenne ou haute temperature.",
          blockerOnNo: "La PAC doit etre de type moyenne ou haute temperature.",
        },
        { name: "heatedSurface", missing: "Renseigner la surface chauffee." },
      ],
      compute: ({ project: p, action: a, derived }) => {
        if (!derived.climateZone) return undefined;
        const surface = toNumber(a.heatedSurface);
        const housingType = String(p.housingType || "");
        const housingTable = (COEFF_BAR_TH_159 as Record<string, unknown>)[housingType] as
          | Record<string, Record<"H1" | "H2" | "H3", number>>
          | undefined;
        const coefficient = housingTable?.[String(a.etasBand)]?.[derived.climateZone];
        if (coefficient == null) return undefined;
        const surfaceFactor = COEFF_BAR_TH_159.surfaceFactor(housingType, surface);
        const kwhCumac = coefficient * surfaceFactor;
        return { kwhCumac, calculationLabel: buildCalculationLabel([coefficient, surfaceFactor], kwhCumac) };
      },
    },
  );
}

export function evaluateBarTh161(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-TH-161",
    "Isolation de points singuliers d'un reseau",
    project,
    action,
    enabled,
    {
      commonOptions: { supportedHousingTypes: [...COLLECTIVE_HOUSING_TYPES] },
      requirements: [
        {
          name: "networkSingularPointInsulation",
          missing: "Preciser si le projet isole des points singuliers d'un reseau.",
          blockerOnNo: "La fiche BAR-TH-161 s'applique a l'isolation de points singuliers.",
        },
        { name: "dnBand", missing: "Preciser le diametre nominal (DN) des points singuliers isoles." },
        { name: "fluidTemperatureBand", missing: "Preciser la temperature du fluide caloporteur." },
        { name: "singularPointCount", missing: "Renseigner le nombre de points singuliers isoles." },
        {
          name: "insulationCompliant",
          missing: "Preciser si les housses respectent les exigences officielles de la fiche.",
          blockerOnNo: "Les housses doivent respecter les exigences officielles de resistance thermique et de composition.",
        },
      ],
      compute: ({ action: a, derived }) => {
        if (!derived.climateZone) return undefined;
        const count = toNumber(a.singularPointCount);
        const coefficient =
          COEFF_BAR_TH_161[String(a.dnBand)]?.[String(a.fluidTemperatureBand)]?.[derived.climateZone];
        if (coefficient == null) return undefined;
        const kwhCumac = coefficient * count;
        return { kwhCumac, calculationLabel: buildCalculationLabel([coefficient, count], kwhCumac) };
      },
    },
  );
}

export function evaluateBarTh162(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-TH-162",
    "Systeme energetique avec capteurs photovoltaiques et thermiques",
    project,
    action,
    enabled,
    {
      commonOptions: { supportedHousingTypes: ["Maison individuelle"] },
      requirements: [
        {
          name: "pvtSystem",
          missing: "Preciser si le projet concerne un systeme energetique photovoltaique + thermique.",
          blockerOnNo: "La fiche BAR-TH-162 s'applique a un systeme energetique avec capteurs photovoltaiques et thermiques.",
        },
        {
          name: "hybridCollectorsOnly",
          missing: "Preciser si les capteurs sont exclusivement hybrides et atteignent la productivite officielle.",
          blockerOnNo: "Les capteurs doivent etre exclusivement hybrides et atteindre la productivite officielle minimale.",
        },
        {
          name: "minimumAreaReached",
          missing: "Preciser si la surface totale de capteurs atteint 6 m2.",
          blockerOnNo: "La surface totale de capteurs hybrides doit etre au minimum de 6 m2.",
        },
      ],
      compute: () => {
        const kwhCumac = COEFF_BAR_TH_162;
        return { kwhCumac, calculationLabel: buildCalculationLabel([kwhCumac], kwhCumac) };
      },
    },
  );
}

export function evaluateBarTh165(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet("BAR-TH-165", "Chaudiere biomasse collective", project, action, enabled, {
    commonOptions: { supportedHousingTypes: [...COLLECTIVE_HOUSING_TYPES] },
    requirements: [
      {
        name: "collectiveBiomassBoiler",
        missing: "Preciser si le projet concerne une chaudiere biomasse collective.",
        blockerOnNo: "La fiche BAR-TH-165 s'applique a une chaudiere biomasse collective.",
      },
      { name: "powerBand", missing: "Preciser la tranche de puissance thermique nominale." },
      { name: "usefulHeatProducedKwh", missing: "Renseigner la chaleur nette utile annuelle produite Q." },
    ],
    compute: ({ action: a }) => {
      const heat = toNumber(a.usefulHeatProducedKwh);
      const factor = COEFF_BAR_TH_165[String(a.powerBand)];
      if (factor == null) return undefined;
      const kwhCumac = heat * factor;
      return { kwhCumac, calculationLabel: buildCalculationLabel([heat, factor], kwhCumac) };
    },
  });
}

export function evaluateBarTh168(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet("BAR-TH-168", "Dispositif solaire thermique", project, action, enabled, {
    commonOptions: { supportedHousingTypes: ["Maison individuelle"] },
    requirements: [
      {
        name: "solarThermalDevice",
        missing: "Preciser si le projet concerne un dispositif solaire thermique.",
        blockerOnNo: "La fiche BAR-TH-168 s'applique a un dispositif solaire thermique.",
      },
      { name: "usage", missing: "Preciser l'usage du dispositif solaire thermique." },
      {
        name: "collectorAreaEligible",
        missing: "Preciser si la surface de capteurs respecte le seuil officiel.",
        blockerOnNo: "La surface de capteurs ne respecte pas le seuil officiel de la fiche.",
      },
      {
        name: "collectorPerformanceEligible",
        missing: "Preciser si les capteurs et le dispositif respectent les exigences officielles.",
        blockerOnNo: "Les capteurs ou le dispositif ne respectent pas les exigences officielles de la fiche.",
      },
      { name: "collectorAreaM2", missing: "Renseigner la surface de capteurs solaires." },
    ],
    compute: ({ action: a, derived }) => {
      if (!derived.climateZone) return undefined;
      const area = toNumber(a.collectorAreaM2);
      const coefficient =
        a.usage === "ECS et chauffage"
          ? COEFF_BAR_TH_168.ecsAndHeating[derived.climateZone]
          : COEFF_BAR_TH_168.ecsOnly[derived.climateZone];
      const kwhCumac = coefficient * area;
      return { kwhCumac, calculationLabel: buildCalculationLabel([coefficient, area], kwhCumac) };
    },
  });
}

export function evaluateBarTh169(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-TH-169",
    "Pompe a chaleur collective pour l'eau chaude sanitaire",
    project,
    action,
    enabled,
    {
      commonOptions: { supportedHousingTypes: [...COLLECTIVE_HOUSING_TYPES] },
      requirements: [
        {
          name: "collectiveDhwPac",
          missing: "Preciser si le projet concerne une PAC collective pour l'ECS.",
          blockerOnNo: "La fiche BAR-TH-169 s'applique a une PAC collective pour l'ECS.",
        },
        { name: "dwellingCount", missing: "Renseigner le nombre de logements concernes." },
        { name: "copBand", missing: "Preciser la tranche de COP de la PAC." },
        { name: "factorR", missing: "Renseigner le facteur R." },
      ],
      compute: ({ action: a, derived }) => {
        if (!derived.climateZone) return undefined;
        const dwellingCount = toNumber(a.dwellingCount);
        const factorR = toNumber(a.factorR);
        const coefficient = COEFF_BAR_TH_169[String(a.copBand)]?.[derived.climateZone];
        if (coefficient == null) return undefined;
        const kwhCumac = coefficient * dwellingCount * factorR;
        return {
          kwhCumac,
          calculationLabel: buildCalculationLabel([coefficient, dwellingCount, factorR], kwhCumac),
        };
      },
    },
  );
}

export function evaluateBarTh170(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-TH-170",
    "Recuperation de chaleur fatale issue de serveurs informatiques pour l'ECS collective",
    project,
    action,
    enabled,
    {
      commonOptions: { supportedHousingTypes: [...COLLECTIVE_HOUSING_TYPES] },
      requirements: [
        {
          name: "serverHeatRecovery",
          missing: "Preciser si le projet recupere la chaleur de serveurs pour l'ECS collective.",
          blockerOnNo: "La fiche BAR-TH-170 s'applique a la recuperation de chaleur de serveurs pour l'ECS collective.",
        },
        { name: "electricalPowerKw", missing: "Renseigner la puissance electrique de l'installation." },
      ],
      compute: ({ action: a }) => {
        const power = toNumber(a.electricalPowerKw);
        const kwhCumac = COEFF_BAR_TH_170 * power;
        return { kwhCumac, calculationLabel: buildCalculationLabel([COEFF_BAR_TH_170, power], kwhCumac) };
      },
    },
  );
}

export function evaluateBarTh171(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet("BAR-TH-171", "Pompe a chaleur de type air/eau", project, action, enabled, {
    commonOptions: { supportedHousingTypes: ["Maison individuelle", "Appartement"] },
    requirements: [
      {
        name: "airWaterPac",
        missing: "Preciser si la PAC installee est de type air/eau.",
        blockerOnNo: "La fiche BAR-TH-171 s'applique aux PAC air/eau.",
      },
      { name: "temperatureType", missing: "Preciser l'application de la PAC." },
      { name: "usage", missing: "Preciser l'usage couvert par la PAC." },
      { name: "etasBand", missing: "Preciser la tranche d'efficacite energetique saisonniere (Etas)." },
      {
        name: "regulatorEligible",
        missing: "Preciser si le regulateur releve d'une classe IV a VIII.",
        blockerOnNo: "Le regulateur doit relever d'une classe IV, V, VI, VII ou VIII.",
      },
      {
        name: "apartmentHeatingMode",
        missing: "Preciser si l'appartement est equipe d'un chauffage individuel.",
        blockerOnNo: "La fiche BAR-TH-171 ne s'applique pas a un appartement avec chauffage collectif.",
        showWhen: (_a, p) => p.housingType === "Appartement",
      },
      { name: "heatedSurface", missing: "Renseigner la surface chauffee." },
    ],
    compute: ({ project: p, action: a, derived }) => {
      if (!derived.climateZone) return undefined;
      const surface = toNumber(a.heatedSurface);
      const housingType = String(p.housingType || "");
      const housingTable = (COEFF_BAR_TH_171 as Record<string, unknown>)[housingType] as
        | Record<string, number>
        | undefined;
      const baseAmount = housingTable?.[String(a.etasBand)];
      if (baseAmount == null) return undefined;
      const surfaceFactor = COEFF_BAR_TH_171.surfaceFactor(housingType, surface);
      const zoneFactor = COEFF_BAR_TH_171.zoneFactor[derived.climateZone];
      const kwhCumac = baseAmount * surfaceFactor * zoneFactor;
      return {
        kwhCumac,
        calculationLabel: buildCalculationLabel([baseAmount, surfaceFactor, zoneFactor], kwhCumac),
      };
    },
  });
}

export function evaluateBarTh172(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-TH-172",
    "Pompe a chaleur de type eau/eau ou eau glycollee/eau",
    project,
    action,
    enabled,
    {
      commonOptions: { supportedHousingTypes: ["Maison individuelle", "Appartement"] },
      requirements: [
        {
          name: "waterWaterPac",
          missing: "Preciser si la PAC installee est de type eau/eau ou eau glycollee/eau.",
          blockerOnNo: "La fiche BAR-TH-172 s'applique aux PAC eau/eau ou eau glycollee/eau.",
        },
        { name: "temperatureType", missing: "Preciser l'application de la PAC." },
        { name: "usage", missing: "Preciser l'usage couvert par la PAC." },
        { name: "performanceBand", missing: "Preciser la tranche de performance de la PAC." },
        {
          name: "regulatorEligible",
          missing: "Preciser si le regulateur releve d'une classe IV a VIII.",
          blockerOnNo: "Le regulateur doit relever d'une classe IV, V, VI, VII ou VIII.",
        },
        { name: "heatedSurface", missing: "Renseigner la surface chauffee." },
      ],
      compute: ({ action: a, derived }) => {
        if (!derived.climateZone) return undefined;
        const surface = toNumber(a.heatedSurface);
        const performanceBand = String(a.performanceBand);
        const baseAmount = (COEFF_BAR_TH_172 as Record<string, unknown>)[performanceBand] as
          | number
          | undefined;
        if (baseAmount == null) return undefined;
        const surfaceFactor = COEFF_BAR_TH_172.surfaceFactor(surface);
        const zoneFactor = COEFF_BAR_TH_172.zoneFactor[derived.climateZone];
        const kwhCumac = baseAmount * surfaceFactor * zoneFactor;
        return {
          kwhCumac,
          calculationLabel: buildCalculationLabel([baseAmount, surfaceFactor, zoneFactor], kwhCumac),
        };
      },
    },
  );
}

export function evaluateBarTh173(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-TH-173",
    "Systeme de regulation par programmation horaire piece par piece",
    project,
    action,
    enabled,
    {
      commonOptions: { supportedHousingTypes: ["Maison individuelle", "Appartement"] },
      requirements: [
        {
          name: "roomByRoomProgramming",
          missing: "Preciser si le projet installe une programmation horaire piece par piece.",
          blockerOnNo: "La fiche BAR-TH-173 s'applique a une programmation horaire piece par piece.",
        },
        { name: "heatingType", missing: "Preciser le type de chauffage." },
        {
          name: "apartmentHeatingMode",
          missing: "Preciser si l'appartement est equipe d'un chauffage individuel.",
          blockerOnNo: "La fiche BAR-TH-173 ne s'applique pas a un appartement avec chauffage collectif.",
          showWhen: (_a, p) => p.housingType === "Appartement",
        },
        { name: "equippedEmitterCount", missing: "Renseigner le nombre d'emetteurs equipes." },
      ],
      compute: ({ project: p, action: a, derived }) => {
        if (!derived.climateZone) return undefined;
        const emitterCount = toNumber(a.equippedEmitterCount);
        const coefficient = COEFF_BAR_TH_173[String(p.housingType || "")]?.[derived.climateZone];
        if (coefficient == null) return undefined;
        const kwhCumac = coefficient * emitterCount;
        return { kwhCumac, calculationLabel: buildCalculationLabel([coefficient, emitterCount], kwhCumac) };
      },
    },
  );
}

export function evaluateBarTh176(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-TH-176",
    "Systeme de regulation de la consommation d'un chauffe-eau electrique a effet Joule",
    project,
    action,
    enabled,
    {
      commonOptions: { supportedHousingTypes: ["Maison individuelle", "Appartement"] },
      requirements: [
        {
          name: "jouleWaterHeaterControl",
          missing: "Preciser si le projet installe un systeme de regulation sur chauffe-eau electrique a effet Joule.",
          blockerOnNo: "La fiche BAR-TH-176 s'applique a un systeme de regulation de chauffe-eau electrique a effet Joule.",
        },
        {
          name: "classACompliantSystem",
          missing: "Preciser si le systeme repond aux fonctionnalites requises de classe A.",
          blockerOnNo: "La fiche BAR-TH-176 exige un dispositif repondant aux fonctionnalites de classe A.",
        },
        { name: "waterHeaterTankSize", missing: "Preciser la taille du chauffe-eau." },
      ],
      compute: ({ project: p, action: a, derived }) => {
        if (!derived.climateZone) return undefined;
        const housingType = String(p.housingType || "") as "Maison individuelle" | "Appartement";
        const housingMap = (COEFF_BAR_TH_176 as Record<string, unknown>)[housingType] as
          | { H1: number; H2: number; H3: number }
          | undefined;
        const coefficient = housingMap?.[derived.climateZone];
        const factor = COEFF_BAR_TH_176.tankFactor[String(a.waterHeaterTankSize)];
        if (coefficient == null || factor == null) return undefined;
        const kwhCumac = coefficient * factor;
        return { kwhCumac, calculationLabel: buildCalculationLabel([coefficient, factor], kwhCumac) };
      },
    },
  );
}

export function evaluateBarTh177(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-TH-177",
    "Renovation thermique globale d'un batiment residentiel collectif",
    project,
    action,
    enabled,
    {
      commonOptions: { supportedHousingTypes: [...COLLECTIVE_HOUSING_TYPES] },
      requirements: [
        {
          name: "globalCollectiveRenovation",
          missing: "Preciser si le projet releve d'une renovation thermique globale.",
          blockerOnNo: "La fiche BAR-TH-177 s'applique a une renovation thermique globale d'un batiment residentiel collectif.",
        },
        {
          name: "atLeastThreeDistinctTaxHouseholds",
          missing: "Preciser si le batiment comprend au moins 3 foyers fiscaux distincts rattaches a des logements distincts.",
          blockerOnNo: "Le batiment doit comprendre au moins 3 foyers fiscaux distincts rattaches a des logements distincts.",
        },
        {
          name: "energyAuditDone",
          missing: "Preciser si un audit energetique prealable a ete realise.",
          blockerOnNo: "Un audit energetique prealable est necessaire.",
        },
        {
          name: "cepBelow331",
          missing: "Preciser si le Cep projet est inferieur a 331 kWh/m2.an.",
          blockerOnNo: "La consommation conventionnelle apres travaux doit etre inferieure a 331 kWh/m2.an.",
        },
        {
          name: "energyGainAtLeast35",
          missing: "Preciser si le gain energetique du projet est d'au moins 35 %.",
          blockerOnNo: "Le gain energetique du projet doit etre d'au moins 35 %.",
        },
        {
          name: "ghgReduced",
          missing: "Preciser si les emissions de GES apres renovation sont inferieures ou egales a l'etat initial.",
          blockerOnNo: "Les emissions de gaz a effet de serre apres renovation doivent etre inferieures ou egales a l'etat initial.",
        },
        {
          name: "fossilHeatingBlocked",
          missing: "Preciser si le projet evite les cas d'exclusion sur charbon, fioul ou gaz > 30 % hors reseau de chaleur.",
          blockerOnNo: "Le projet entre dans un cas d'exclusion de la fiche sur les equipements de chauffage ou d'ECS.",
        },
      ],
      compute: ({ project: p }) => {
        const shab = toNumber(p.buildingSurface);
        const kwhCumac = COEFF_BAR_TH_177 * shab;
        return { kwhCumac, calculationLabel: buildCalculationLabel([COEFF_BAR_TH_177, shab], kwhCumac) };
      },
    },
  );
}

export function evaluateBarTh178(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet("BAR-TH-178", "Systeme geothermique", project, action, enabled, {
    commonOptions: { supportedHousingTypes: [...COLLECTIVE_HOUSING_TYPES] },
    requirements: [
      {
        name: "geothermalSystem",
        missing: "Preciser si le projet concerne un systeme geothermique.",
        blockerOnNo: "La fiche BAR-TH-178 s'applique a un systeme geothermique.",
      },
      { name: "dwellingCount", missing: "Renseigner le nombre d'appartements chauffes." },
      { name: "usage", missing: "Preciser l'usage couvert." },
      { name: "powerBand", missing: "Preciser la tranche de puissance thermique nominale totale des PAC." },
      { name: "performanceBand", missing: "Preciser la tranche de performance retenue." },
      { name: "pacPowerSharePercent", missing: "Renseigner la part de puissance PAC dans la nouvelle chaufferie." },
    ],
    compute: ({ action: a, derived }) => {
      if (!derived.climateZone) return undefined;
      const dwellingCount = toNumber(a.dwellingCount);
      const powerSharePercent = toNumber(a.pacPowerSharePercent);
      const table =
        a.powerBand === "400 ou moins" ? COEFF_BAR_TH_178.underOrEqual400 : COEFF_BAR_TH_178.over400;
      const baseAmount =
        table[String(a.performanceBand)]?.[String(a.usage)]?.[derived.climateZone];
      if (baseAmount == null) return undefined;
      const ratio = powerSharePercent > 0 ? powerSharePercent / 100 : 0;
      const factorR = ratio < 0.4 ? ratio : 1;
      const kwhCumac = baseAmount * dwellingCount * factorR;
      return {
        kwhCumac,
        calculationLabel: buildCalculationLabel([baseAmount, dwellingCount, factorR], kwhCumac),
      };
    },
  });
}

export function evaluateBarTh179(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-TH-179",
    "Pompe a chaleur collective de type air/eau",
    project,
    action,
    enabled,
    {
      commonOptions: { supportedHousingTypes: [...COLLECTIVE_HOUSING_TYPES] },
      requirements: [
        {
          name: "collectiveAirWaterPac",
          missing: "Preciser si le projet concerne une PAC collective air/eau.",
          blockerOnNo: "La fiche BAR-TH-179 s'applique a une PAC collective air/eau.",
        },
        { name: "dwellingCount", missing: "Renseigner le nombre de logements desservis." },
        { name: "usage", missing: "Preciser l'usage couvert par la PAC." },
        { name: "etasBand", missing: "Preciser la tranche d'efficacite energetique saisonniere (Etas)." },
        { name: "pacPowerSharePercent", missing: "Renseigner la part de puissance PAC dans la nouvelle chaufferie." },
      ],
      compute: ({ action: a, derived }) => {
        if (!derived.climateZone) return undefined;
        const dwellingCount = toNumber(a.dwellingCount);
        const powerSharePercent = toNumber(a.pacPowerSharePercent);
        const baseAmount =
          COEFF_BAR_TH_179.under400[String(a.etasBand)]?.[String(a.usage)]?.[derived.climateZone];
        if (baseAmount == null) return undefined;
        const ratio = powerSharePercent > 0 ? powerSharePercent / 100 : 0;
        const factorR = ratio < 0.4 ? ratio : 1;
        const kwhCumac = baseAmount * dwellingCount * factorR;
        return {
          kwhCumac,
          calculationLabel: buildCalculationLabel([baseAmount, dwellingCount, factorR], kwhCumac),
        };
      },
    },
  );
}

export function evaluateBarTh180(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateGenericConfigSheet(
    "BAR-TH-180",
    "Pompe a chaleur collective de type eau/eau ou eau glycollee/eau",
    project,
    action,
    enabled,
    {
      commonOptions: { supportedHousingTypes: [...COLLECTIVE_HOUSING_TYPES] },
      requirements: [
        {
          name: "collectiveWaterWaterPac",
          missing: "Preciser si le projet concerne une PAC collective eau/eau ou eau glycollee/eau.",
          blockerOnNo: "La fiche BAR-TH-180 s'applique a une PAC collective eau/eau ou eau glycollee/eau.",
        },
        { name: "dwellingCount", missing: "Renseigner le nombre de logements desservis." },
        { name: "usage", missing: "Preciser l'usage couvert par la PAC." },
        { name: "powerBand", missing: "Preciser la puissance thermique nominale totale des PAC." },
        { name: "performanceBand", missing: "Preciser la tranche de performance retenue." },
        { name: "pacPowerSharePercent", missing: "Renseigner la part de puissance PAC dans la nouvelle chaufferie." },
      ],
      compute: ({ action: a, derived }) => {
        if (!derived.climateZone) return undefined;
        const dwellingCount = toNumber(a.dwellingCount);
        const powerSharePercent = toNumber(a.pacPowerSharePercent);
        const table =
          a.powerBand === "Inferieure ou egale a 400 kW"
            ? COEFF_BAR_TH_180.underOrEqual400
            : COEFF_BAR_TH_180.over400;
        const baseAmount =
          table[String(a.performanceBand)]?.[String(a.usage)]?.[derived.climateZone];
        if (baseAmount == null) return undefined;
        const ratio = powerSharePercent > 0 ? powerSharePercent / 100 : 0;
        const factorR = ratio < 0.4 ? ratio : 1;
        const kwhCumac = baseAmount * dwellingCount * factorR;
        return {
          kwhCumac,
          calculationLabel: buildCalculationLabel([baseAmount, dwellingCount, factorR], kwhCumac),
        };
      },
    },
  );
}

// Silence unused-import warning when only used as type
export type { HousingType };
