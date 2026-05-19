/**
 * Fonctions utilitaires du moteur CEE.
 *
 * Port TypeScript de `simulateur-cee-main/js/core.js`. Les fonctions qui
 * dépendaient d'un `state.project` global ont été refactorées en fonctions
 * pures qui prennent le `Project` en argument explicite.
 */
import {
  CURRENT_YEAR,
  DEPARTMENT_TO_CLIMATE,
  RESOURCE_THRESHOLDS_2026,
  RESOURCE_CATEGORY_KEYS,
  RESOURCE_CATEGORIES,
  STATUS,
  type ResourceThresholdRow,
} from "./config";
import type {
  ClimateZone,
  DerivedProjectData,
  Evaluation,
  Project,
  ResourceCategoryKey,
  ResourceRegion,
} from "./types";

// === Initialisation d'une évaluation =====================================

export function baseEvaluation(
  code: string,
  title: string,
  enabled: boolean,
): Evaluation {
  return {
    code,
    title,
    enabled,
    status: STATUS.POTENTIAL,
    kwhCumac: null,
    euroAmount: null,
    standardKwhCumac: null,
    standardEuroAmount: null,
    blockers: [],
    missing: [],
    notes: [],
    calculationLabel: "",
    standardCalculationLabel: "",
    calculationEngineMissing: false,
    coupDePouce: null,
  };
}

// === Numérique / formatage ==============================================

export function toNumber(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

export function formatKwh(value: number | null): string {
  return value == null ? "Non calcule" : Math.round(value).toLocaleString("fr-FR");
}

export function formatEuros(value: number | null): string {
  return value == null
    ? "Non calculee"
    : value.toLocaleString("fr-FR", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      });
}

export function formatFormulaNumber(value: number, decimals = 2): string {
  if (!Number.isFinite(value)) return String(value ?? "");
  const rounded = Math.round(value * 10 ** decimals) / 10 ** decimals;
  const hasDecimals = Math.abs(rounded % 1) > 0;
  return rounded.toLocaleString("fr-FR", {
    minimumFractionDigits: 0,
    maximumFractionDigits: hasDecimals ? decimals : 0,
  });
}

export function buildCalculationLabel(
  parts: (number | string)[],
  result: number,
  unit = "kWh cumac",
): string {
  return `${parts
    .map((part) => (typeof part === "number" ? formatFormulaNumber(part) : part))
    .join(" x ")} = ${formatFormulaNumber(result)} ${unit}`;
}

// === Dérivation depuis le projet ========================================

export function deriveClimateFromPostalCode(postalCode: string | undefined): {
  climateZone: ClimateZone | "";
  departmentCode: string;
  message: string;
} {
  const cleanPostalCode = String(postalCode || "").replace(/\s+/g, "");
  if (!cleanPostalCode) {
    return {
      climateZone: "",
      departmentCode: "",
      message: "A deduire depuis le code postal",
    };
  }
  if (!/^\d{5}$/.test(cleanPostalCode)) {
    return {
      climateZone: "",
      departmentCode: "",
      message: "Le code postal doit contenir 5 chiffres.",
    };
  }
  if (cleanPostalCode.startsWith("97") || cleanPostalCode.startsWith("98")) {
    return {
      climateZone: "",
      departmentCode: cleanPostalCode.slice(0, 3),
      message:
        "Ce simulateur est actuellement concu pour la France metropolitaine uniquement. Les projets Outre-mer ne sont pas pris en charge.",
    };
  }

  const departmentCode = cleanPostalCode.startsWith("20")
    ? "20"
    : cleanPostalCode.slice(0, 2);
  const climateZone = DEPARTMENT_TO_CLIMATE[departmentCode];
  if (!climateZone) {
    return {
      climateZone: "",
      departmentCode,
      message: "Aucune zone climatique n'a pu etre deduite de ce code postal.",
    };
  }
  return { climateZone, departmentCode, message: "" };
}

export function deriveResourceRegionFromPostalCode(
  postalCode: string | undefined,
): ResourceRegion | "" {
  const cleanPostalCode = String(postalCode || "").replace(/\s+/g, "");
  if (!/^\d{5}$/.test(cleanPostalCode)) return "";
  if (cleanPostalCode.startsWith("97") || cleanPostalCode.startsWith("98")) {
    return "NON_IDF";
  }
  const departmentCode = cleanPostalCode.startsWith("20")
    ? "20"
    : cleanPostalCode.slice(0, 2);
  return ["75", "77", "78", "91", "92", "93", "94", "95"].includes(departmentCode)
    ? "IDF"
    : "NON_IDF";
}

export function getProjectDerivedData(project: Project): DerivedProjectData {
  const climateData = deriveClimateFromPostalCode(project.postalCode);
  const constructionYear = toNumber(project.constructionYear);
  const isOlderThanTwoYears =
    constructionYear > 0 && constructionYear <= CURRENT_YEAR - 2;

  return {
    climateZone: climateData.climateZone,
    departmentCode: climateData.departmentCode,
    postalMessage: climateData.message,
    isOlderThanTwoYears,
    buildingAgeLabel: constructionYear
      ? isOlderThanTwoYears
        ? `Construit en ${constructionYear} - plus de 2 ans`
        : `Construit en ${constructionYear} - trop recent pour les fiches actuelles`
      : "A deduire a partir de l'annee de construction",
  };
}

export function getResourceThresholds(
  regionKey: ResourceRegion | "",
  householdSize: number | string | undefined,
): ResourceThresholdRow | null {
  if (!regionKey) return null;
  const regionThresholds = RESOURCE_THRESHOLDS_2026[regionKey];
  const size = toNumber(householdSize);
  if (!regionThresholds || !size) return null;

  if (size <= 5) {
    return (regionThresholds.base[size as 1 | 2 | 3 | 4 | 5]) || null;
  }

  const extraPeople = size - 5;
  const baseFive = regionThresholds.base[5];
  return {
    veryModest:
      baseFive.veryModest + regionThresholds.extraPerPerson.veryModest * extraPeople,
    modest: baseFive.modest + regionThresholds.extraPerPerson.modest * extraPeople,
    intermediate:
      baseFive.intermediate +
      regionThresholds.extraPerPerson.intermediate * extraPeople,
  };
}

export interface IncomeCategoryMeta {
  key: ResourceCategoryKey;
  label: string;
  className: string;
}

export function getIncomeCategoryMeta(
  incomeBracket: string | undefined,
): IncomeCategoryMeta | null {
  if (!incomeBracket) return null;
  if (
    incomeBracket === RESOURCE_CATEGORY_KEYS.VERY_MODEST ||
    incomeBracket === RESOURCE_CATEGORIES.VERY_MODEST
  ) {
    return {
      key: RESOURCE_CATEGORY_KEYS.VERY_MODEST,
      label: RESOURCE_CATEGORIES.VERY_MODEST,
      className: "income-category-very-modest",
    };
  }
  if (
    incomeBracket === RESOURCE_CATEGORY_KEYS.MODEST ||
    incomeBracket === RESOURCE_CATEGORIES.MODEST
  ) {
    return {
      key: RESOURCE_CATEGORY_KEYS.MODEST,
      label: RESOURCE_CATEGORIES.MODEST,
      className: "income-category-modest",
    };
  }
  if (
    incomeBracket === RESOURCE_CATEGORY_KEYS.INTERMEDIATE ||
    incomeBracket === RESOURCE_CATEGORIES.INTERMEDIATE
  ) {
    return {
      key: RESOURCE_CATEGORY_KEYS.INTERMEDIATE,
      label: RESOURCE_CATEGORIES.INTERMEDIATE,
      className: "income-category-intermediate",
    };
  }
  if (
    incomeBracket === RESOURCE_CATEGORY_KEYS.HIGH ||
    incomeBracket === RESOURCE_CATEGORIES.HIGH
  ) {
    return {
      key: RESOURCE_CATEGORY_KEYS.HIGH,
      label: RESOURCE_CATEGORIES.HIGH,
      className: "income-category-high",
    };
  }
  return null;
}

export function isModestHousehold(project: Project): boolean {
  const incomeCategory = getIncomeCategoryMeta(project.incomeBracket)?.key;
  return (
    incomeCategory === RESOURCE_CATEGORY_KEYS.VERY_MODEST ||
    incomeCategory === RESOURCE_CATEGORY_KEYS.MODEST
  );
}

// === Prix CEE et conversion kWh cumac → € ===============================

export function getEffectiveMwhCumacPrice(project: Project): number | null {
  const incomeCategory = getIncomeCategoryMeta(project.incomeBracket);
  const rawPrice =
    incomeCategory?.key === RESOURCE_CATEGORY_KEYS.VERY_MODEST
      ? project.mwhCumacPricePrecarious
      : project.mwhCumacPrice;
  const price = Number.parseFloat(String(rawPrice).replace(",", "."));
  return Number.isFinite(price) ? price : null;
}

export function getEstimatedPrimeEuros(
  kwhCumac: number,
  mwhCumacPrice: number | null,
): number | null {
  if (mwhCumacPrice == null) return null;
  if (!Number.isFinite(mwhCumacPrice)) return null;
  return (kwhCumac / 1000) * mwhCumacPrice;
}

// === Statut final =======================================================

export function finalizeStatus(
  evaluation: Evaluation,
  enabled: boolean,
  project: Project,
  commonMissingCount = 0,
): void {
  evaluation.blockers = evaluation.blockers.filter(Boolean);
  evaluation.missing = evaluation.missing.filter(Boolean);
  evaluation.notes = [...new Set(evaluation.notes.filter(Boolean))];

  if (evaluation.blockers.length) {
    evaluation.status = STATUS.INELIGIBLE;
    return;
  }
  if (evaluation.missing.length) {
    evaluation.status =
      enabled || commonMissingCount > 0 ? STATUS.CONFIRM : STATUS.POTENTIAL;
    return;
  }
  if (!enabled) {
    evaluation.status = STATUS.POTENTIAL;
    return;
  }
  if (evaluation.kwhCumac != null) {
    const price = getEffectiveMwhCumacPrice(project);
    evaluation.euroAmount = getEstimatedPrimeEuros(evaluation.kwhCumac, price);
    evaluation.status = STATUS.ELIGIBLE;
    return;
  }
  evaluation.status = STATUS.POTENTIAL;
}

// === Règles communes ====================================================

const PROJECT_SYSTEM_LABELS: Record<string, string> = {
  projectSystemHeating: "Chauffage absent",
  projectSystemDhw: "ECS absente",
  projectSystemCooling: "Climatisation / refroidissement absent",
  projectSystemVentilation: "Ventilation absente",
  projectSystemLighting: "Eclairage absent",
  projectSystemFridge: "Systeme frigorifique absent",
  projectSystemRefrigeratedDisplay: "Meubles frigorifiques absents",
  projectSystemDataCenter: "Centre de donnees absent",
  projectSystemHeatNetwork: "Reseau de chaleur absent",
  projectSystemColdNetwork: "Reseau de froid absent",
};

function hasAnySelectedProjectSystems(project: Project): boolean {
  return Object.keys(PROJECT_SYSTEM_LABELS).some(
    (k) => Boolean((project as unknown as Record<string, unknown>)[k]),
  );
}

import type { CommonRulesOptions } from "./types";

export function applyCommonRules(
  evaluation: Evaluation,
  project: Project,
  options: CommonRulesOptions = {},
): DerivedProjectData {
  const derived = getProjectDerivedData(project);
  const supportedBuildingTypes =
    options.supportedBuildingTypes ??
    (evaluation.code.startsWith("BAR-")
      ? (["Habitation"] as const)
      : evaluation.code.startsWith("BAT-")
        ? (["Tertiaire"] as const)
        : []);
  const needsSector =
    options.needsSector ?? supportedBuildingTypes.includes("Tertiaire");
  const needsHousingType =
    options.needsHousingType ?? supportedBuildingTypes.includes("Habitation");
  const requiresExistingBuilding = options.requiresExistingBuilding !== false;
  const supportedSectors = options.supportedSectors ?? null;
  const supportedHousingTypes = options.supportedHousingTypes ?? null;
  const requiredProjectSystemsAll = options.requiredProjectSystemsAll ?? [];
  const requiredProjectSystemsAny = options.requiredProjectSystemsAny ?? [];
  const shouldRequireProjectFields = Boolean(evaluation.enabled);
  const shouldApplyProjectSystems =
    shouldRequireProjectFields || hasAnySelectedProjectSystems(project);

  if (!project.buildingType) {
    if (shouldRequireProjectFields) {
      evaluation.missing.push("Renseigner d'abord le type de batiment.");
    }
  } else if (!supportedBuildingTypes.includes(project.buildingType as never)) {
    evaluation.blockers.push(
      `La fiche ${evaluation.code} n'est pas prevue pour un projet de type ${String(project.buildingType).toLowerCase()}.`,
    );
  }

  if (!project.postalCode) {
    if (shouldRequireProjectFields) {
      evaluation.missing.push("Renseigner le code postal du projet.");
    }
  } else if (!derived.climateZone) {
    evaluation.blockers.push(derived.postalMessage);
  }

  if (!project.constructionYear) {
    if (shouldRequireProjectFields) {
      evaluation.missing.push("Renseigner l'annee de construction.");
    }
  } else if (requiresExistingBuilding && !derived.isOlderThanTwoYears) {
    evaluation.blockers.push("Le batiment doit exister depuis plus de 2 ans.");
  }

  if (needsSector) {
    if (!project.sector) {
      if (shouldRequireProjectFields) {
        evaluation.missing.push("Renseigner le secteur d'activite principal.");
      }
    } else if (supportedSectors && !supportedSectors.includes(project.sector as never)) {
      evaluation.blockers.push("Le secteur choisi n'est pas prevu par cette fiche.");
    }
  }

  if (needsHousingType) {
    if (!project.housingType) {
      if (shouldRequireProjectFields) {
        evaluation.missing.push("Preciser le type d'habitation.");
      }
    } else if (
      supportedHousingTypes &&
      !supportedHousingTypes.includes(project.housingType as never)
    ) {
      evaluation.blockers.push(
        "Le type d'habitation choisi n'est pas prevu par cette fiche.",
      );
    }
  }

  if (shouldApplyProjectSystems) {
    requiredProjectSystemsAll.forEach((systemKey) => {
      if (!(project as unknown as Record<string, unknown>)[systemKey]) {
        evaluation.blockers.push(
          `${PROJECT_SYSTEM_LABELS[systemKey]} non renseigne dans les systemes presents du batiment.`,
        );
      }
    });

    if (
      requiredProjectSystemsAny.length &&
      !requiredProjectSystemsAny.some((systemKey) =>
        Boolean((project as unknown as Record<string, unknown>)[systemKey]),
      )
    ) {
      const labels = requiredProjectSystemsAny
        .map((systemKey) => PROJECT_SYSTEM_LABELS[systemKey])
        .join(", ");
      evaluation.blockers.push(
        `La fiche suppose au moins l'un de ces systemes presents dans le batiment : ${labels}.`,
      );
    }
  }

  return derived;
}
