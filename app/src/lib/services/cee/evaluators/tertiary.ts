/**
 * Évaluateurs tertiaires (BAT-*).
 *
 * Port TypeScript de `simulateur-cee-main/js/evaluators-tertiary.js`.
 * Toutes les références à des fonctions globales sont importées explicitement.
 * Les notes de chaque fiche (auparavant `SHEETS[code].notes`) sont stockées
 * directement dans `sheets.ts` et passées à l'évaluateur via le catalogue.
 */
import {
  applyCommonRules,
  baseEvaluation,
  buildCalculationLabel,
  finalizeStatus,
  formatFormulaNumber,
  getEffectiveMwhCumacPrice,
  getEstimatedPrimeEuros,
  toNumber,
} from "../core";
import {
  CLIMATE_FACTORS_116,
  COEFF_101,
  COEFF_102,
  COEFF_103,
  COEFF_104,
  COEFF_107,
  COEFF_111,
  COEFF_112,
  COEFF_113,
  COEFF_116,
  COEFF_117,
  COEFF_123,
  COEFF_124,
  COEFF_125,
  COEFF_129,
  COEFF_130,
  COEFF_131,
  COEFF_134,
  COEFF_135,
  COEFF_142,
  COEFF_158,
  COEFF_163,
  COEFF_BAT_103,
  COEFF_BAT_105,
  COEFF_BAT_108,
  COEFF_BAT_109,
  COEFF_BAT_110,
  COEFF_BAT_112,
  COEFF_BAT_125,
  COEFF_BAT_126,
  COEFF_BAT_127,
  COEFF_BAT_143,
  COEFF_BAT_145,
  COEFF_BAT_153,
  COEFF_BAT_154,
  COEFF_BAT_156,
  COEFF_BAT_157,
  COEFF_BAT_159,
  COEFF_BAT_161,
  COEFF_BAT_162,
  COEFF_BAT_164,
  COEFF_BAT_SE_104,
  COEFF_BAT_SE_105,
  COEFF_BAT_TH_134_TABLES,
} from "../config";
import type {
  Action,
  ClimateZone,
  CoupDePouceInfo,
  Evaluation,
  Project,
} from "../types";

// === Helpers locaux =====================================================

function findEq130Threshold(systemType: string, deltaT: number): number {
  const tableEntries = Object.keys(COEFF_130[systemType]).map(Number).sort((a, b) => b - a);
  for (const threshold of tableEntries) {
    if (deltaT <= threshold) return threshold;
  }
  return tableEntries[tableEntries.length - 1];
}

function mapEq130Application(applicationType: string): "comfort" | "datacenter" | "refrigeration" {
  if (applicationType === "Climatisation de confort") return "comfort";
  if (applicationType === "Climatisation en datacenter") return "datacenter";
  return "refrigeration";
}

function getEq131SectorBucket(sector: string | undefined): string {
  if (sector === "Bureaux") return "Bureaux";
  if (sector === "Commerces") return "Commerces";
  return "Autres secteurs";
}

function getEq131SectorFactor(sectorBucket: string): number {
  if (sectorBucket === "Bureaux") return 0.75;
  if (sectorBucket === "Commerces") return 1;
  return 0.6;
}

function getBatTh134Coeff(
  application: string,
  condensationFamily: string,
  climateZone: ClimateZone,
): number | null {
  return COEFF_BAT_TH_134_TABLES[application]?.[condensationFamily]?.[climateZone] ?? null;
}

// === Coup de pouce BRCT tertiaire =======================================

function createBrctCoupDePouceBase(
  standardKwhCumac: number,
  standardCalculationLabel: string,
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
    name: "Coup de pouce Chauffage des batiments residentiels collectifs et tertiaires",
    status: "Eligibilite a confirmer",
    factor: 1,
    notes: [
      "Une offre Coup de pouce d'un signataire doit etre acceptee avant signature du devis.",
    ],
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

function applyCoupDePouceBrctTertiary(
  evaluation: Evaluation,
  action: Action,
  project: Project,
): void {
  const coupDePouce = createBrctCoupDePouceBase(
    evaluation.kwhCumac as number,
    evaluation.calculationLabel,
    project,
  );
  const standardKwhCumac = evaluation.kwhCumac as number;
  let bonusKwhCumac: number | null = null;

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

  if (evaluation.code === "BAT-TH-127") {
    const surface = toNumber(action.connectedSurface);
    if (!surface) {
      coupDePouce.missing.push("Renseigner la surface chauffee concernee par la sous-station.");
    } else if (surface <= 7500) {
      bonusKwhCumac = 200 * surface + 9500000;
      coupDePouce.calculationLabel = `${formatFormulaNumber(200)} x ${formatFormulaNumber(surface)} + ${formatFormulaNumber(9500000)} = ${formatFormulaNumber(bonusKwhCumac)} kWh cumac`;
    } else {
      bonusKwhCumac = 800 * surface + 5000000;
      coupDePouce.calculationLabel = `${formatFormulaNumber(800)} x ${formatFormulaNumber(surface)} + ${formatFormulaNumber(5000000)} = ${formatFormulaNumber(bonusKwhCumac)} kWh cumac`;
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
      evaluation.code === "BAT-TH-162" ? 5 : evaluation.code === "BAT-TH-163" ? 3 : 4;
    bonusKwhCumac = standardKwhCumac * factor;
    coupDePouce.factor = factor;
    coupDePouce.calculationLabel = `${formatFormulaNumber(standardKwhCumac)} x ${formatFormulaNumber(factor)} = ${formatFormulaNumber(bonusKwhCumac)} kWh cumac`;
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

  evaluation.standardKwhCumac = standardKwhCumac;
  evaluation.standardEuroAmount = coupDePouce.standardEuroAmount;
  evaluation.standardCalculationLabel = evaluation.calculationLabel;
  evaluation.kwhCumac = coupDePouce.kwhCumac;
  evaluation.euroAmount = coupDePouce.euroAmount;
  evaluation.calculationLabel = coupDePouce.calculationLabel;
  evaluation.coupDePouce = coupDePouce;
}

// === Évaluateurs BAT-* ==================================================

export function evaluate116(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-TH-116", "GTB tertiaire", enabled);
  const derived = applyCommonRules(e, project, {
    supportedSectors: ["Bureaux", "Enseignement", "Commerces", "Hotellerie / Restauration", "Sante"],
    requiredProjectSystemsAny: [
      "projectSystemHeating",
      "projectSystemCooling",
      "projectSystemLighting",
      "projectSystemVentilation",
      "projectSystemDhw",
    ],
  });
  const commonMissingCount = e.missing.length;

  if (!action.operationMode) e.missing.push(enabled ? "Preciser la nature de l'operation GTB." : "");
  if (!action.installedClass) e.missing.push(enabled ? "Preciser la classe du systeme installe." : "");
  if (action.operationMode === "Amelioration d'un systeme existant" && !action.existingClassAtMostC) {
    e.blockers.push("En cas d'amelioration, le systeme existant doit etre au plus de classe C.");
  }

  const usageFields: [string, string, string, string][] = [
    ["heating", "heatingPresent", "heatingSurface", "chauffage"],
    ["dhw", "dhwPresent", "dhwSurface", "ECS"],
    ["cooling", "coolingPresent", "coolingSurface", "refroidissement / climatisation"],
    ["lighting", "lightingPresent", "lightingSurface", "eclairage"],
    ["auxiliaries", "auxiliariesPresent", "auxSurface", "auxiliaires"],
  ];
  const selectedUsages = usageFields.filter(([, presentField]) => Boolean(action[presentField]));

  if (!selectedUsages.length) e.missing.push(enabled ? "Selectionner au moins un usage gere par la GTB." : "");
  selectedUsages.forEach(([, , surfaceField, label]) => {
    if (!toNumber(action[surfaceField])) e.missing.push(enabled ? `Renseigner la surface geree pour ${label}.` : "");
  });

  const installedClass = String(action.installedClass);
  const projectSector = String(project.sector || "");
  const sectorCoefficients = COEFF_116[installedClass]?.[projectSector];
  if (sectorCoefficients) {
    selectedUsages.forEach(([usage, , surfaceField, label]) => {
      if (toNumber(action[surfaceField]) > 0 && sectorCoefficients[usage] == null) {
        e.blockers.push(`Aucun coefficient n'est prevu pour l'usage ${label} sur ce secteur.`);
      }
    });
  }

  if (
    enabled &&
    !e.blockers.length &&
    !e.missing.length &&
    sectorCoefficients &&
    derived.climateZone
  ) {
    const zoneFactor = CLIMATE_FACTORS_116[derived.climateZone];
    e.kwhCumac = selectedUsages.reduce((total, [usage, , surfaceField]) => {
      const surface = toNumber(action[surfaceField]);
      const coeff = sectorCoefficients[usage];
      return coeff != null ? total + surface * coeff * zoneFactor : total;
    }, 0);
    const details = selectedUsages
      .map(([usage, , surfaceField]) => {
        const surface = toNumber(action[surfaceField]);
        const coeff = sectorCoefficients[usage];
        return coeff != null
          ? `${formatFormulaNumber(surface)} x ${formatFormulaNumber(coeff)} x ${formatFormulaNumber(zoneFactor)}`
          : null;
      })
      .filter(Boolean);
    e.calculationLabel = `${details.join(" + ")} = ${formatFormulaNumber(e.kwhCumac)} kWh cumac`;
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluate163(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-TH-163", "PAC air/eau", enabled);
  const derived = applyCommonRules(e, project, { requiredProjectSystemsAll: ["projectSystemHeating"] });
  const commonMissingCount = e.missing.length;

  const heatedSurface = toNumber(action.heatedSurface);
  if (!heatedSurface) e.missing.push(enabled ? "Renseigner la surface totale chauffee." : "");
  if (!action.usage) e.missing.push(enabled ? "Preciser l'usage de la PAC." : "");
  if (action.usage === "ECS uniquement") e.blockers.push("Une PAC utilisee uniquement pour l'ECS n'est pas eligible.");
  if (!action.powerBand) e.missing.push(enabled ? "Preciser si la PAC est <= 400 kW ou > 400 kW." : "");

  let baseCoeff: number | null = null;
  let regimeLabel = "";

  if (action.powerBand === "Inferieure ou egale a 400 kW") {
    if (!action.temperatureType) e.missing.push(enabled ? "Preciser le type de PAC." : "");
    if (!action.etasBand) e.missing.push(enabled ? "Choisir une tranche d'Etas." : "");

    if (action.etasBand === "Inferieur a 111 %") {
      e.blockers.push("Pour une PAC <= 400 kW, cette tranche d'Etas n'est pas eligible.");
    }
    if (
      action.temperatureType === "Basse temperature" &&
      action.etasBand === "De 111 % a moins de 126 %"
    ) {
      e.blockers.push("Pour une PAC basse temperature <= 400 kW, l'Etas doit etre >= 126%.");
    }

    if (derived.climateZone) {
      if (action.etasBand === "175 % ou plus") {
        baseCoeff = COEFF_163.highBand[derived.climateZone];
        regimeLabel = "Etas >= 175%";
      } else if (action.etasBand === "De 126 % a moins de 175 %") {
        baseCoeff = COEFF_163.midBand[derived.climateZone];
        regimeLabel = "126% <= Etas < 175%";
      } else if (
        action.etasBand === "De 111 % a moins de 126 %" &&
        action.temperatureType === "Moyenne ou haute temperature"
      ) {
        baseCoeff = COEFF_163.lowBand[derived.climateZone];
        regimeLabel = "111% <= Etas < 126%";
      }
    }
  }

  if (action.powerBand === "Superieure a 400 kW") {
    if (!action.copBand) e.missing.push(enabled ? "Choisir une tranche de COP." : "");
    if (action.copBand === "Inferieur a 3,4") e.blockers.push("Pour une PAC > 400 kW, le COP doit etre >= 3,4.");
    if (derived.climateZone) {
      if (action.copBand === "4,5 ou plus") {
        baseCoeff = COEFF_163.midBand[derived.climateZone];
        regimeLabel = "COP >= 4,5";
      } else if (action.copBand === "De 3,4 a moins de 4,5") {
        baseCoeff = COEFF_163.lowBand[derived.climateZone];
        regimeLabel = "3,4 <= COP < 4,5";
      }
    }
  }

  const sectorFactor = COEFF_163.sectorFactor[String(project.sector || "")];
  if (enabled && !e.blockers.length && !e.missing.length && baseCoeff && sectorFactor != null) {
    e.kwhCumac = heatedSurface * baseCoeff * sectorFactor;
    e.calculationLabel = `${regimeLabel} : ${buildCalculationLabel([heatedSurface, baseCoeff, sectorFactor], e.kwhCumac)}`;
  }

  if (!e.blockers.length && e.kwhCumac != null) {
    applyCoupDePouceBrctTertiary(e, action, project);
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluate101(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-EN-101", "Isolation combles / toitures", enabled);
  const derived = applyCommonRules(e, project, { requiredProjectSystemsAll: ["projectSystemHeating"] });
  const commonMissingCount = e.missing.length;

  const surface = toNumber(action.insulatedSurface);
  if (!surface) e.missing.push(enabled ? "Renseigner la surface a isoler." : "");

  const sectorFactor = COEFF_101.sectorFactor[String(project.sector || "")];
  const baseCoeff = derived.climateZone ? COEFF_101.base[derived.climateZone] : null;
  if (enabled && !e.blockers.length && !e.missing.length && baseCoeff && sectorFactor != null) {
    e.kwhCumac = surface * baseCoeff * sectorFactor;
    e.calculationLabel = buildCalculationLabel([surface, baseCoeff, sectorFactor], e.kwhCumac);
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluate102(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-EN-102", "Isolation des murs", enabled);
  const derived = applyCommonRules(e, project, { requiredProjectSystemsAll: ["projectSystemHeating"] });
  const commonMissingCount = e.missing.length;

  const surface = toNumber(action.insulatedWallSurface);
  if (!surface) e.missing.push(enabled ? "Renseigner la surface de murs a isoler." : "");
  if (!action.heatingEnergy) e.missing.push(enabled ? "Preciser l'energie de chauffage du local." : "");

  const baseCoeff = derived.climateZone
    ? action.heatingEnergy === "Electricite"
      ? COEFF_102.electric[derived.climateZone]
      : COEFF_102.combustible[derived.climateZone]
    : null;
  const sectorFactor = COEFF_102.sectorFactor[String(project.sector || "")];

  if (enabled && !e.blockers.length && !e.missing.length && baseCoeff && sectorFactor != null) {
    e.kwhCumac = surface * baseCoeff * sectorFactor;
    e.calculationLabel = buildCalculationLabel([surface, baseCoeff, sectorFactor], e.kwhCumac);
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluate103(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-EN-103", "Isolation d'un plancher", enabled);
  const derived = applyCommonRules(e, project, { requiredProjectSystemsAll: ["projectSystemHeating"] });
  const commonMissingCount = e.missing.length;

  const surface = toNumber(action.insulatedFloorSurface);
  if (!surface) e.missing.push(enabled ? "Renseigner la surface de plancher a isoler." : "");

  const baseCoeff = derived.climateZone ? COEFF_103.base[derived.climateZone] : null;
  const sectorFactor = COEFF_103.sectorFactor[String(project.sector || "")];

  if (enabled && !e.blockers.length && !e.missing.length && baseCoeff && sectorFactor != null) {
    e.kwhCumac = surface * baseCoeff * sectorFactor;
    e.calculationLabel = buildCalculationLabel([surface, baseCoeff, sectorFactor], e.kwhCumac);
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluate104(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-EN-104", "Fenetres / portes-fenetres avec vitrage isolant", enabled);
  const derived = applyCommonRules(e, project);
  const commonMissingCount = e.missing.length;

  const surface = toNumber(action.installedWindowSurface);
  if (!surface) e.missing.push(enabled ? "Renseigner la surface de fenetres / portes-fenetres installees." : "");

  const baseCoeff = derived.climateZone ? COEFF_104.base[derived.climateZone] : null;
  const sectorFactor = COEFF_104.sectorFactor[String(project.sector || "")];

  if (enabled && !e.blockers.length && !e.missing.length && baseCoeff && sectorFactor != null) {
    e.kwhCumac = surface * baseCoeff * sectorFactor;
    e.calculationLabel = buildCalculationLabel([surface, baseCoeff, sectorFactor], e.kwhCumac);
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluate107(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-EN-107", "Isolation des toitures-terrasses", enabled);
  const derived = applyCommonRules(e, project);
  const commonMissingCount = e.missing.length;

  const surface = toNumber(action.terraceInsulatedSurface);
  if (!surface) e.missing.push(enabled ? "Renseigner la surface de toiture-terrasse a isoler." : "");
  if (!action.heatingEnergy) e.missing.push(enabled ? "Preciser l'energie de chauffage du local." : "");

  const baseCoeff = derived.climateZone
    ? action.heatingEnergy === "Electricite"
      ? COEFF_107.electric[derived.climateZone]
      : COEFF_107.combustible[derived.climateZone]
    : null;
  const sectorFactor = COEFF_107.sectorFactor[String(project.sector || "")];

  if (enabled && !e.blockers.length && !e.missing.length && baseCoeff && sectorFactor != null) {
    e.kwhCumac = surface * baseCoeff * sectorFactor;
    e.calculationLabel = buildCalculationLabel([surface, baseCoeff, sectorFactor], e.kwhCumac);
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluate111(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-EN-111", "Vitrage parietodynamique", enabled);
  const derived = applyCommonRules(e, project);
  const commonMissingCount = e.missing.length;

  const surface = toNumber(action.parietodynamicSurface);
  if (!surface) e.missing.push(enabled ? "Renseigner la surface de fenetres / portes-fenetres parietodynamiques." : "");
  if (!action.compatibleVentilation) {
    e.missing.push(enabled ? "Preciser si les locaux sont equipes d'une ventilation compatible." : "");
  } else if (action.compatibleVentilation === "Non") {
    e.blockers.push("Les locaux doivent etre equipes d'une ventilation compatible avec la fiche BAT-EN-111.");
  }

  const sectorCoeff = derived.climateZone
    ? COEFF_111[String(project.sector || "")]?.[derived.climateZone]
    : null;
  if (enabled && !e.blockers.length && !e.missing.length && sectorCoeff) {
    e.kwhCumac = surface * sectorCoeff;
    e.calculationLabel = `${formatFormulaNumber(surface)} x ${formatFormulaNumber(sectorCoeff)} = ${formatFormulaNumber(e.kwhCumac)} kWh cumac`;
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluate112(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-EN-112", "Revetement reflectif en toiture", enabled);
  const derived = applyCommonRules(e, project, { supportedSectors: ["Commerces"] });
  const commonMissingCount = e.missing.length;

  const surface = toNumber(action.reflectiveRoofSurface);
  if (!surface) e.missing.push(enabled ? "Renseigner la surface de toiture couverte par le revetement reflectif." : "");
  if (!action.heatCoolByPac) {
    e.missing.push(enabled ? "Preciser si la production de chaud et de froid est assuree par une PAC." : "");
  } else if (action.heatCoolByPac === "Non") {
    e.blockers.push("La production de chaud et de froid doit etre assuree par une pompe a chaleur.");
  }

  const coeff = derived.climateZone ? COEFF_112[derived.climateZone] : null;
  if (enabled && !e.blockers.length && !e.missing.length && coeff) {
    e.kwhCumac = surface * coeff;
    e.calculationLabel = `${formatFormulaNumber(surface)} x ${formatFormulaNumber(coeff)} = ${formatFormulaNumber(e.kwhCumac)} kWh cumac`;
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluate113(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-EN-113", "Facade rideau avec vitrage isolant", enabled);
  const derived = applyCommonRules(e, project);
  const commonMissingCount = e.missing.length;

  const surface = toNumber(action.curtainWallSurface);
  if (!surface) e.missing.push(enabled ? "Renseigner la surface de facade rideau / semi-rideau installee." : "");

  const baseCoeff = derived.climateZone ? COEFF_113.base[derived.climateZone] : null;
  const sectorFactor = COEFF_113.sectorFactor[String(project.sector || "")];
  if (enabled && !e.blockers.length && !e.missing.length && baseCoeff && sectorFactor != null) {
    e.kwhCumac = surface * baseCoeff * sectorFactor;
    e.calculationLabel = buildCalculationLabel([surface, baseCoeff, sectorFactor], e.kwhCumac);
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluateEq117(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-EQ-117", "Installation frigorifique au CO2", enabled);
  applyCommonRules(e, project, { needsSector: false, requiredProjectSystemsAll: ["projectSystemFridge"] });
  const commonMissingCount = e.missing.length;

  if (!action.caseType) e.missing.push(enabled ? "Preciser le cas d'operation retenu." : "");

  const positivePower = toNumber(action.positivePowerKw);
  const negativePower = toNumber(action.negativePowerKw);

  if (action.caseType === "Cas 1" && !positivePower) e.missing.push(enabled ? "Renseigner la puissance frigorifique utile positive." : "");
  if (action.caseType === "Cas 2" && !negativePower) e.missing.push(enabled ? "Renseigner la puissance frigorifique utile negative." : "");
  if (action.caseType === "Cas 3") {
    if (!action.case3Option) e.missing.push(enabled ? "Preciser l'option CO2 transcritique retenue." : "");
    if (!action.saturatedFeed) e.missing.push(enabled ? "Preciser si les evaporateurs sont alimentes en regime sature." : "");
    if (!positivePower && !negativePower) e.missing.push(enabled ? "Renseigner au moins une puissance frigorifique utile." : "");
  }

  if (enabled && !e.blockers.length && !e.missing.length) {
    if (action.caseType === "Cas 1") {
      e.kwhCumac = positivePower * COEFF_117.case1;
      e.calculationLabel = `${formatFormulaNumber(positivePower)} x ${formatFormulaNumber(COEFF_117.case1)} = ${formatFormulaNumber(e.kwhCumac)} kWh cumac`;
    } else if (action.caseType === "Cas 2") {
      e.kwhCumac = negativePower * COEFF_117.case2;
      e.calculationLabel = `${formatFormulaNumber(negativePower)} x ${formatFormulaNumber(COEFF_117.case2)} = ${formatFormulaNumber(e.kwhCumac)} kWh cumac`;
    } else if (action.caseType === "Cas 3") {
      const optionCoeff = COEFF_117.case3[String(action.case3Option)];
      if (optionCoeff) {
        const saturatedKey: "saturated" | "standard" = action.saturatedFeed === "Oui" ? "saturated" : "standard";
        e.kwhCumac = (positivePower + negativePower) * optionCoeff[saturatedKey];
        e.calculationLabel = buildCalculationLabel(
          [positivePower + negativePower, optionCoeff[saturatedKey]],
          e.kwhCumac,
        );
      }
    }
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluateEq123(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-EQ-123", "Moto-variateur", enabled);
  applyCommonRules(e, project, { needsSector: false });
  const commonMissingCount = e.missing.length;

  const totalPower = toNumber(action.totalPowerKw);

  if (!action.eligibleTechnology) e.missing.push(enabled ? "Preciser si le moto-variateur est a aimants permanents ou a reluctance." : "");
  else if (action.eligibleTechnology === "Non") e.blockers.push("Le moto-variateur doit etre a aimants permanents ou a reluctance.");
  if (!action.application) e.missing.push(enabled ? "Preciser l'application principale." : "");
  if (!action.unitPowerLimit) e.missing.push(enabled ? "Preciser si chaque moteur unitaire est <= 1 MW." : "");
  else if (action.unitPowerLimit === "Non") e.blockers.push("Chaque moteur unitaire doit etre inferieur ou egal a 1 MW.");
  if (!totalPower) e.missing.push(enabled ? "Renseigner la somme des puissances installees." : "");

  const coeff = COEFF_123[String(action.application)];
  if (enabled && !e.blockers.length && !e.missing.length && coeff != null) {
    e.kwhCumac = totalPower * coeff;
    e.calculationLabel = `${formatFormulaNumber(totalPower)} x ${formatFormulaNumber(coeff)} = ${formatFormulaNumber(e.kwhCumac)} kWh cumac`;
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluateEq124(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-EQ-124", "Fermeture des meubles frigorifiques a temperature positive", enabled);
  applyCommonRules(e, project, { needsSector: false, requiredProjectSystemsAll: ["projectSystemRefrigeratedDisplay"] });
  const commonMissingCount = e.missing.length;

  const length = toNumber(action.linearGlassLength);

  if (!action.foodRetailOpenToPublic) e.missing.push(enabled ? "Preciser si le local est un commerce alimentaire ouvert au public." : "");
  else if (action.foodRetailOpenToPublic === "Non") e.blockers.push("La fiche BAT-EQ-124 vise les locaux de distribution alimentaire ouverts au public.");
  if (!length) e.missing.push(enabled ? "Renseigner la longueur lineaire de portes en verre." : "");

  if (enabled && !e.blockers.length && !e.missing.length) {
    e.kwhCumac = length * COEFF_124;
    e.calculationLabel = `${formatFormulaNumber(length)} x ${formatFormulaNumber(COEFF_124)} = ${formatFormulaNumber(e.kwhCumac)} kWh cumac`;
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluateEq125(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-EQ-125", "Fermeture des meubles frigorifiques a temperature negative", enabled);
  applyCommonRules(e, project, { needsSector: false, requiredProjectSystemsAll: ["projectSystemRefrigeratedDisplay"] });
  const commonMissingCount = e.missing.length;

  const length = toNumber(action.coverLength);

  if (!action.foodRetailOpenToPublic) e.missing.push(enabled ? "Preciser si le local est un commerce alimentaire ouvert au public." : "");
  else if (action.foodRetailOpenToPublic === "Non") e.blockers.push("La fiche BAT-EQ-125 vise les commerces alimentaires ouverts au public, hors drive.");
  if (!action.meubleArchitecture) e.missing.push(enabled ? "Preciser l'architecture du meuble." : "");
  if (!length) e.missing.push(enabled ? "Renseigner la longueur totale de couvercles installes." : "");

  if (enabled && !e.blockers.length && !e.missing.length) {
    const coeff = COEFF_125[String(action.meubleArchitecture)];
    if (coeff != null) {
      e.kwhCumac = length * coeff;
      e.calculationLabel = `${formatFormulaNumber(length)} x ${formatFormulaNumber(coeff)} = ${formatFormulaNumber(e.kwhCumac)} kWh cumac`;
    }
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluateEq129(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-EQ-129", "Lanterneaux d'eclairage zenithal", enabled);
  const derived = applyCommonRules(e, project, { needsSector: false, requiredProjectSystemsAll: ["projectSystemLighting"] });
  const commonMissingCount = e.missing.length;

  const area = toNumber(action.atFlatArea);
  const sectorBucket = project.sector === "Commerces" ? "Commerces" : "Autres secteurs";

  if (!action.lightingPilot) e.missing.push(enabled ? "Preciser si l'eclairage electrique est pilote automatiquement." : "");
  else if (action.lightingPilot === "Non") e.blockers.push("Le pilotage automatique de l'eclairage est requis.");
  if (!action.withCurb) e.missing.push(enabled ? "Preciser si les lanterneaux sont tous avec costiere." : "");
  else if (action.withCurb === "Non") e.blockers.push("Les lanterneaux doivent etre avec costiere.");
  if (!action.lanternType) e.missing.push(enabled ? "Preciser le type de lanterneaux." : "");
  if (!area) e.missing.push(enabled ? "Renseigner l'aire At flat totale." : "");

  const coeff = derived.climateZone ? COEFF_129[sectorBucket][derived.climateZone] : null;
  if (enabled && !e.blockers.length && !e.missing.length && coeff) {
    e.kwhCumac = area * coeff;
    e.calculationLabel = `${formatFormulaNumber(area)} x ${formatFormulaNumber(coeff)} = ${formatFormulaNumber(e.kwhCumac)} kWh cumac`;
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluateEq130(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-EQ-130", "Systeme de condensation frigorifique a haute efficacite", enabled);
  applyCommonRules(e, project, { needsSector: false, requiredProjectSystemsAll: ["projectSystemFridge"] });
  const commonMissingCount = e.missing.length;

  const deltaT = toNumber(action.deltaT);
  const power = toNumber(action.nominalColdPowerKw);

  if (!action.condensationSystemType) e.missing.push(enabled ? "Preciser le type de systeme de condensation." : "");
  if (!action.coldApplicationType) e.missing.push(enabled ? "Preciser l'application du groupe de froid." : "");
  if (action.coldApplicationType === "Climatisation de confort" && !action.newCe1Building) {
    e.missing.push(enabled ? "Preciser si le batiment neuf releve de la categorie CE1." : "");
  } else if (action.coldApplicationType === "Climatisation de confort" && action.newCe1Building === "Oui") {
    e.blockers.push("La climatisation de confort n'est pas eligible pour un batiment neuf de categorie CE1.");
  }
  if (action.deltaT === "") e.missing.push(enabled ? "Choisir le Delta T retenu pour le calcul." : "");
  if (!power) e.missing.push(enabled ? "Renseigner la puissance electrique nominale totale du groupe de froid." : "");

  const systemType = String(action.condensationSystemType);
  if (action.condensationSystemType && deltaT) {
    const table = COEFF_130[systemType];
    if (table) {
      const thresholds = Object.keys(table).map(Number).sort((a, b) => b - a);
      const maxThreshold = thresholds[0];
      if (deltaT > maxThreshold) {
        e.blockers.push(`Le Delta T doit etre inferieur ou egal a ${maxThreshold} deg C pour ce type de systeme.`);
      }
    }
  }

  if (enabled && !e.blockers.length && !e.missing.length) {
    const threshold = findEq130Threshold(systemType, deltaT);
    const applicationKey = mapEq130Application(String(action.coldApplicationType));
    const coeff = COEFF_130[systemType]?.[threshold]?.[applicationKey];
    if (coeff != null) {
      e.kwhCumac = power * coeff;
      e.calculationLabel = `${formatFormulaNumber(power)} x ${formatFormulaNumber(coeff)} = ${formatFormulaNumber(e.kwhCumac)} kWh cumac`;
    }
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluateEq131(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-EQ-131", "Conduits de lumiere naturelle", enabled);
  applyCommonRules(e, project, { needsSector: false, requiredProjectSystemsAll: ["projectSystemLighting"] });
  const commonMissingCount = e.missing.length;

  const section = toNumber(action.totalTubeSection);
  const sectorBucket = getEq131SectorBucket(project.sector);

  if (!action.lightingPilot) e.missing.push(enabled ? "Preciser si l'eclairage electrique est pilote selon la lumiere naturelle." : "");
  else if (action.lightingPilot === "Non") e.blockers.push("Le pilotage de l'eclairage en fonction des apports de lumiere naturelle est requis.");
  if (!section) e.missing.push(enabled ? "Renseigner la somme des sections des conduits installes." : "");

  const coeff = COEFF_131[sectorBucket];
  if (enabled && !e.blockers.length && !e.missing.length && coeff) {
    const baseCoeff = 28500;
    const sectorFactor = getEq131SectorFactor(sectorBucket);
    const zoneFactor = 1;
    e.kwhCumac = baseCoeff * sectorFactor * zoneFactor * section;
    e.calculationLabel = `${formatFormulaNumber(baseCoeff)} x ${formatFormulaNumber(sectorFactor)} x ${formatFormulaNumber(zoneFactor)} x ${formatFormulaNumber(section)} = ${formatFormulaNumber(e.kwhCumac)} kWh cumac`;
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluateEq134(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-EQ-134", "Meuble frigorifique performant avec groupe integre", enabled);
  applyCommonRules(e, project, { needsSector: false, requiredProjectSystemsAll: ["projectSystemRefrigeratedDisplay"] });
  const commonMissingCount = e.missing.length;

  const length = toNumber(action.installedLength);

  if (!action.foodRetailOpenToPublic) e.missing.push(enabled ? "Preciser si le site est un commerce alimentaire ouvert au public." : "");
  else if (action.foodRetailOpenToPublic === "Non") e.blockers.push("La fiche BAT-EQ-134 vise les commerces alimentaires ouverts au public.");
  if (!action.fridgeType) e.missing.push(enabled ? "Preciser le type de meuble frigorifique." : "");
  if (!action.efficiencyClass) e.missing.push(enabled ? "Preciser la classe d'efficacite energetique." : "");
  if (!length) e.missing.push(enabled ? "Renseigner la longueur totale de meubles installes." : "");

  if (enabled && !e.blockers.length && !e.missing.length) {
    const coeff = COEFF_134[String(action.efficiencyClass)]?.[String(action.fridgeType)];
    if (coeff != null) {
      e.kwhCumac = length * coeff;
      e.calculationLabel = `${formatFormulaNumber(length)} x ${formatFormulaNumber(coeff)} = ${formatFormulaNumber(e.kwhCumac)} kWh cumac`;
    }
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluateEq135(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-EQ-135", "Dispositif performant d'alimentation sans interruption", enabled);
  applyCommonRules(e, project, { needsSector: false });
  const commonMissingCount = e.missing.length;

  const power = toNumber(action.asiPowerKw);

  if (!action.isDataCenter) e.missing.push(enabled ? "Preciser si le dispositif est installe pour un centre de donnees." : "");
  else if (action.isDataCenter === "Non") e.blockers.push("La fiche BAT-EQ-135 s'applique uniquement aux centres de donnees.");
  if (!action.class1Asi) e.missing.push(enabled ? "Preciser si le dispositif ASI est de classe 1." : "");
  else if (action.class1Asi === "Non") e.blockers.push("Le dispositif doit etre de classe 1.");
  if (!action.efficiency98) e.missing.push(enabled ? "Preciser si le rendement du dispositif atteint 98 %." : "");
  else if (action.efficiency98 === "Non") e.blockers.push("Le rendement du dispositif doit etre superieur ou egal a 98 %.");
  if (!power) e.missing.push(enabled ? "Renseigner la puissance active de sortie assignee." : "");
  else if (power < 100) e.blockers.push("La puissance active de sortie assignee doit etre au moins de 100 kW.");

  if (enabled && !e.blockers.length && !e.missing.length) {
    const coeff = power <= 200 ? COEFF_135.midBand : COEFF_135.highBand;
    e.kwhCumac = power * coeff;
    e.calculationLabel = `${formatFormulaNumber(power)} x ${formatFormulaNumber(coeff)} = ${formatFormulaNumber(e.kwhCumac)} kWh cumac`;
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluate142(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-TH-142", "Destratification d'air", enabled);
  const derived = applyCommonRules(e, project, { needsSector: false, requiredProjectSystemsAll: ["projectSystemHeating"] });
  const commonMissingCount = e.missing.length;

  if (!action.heightBand) e.missing.push(enabled ? "Preciser si la hauteur est inferieure a 5 m ou non." : "");
  if (action.heightBand === "Inferieure a 5 m") e.blockers.push("La hauteur sous plafond ou sous faitage doit etre au moins egale a 5 m.");
  if (!action.heatingMode) e.missing.push(enabled ? "Preciser si le chauffage est convectif, radiatif ou mixte." : "");

  const needsConvective = action.heatingMode === "Convectif" || action.heatingMode === "Mixte";
  const needsRadiative = action.heatingMode === "Radiatif" || action.heatingMode === "Mixte";
  const convectivePower = toNumber(action.convectivePowerKw);
  const radiativePower = toNumber(action.radiativePowerKw);

  if (needsConvective && !convectivePower) e.missing.push(enabled ? "Renseigner la puissance nominale convective." : "");
  if (needsRadiative && !radiativePower) e.missing.push(enabled ? "Renseigner la puissance nominale radiative." : "");

  if (enabled && !e.blockers.length && !e.missing.length && derived.climateZone) {
    e.kwhCumac =
      convectivePower * COEFF_142.convective[derived.climateZone] +
      radiativePower * COEFF_142.radiative[derived.climateZone];
    const parts: string[] = [];
    if (convectivePower) parts.push(`${formatFormulaNumber(convectivePower)} x ${formatFormulaNumber(COEFF_142.convective[derived.climateZone])}`);
    if (radiativePower) parts.push(`${formatFormulaNumber(radiativePower)} x ${formatFormulaNumber(COEFF_142.radiative[derived.climateZone])}`);
    e.calculationLabel = `${parts.join(" + ")} = ${formatFormulaNumber(e.kwhCumac)} kWh cumac`;
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluateSe103(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-SE-103", "Equilibrage d'une installation de chauffage a eau chaude", enabled);
  const derived = applyCommonRules(e, project, { needsSector: false, requiredProjectSystemsAll: ["projectSystemHeating"] });
  const commonMissingCount = e.missing.length;

  const surface = toNumber(action.heatedSurface);

  if (!action.collectiveHotWaterHeating) e.missing.push(enabled ? "Preciser si le batiment est equipe d'une installation collective de chauffage a eau chaude." : "");
  else if (action.collectiveHotWaterHeating === "Non") e.blockers.push("La fiche BAT-SE-103 s'applique aux installations collectives de chauffage a eau chaude.");
  if (!action.temperatureGapUnder2) e.missing.push(enabled ? "Preciser si l'ecart de temperature est inferieur a 2 deg C apres equilibrage." : "");
  else if (action.temperatureGapUnder2 === "Non") e.blockers.push("L'ecart de temperature apres equilibrage doit etre strictement inferieur a 2 deg C.");
  if (!surface) e.missing.push(enabled ? "Renseigner la surface totale chauffee." : "");

  const coeff = derived.climateZone ? { H1: 120, H2: 100, H3: 67 }[derived.climateZone] : null;
  if (enabled && !e.blockers.length && !e.missing.length && coeff) {
    e.kwhCumac = surface * coeff;
    e.calculationLabel = `${formatFormulaNumber(surface)} x ${formatFormulaNumber(coeff)} = ${formatFormulaNumber(e.kwhCumac)} kWh cumac`;
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluateBatSe104(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-SE-104", "CPE Services", enabled);
  const derived = applyCommonRules(e, project, {
    supportedSectors: ["Bureaux", "Enseignement", "Commerces", "Hotellerie / Restauration", "Sante"],
    requiredProjectSystemsAll: ["projectSystemHeating"],
  });
  const commonMissingCount = e.missing.length;

  const surface = toNumber(action.heatedSurface104);
  if (!action.cpeServicesContract) e.missing.push(enabled ? "Preciser si le contrat est bien un CPE Services sur chauffage collectif." : "");
  else if (action.cpeServicesContract === "Non") e.blockers.push("Le contrat doit etre un CPE Services portant sur une installation collective de chauffage.");
  if (!surface) e.missing.push(enabled ? "Renseigner la surface chauffee." : "");
  if (!action.contractDurationBand) e.missing.push(enabled ? "Preciser la duree du contrat." : "");
  const coveredUses: string[] = [
    action.useEcs104 === "Oui" ? "ECS" : "",
    action.useComfortCooling104 === "Oui" ? "Climatisation pour le confort" : "",
    action.useSpecificElectricity104 === "Oui" ? "Electricite specifique" : "",
  ].filter(Boolean);
  if (!coveredUses.length) e.missing.push(enabled ? "Selectionner au moins un usage couvert par le contrat." : "");

  if (enabled && !e.blockers.length && !e.missing.length && derived.climateZone) {
    const durationCoeff = COEFF_BAT_SE_104.duration[String(action.contractDurationBand)]?.[derived.climateZone];
    const sectorKey = String(project.sector || "");
    const useCoeff = coveredUses.reduce(
      (sum, use) => sum + (COEFF_BAT_SE_104.correctionByUse[use]?.[sectorKey] ?? 0),
      0,
    );
    if (durationCoeff == null) {
      e.missing.push("La duree du contrat selectionnee n'est pas exploitable.");
    } else {
      const correctionFactor = 1 + useCoeff;
      e.kwhCumac = surface * durationCoeff * correctionFactor;
      e.calculationLabel = buildCalculationLabel([surface, durationCoeff, correctionFactor], e.kwhCumac);
    }
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluateBatSe105(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-SE-105", "Abaissement de la temperature de retour vers un reseau de chaleur", enabled);
  const derived = applyCommonRules(e, project, { requiredProjectSystemsAll: ["projectSystemHeatNetwork"] });
  const commonMissingCount = e.missing.length;

  const surface = toNumber(action.heatedSurface105);
  if (!action.connectedToHeatNetwork) e.missing.push(enabled ? "Preciser si le batiment est raccorde a un reseau de chaleur." : "");
  else if (action.connectedToHeatNetwork === "Non") e.blockers.push("La fiche BAT-SE-105 s'applique aux installations raccordees a un reseau de chaleur.");
  if (!surface) e.missing.push(enabled ? "Renseigner la surface chauffee." : "");

  const coeff = derived.climateZone ? COEFF_BAT_SE_105.base[derived.climateZone] : null;
  const factor = COEFF_BAT_SE_105.sectorFactor[String(project.sector || "")];
  if (enabled && !e.blockers.length && !e.missing.length && coeff && factor != null) {
    e.kwhCumac = surface * coeff * factor;
    e.calculationLabel = `${formatFormulaNumber(surface)} x ${formatFormulaNumber(coeff)} x ${formatFormulaNumber(factor)} = ${formatFormulaNumber(e.kwhCumac)} kWh cumac`;
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

interface SimpleSurfaceOptions {
  eligibleField: string;
  missingEligible: string;
  blockerEligible: string;
  surfaceField: string;
  surfaceLabel: string;
  coeff: { base: { H1: number; H2: number; H3: number }; sectorFactor: Record<string, number> };
}

function evaluateSimpleSurfaceBatTh(
  code: string,
  label: string,
  project: Project,
  action: Action,
  enabled: boolean,
  options: SimpleSurfaceOptions,
): Evaluation {
  const e = baseEvaluation(code, label, enabled);
  const derived = applyCommonRules(e, project, { requiredProjectSystemsAll: ["projectSystemHeating"] });
  const commonMissingCount = e.missing.length;

  const surface = toNumber(action[options.surfaceField]);
  if (!action[options.eligibleField]) e.missing.push(enabled ? options.missingEligible : "");
  else if (action[options.eligibleField] === "Non") e.blockers.push(options.blockerEligible);
  if (!surface) e.missing.push(enabled ? `Renseigner la surface ${options.surfaceLabel}.` : "");

  const coeff = derived.climateZone ? options.coeff.base[derived.climateZone] : null;
  const factor = options.coeff.sectorFactor[String(project.sector || "")];
  if (enabled && !e.blockers.length && !e.missing.length && coeff && factor != null) {
    e.kwhCumac = surface * coeff * factor;
    e.calculationLabel = `${formatFormulaNumber(surface)} x ${formatFormulaNumber(coeff)} x ${formatFormulaNumber(factor)} = ${formatFormulaNumber(e.kwhCumac)} kWh cumac`;
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluateBatTh103(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateSimpleSurfaceBatTh("BAT-TH-103", "Plancher chauffant hydraulique basse temperature", project, action, enabled, {
    eligibleField: "lowTempHydraulicFloor",
    missingEligible: "Preciser s'il s'agit bien d'un plancher chauffant hydraulique basse temperature.",
    blockerEligible: "La fiche BAT-TH-103 s'applique a un plancher chauffant hydraulique basse temperature.",
    surfaceField: "heatedSurface",
    surfaceLabel: "chauffee",
    coeff: COEFF_BAT_103,
  });
}

export function evaluateBatTh105(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateSimpleSurfaceBatTh("BAT-TH-105", "Radiateur basse temperature", project, action, enabled, {
    eligibleField: "lowTempRadiators",
    missingEligible: "Preciser s'il s'agit bien de radiateurs basse temperature.",
    blockerEligible: "La fiche BAT-TH-105 s'applique aux radiateurs basse temperature.",
    surfaceField: "heatedSurface",
    surfaceLabel: "concernee",
    coeff: COEFF_BAT_105,
  });
}

export function evaluateBatTh108(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-TH-108", "Programmation d'intermittence", enabled);
  const derived = applyCommonRules(e, project, { requiredProjectSystemsAll: ["projectSystemHeating"] });
  const commonMissingCount = e.missing.length;

  const surface = toNumber(action.heatedSurface);
  if (!action.intermittentRegulation) e.missing.push(enabled ? "Preciser si le systeme assure une programmation d'intermittence." : "");
  else if (action.intermittentRegulation === "Non") e.blockers.push("La fiche BAT-TH-108 s'applique aux systemes de regulation par programmation d'intermittence.");
  if (!surface) e.missing.push(enabled ? "Renseigner la surface regulee." : "");
  if (!action.heatingEnergy) e.missing.push(enabled ? "Preciser si le chauffage est electrique ou combustible." : "");

  const factor = derived.climateZone ? COEFF_BAT_108.climateFactor[derived.climateZone] : null;
  const amount = COEFF_BAT_108.bySectorAndEnergy[String(project.sector || "")]?.[String(action.heatingEnergy)];
  if (enabled && !e.blockers.length && !e.missing.length && factor && amount != null) {
    e.kwhCumac = surface * factor * amount;
    e.calculationLabel = buildCalculationLabel([surface, factor, amount], e.kwhCumac);
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluateBatTh109(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-TH-109", "Optimiseur de relance", enabled);
  const derived = applyCommonRules(e, project, { requiredProjectSystemsAll: ["projectSystemHeating"] });
  const commonMissingCount = e.missing.length;

  const surface = toNumber(project.buildingSurface);
  if (!action.collectiveHeating) e.missing.push(enabled ? "Preciser si l'installation de chauffage est collective." : "");
  else if (action.collectiveHeating === "Non") e.blockers.push("La fiche BAT-TH-109 vise le chauffage collectif.");
  if (!action.autoAdaptive) e.missing.push(enabled ? "Preciser si l'optimiseur comprend une fonction auto-adaptative." : "");
  else if (action.autoAdaptive === "Non") e.blockers.push("L'optimiseur doit comprendre une fonction auto-adaptative.");
  if (!surface) e.missing.push(enabled ? "Renseigner la surface du batiment dans les donnees projet." : "");

  const factor = derived.climateZone ? COEFF_BAT_109.climateFactor[derived.climateZone] : null;
  const amount = COEFF_BAT_109.bySector[String(project.sector || "")];
  if (enabled && !e.blockers.length && !e.missing.length && factor && amount != null) {
    e.kwhCumac = surface * factor * amount;
    e.calculationLabel = buildCalculationLabel([surface, factor, amount], e.kwhCumac);
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluateBatTh110(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-TH-110", "Recuperateur de chaleur a condensation", enabled);
  const derived = applyCommonRules(e, project);
  const commonMissingCount = e.missing.length;

  const surface = toNumber(project.buildingSurface);
  if (!action.condensingRecovery) e.missing.push(enabled ? "Preciser s'il s'agit bien d'un recuperateur de chaleur a condensation." : "");
  else if (action.condensingRecovery === "Non") e.blockers.push("La fiche BAT-TH-110 s'applique aux recuperateurs de chaleur a condensation.");
  if (!action.recoveredUse) e.missing.push(enabled ? "Preciser si l'operation porte sur le chauffage seul ou sur chauffage et ECS." : "");
  if (!surface) e.missing.push(enabled ? "Renseigner la surface du batiment dans les donnees projet." : "");
  if (!action.replacesWholeBoilerRoom) e.missing.push(enabled ? "Preciser si la nouvelle chaufferie est composee uniquement des equipements remplaces." : "");

  let rFactor: number | null = null;
  if (action.replacesWholeBoilerRoom === "Oui") {
    rFactor = 1;
  } else if (action.replacesWholeBoilerRoom === "Non") {
    const p1 = toNumber(action.p1RecoveredEquipmentPowerKw);
    const p2 = toNumber(action.p2OtherHeatingPowerKw);
    const p3 = toNumber(action.p3OtherEcsPowerKw);
    const p4 = toNumber(action.p4DhwPacPowerKw);
    if (!p1) e.missing.push(enabled ? "Renseigner la puissance P1 des equipements remplaces." : "");
    if (action.recoveredUse === "Chauffage" && !p2) e.missing.push(enabled ? "Renseigner la puissance P2 des autres equipements de chauffage." : "");
    if (action.recoveredUse === "Chauffage et ECS") {
      if (!p2) e.missing.push(enabled ? "Renseigner la puissance P2 des autres equipements de chauffage." : "");
      if (p1) {
        const denominator = p1 + p2 + 0.6 * p3 + p4;
        if (denominator > 0) rFactor = p1 / denominator;
      }
    } else if (p1) {
      const denominator = p1 + p2 + 0.6 * p3;
      if (denominator > 0) rFactor = p1 / denominator;
    }
  }

  const tableForUse = COEFF_BAT_110[String(action.recoveredUse)];
  const coeff = derived.climateZone && tableForUse ? tableForUse.base[derived.climateZone] : null;
  const factor = tableForUse?.sectorFactor[String(project.sector || "")];
  if (enabled && !e.blockers.length && !e.missing.length && coeff && factor != null && rFactor != null) {
    e.kwhCumac = surface * coeff * factor * rFactor;
    e.calculationLabel = `${formatFormulaNumber(surface)} x ${formatFormulaNumber(coeff)} x ${formatFormulaNumber(factor)} x ${formatFormulaNumber(rFactor)} = ${formatFormulaNumber(e.kwhCumac)} kWh cumac`;
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluateBatTh111(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-TH-111", "Chauffe-eau solaire collectif", enabled);
  applyCommonRules(e, project, { needsSector: false, requiredProjectSystemsAny: ["projectSystemDhw"] });
  const commonMissingCount = e.missing.length;

  const annualNeed = toNumber(action.annualDhwNeedKwh);
  const solarCoverage = toNumber(action.solarCoveragePercent);
  if (!action.collectiveSolarWaterHeater) e.missing.push(enabled ? "Preciser s'il s'agit bien d'un chauffe-eau solaire collectif." : "");
  else if (action.collectiveSolarWaterHeater === "Non") e.blockers.push("La fiche BAT-TH-111 s'applique aux chauffe-eau solaires collectifs.");
  if (!annualNeed) e.missing.push(enabled ? "Renseigner le besoin annuel en ECS B." : "");
  if (!solarCoverage) e.missing.push(enabled ? "Renseigner le taux de couverture solaire T." : "");

  if (enabled && !e.blockers.length && !e.missing.length) {
    e.kwhCumac = annualNeed * solarCoverage * 0.196;
    e.calculationLabel = buildCalculationLabel([annualNeed, solarCoverage, 0.196], e.kwhCumac);
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluateBatTh112(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-TH-112", "Variation electronique sur moteur asynchrone", enabled);
  applyCommonRules(e, project, {
    needsSector: false,
    requiredProjectSystemsAny: ["projectSystemLighting", "projectSystemVentilation", "projectSystemFridge"],
  });
  const commonMissingCount = e.missing.length;

  const power = toNumber(action.totalPowerKw);
  if (!action.asynchronousMotorVariation) e.missing.push(enabled ? "Preciser s'il s'agit d'une variation de vitesse sur moteur asynchrone." : "");
  else if (action.asynchronousMotorVariation === "Non") e.blockers.push("La fiche BAT-TH-112 s'applique a un systeme de variation sur moteur asynchrone.");
  if (!action.motorApplication) e.missing.push(enabled ? "Preciser l'application principale du moteur." : "");
  if (!power) e.missing.push(enabled ? "Renseigner la somme des puissances concernees." : "");

  const coeff = COEFF_BAT_112[String(action.motorApplication)];
  if (enabled && !e.blockers.length && !e.missing.length && coeff != null) {
    e.kwhCumac = power * coeff;
    e.calculationLabel = `${formatFormulaNumber(power)} x ${formatFormulaNumber(coeff)} = ${formatFormulaNumber(e.kwhCumac)} kWh cumac`;
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

interface VentilationOptions {
  eligibleField: string;
  missingEligible: string;
  blockerEligible: string;
  typeField: string;
  typeMissing: string;
  sectorField: string;
  sectorMissing: string;
  surfaceField: string;
  surfaceMissing: string;
  coeff: Record<string, { base: { H1: number; H2: number; H3: number }; sectorFactor: Record<string, number> }>;
}

function evaluateBatVentilation(
  code: string,
  label: string,
  project: Project,
  action: Action,
  enabled: boolean,
  options: VentilationOptions,
): Evaluation {
  const e = baseEvaluation(code, label, enabled);
  const derived = applyCommonRules(e, project, { needsSector: false });
  const commonMissingCount = e.missing.length;

  const surface = toNumber(action[options.surfaceField]);
  if (!action[options.eligibleField]) e.missing.push(enabled ? options.missingEligible : "");
  else if (action[options.eligibleField] === "Non") e.blockers.push(options.blockerEligible);
  if (!action[options.typeField]) e.missing.push(enabled ? options.typeMissing : "");
  if (!action[options.sectorField]) e.missing.push(enabled ? options.sectorMissing : "");
  if (!surface) e.missing.push(enabled ? options.surfaceMissing : "");

  const coeffDef = options.coeff[String(action[options.typeField])];
  const coeff = derived.climateZone && coeffDef ? coeffDef.base[derived.climateZone] : null;
  const factor = coeffDef?.sectorFactor[String(action[options.sectorField])];
  if (enabled && !e.blockers.length && !e.missing.length && coeff && factor != null) {
    e.kwhCumac = surface * coeff * factor;
    e.calculationLabel = `${formatFormulaNumber(surface)} x ${formatFormulaNumber(coeff)} x ${formatFormulaNumber(factor)} = ${formatFormulaNumber(e.kwhCumac)} kWh cumac`;
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluateBatTh125(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateBatVentilation("BAT-TH-125", "Ventilation simple flux", project, action, enabled, {
    eligibleField: "simpleFlowVentilation",
    missingEligible: "Preciser s'il s'agit d'une ventilation simple flux.",
    blockerEligible: "La fiche BAT-TH-125 s'applique a une ventilation mecanique simple flux.",
    typeField: "simpleFlowType",
    typeMissing: "Preciser le type de ventilation simple flux.",
    sectorField: "ventilationSector125",
    sectorMissing: "Preciser la categorie de locaux.",
    surfaceField: "ventilatedSurface",
    surfaceMissing: "Renseigner la surface ventilee.",
    coeff: COEFF_BAT_125,
  });
}

export function evaluateBatTh126(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateBatVentilation("BAT-TH-126", "Ventilation double flux", project, action, enabled, {
    eligibleField: "doubleFlowVentilation",
    missingEligible: "Preciser s'il s'agit d'une ventilation double flux.",
    blockerEligible: "La fiche BAT-TH-126 s'applique a une ventilation mecanique double flux.",
    typeField: "doubleFlowType",
    typeMissing: "Preciser le type de ventilation double flux.",
    sectorField: "ventilationSector126",
    sectorMissing: "Preciser la categorie de locaux.",
    surfaceField: "ventilatedSurface",
    surfaceMissing: "Renseigner la surface ventilee.",
    coeff: COEFF_BAT_126,
  });
}

export function evaluateBatTh127(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-TH-127", "Raccordement a un reseau de chaleur", enabled);
  const derived = applyCommonRules(e, project, { requiredProjectSystemsAll: ["projectSystemHeating"] });
  const commonMissingCount = e.missing.length;

  const surface = toNumber(action.connectedSurface);
  if (!action.heatNetworkConnection) e.missing.push(enabled ? "Preciser si le projet concerne un raccordement a un reseau de chaleur." : "");
  else if (action.heatNetworkConnection === "Non") e.blockers.push("La fiche BAT-TH-127 s'applique au raccordement a un reseau de chaleur.");
  if (!surface) e.missing.push(enabled ? "Renseigner la surface concernee." : "");
  if (!action.connectionUse) e.missing.push(enabled ? "Preciser si le raccordement couvre le chauffage seul ou le chauffage et l'ECS." : "");
  if (!action.subscribedPowerBand) e.missing.push(enabled ? "Preciser si la puissance souscrite est <= 400 kW ou > 400 kW." : "");

  const table = action.subscribedPowerBand === "<= 400 kW" ? COEFF_BAT_127.under400 : COEFF_BAT_127.over400;
  const coeff = table[String(action.connectionUse)]?.[String(project.sector || "")];
  const climate = derived.climateZone ? COEFF_BAT_127.climateFactor[derived.climateZone] : null;
  if (enabled && !e.blockers.length && !e.missing.length && coeff != null && climate != null) {
    e.kwhCumac = surface * coeff * climate;
    e.calculationLabel = `${formatFormulaNumber(surface)} x ${formatFormulaNumber(coeff)} x ${formatFormulaNumber(climate)} = ${formatFormulaNumber(e.kwhCumac)} kWh cumac`;
  }

  if (!e.blockers.length && e.kwhCumac != null) {
    applyCoupDePouceBrctTertiary(e, action, project);
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluateBatTh134(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-TH-134", "Haute pression flottante", enabled);
  const derived = applyCommonRules(e, project, {
    needsSector: false,
    requiredProjectSystemsAny: ["projectSystemFridge", "projectSystemCooling", "projectSystemDataCenter"],
  });
  const commonMissingCount = e.missing.length;

  const power = toNumber(action.nominalColdPowerKw);
  if (!action.highPressureFloating) e.missing.push(enabled ? "Preciser si le systeme permet une haute pression flottante." : "");
  else if (action.highPressureFloating === "Non") e.blockers.push("La fiche BAT-TH-134 s'applique a un systeme de haute pression flottante.");
  if (!action.coldApplication134) e.missing.push(enabled ? "Preciser l'application du groupe de froid." : "");
  if (!action.condensationFamily134) e.missing.push(enabled ? "Preciser la famille de condensation." : "");
  if (!power) e.missing.push(enabled ? "Renseigner la puissance nominale du groupe de froid." : "");
  if (action.coldApplication134 === "Climatisation de confort" && !action.newCe1Building134)
    e.missing.push(enabled ? "Preciser si le batiment neuf releve de la categorie CE1." : "");
  else if (action.coldApplication134 === "Climatisation de confort" && action.newCe1Building134 === "Oui")
    e.blockers.push("La climatisation de confort n'est pas eligible pour un batiment neuf de categorie CE1.");

  if (enabled && !e.blockers.length && !e.missing.length && derived.climateZone) {
    const coeff = getBatTh134Coeff(
      String(action.coldApplication134),
      String(action.condensationFamily134),
      derived.climateZone,
    );
    if (coeff == null) e.missing.push("La combinaison d'application et de condensation n'est pas exploitable.");
    else {
      e.kwhCumac = power * coeff;
      e.calculationLabel = `${formatFormulaNumber(power)} x ${formatFormulaNumber(coeff)} = ${formatFormulaNumber(e.kwhCumac)} kWh cumac`;
    }
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluateBatTh139(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-TH-139", "Recuperation de chaleur sur groupe de froid", enabled);
  applyCommonRules(e, project, {
    needsSector: false,
    requiredProjectSystemsAny: ["projectSystemFridge", "projectSystemCooling"],
  });
  const commonMissingCount = e.missing.length;

  const duration = toNumber(action.heatRecoveryDurationHours);
  const recoveredPower = toNumber(action.recoveredHeatPowerKw);
  if (!action.heatRecoveryColdGroup) e.missing.push(enabled ? "Preciser si le projet recupere la chaleur sur un groupe de froid." : "");
  else if (action.heatRecoveryColdGroup === "Non") e.blockers.push("La fiche BAT-TH-139 s'applique a la recuperation de chaleur sur groupe de froid.");
  if (!duration) e.missing.push(enabled ? "Renseigner la duree annuelle d'utilisation de la chaleur recuperee." : "");
  if (!recoveredPower) e.missing.push(enabled ? "Renseigner la puissance thermique recuperee." : "");

  if (enabled && !e.blockers.length && !e.missing.length) {
    e.kwhCumac = duration * 9.9 * recoveredPower;
    e.calculationLabel = buildCalculationLabel([duration, 9.9, recoveredPower], e.kwhCumac);
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluateBatTh143(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-TH-143", "Ventilo-convecteur performant", enabled);
  const derived = applyCommonRules(e, project, {
    needsSector: false,
    requiredProjectSystemsAny: ["projectSystemHeating", "projectSystemCooling"],
  });
  const commonMissingCount = e.missing.length;

  const heatedSurface = toNumber(action.heatedSurface143);
  const cooledSurface = toNumber(action.cooledSurface143);
  if (!action.efficientFanCoil) e.missing.push(enabled ? "Preciser s'il s'agit d'un ventilo-convecteur performant." : "");
  else if (action.efficientFanCoil === "Non") e.blockers.push("La fiche BAT-TH-143 s'applique aux ventilo-convecteurs performants.");
  if (!action.usageCategory143) e.missing.push(enabled ? "Preciser la categorie d'usage du batiment." : "");
  if (!heatedSurface && !cooledSurface) e.missing.push(enabled ? "Renseigner au moins une surface chauffee ou rafraichie." : "");

  if (enabled && !e.blockers.length && !e.missing.length && derived.climateZone) {
    const zone = derived.climateZone;
    const heatingBase = COEFF_BAT_143.heating.base[zone];
    const heatingFactor = COEFF_BAT_143.heating.factor[String(action.usageCategory143)]?.[zone] ?? 0;
    const coolingBase = COEFF_BAT_143.cooling.base[zone];
    const coolingFactor = COEFF_BAT_143.cooling.factor[String(action.usageCategory143)]?.[zone] ?? 0;
    e.kwhCumac = heatedSurface * heatingBase * heatingFactor + cooledSurface * coolingBase * coolingFactor;
    const parts: string[] = [];
    if (heatedSurface) parts.push(`${formatFormulaNumber(heatedSurface)} x ${formatFormulaNumber(heatingBase)} x ${formatFormulaNumber(heatingFactor)}`);
    if (cooledSurface) parts.push(`${formatFormulaNumber(cooledSurface)} x ${formatFormulaNumber(coolingBase)} x ${formatFormulaNumber(coolingFactor)}`);
    e.calculationLabel = `${parts.join(" + ")} = ${formatFormulaNumber(e.kwhCumac)} kWh cumac`;
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluateBatTh145(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-TH-145", "Basse pression flottante", enabled);
  applyCommonRules(e, project, {
    needsSector: false,
    requiredProjectSystemsAny: ["projectSystemFridge", "projectSystemCooling"],
  });
  const commonMissingCount = e.missing.length;

  const power = toNumber(action.nominalColdPowerKw);
  if (!action.lowPressureFloating) e.missing.push(enabled ? "Preciser si le systeme permet une basse pression flottante." : "");
  else if (action.lowPressureFloating === "Non") e.blockers.push("La fiche BAT-TH-145 s'applique a un systeme de basse pression flottante.");
  if (!action.coldApplication145) e.missing.push(enabled ? "Preciser l'application du groupe de froid." : "");
  if (action.coldApplication145 === "Climatisation de confort" && !action.newCe1Building145)
    e.missing.push(enabled ? "Preciser si le batiment neuf releve de la categorie CE1." : "");
  else if (action.coldApplication145 === "Climatisation de confort" && action.newCe1Building145 === "Oui")
    e.blockers.push("La climatisation de confort n'est pas eligible pour un batiment neuf de categorie CE1.");
  if (!power) e.missing.push(enabled ? "Renseigner la puissance nominale du groupe de froid." : "");

  const coeff = COEFF_BAT_145[String(action.coldApplication145)];
  if (enabled && !e.blockers.length && !e.missing.length && coeff != null) {
    e.kwhCumac = power * coeff;
    e.calculationLabel = `${formatFormulaNumber(power)} x ${formatFormulaNumber(coeff)} = ${formatFormulaNumber(e.kwhCumac)} kWh cumac`;
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluateBatTh153(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-TH-153", "Confinement des allees", enabled);
  applyCommonRules(e, project, { needsSector: false, requiredProjectSystemsAll: ["projectSystemDataCenter"] });
  const commonMissingCount = e.missing.length;

  const area = toNumber(action.dataRoomArea);
  if (!action.isDataCenter) e.missing.push(enabled ? "Preciser si le projet concerne un centre de donnees." : "");
  else if (action.isDataCenter === "Non") e.blockers.push("La fiche BAT-TH-153 s'applique aux centres de donnees.");
  if (!action.aisleContainmentType) e.missing.push(enabled ? "Preciser le type de confinement mis en place." : "");
  if (!area) e.missing.push(enabled ? "Renseigner la surface du centre de donnees." : "");

  if (enabled && !e.blockers.length && !e.missing.length) {
    e.kwhCumac = area * COEFF_BAT_153;
    e.calculationLabel = `${formatFormulaNumber(area)} x ${formatFormulaNumber(COEFF_BAT_153)} = ${formatFormulaNumber(e.kwhCumac)} kWh cumac`;
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluateBatTh154(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-TH-154", "Recuperation de chaleur sur eaux grises", enabled);
  const derived = applyCommonRules(e, project, {
    needsSector: false,
    requiredProjectSystemsAny: ["projectSystemDhw"],
  });
  const commonMissingCount = e.missing.length;

  const unitCount = toNumber(action.unitCount154);
  const efficiency = toNumber(action.efficiency154);
  if (!action.greyWaterRecovery) e.missing.push(enabled ? "Preciser si le projet met en place une recuperation de chaleur sur eaux grises." : "");
  else if (action.greyWaterRecovery === "Non") e.blockers.push("La fiche BAT-TH-154 s'applique a la recuperation de chaleur sur eaux grises.");
  if (!action.usage154) e.missing.push(enabled ? "Preciser l'usage du batiment." : "");
  if (!action.installationMode154) e.missing.push(enabled ? "Preciser si le systeme est en debits egaux ou inegaux." : "");
  if (!unitCount) e.missing.push(enabled ? "Renseigner le nombre d'unites raccordees." : "");
  if (!efficiency) e.missing.push(enabled ? "Renseigner l'efficacite du recuperateur." : "");

  const mode = String(action.installationMode154) as "Debits inegaux" | "Debits egaux";
  const modeTable = COEFF_BAT_154[mode];
  const row = modeTable?.[String(action.usage154)];
  const coeff = derived.climateZone && row ? row[derived.climateZone] : null;
  if (enabled && !e.blockers.length && !e.missing.length && coeff != null) {
    e.kwhCumac = unitCount * coeff * (efficiency / 100);
    e.calculationLabel = `${formatFormulaNumber(unitCount)} x ${formatFormulaNumber(coeff)} x ${formatFormulaNumber(efficiency / 100)} = ${formatFormulaNumber(e.kwhCumac)} kWh cumac`;
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluateBatTh156(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-TH-156", "Refroidissement gratuit par air exterieur", enabled);
  const derived = applyCommonRules(e, project, {
    needsSector: false,
    requiredProjectSystemsAny: ["projectSystemCooling", "projectSystemDataCenter"],
  });
  const commonMissingCount = e.missing.length;

  const power = toNumber(action.networkPowerKw156);
  if (!action.freeCooling) e.missing.push(enabled ? "Preciser si le projet met en place un refroidissement gratuit par air exterieur." : "");
  else if (action.freeCooling === "Non") e.blockers.push("La fiche BAT-TH-156 s'applique au refroidissement gratuit par air exterieur.");
  if (!action.temperatureBand156) e.missing.push(enabled ? "Preciser la plage de temperature de consigne du reseau." : "");
  if (!action.application156) e.missing.push(enabled ? "Preciser si l'application releve d'un data center ou non." : "");
  if (!power) e.missing.push(enabled ? "Renseigner la puissance thermique du reseau." : "");

  const coeff = derived.climateZone
    ? COEFF_BAT_156.climate[String(action.temperatureBand156)]?.[derived.climateZone]
    : null;
  const factor = COEFF_BAT_156.sectorFactor[String(action.application156)];
  if (enabled && !e.blockers.length && !e.missing.length && coeff != null && factor != null) {
    e.kwhCumac = power * coeff * factor;
    e.calculationLabel = `${formatFormulaNumber(power)} x ${formatFormulaNumber(coeff)} x ${formatFormulaNumber(factor)} = ${formatFormulaNumber(e.kwhCumac)} kWh cumac`;
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluateBatTh157(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-TH-157", "Chaudiere biomasse collective", enabled);
  applyCommonRules(e, project, { requiredProjectSystemsAll: ["projectSystemHeating"] });
  const commonMissingCount = e.missing.length;

  const heatProduced = toNumber(action.usefulHeatProducedKwh157);
  if (!action.collectiveBiomassBoiler) e.missing.push(enabled ? "Preciser si le projet concerne une chaudiere biomasse collective." : "");
  else if (action.collectiveBiomassBoiler === "Non") e.blockers.push("La fiche BAT-TH-157 s'applique a une chaudiere biomasse collective.");
  if (!action.biomassFuel157) e.missing.push(enabled ? "Preciser si la biomasse utilisee est bien une biomasse ligneuse eligibile." : "");
  else if (action.biomassFuel157 === "Non") e.blockers.push("La biomasse utilisee doit etre une biomasse ligneuse eligibile.");
  if (!action.boilerPowerBand157) e.missing.push(enabled ? "Preciser si la chaudiere est <= 500 kW ou > 500 kW." : "");
  if (!heatProduced) e.missing.push(enabled ? "Renseigner la quantite de chaleur nette utile produite Q." : "");

  const factor = action.boilerPowerBand157 === "<= 500 kW" ? COEFF_BAT_157.under500 : COEFF_BAT_157.over500;
  if (enabled && !e.blockers.length && !e.missing.length && factor != null) {
    e.kwhCumac = heatProduced * factor;
    e.calculationLabel = `${formatFormulaNumber(heatProduced)} x ${formatFormulaNumber(factor)} = ${formatFormulaNumber(e.kwhCumac)} kWh cumac`;
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluateBatTh159(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-TH-159", "Raccordement a un reseau de froid", enabled);
  const derived = applyCommonRules(e, project, {
    needsSector: false,
    requiredProjectSystemsAll: ["projectSystemColdNetwork"],
  });
  const commonMissingCount = e.missing.length;

  const power = toNumber(action.connectedColdPowerKw159);
  if (!action.coldNetworkConnection) e.missing.push(enabled ? "Preciser si le projet concerne un raccordement a un reseau de froid." : "");
  else if (action.coldNetworkConnection === "Non") e.blockers.push("La fiche BAT-TH-159 s'applique au raccordement a un reseau de froid.");
  if (!action.sector159) e.missing.push(enabled ? "Preciser le secteur d'usage du batiment." : "");
  if (!power) e.missing.push(enabled ? "Renseigner la puissance thermique souscrite." : "");

  const coeff = COEFF_BAT_159.sectorFactor[String(action.sector159)];
  const climate = derived.climateZone ? COEFF_BAT_159.climateFactor[derived.climateZone] : null;
  if (enabled && !e.blockers.length && !e.missing.length && coeff != null && climate != null) {
    e.kwhCumac = power * coeff * climate;
    e.calculationLabel = `${formatFormulaNumber(power)} x ${formatFormulaNumber(coeff)} x ${formatFormulaNumber(climate)} = ${formatFormulaNumber(e.kwhCumac)} kWh cumac`;
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluateBatTh161(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-TH-161", "Maintien en temperature des groupes electrogenes", enabled);
  applyCommonRules(e, project, { needsSector: false, requiredProjectSystemsAll: ["projectSystemDataCenter"] });
  const commonMissingCount = e.missing.length;

  const count = toNumber(action.generatorCount161);
  const power = toNumber(action.generatorRoomPowerKw);
  if (!action.backupGenerators) e.missing.push(enabled ? "Preciser si le projet concerne des groupes electrogenes de secours." : "");
  else if (action.backupGenerators === "Non") e.blockers.push("La fiche BAT-TH-161 s'applique aux groupes electrogenes de secours.");
  if (!power) e.missing.push(enabled ? "Renseigner la puissance nominale d'un groupe electrogene equipe." : "");
  if (!count) e.missing.push(enabled ? "Renseigner le nombre de groupes equipes." : "");
  else if (power > 0 && power < 800) e.blockers.push("La puissance nominale du groupe electrogene doit etre au moins egale a 800 kW.");

  if (enabled && !e.blockers.length && !e.missing.length) {
    const coeff = power <= 1200 ? COEFF_BAT_161.underOrEqual1200 : COEFF_BAT_161.over1200;
    e.kwhCumac = count * coeff;
    e.calculationLabel = `${formatFormulaNumber(count)} x ${formatFormulaNumber(coeff)} = ${formatFormulaNumber(e.kwhCumac)} kWh cumac`;
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

interface Bat162Coeffs {
  etas: Record<string, unknown>;
  cop: Record<string, unknown>;
  sectorFactor: Record<string, number>;
}

function evaluateBatTh162Like(
  code: string,
  label: string,
  project: Project,
  action: Action,
  enabled: boolean,
  coeffs: Bat162Coeffs,
): Evaluation {
  const e = baseEvaluation(code, label, enabled);
  const derived = applyCommonRules(e, project, {
    supportedSectors: ["Bureaux", "Enseignement", "Commerces", "Hotellerie / Restauration", "Sante", "Autres secteurs"],
    requiredProjectSystemsAny: ["projectSystemHeating", "projectSystemCooling"],
  });
  const commonMissingCount = e.missing.length;

  const surface = toNumber(action.heatedSurface);
  if (!action.sourcePacEligible) e.missing.push(enabled ? "Preciser si l'equipement installe correspond bien a la fiche." : "");
  else if (action.sourcePacEligible === "Non") e.blockers.push(`La fiche ${code} s'applique uniquement a l'equipement mentionne.`);
  if (!surface) e.missing.push(enabled ? "Renseigner la surface totale chauffee." : "");
  if (!action.usage) e.missing.push(enabled ? "Preciser l'usage de l'equipement." : "");
  if (action.usage === "ECS uniquement") e.blockers.push("Un equipement utilise uniquement pour l'ECS n'est pas eligible.");
  if (!action.powerBand) e.missing.push(enabled ? "Preciser si la puissance est <= 400 kW ou > 400 kW." : "");
  if (!action.replacesWholeBoilerRoom) e.missing.push(enabled ? "Preciser si la nouvelle chaufferie est composee uniquement des equipements remplaces." : "");

  let baseCoeff: number | null = null;
  if (action.powerBand === "<= 400 kW") {
    if (!action.temperatureType) e.missing.push(enabled ? "Preciser le type de PAC." : "");
    if (!action.etasBand) e.missing.push(enabled ? "Choisir une tranche d'Etas." : "");
    if (action.temperatureType === "Basse temperature" && action.etasBand === "111 % a moins de 126 %") {
      e.blockers.push("Pour une PAC basse temperature <= 400 kW, l'Etas doit etre >= 126 %.");
    }
    const etasRow = coeffs.etas[String(action.etasBand)] as
      | (Record<string, { H1: number; H2: number; H3: number }> & { H1?: number; H2?: number; H3?: number })
      | undefined;
    if (derived.climateZone && etasRow) {
      const usageRow = etasRow[String(action.usage)];
      if (usageRow && typeof usageRow === "object") {
        baseCoeff = usageRow[derived.climateZone];
      } else if (typeof etasRow[derived.climateZone] === "number") {
        baseCoeff = etasRow[derived.climateZone] as number;
      }
    }
  }
  if (action.powerBand === "> 400 kW") {
    if (!action.copBand) e.missing.push(enabled ? "Choisir une tranche de COP." : "");
    if (action.copBand === "Inferieur au seuil") e.blockers.push("La performance minimale de COP n'est pas atteinte.");
    const copRow = coeffs.cop[String(action.copBand)] as
      | (Record<string, { H1: number; H2: number; H3: number }> & { H1?: number; H2?: number; H3?: number })
      | undefined;
    if (derived.climateZone && copRow) {
      const usageRow = copRow[String(action.usage)];
      if (usageRow && typeof usageRow === "object") {
        baseCoeff = usageRow[derived.climateZone];
      } else if (typeof copRow[derived.climateZone] === "number") {
        baseCoeff = copRow[derived.climateZone] as number;
      }
    }
  }

  let rFactor: number | null = null;
  if (action.replacesWholeBoilerRoom === "Oui") {
    rFactor = 1;
  } else if (action.replacesWholeBoilerRoom === "Non") {
    const pacPower = toNumber(action.installedPacPowerKw);
    const totalPower = toNumber(action.totalBoilerRoomPowerKw);
    if (!pacPower) e.missing.push(enabled ? "Renseigner la puissance nominale de l'equipement installe." : "");
    if (!totalPower) e.missing.push(enabled ? "Renseigner la puissance totale utile de la nouvelle chaufferie." : "");
    else if (pacPower > 0) rFactor = pacPower < 0.4 * totalPower ? pacPower / totalPower : 1;
  }

  const sectorFactor = coeffs.sectorFactor[String(project.sector || "")];
  if (enabled && !e.blockers.length && !e.missing.length && baseCoeff && sectorFactor != null && rFactor != null) {
    e.kwhCumac = surface * baseCoeff * sectorFactor * rFactor;
    e.calculationLabel = buildCalculationLabel([surface, baseCoeff, sectorFactor, rFactor], e.kwhCumac);
  }

  if (!e.blockers.length && e.kwhCumac != null) {
    applyCoupDePouceBrctTertiary(e, action, project);
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}

export function evaluateBatTh162(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateBatTh162Like("BAT-TH-162", "Systeme geothermique", project, action, enabled, COEFF_BAT_162);
}

export function evaluateBatTh164(project: Project, action: Action, enabled: boolean): Evaluation {
  return evaluateBatTh162Like("BAT-TH-164", "PAC eau/eau ou eau glycollee/eau", project, action, enabled, COEFF_BAT_164);
}

export function evaluateTh158(project: Project, action: Action, enabled: boolean): Evaluation {
  const e = baseEvaluation("BAT-TH-158", "PAC reversible air/air", enabled);
  const derived = applyCommonRules(e, project, {
    supportedSectors: ["Bureaux", "Enseignement", "Commerces", "Hotellerie / Restauration", "Sante", "Autres secteurs"],
  });
  const commonMissingCount = e.missing.length;

  const surface = toNumber(action.heatedSurface);

  if (!action.pacFamily) e.missing.push(enabled ? "Preciser la famille de PAC." : "");
  if (!surface) e.missing.push(enabled ? "Renseigner la surface totale chauffee ou traitee." : "");

  let baseCoeff: number | null = null;
  if (derived.climateZone) {
    if (action.pacFamily === "PAC <= 12 kW") baseCoeff = COEFF_158.smallPac[derived.climateZone];
    if (action.pacFamily === "PAC > 12 kW") baseCoeff = COEFF_158.largePac[derived.climateZone];
    if (action.pacFamily === "PAC rooftop") baseCoeff = COEFF_158.rooftop[derived.climateZone];
  }
  const sectorFactor = COEFF_158.sectorFactor[String(project.sector || "")];

  if (enabled && !e.blockers.length && !e.missing.length && baseCoeff && sectorFactor != null) {
    e.kwhCumac = surface * baseCoeff * sectorFactor;
    e.calculationLabel = buildCalculationLabel([surface, baseCoeff, sectorFactor], e.kwhCumac);
  }

  finalizeStatus(e, enabled, project, commonMissingCount);
  return e;
}
