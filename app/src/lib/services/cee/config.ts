/**
 * Configuration CEE — coefficients, seuils, mappings.
 *
 * Port mécanique du fichier `simulateur-cee-main/js/config.js` (1083 lignes,
 * source : équipe Citeen). Aucun changement de valeurs — seuls les `const`
 * deviennent `export const` et les types sont annotés.
 *
 * Ne modifier qu'en cas de changement réglementaire CEE (annexes BO de
 * l'arrêté CEE, mise à jour des coefficients par fiche).
 */
import type {
  ClimateZone,
  EvaluationStatus,
  ResourceCategoryKey,
} from "./types";

export const CURRENT_YEAR = new Date().getFullYear();
export const LAST_UPDATE_LABEL = "19 avril 2026";

export const STATUS: Record<string, EvaluationStatus> = {
  POTENTIAL: "Potentiellement eligible",
  CONFIRM: "Eligibilite a confirmer",
  INELIGIBLE: "Non eligible",
  ELIGIBLE: "Eligible",
};

export const STATUS_SUMMARY_ORDER: EvaluationStatus[] = [
  STATUS.ELIGIBLE,
  STATUS.CONFIRM,
  STATUS.POTENTIAL,
  STATUS.INELIGIBLE,
];

export const FAMILY_ORDER = ["Enveloppe", "Thermique", "Equipement", "Services"] as const;

export const STATUS_PRIORITY: Record<EvaluationStatus, number> = {
  Eligible: 0,
  "Eligibilite a confirmer": 1,
  "Potentiellement eligible": 2,
  "Non eligible": 3,
};

export const COLLECTIVE_HOUSING_TYPES = [
  "Batiment d'habitation collectif en monopropriete",
  "Batiment d'habitation collectif en copropriete",
] as const;

export const RESOURCE_REGION_LABELS = {
  IDF: "Ile-de-France",
  NON_IDF: "Hors Ile-de-France",
} as const;

export const RESOURCE_CATEGORIES = {
  VERY_MODEST: "Tres modestes",
  MODEST: "Modestes",
  INTERMEDIATE: "Intermediaires",
  HIGH: "Superieurs",
} as const;

export const RESOURCE_CATEGORY_KEYS: Record<keyof typeof RESOURCE_CATEGORIES, ResourceCategoryKey> = {
  VERY_MODEST: "veryModest",
  MODEST: "modest",
  INTERMEDIATE: "intermediate",
  HIGH: "high",
};

export const COUP_DE_POUCE_SHEET_CODES = [
  "BAR-TH-112", "BAR-TH-113", "BAR-TH-137", "BAR-TH-143", "BAR-TH-171",
  "BAR-TH-172", "BAR-TH-174", "BAR-TH-175", "BAR-TH-178", "BAR-TH-179",
  "BAR-TH-180", "BAT-TH-127", "BAT-TH-162", "BAT-TH-163", "BAT-TH-164",
];

export interface NonCumulRule {
  id: string;
  sourceCode: string;
  conflictPrefixes: string[];
  excludeCodes: string[];
  message: string;
  sourceUrl: string;
}

export const NON_CUMUL_RULES: NonCumulRule[] = [
  {
    id: "bar-th-174-ampleur",
    sourceCode: "BAR-TH-174",
    conflictPrefixes: ["BAR-EN-", "BAR-TH-"],
    excludeCodes: ["BAR-TH-174"],
    message:
      "Cette fiche n'est pas cumulable avec d'autres fiches valorisant des travaux deja integres dans la renovation d'ampleur.",
    sourceUrl:
      "https://www.ecologie.gouv.fr/sites/default/files/documents/BAR-TH-174%20vA70-2%20%C3%A0%20compter%20du%2015-06-2025%20R%C3%A9novation%20d%E2%80%99ampleur%20d%E2%80%99une%20maison%20individuelle%20%28France%20m%C3%A9tropolitaine%29.pdf",
  },
  {
    id: "bar-th-175-ampleur",
    sourceCode: "BAR-TH-175",
    conflictPrefixes: ["BAR-EN-", "BAR-TH-"],
    excludeCodes: ["BAR-TH-175"],
    message:
      "Cette fiche n'est pas cumulable avec d'autres fiches valorisant des travaux deja integres dans la renovation d'ampleur.",
    sourceUrl:
      "https://www.ecologie.gouv.fr/sites/default/files/documents/BAR-TH-175%20vA70-2%20%C3%A0%20compter%20du%2015-06-2025%20R%C3%A9novation%20d%E2%80%99ampleur%20d%E2%80%99un%20appartement%20%28France%20m%C3%A9tropolitaine%29_0.pdf",
  },
];

export interface ResourceThresholdRow {
  veryModest: number;
  modest: number;
  intermediate: number;
}

export interface ResourceRegionThresholds {
  base: Record<1 | 2 | 3 | 4 | 5, ResourceThresholdRow>;
  extraPerPerson: ResourceThresholdRow;
}

export const RESOURCE_THRESHOLDS_2026: Record<"IDF" | "NON_IDF", ResourceRegionThresholds> = {
  IDF: {
    base: {
      1: { veryModest: 24031, modest: 29253, intermediate: 40851 },
      2: { veryModest: 35270, modest: 42933, intermediate: 60051 },
      3: { veryModest: 42357, modest: 51564, intermediate: 71846 },
      4: { veryModest: 49455, modest: 60208, intermediate: 84562 },
      5: { veryModest: 56580, modest: 68877, intermediate: 96817 },
    },
    extraPerPerson: { veryModest: 7116, modest: 8663, intermediate: 12257 },
  },
  NON_IDF: {
    base: {
      1: { veryModest: 17363, modest: 22559, intermediate: 31185 },
      2: { veryModest: 25393, modest: 32553, intermediate: 45842 },
      3: { veryModest: 30540, modest: 39148, intermediate: 55196 },
      4: { veryModest: 35676, modest: 45735, intermediate: 64550 },
      5: { veryModest: 40835, modest: 52348, intermediate: 73907 },
    },
    extraPerPerson: { veryModest: 5151, modest: 6598, intermediate: 9357 },
  },
};

export const ACTION_DEFAULTS: Record<string, Record<string, string>> = {
  "BAR-TH-174": { priorWorkStageDone: "Non" },
  "BAR-TH-175": { priorWorkStageDone: "Non" },
  "BAR-TH-178": { pacPowerSharePercent: "100" },
  "BAR-TH-179": { pacPowerSharePercent: "100" },
  "BAR-TH-180": { pacPowerSharePercent: "100" },
};

// === Coefficients par fiche (port direct du config.js d'origine) =========

type ByClimate<T = number> = Record<ClimateZone, T>;

export const CLIMATE_FACTORS_116: ByClimate = { H1: 1.1, H2: 0.9, H3: 0.6 };

export const COEFF_174_175 = {
  classJump: {
    "2": 360200,
    "3": 447900,
    "4 ou plus": 568600,
  } as Record<string, number>,
  surfaceFactor(surface: number): number {
    if (surface < 35) return 0.4;
    if (surface < 60) return 0.5;
    if (surface < 90) return 0.8;
    if (surface < 110) return 1;
    if (surface <= 130) return 1.2;
    return 1.3;
  },
};

export const COEFF_BAR_EN_101: ByClimate = { H1: 1700, H2: 1400, H3: 920 };
export const COEFF_BAR_EN_102: ByClimate = { H1: 1600, H2: 1300, H3: 880 };
export const COEFF_BAR_EN_103: ByClimate = { H1: 1100, H2: 890, H3: 590 };
export const COEFF_BAR_EN_104: ByClimate = { H1: 3800, H2: 3100, H3: 2100 };
export const COEFF_BAR_EN_105: ByClimate = { H1: 1200, H2: 1000, H3: 670 };
export const COEFF_BAR_EN_108: ByClimate = { H1: 510, H2: 420, H3: 280 };
export const COEFF_BAR_EN_110: ByClimate = { H1: 7100, H2: 6000, H3: 4200 };

export const COEFF_BAR_TH_101: ByClimate = { H1: 18500, H2: 21000, H3: 24200 };
export const COEFF_BAR_TH_112: ByClimate = { H1: 38200, H2: 31300, H3: 20900 };
export const COEFF_BAR_TH_113: ByClimate = { H1: 142300, H2: 116400, H3: 77600 };
export const COEFF_BAR_TH_122: ByClimate = { H1: 16300, H2: 14000, H3: 10200 };
export const COEFF_BAR_TH_123: ByClimate = { H1: 6400, H2: 5200, H3: 3500 };
export const COEFF_BAR_TH_139 = 14600;
export const COEFF_BAR_TH_143: ByClimate = { H1: 134800, H2: 121000, H3: 100500 };
export const COEFF_BAR_TH_170 = 82300;
export const COEFF_BAR_TH_177 = 2100;

export const COEFF_BAR_TH_148 = {
  "Maison individuelle": 14700,
  Appartement: 11800,
} as Record<string, number>;

export const COEFF_BAR_TH_158 = {
  "Maison individuelle": { H1: 1800, H2: 1500, H3: 1100 },
  Appartement: { H1: 1500, H2: 1200, H3: 900 },
} as Record<string, ByClimate>;

export const COEFF_BAR_TH_173 = {
  "Maison individuelle": { H1: 3200, H2: 2600, H3: 1900 },
  Appartement: { H1: 2500, H2: 2100, H3: 1500 },
} as Record<string, ByClimate>;

export const COEFF_BAR_TH_110 = {
  "Maison individuelle": { H1: 1700, H2: 1400, H3: 910 },
  "Appartement avec chauffage individuel": { H1: 1100, H2: 880, H3: 590 },
  "Appartement avec chauffage collectif": { H1: 1000, H2: 850, H3: 560 },
} as Record<string, ByClimate>;

export const COEFF_BAR_TH_116 = {
  "Maison individuelle": { H1: 300, H2: 250, H3: 160 },
  "Appartement avec chauffage individuel": { H1: 210, H2: 170, H3: 110 },
  "Appartement avec chauffage collectif": { H1: 280, H2: 230, H3: 150 },
} as Record<string, ByClimate>;

export const COEFF_BAR_TH_117 = {
  "Maison individuelle": { H1: 1700, H2: 1400, H3: 930 },
  "Appartement avec chauffage individuel": { H1: 1200, H2: 980, H3: 650 },
  "Appartement avec chauffage collectif": { H1: 1600, H2: 1300, H3: 890 },
} as Record<string, ByClimate>;

export const COEFF_BAR_TH_111 = {
  Electricite: { H1: 2200, H2: 1800, H3: 1200 },
  Combustible: { H1: 3300, H2: 2700, H3: 1800 },
  surfaceFactor(surface: number): number {
    if (surface < 35) return 0.3;
    if (surface < 60) return 0.5;
    if (surface < 70) return 0.6;
    if (surface < 90) return 0.7;
    if (surface < 110) return 1;
    if (surface <= 130) return 1.1;
    return 1.6;
  },
};

export const COEFF_BAR_EQ_115 = {
  base: {
    "Maison individuelle": { H1: 4400, H2: 3700, H3: 2700 },
    Appartement: { H1: 2600, H2: 2200, H3: 1700 },
  } as Record<string, ByClimate>,
  comfortFactor: { Oui: 1, Non: 0.8 } as Record<string, number>,
  fixedPart: {
    "Maison individuelle": 650,
    Appartement: 410,
  } as Record<string, number>,
  surfaceFactor(housingType: string, surface: number): number {
    if (housingType === "Maison individuelle") {
      if (surface < 35) return 0.3;
      if (surface < 60) return 0.5;
      if (surface < 70) return 0.6;
      if (surface < 90) return 0.7;
      if (surface < 110) return 1;
      if (surface <= 130) return 1.1;
      return 1.6;
    }
    if (surface < 35) return 0.5;
    if (surface < 60) return 0.7;
    if (surface < 70) return 1;
    if (surface < 90) return 1.2;
    if (surface < 110) return 1.5;
    if (surface <= 130) return 1.9;
    return 2.5;
  },
};

export const DEPARTMENT_TO_CLIMATE: Record<string, ClimateZone> = {
  "01": "H1", "02": "H1", "03": "H1", "04": "H2", "05": "H1", "06": "H3", "07": "H2", "08": "H1",
  "09": "H2", "10": "H1", "11": "H3", "12": "H2", "13": "H3", "14": "H1", "15": "H1", "16": "H2",
  "17": "H2", "18": "H2", "19": "H1", "20": "H3", "21": "H1", "22": "H2", "23": "H1", "24": "H2",
  "25": "H1", "26": "H2", "27": "H1", "28": "H1", "29": "H2", "30": "H3", "31": "H2", "32": "H2",
  "33": "H2", "34": "H3", "35": "H2", "36": "H2", "37": "H2", "38": "H1", "39": "H1", "40": "H2",
  "41": "H2", "42": "H1", "43": "H1", "44": "H2", "45": "H1", "46": "H2", "47": "H2", "48": "H2",
  "49": "H2", "50": "H2", "51": "H1", "52": "H1", "53": "H2", "54": "H1", "55": "H1", "56": "H2",
  "57": "H1", "58": "H1", "59": "H1", "60": "H1", "61": "H1", "62": "H1", "63": "H1", "64": "H2",
  "65": "H2", "66": "H3", "67": "H1", "68": "H1", "69": "H1", "70": "H1", "71": "H1", "72": "H2",
  "73": "H1", "74": "H1", "75": "H1", "76": "H1", "77": "H1", "78": "H1", "79": "H2", "80": "H1",
  "81": "H2", "82": "H2", "83": "H3", "84": "H2", "85": "H2", "86": "H2", "87": "H1", "88": "H1",
  "89": "H1", "90": "H1", "91": "H1", "92": "H1", "93": "H1", "94": "H1", "95": "H1",
};
