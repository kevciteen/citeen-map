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

// === Coefficients résidentiels manquants =================================

export const COEFF_BAR_TH_125 = {
  collective: { H1: 23000, H2: 18800, H3: 12500 } as ByClimate,
  individual: {
    autoreglable: { H1: 39700, H2: 32500, H3: 21600 } as ByClimate,
    modulee: { H1: 42000, H2: 34400, H3: 22900 } as ByClimate,
  },
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

export const COEFF_BAR_TH_127 = {
  collective: { H1: 21800, H2: 17800, H3: 11900 } as ByClimate,
  individual: { H1: 31600, H2: 25900, H3: 17200 } as ByClimate,
  surfaceFactor(surface: number): number {
    if (surface < 35) return 0.3;
    if (surface < 60) return 0.5;
    if (surface < 70) return 0.6;
    if (surface < 90) return 0.7;
    if (surface < 110) return 1;
    if (surface <= 130) return 1.1;
    return 1.6;
  },
  rFactor: {
    collective: {
      "Type A|Caisson basse consommation": 0.96,
      "Type A|Caisson standard": 0.91,
      "Type A|Caisson basse pression": 0.76,
      "Type B|Caisson basse consommation": 1,
      "Type B|Caisson standard": 0.95,
      "Type B|Caisson basse pression": 0.78,
    } as Record<string, number>,
    individual: {
      "Type A|Caisson basse consommation": 0.9,
      "Type B|Caisson standard": 1,
    } as Record<string, number>,
  },
};

export const COEFF_BAR_TH_129 = {
  apartment: { H1: 21300, H2: 17400, H3: 11600 } as ByClimate,
  house: {
    "3,9 a moins de 4,3": { H1: 77900, H2: 63700, H3: 42500 } as ByClimate,
    "4,3 ou plus": { H1: 80200, H2: 65600, H3: 43700 } as ByClimate,
  } as Record<string, ByClimate>,
  apartmentSurfaceFactor(surface: number): number {
    if (surface < 35) return 0.5;
    if (surface < 60) return 0.7;
    if (surface < 70) return 1;
    if (surface < 90) return 1.2;
    if (surface < 110) return 1.5;
    if (surface <= 130) return 1.9;
    return 2.5;
  },
  houseSurfaceFactor(surface: number): number {
    if (surface < 35) return 0.3;
    if (surface < 60) return 0.5;
    if (surface < 70) return 0.6;
    if (surface < 90) return 0.7;
    if (surface < 110) return 1;
    if (surface <= 130) return 1.1;
    return 1.6;
  },
};

export const COEFF_BAR_TH_137 = {
  collective: { H1: 47700, H2: 39500, H3: 30800 } as ByClimate,
  house: { H1: 48300, H2: 40200, H3: 29600 } as ByClimate,
  houseSurfaceFactor(surface: number): number {
    if (surface < 70) return 0.5;
    if (surface < 90) return 0.7;
    return 1;
  },
};

export const COEFF_BAR_TH_155 = {
  base: { H1: 17700, H2: 14500, H3: 9700 } as ByClimate,
  factorR: {
    "Type A": { "Basse consommation": 0.98, Standard: 0.93 },
    "Type B": { "Basse consommation": 1, Standard: 0.95 },
  } as Record<string, Record<string, number>>,
};

export const COEFF_BAR_TH_159 = {
  Appartement: {
    "111 % a moins de 120 %": { H1: 39600, H2: 33900, H3: 25600 } as ByClimate,
    "120 % a moins de 130 %": { H1: 48200, H2: 41300, H3: 31200 } as ByClimate,
    "130 % a moins de 140 %": { H1: 55900, H2: 47900, H3: 36200 } as ByClimate,
    "140 % a moins de 150 %": { H1: 62600, H2: 53600, H3: 40500 } as ByClimate,
    "150 % a moins de 160 %": { H1: 68400, H2: 58600, H3: 44200 } as ByClimate,
    "160 % ou plus": { H1: 73400, H2: 62900, H3: 47500 } as ByClimate,
  } as Record<string, ByClimate>,
  "Maison individuelle": {
    "111 % a moins de 120 %": { H1: 74100, H2: 62800, H3: 45600 } as ByClimate,
    "120 % a moins de 130 %": { H1: 90300, H2: 76500, H3: 55400 } as ByClimate,
    "130 % a moins de 140 %": { H1: 104800, H2: 88800, H3: 64400 } as ByClimate,
    "140 % a moins de 150 %": { H1: 117200, H2: 99400, H3: 72000 } as ByClimate,
    "150 % a moins de 160 %": { H1: 128000, H2: 108500, H3: 78700 } as ByClimate,
    "160 % ou plus": { H1: 137500, H2: 116600, H3: 84500 } as ByClimate,
  } as Record<string, ByClimate>,
  surfaceFactor(housingType: string, surface: number): number {
    if (housingType === "Maison individuelle") {
      if (surface < 70) return 0.5;
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

export const COEFF_BAR_TH_161: Record<string, Record<string, ByClimate>> = {
  "20 a 65": {
    "50 a 120": { H1: 11700, H2: 10500, H3: 8800 },
    "plus de 120": { H1: 12900, H2: 11600, H3: 9700 },
  },
  "65 a 100": {
    "50 a 120": { H1: 25100, H2: 22700, H3: 18900 },
    "plus de 120": { H1: 27800, H2: 25100, H3: 20900 },
  },
  "plus de 100": {
    "50 a 120": { H1: 40900, H2: 37000, H3: 30800 },
    "plus de 120": { H1: 45400, H2: 41000, H3: 34100 },
  },
};

export const COEFF_BAR_TH_162 = 20900;

export const COEFF_BAR_TH_165: Record<string, number> = {
  "500 ou moins": 4.8,
  "plus de 500": 3.4,
};

export const COEFF_BAR_TH_168 = {
  ecsOnly: { H1: 6000, H2: 7200, H3: 9600 } as ByClimate,
  ecsAndHeating: { H1: 14000, H2: 12700, H3: 10300 } as ByClimate,
};

export const COEFF_BAR_TH_169: Record<string, ByClimate> = {
  "2,8 a moins de 3,2": { H1: 49200, H2: 46800, H3: 44000 },
  "3,2 a moins de 3,6": { H1: 51000, H2: 48500, H3: 45500 },
  "3,6 a moins de 4": { H1: 52400, H2: 49800, H3: 46800 },
  "4 a moins de 4,4": { H1: 53500, H2: 50900, H3: 47800 },
  "4,4 a moins de 4,8": { H1: 54400, H2: 51800, H3: 48600 },
  "4,8 ou plus": { H1: 55000, H2: 52300, H3: 49200 },
};

export const COEFF_BAR_TH_171 = {
  Appartement: {
    "111 % a moins de 140 %": 48700,
    "140 % ou plus": 58900,
  } as Record<string, number>,
  "Maison individuelle": {
    "111 % a moins de 140 %": 90900,
    "140 % ou plus": 109200,
  } as Record<string, number>,
  surfaceFactor(housingType: string, surface: number): number {
    if (housingType === "Maison individuelle") {
      if (surface < 70) return 0.5;
      if (surface < 90) return 0.7;
      return 1;
    }
    if (surface < 35) return 0.5;
    if (surface < 60) return 0.7;
    return 1;
  },
  zoneFactor: { H1: 1.2, H2: 1, H3: 0.7 } as ByClimate,
};

export const COEFF_BAR_TH_172 = {
  "111 % a moins de 170 %": 101400,
  "170 % ou plus": 119400,
  surfaceFactor(surface: number): number {
    if (surface < 70) return 0.5;
    if (surface < 90) return 0.7;
    return 1;
  },
  zoneFactor: { H1: 1.2, H2: 1, H3: 0.7 } as ByClimate,
};

export const COEFF_BAR_TH_176 = {
  "Maison individuelle": { H1: 9000, H2: 8600, H3: 8000 } as ByClimate,
  Appartement: { H1: 7100, H2: 7100, H3: 6500 } as ByClimate,
  tankFactor: {
    "10 L a 150 L": 0.96,
    "Plus de 150 L": 1.04,
  } as Record<string, number>,
};

export const COEFF_BAR_TH_178 = {
  underOrEqual400: {
    "111 % a moins de 126 %": {
      Chauffage: { H1: 108700, H2: 90600, H3: 64700 } as ByClimate,
      "Chauffage et ECS": { H1: 157900, H2: 137400, H3: 108600 } as ByClimate,
    },
    "126 % a moins de 150 %": {
      Chauffage: { H1: 115000, H2: 95900, H3: 68500 } as ByClimate,
      "Chauffage et ECS": { H1: 167100, H2: 145300, H3: 115000 } as ByClimate,
    },
    "150 % a moins de 175 %": {
      Chauffage: { H1: 120300, H2: 100300, H3: 71600 } as ByClimate,
      "Chauffage et ECS": { H1: 174800, H2: 152000, H3: 120200 } as ByClimate,
    },
    "175 % a moins de 190 %": {
      Chauffage: { H1: 123900, H2: 103300, H3: 73800 } as ByClimate,
      "Chauffage et ECS": { H1: 180000, H2: 156600, H3: 123900 } as ByClimate,
    },
    "190 % ou plus": {
      Chauffage: { H1: 126200, H2: 105100, H3: 75100 } as ByClimate,
      "Chauffage et ECS": { H1: 183200, H2: 159400, H3: 126100 } as ByClimate,
    },
  } as Record<string, Record<string, ByClimate>>,
  over400: {
    "4 a moins de 4,5": {
      Chauffage: { H1: 118500, H2: 98800, H3: 70600 } as ByClimate,
      "Chauffage et ECS": { H1: 172200, H2: 149800, H3: 118500 } as ByClimate,
    },
    "4,5 a moins de 5": {
      Chauffage: { H1: 122300, H2: 101900, H3: 72800 } as ByClimate,
      "Chauffage et ECS": { H1: 177700, H2: 154600, H3: 122200 } as ByClimate,
    },
    "5 a moins de 5,5": {
      Chauffage: { H1: 125400, H2: 104500, H3: 74600 } as ByClimate,
      "Chauffage et ECS": { H1: 182100, H2: 158400, H3: 125300 } as ByClimate,
    },
    "5,5 ou plus": {
      Chauffage: { H1: 127800, H2: 106500, H3: 76100 } as ByClimate,
      "Chauffage et ECS": { H1: 185700, H2: 161500, H3: 127800 } as ByClimate,
    },
  } as Record<string, Record<string, ByClimate>>,
};

export const COEFF_BAR_TH_179 = {
  under400: {
    "111 % a moins de 126 %": {
      Chauffage: { H1: 100000, H2: 84000, H3: 60000 } as ByClimate,
      "Chauffage et ECS": { H1: 146000, H2: 127000, H3: 100000 } as ByClimate,
    },
    "126 % a moins de 150 %": {
      Chauffage: { H1: 107000, H2: 89000, H3: 64000 } as ByClimate,
      "Chauffage et ECS": { H1: 155000, H2: 135000, H3: 107000 } as ByClimate,
    },
    "150 % a moins de 175 %": {
      Chauffage: { H1: 112000, H2: 93000, H3: 67000 } as ByClimate,
      "Chauffage et ECS": { H1: 163000, H2: 142000, H3: 112000 } as ByClimate,
    },
    "175 % a moins de 190 %": {
      Chauffage: { H1: 115000, H2: 96000, H3: 69000 } as ByClimate,
      "Chauffage et ECS": { H1: 167000, H2: 146000, H3: 115000 } as ByClimate,
    },
    "190 % ou plus": {
      Chauffage: { H1: 117000, H2: 97000, H3: 70000 } as ByClimate,
      "Chauffage et ECS": { H1: 170000, H2: 148000, H3: 117000 } as ByClimate,
    },
  } as Record<string, Record<string, ByClimate>>,
};

export const COEFF_BAR_TH_180 = {
  underOrEqual400: {
    "111 % a moins de 126 %": {
      Chauffage: { H1: 100000, H2: 84000, H3: 60000 } as ByClimate,
      "Chauffage et ECS": { H1: 146000, H2: 127000, H3: 100000 } as ByClimate,
    },
    "126 % a moins de 150 %": {
      Chauffage: { H1: 107000, H2: 89000, H3: 64000 } as ByClimate,
      "Chauffage et ECS": { H1: 155000, H2: 135000, H3: 107000 } as ByClimate,
    },
    "150 % a moins de 175 %": {
      Chauffage: { H1: 112000, H2: 93000, H3: 67000 } as ByClimate,
      "Chauffage et ECS": { H1: 163000, H2: 142000, H3: 112000 } as ByClimate,
    },
    "175 % a moins de 190 %": {
      Chauffage: { H1: 115000, H2: 96000, H3: 69000 } as ByClimate,
      "Chauffage et ECS": { H1: 167000, H2: 146000, H3: 115000 } as ByClimate,
    },
    "190 % ou plus": {
      Chauffage: { H1: 117000, H2: 97000, H3: 70000 } as ByClimate,
      "Chauffage et ECS": { H1: 170000, H2: 148000, H3: 117000 } as ByClimate,
    },
  } as Record<string, Record<string, ByClimate>>,
  over400: {
    "4 a moins de 4,5": {
      Chauffage: { H1: 118500, H2: 98800, H3: 70600 } as ByClimate,
      "Chauffage et ECS": { H1: 172200, H2: 149800, H3: 118500 } as ByClimate,
    },
    "4,5 a moins de 5": {
      Chauffage: { H1: 122300, H2: 101900, H3: 72800 } as ByClimate,
      "Chauffage et ECS": { H1: 177700, H2: 154600, H3: 122200 } as ByClimate,
    },
    "5 a moins de 5,5": {
      Chauffage: { H1: 125400, H2: 104500, H3: 74600 } as ByClimate,
      "Chauffage et ECS": { H1: 182100, H2: 158400, H3: 125300 } as ByClimate,
    },
    "5,5 ou plus": {
      Chauffage: { H1: 127800, H2: 106500, H3: 76100 } as ByClimate,
      "Chauffage et ECS": { H1: 185700, H2: 161500, H3: 127800 } as ByClimate,
    },
  } as Record<string, Record<string, ByClimate>>,
};

export const COEFF_BAR_SE_104: ByClimate = { H1: 9800, H2: 8000, H3: 5300 };

export const COEFF_BAR_SE_105: Record<string, ByClimate> = {
  "2": { H1: 2400, H2: 2000, H3: 1500 },
  "3": { H1: 3500, H2: 2900, H3: 2200 },
  "4": { H1: 4600, H2: 3800, H3: 2800 },
  "5": { H1: 5600, H2: 4700, H3: 3400 },
  "6": { H1: 6600, H2: 5500, H3: 4100 },
  "7": { H1: 7600, H2: 6300, H3: 4700 },
  "8": { H1: 8500, H2: 7100, H3: 5200 },
  "9": { H1: 9400, H2: 7800, H3: 5800 },
  "10 ou plus": { H1: 10200, H2: 8500, H3: 6300 },
};

export const COEFF_BAR_SE_106: Record<string, Record<string, ByClimate>> = {
  "Maison individuelle": {
    "Electricite specifique uniquement": { H1: 90, H2: 90, H3: 90 },
    "Electricite totale (chauffage electrique + electricite specifique)": { H1: 490, H2: 430, H3: 340 },
    "Gaz + electricite specifique": { H1: 710, H2: 600, H3: 470 },
  },
  Appartement: {
    "Electricite specifique uniquement": { H1: 60, H2: 60, H3: 60 },
    "Electricite totale (chauffage electrique + electricite specifique)": { H1: 220, H2: 200, H3: 170 },
    "Gaz + electricite specifique": { H1: 400, H2: 350, H3: 280 },
  },
};

export const COEFF_BAR_SE_107: ByClimate = { H1: 13000, H2: 10900, H3: 8500 };

export const COEFF_BAR_SE_108: Record<string, ByClimate> = {
  "Maison individuelle": { H1: 23600, H2: 19700, H3: 14100 },
  Appartement: { H1: 12200, H2: 10200, H3: 7300 },
};

// === Coefficients tertiaires =============================================

type TertiarySectorFactor = Record<string, number>;

export const COEFF_116: Record<string, Record<string, Record<string, number | null>>> = {
  A: {
    Bureaux: { heating: 360, cooling: 233, dhw: 15, lighting: 184, auxiliaries: 19 },
    Enseignement: { heating: 170, cooling: 60, dhw: 82, lighting: 46, auxiliaries: 6 },
    Commerces: { heating: 520, cooling: 150, dhw: 30, lighting: null, auxiliaries: 6 },
    "Hotellerie / Restauration": { heating: 400, cooling: 60, dhw: 32, lighting: 65, auxiliaries: 6 },
    Sante: { heating: 150, cooling: 60, dhw: 87, lighting: null, auxiliaries: 19 },
  },
  B: {
    Bureaux: { heating: 240, cooling: 97, dhw: 7, lighting: 90, auxiliaries: 8 },
    Enseignement: { heating: 100, cooling: 23, dhw: 38, lighting: 21, auxiliaries: 3 },
    Commerces: { heating: 250, cooling: 44, dhw: 13, lighting: null, auxiliaries: 3 },
    "Hotellerie / Restauration": { heating: 200, cooling: 23, dhw: 14, lighting: 30, auxiliaries: 3 },
    Sante: { heating: 90, cooling: 23, dhw: 40, lighting: null, auxiliaries: 9 },
  },
};

export const COEFF_163 = {
  lowBand: { H1: 1100, H2: 900, H3: 600 } as ByClimate,
  midBand: { H1: 1200, H2: 1000, H3: 700 } as ByClimate,
  highBand: { H1: 1300, H2: 1000, H3: 700 } as ByClimate,
  sectorFactor: {
    Bureaux: 1.2,
    Enseignement: 0.8,
    Commerces: 0.9,
    "Hotellerie / Restauration": 0.7,
    Sante: 1.1,
    "Autres secteurs": 0.7,
  } as TertiarySectorFactor,
};

export const COEFF_101 = {
  base: { H1: 2600, H2: 2100, H3: 1400 } as ByClimate,
  sectorFactor: {
    Bureaux: 0.6,
    Enseignement: 0.6,
    Commerces: 0.6,
    "Hotellerie / Restauration": 0.7,
    Sante: 1.2,
    "Autres secteurs": 0.6,
  } as TertiarySectorFactor,
};

export const COEFF_BAT_103 = {
  base: { H1: 210, H2: 170, H3: 110 } as ByClimate,
  sectorFactor: {
    Bureaux: 1.2,
    Enseignement: 0.8,
    Commerces: 0.9,
    "Hotellerie / Restauration": 1.3,
    Sante: 0.9,
    "Autres secteurs": 0.8,
  } as TertiarySectorFactor,
};

export const COEFF_BAT_105 = {
  base: { H1: 56, H2: 46, H3: 31 } as ByClimate,
  sectorFactor: {
    Bureaux: 1.2,
    Enseignement: 0.8,
    Commerces: 0.9,
    "Hotellerie / Restauration": 1.3,
    Sante: 1,
    "Autres secteurs": 0.8,
  } as TertiarySectorFactor,
};

export const COEFF_BAT_108 = {
  climateFactor: { H1: 1.1, H2: 0.9, H3: 0.6 } as ByClimate,
  bySectorAndEnergy: {
    Bureaux: { Combustible: 66, Electricite: 37 },
    Enseignement: { Combustible: 43, Electricite: 24 },
    Commerces: { Combustible: 47, Electricite: 27 },
    "Hotellerie / Restauration": { Combustible: 78, Electricite: 29 },
    Sante: { Combustible: 54, Electricite: 31 },
    "Autres secteurs": { Combustible: 43, Electricite: 24 },
  } as Record<string, Record<string, number>>,
};

export const COEFF_BAT_109 = {
  climateFactor: { H1: 1.1, H2: 0.9, H3: 0.6 } as ByClimate,
  bySector: {
    Bureaux: 69,
    Enseignement: 43,
    Commerces: 55,
    "Hotellerie / Restauration": 82,
    Sante: 53,
    "Autres secteurs": 43,
  } as TertiarySectorFactor,
};

export const COEFF_BAT_110: Record<string, { base: ByClimate; sectorFactor: TertiarySectorFactor }> = {
  Chauffage: {
    base: { H1: 150, H2: 130, H3: 80 },
    sectorFactor: {
      Bureaux: 1.2,
      Enseignement: 0.8,
      Commerces: 0.9,
      "Hotellerie / Restauration": 1.4,
      Sante: 1,
      "Autres secteurs": 0.8,
    },
  },
  "Chauffage + ECS": {
    base: { H1: 190, H2: 160, H3: 120 },
    sectorFactor: {
      Bureaux: 1.1,
      Enseignement: 0.7,
      Commerces: 0.8,
      "Hotellerie / Restauration": 1.6,
      Sante: 1.1,
      "Autres secteurs": 0.7,
    },
  },
};

export const COEFF_BAT_112: Record<string, number> = {
  "Chauffage, pompage": 9600,
  "Ventilation, renouvellement d'air": 11400,
  Refrigeration: 3900,
  Climatisation: 990,
  "Autres applications": 990,
};

export const COEFF_BAT_125: Record<string, { base: ByClimate; sectorFactor: Record<string, number> }> = {
  "Simple flux modulee proportionnelle": {
    base: { H1: 770, H2: 630, H3: 420 },
    sectorFactor: { Bureaux: 0.48, Enseignement: 1, Restauration: 0.59, "Autres locaux": 0.54 },
  },
  "Simple flux modulee a detection de presence": {
    base: { H1: 690, H2: 560, H3: 380 },
    sectorFactor: { Bureaux: 0.4, Enseignement: 1, Restauration: 0.45, "Autres locaux": 0.51 },
  },
  "Simple flux a debit d'air constant": {
    base: { H1: 400, H2: 330, H3: 220 },
    sectorFactor: { Bureaux: 0.4, Enseignement: 1, Restauration: 0.53, "Autres locaux": 0.58 },
  },
};

export const COEFF_BAT_126: Record<string, { base: ByClimate; sectorFactor: Record<string, number> }> = {
  "Double flux modulee proportionnelle": {
    base: { H1: 1000, H2: 830, H3: 560 },
    sectorFactor: { Bureaux: 0.53, Enseignement: 1, Restauration: 0.68, "Etablissement sportif": 0.22, "Salles > 250 m3": 1.88, "Autres locaux": 0.71 },
  },
  "Double flux modulee a detection de presence": {
    base: { H1: 970, H2: 800, H3: 530 },
    sectorFactor: { Bureaux: 0.51, Enseignement: 1, Restauration: 0.63, "Etablissement sportif": 0.17, "Autres locaux": 0.71 },
  },
  "Double flux a debit d'air constant": {
    base: { H1: 850, H2: 700, H3: 460 },
    sectorFactor: { Bureaux: 0.48, Enseignement: 1, Restauration: 0.61, "Etablissement sportif": 0.52, "Salles > 250 m3": 1.44, "Autres locaux": 0.71 },
  },
};

export const COEFF_BAT_153 = 1500;

export const COEFF_BAT_SE_104 = {
  duration: {
    "2": { H1: 23, H2: 19, H3: 13 } as ByClimate,
    "3": { H1: 34, H2: 28, H3: 18 } as ByClimate,
    "4": { H1: 44, H2: 36, H3: 24 } as ByClimate,
    "5": { H1: 54, H2: 44, H3: 30 } as ByClimate,
    "6": { H1: 64, H2: 52, H3: 35 } as ByClimate,
    "7": { H1: 73, H2: 60, H3: 40 } as ByClimate,
    "8": { H1: 82, H2: 67, H3: 45 } as ByClimate,
    "9": { H1: 90, H2: 74, H3: 49 } as ByClimate,
    "10 ou plus": { H1: 99, H2: 81, H3: 54 } as ByClimate,
  } as Record<string, ByClimate>,
  correctionByUse: {
    ECS: {
      Bureaux: 0.06,
      "Hotellerie / Restauration": 0.38,
      Commerces: 0.16,
      Sante: 0.32,
      Enseignement: 0.14,
      "Sport, Loisirs, Culture": 0.52,
    },
    "Climatisation pour le confort": {
      Bureaux: 0.28,
      "Hotellerie / Restauration": 0.26,
      Commerces: 0.25,
      Sante: 0.13,
      Enseignement: 0.02,
      "Sport, Loisirs, Culture": 0.13,
    },
    "Electricite specifique": {
      Bureaux: 0.78,
      "Hotellerie / Restauration": 1.09,
      Commerces: 0.82,
      Sante: 0.32,
      Enseignement: 0.2,
      "Sport, Loisirs, Culture": 0.41,
    },
  } as Record<string, Record<string, number>>,
};

export const COEFF_BAT_SE_105 = {
  base: { H1: 130, H2: 110, H3: 72 } as ByClimate,
  sectorFactor: {
    Bureaux: 1.2,
    Enseignement: 0.8,
    Commerces: 0.9,
    "Hotellerie / Restauration": 1.3,
    Sante: 1,
    "Autres secteurs": 0.8,
  } as TertiarySectorFactor,
};

export const COEFF_BAT_127 = {
  climateFactor: { H1: 1.1, H2: 0.9, H3: 0.6 } as ByClimate,
  under400: {
    Chauffage: {
      Bureaux: 500,
      Enseignement: 320,
      Sante: 400,
      Commerces: 390,
      "Hotellerie / Restauration": 550,
      "Autres secteurs": 320,
    },
    "Chauffage et ECS": {
      Bureaux: 520,
      Enseignement: 370,
      Sante: 530,
      Commerces: 440,
      "Hotellerie / Restauration": 690,
      "Autres secteurs": 350,
    },
  } as Record<string, TertiarySectorFactor>,
  over400: {
    Chauffage: {
      Bureaux: 380,
      Enseignement: 250,
      Sante: 310,
      Commerces: 300,
      "Hotellerie / Restauration": 420,
      "Autres secteurs": 250,
    },
    "Chauffage et ECS": {
      Bureaux: 400,
      Enseignement: 290,
      Sante: 410,
      Commerces: 340,
      "Hotellerie / Restauration": 530,
      "Autres secteurs": 270,
    },
  } as Record<string, TertiarySectorFactor>,
};

export const COEFF_BAT_143 = {
  heating: {
    base: { H1: 65, H2: 57, H3: 48 } as ByClimate,
    factor: {
      "Sante avec hebergement": { H1: 2.3, H2: 2.35, H3: 2.35 } as ByClimate,
      "Hotels et autres hebergements": { H1: 2.2, H2: 2.2, H3: 2.2 } as ByClimate,
      "Sante sans hebergement": { H1: 0.65, H2: 0.6, H3: 0.65 } as ByClimate,
      "Bureaux, restauration, commerces": { H1: 0.6, H2: 0.6, H3: 0.6 } as ByClimate,
      "Autres secteurs": { H1: 0.45, H2: 0.45, H3: 0.4 } as ByClimate,
    } as Record<string, ByClimate>,
  },
  cooling: {
    base: { H1: 9, H2: 13, H3: 24 } as ByClimate,
    factor: {
      "Sante avec hebergement": { H1: 2.05, H2: 2.1, H3: 2.05 } as ByClimate,
      "Hotels et autres hebergements": { H1: 3.1, H2: 3.35, H3: 2.6 } as ByClimate,
      "Sante sans hebergement": { H1: 0, H2: 0, H3: 0.8 } as ByClimate,
      "Bureaux, restauration, commerces": { H1: 1.85, H2: 1.55, H3: 0.95 } as ByClimate,
      "Autres secteurs": { H1: 0, H2: 0, H3: 0 } as ByClimate,
    } as Record<string, ByClimate>,
  },
};

export const COEFF_BAT_145: Record<string, number> = {
  "Refrigeration / ambiance hors confort": 3600,
  "Climatisation de confort": 310,
};

type Bat154Row = ByClimate & { unitLabel: string };

export const COEFF_BAT_154 = {
  "Debits inegaux": {
    Hotellerie: { H1: 13000, H2: 12200, H3: 10900, unitLabel: "chambres equipees" },
    "Etablissement sportif": { H1: 18100, H2: 17100, H3: 15300, unitLabel: "douches raccordees" },
    Sante: { H1: 6400, H2: 6000, H3: 5300, unitLabel: "chambres equipees" },
    "Terrain de camping": { H1: 72200, H2: 67900, H3: 60900, unitLabel: "douches raccordees" },
    "Salon de coiffure": { H1: 33300, H2: 27100, H3: 17900, unitLabel: "salons equipes" },
  } as Record<string, Bat154Row>,
  "Debits egaux": {
    Hotellerie: { H1: 16500, H2: 15500, H3: 13800, unitLabel: "chambres equipees" },
    "Etablissement sportif": { H1: 22900, H2: 21600, H3: 19300, unitLabel: "douches raccordees" },
    Sante: { H1: 8100, H2: 7600, H3: 6800, unitLabel: "chambres equipees" },
    "Terrain de camping": { H1: 91400, H2: 86000, H3: 77000, unitLabel: "douches raccordees" },
    "Salon de coiffure": { H1: 42100, H2: 34300, H3: 22700, unitLabel: "salons equipes" },
    "Piscine - renouvellement + lavages": { H1: 35, H2: 32, H3: 27, unitLabel: "baigneurs/an" },
    "Piscine - renouvellement seul": { H1: 11, H2: 10, H3: 8, unitLabel: "baigneurs/an" },
  } as Record<string, Bat154Row>,
};

export const COEFF_BAT_156 = {
  climate: {
    "[15C ; 18C[": { H1: 5100, H2: 4200, H3: 3000 } as ByClimate,
    "[18C ; 20C]": { H1: 6400, H2: 5900, H3: 4700 } as ByClimate,
  } as Record<string, ByClimate>,
  sectorFactor: {
    "Climatisation hors Data Center": 1,
    "Climatisation Data Center": 4.5,
  } as Record<string, number>,
};

export const COEFF_BAT_157 = {
  under500: 4.8,
  over500: 3.4,
};

export const COEFF_BAT_159 = {
  climateFactor: { H1: 1, H2: 1.3, H3: 1.8 } as ByClimate,
  sectorFactor: {
    "Data Center": 26000,
    "Cafes, hotels, restaurants": 10400,
    Sante: 26000,
    "Enseignement, recherche": 4900,
    "Sport, loisirs, culture": 19800,
    Bureaux: 7800,
    Commerces: 11300,
    Autres: 4900,
  } as Record<string, number>,
};

export const COEFF_BAT_161 = {
  underOrEqual1200: 167800,
  over1200: 279600,
};

type Bat162LikeUsageTable = Record<string, ByClimate>;

export const COEFF_BAT_162 = {
  etas: {
    "111 % a moins de 126 %": {
      Chauffage: { H1: 1400, H2: 1100, H3: 800 },
      "Chauffage et ECS": { H1: 1600, H2: 1400, H3: 1000 },
    },
    "126 % a moins de 175 %": {
      Chauffage: { H1: 1500, H2: 1200, H3: 900 },
      "Chauffage et ECS": { H1: 1800, H2: 1500, H3: 1200 },
    },
    "175 % ou plus": {
      Chauffage: { H1: 1600, H2: 1300, H3: 1000 },
      "Chauffage et ECS": { H1: 1900, H2: 1600, H3: 1300 },
    },
  } as Record<string, Bat162LikeUsageTable>,
  cop: {
    "4 a moins de 5": {
      Chauffage: { H1: 1500, H2: 1300, H3: 800 },
      "Chauffage et ECS": { H1: 1800, H2: 1500, H3: 1100 },
    },
    "5 ou plus": {
      Chauffage: { H1: 1600, H2: 1300, H3: 900 },
      "Chauffage et ECS": { H1: 1900, H2: 1600, H3: 1200 },
    },
  } as Record<string, Bat162LikeUsageTable>,
  sectorFactor: {
    Bureaux: 1.2,
    Enseignement: 0.8,
    Commerces: 0.9,
    "Hotellerie / Restauration": 0.7,
    Sante: 1.1,
    "Autres secteurs": 0.7,
  } as TertiarySectorFactor,
};

export const COEFF_BAT_164 = {
  etas: {
    "111 % a moins de 126 %": { H1: 1100, H2: 900, H3: 600 } as ByClimate,
    "126 % a moins de 175 %": { H1: 1200, H2: 1000, H3: 700 } as ByClimate,
    "175 % ou plus": { H1: 1300, H2: 1000, H3: 700 } as ByClimate,
  } as Record<string, ByClimate>,
  cop: {
    "3,4 a moins de 4,5": { H1: 1100, H2: 900, H3: 600 } as ByClimate,
    "4,5 ou plus": { H1: 1200, H2: 1000, H3: 700 } as ByClimate,
  } as Record<string, ByClimate>,
  sectorFactor: {
    Bureaux: 1.2,
    Enseignement: 0.8,
    Commerces: 0.9,
    "Hotellerie / Restauration": 0.7,
    Sante: 1.1,
    "Autres secteurs": 0.7,
  } as TertiarySectorFactor,
};

export const COEFF_102 = {
  electric: { H1: 3000, H2: 2500, H3: 1600 } as ByClimate,
  combustible: { H1: 4800, H2: 3900, H3: 2600 } as ByClimate,
  sectorFactor: {
    Bureaux: 0.6,
    Enseignement: 0.6,
    Commerces: 0.6,
    "Hotellerie / Restauration": 0.7,
    Sante: 1.3,
    "Autres secteurs": 0.6,
  } as TertiarySectorFactor,
};

export const COEFF_103 = {
  base: { H1: 5200, H2: 4200, H3: 2800 } as ByClimate,
  sectorFactor: {
    Bureaux: 0.6,
    Enseignement: 0.6,
    Commerces: 0.6,
    "Hotellerie / Restauration": 0.7,
    Sante: 1.2,
    "Autres secteurs": 0.6,
  } as TertiarySectorFactor,
};

export const COEFF_104 = {
  base: { H1: 5300, H2: 4300, H3: 2900 } as ByClimate,
  sectorFactor: {
    Bureaux: 0.6,
    Enseignement: 0.6,
    Commerces: 0.6,
    "Hotellerie / Restauration": 0.7,
    Sante: 1.3,
    "Autres secteurs": 0.6,
  } as TertiarySectorFactor,
};

export const COEFF_107 = {
  electric: { H1: 1800, H2: 1500, H3: 1000 } as ByClimate,
  combustible: { H1: 2800, H2: 2300, H3: 1500 } as ByClimate,
  sectorFactor: {
    Bureaux: 0.6,
    Enseignement: 0.6,
    Commerces: 0.6,
    "Hotellerie / Restauration": 0.7,
    Sante: 1.3,
    "Autres secteurs": 0.6,
  } as TertiarySectorFactor,
};

export const COEFF_111: Record<string, ByClimate> = {
  Bureaux: { H1: 3300, H2: 2800, H3: 2100 },
  "Hotellerie / Restauration": { H1: 3700, H2: 3200, H3: 2300 },
  Commerces: { H1: 3300, H2: 2900, H3: 2100 },
  Enseignement: { H1: 4000, H2: 3500, H3: 2500 },
  Sante: { H1: 6600, H2: 5500, H3: 3900 },
  "Autres secteurs": { H1: 3300, H2: 2800, H3: 2100 },
};

export const COEFF_112: ByClimate = { H1: 160, H2: 170, H3: 270 };

export const COEFF_113 = {
  base: { H1: 5900, H2: 4800, H3: 3200 } as ByClimate,
  sectorFactor: {
    Bureaux: 0.6,
    Enseignement: 0.6,
    Commerces: 0.6,
    "Hotellerie / Restauration": 0.7,
    Sante: 1.3,
    "Autres secteurs": 0.6,
  } as TertiarySectorFactor,
};

export const COEFF_142 = {
  convective: { H1: 3900, H2: 4500, H3: 4600 } as ByClimate,
  radiative: { H1: 1400, H2: 1600, H3: 1600 } as ByClimate,
};

type Eq117OptionRow = { standard: number; saturated: number };

export const COEFF_117 = {
  case1: 7300,
  case2: 8400,
  case3: {
    "Option 0": { standard: 8500, saturated: 12500 },
    "Option 1": { standard: 10300, saturated: 14100 },
    "Option 1 bis": { standard: 10300, saturated: 14100 },
    "Option 2": { standard: 12700, saturated: 16300 },
  } as Record<string, Eq117OptionRow>,
};

export const COEFF_123: Record<string, number> = {
  "Chauffage, pompage": 13700,
  "Ventilation, renouvellement d'air": 16300,
  Refrigeration: 8000,
  Climatisation: 2000,
  "Autres applications": 2000,
};

export const COEFF_124 = 25600;

export const COEFF_125: Record<string, number> = { Simple: 6700, Double: 8200, Combine: 4600 };

export const COEFF_129: Record<string, ByClimate> = {
  Commerces: { H1: 9500, H2: 10800, H3: 16000 },
  "Autres secteurs": { H1: 3400, H2: 4000, H3: 6400 },
};

type Eq130Row = { comfort: number; datacenter: number; refrigeration: number };

export const COEFF_130: Record<string, Record<number, Eq130Row>> = {
  "Condenseur a eau seul": {
    8: { comfort: 500, datacenter: 1900, refrigeration: 1300 },
    7: { comfort: 770, datacenter: 3000, refrigeration: 2000 },
    6: { comfort: 1100, datacenter: 4100, refrigeration: 2700 },
  },
  "Condenseur a air sec": {
    12: { comfort: 580, datacenter: 2300, refrigeration: 1600 },
    11: { comfort: 790, datacenter: 3100, refrigeration: 2200 },
    10: { comfort: 1000, datacenter: 3900, refrigeration: 2800 },
    9: { comfort: 1200, datacenter: 4800, refrigeration: 3400 },
    8: { comfort: 1500, datacenter: 5800, refrigeration: 4000 },
    7: { comfort: 1700, datacenter: 6800, refrigeration: 4700 },
    6: { comfort: 2000, datacenter: 7800, refrigeration: 5300 },
    5: { comfort: 2300, datacenter: 8900, refrigeration: 6000 },
    4: { comfort: 2600, datacenter: 10100, refrigeration: 6800 },
    3: { comfort: 2900, datacenter: 11300, refrigeration: 7500 },
    2: { comfort: 3200, datacenter: 12600, refrigeration: 8300 },
    1: { comfort: 3600, datacenter: 14000, refrigeration: 9100 },
    0: { comfort: 4000, datacenter: 15500, refrigeration: 10000 },
  },
  "Condenseur evaporatif / tour": {
    22: { comfort: 580, datacenter: 2300, refrigeration: 1600 },
    21: { comfort: 790, datacenter: 3100, refrigeration: 2200 },
    20: { comfort: 1000, datacenter: 3900, refrigeration: 2800 },
    19: { comfort: 1200, datacenter: 4800, refrigeration: 3400 },
    18: { comfort: 1500, datacenter: 5800, refrigeration: 4000 },
    17: { comfort: 1700, datacenter: 6800, refrigeration: 4700 },
    16: { comfort: 2000, datacenter: 7800, refrigeration: 5300 },
    15: { comfort: 2300, datacenter: 8900, refrigeration: 6000 },
    14: { comfort: 2600, datacenter: 10100, refrigeration: 6800 },
    13: { comfort: 2900, datacenter: 11300, refrigeration: 7500 },
    12: { comfort: 3200, datacenter: 12600, refrigeration: 8300 },
    11: { comfort: 3600, datacenter: 14000, refrigeration: 9100 },
    10: { comfort: 4000, datacenter: 15500, refrigeration: 10000 },
  },
};

export const COEFF_131: Record<string, number> = {
  Bureaux: 21375,
  Commerces: 28500,
  "Autres secteurs": 17100,
};

export const COEFF_134: Record<string, Record<string, number>> = {
  D: { "Armoire verticale / semi-verticale / mixte": 22600, "Armoire horizontale": 6300, "Congelateur vertical / mixte": 18400, "Congelateur horizontal": 9900 },
  C: { "Armoire verticale / semi-verticale / mixte": 31000, "Armoire horizontale": 8700, "Congelateur vertical / mixte": 30800, "Congelateur horizontal": 14700 },
  B: { "Armoire verticale / semi-verticale / mixte": 38200, "Armoire horizontale": 10500, "Congelateur vertical / mixte": 41200, "Congelateur horizontal": 18800 },
  A: { "Armoire verticale / semi-verticale / mixte": 43800, "Armoire horizontale": 12100, "Congelateur vertical / mixte": 49400, "Congelateur horizontal": 21900 },
};

export const COEFF_135 = {
  midBand: 3100,
  highBand: 2500,
};

export const COEFF_158 = {
  smallPac: { H1: 860, H2: 760, H3: 620 } as ByClimate,
  largePac: { H1: 870, H2: 770, H3: 630 } as ByClimate,
  rooftop: { H1: 660, H2: 540, H3: 360 } as ByClimate,
  sectorFactor: COEFF_163.sectorFactor,
};

export const COEFF_BAT_TH_134_TABLES: Record<string, Record<string, ByClimate>> = {
  "Climatisation de confort": {
    "Condensation par rapport a l'atmosphere": { H1: 2000, H2: 1800, H3: 1600 },
    "Condensation a eau seule": { H1: 670, H2: 480, H3: 290 },
  },
  "Climatisation Data Center": {
    "Condensation par rapport a l'atmosphere": { H1: 22800, H2: 20500, H3: 20200 },
    "Condensation a eau seule": { H1: 14500, H2: 13900, H3: 11300 },
  },
  "Refrigeration / ambiance hors confort": {
    "Condensation par rapport a l'atmosphere": { H1: 19100, H2: 17000, H3: 16400 },
    "Condensation a eau seule": { H1: 13400, H2: 12800, H3: 10500 },
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
