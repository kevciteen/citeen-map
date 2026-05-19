/**
 * Types du moteur CEE (Certificats d'Économies d'Énergie).
 *
 * Port TypeScript du simulateur CEE interne (anciennement JS vanilla).
 * Le moteur calcule les kWh cumac et la prime estimée en euros pour les
 * fiches CEE applicables à un projet de rénovation énergétique.
 *
 * Périmètre couvert :
 *   - Habitations (BAR-* : Bâtiments Anciens Résidentiels)
 *   - Tertiaire (BAT-*) — en chantier, port progressif
 */

export type BuildingType = "Habitation" | "Tertiaire";

export type ClimateZone = "H1" | "H2" | "H3";

export type HousingType =
  | "Maison individuelle"
  | "Appartement"
  | "Batiment d'habitation collectif en monopropriete"
  | "Batiment d'habitation collectif en copropriete";

export type ResourceRegion = "IDF" | "NON_IDF";

export type ResourceCategoryKey =
  | "veryModest"
  | "modest"
  | "intermediate"
  | "high";

export type Sector =
  | "Bureaux"
  | "Enseignement"
  | "Commerces"
  | "Hotellerie / Restauration"
  | "Sante"
  | "Autres secteurs";

export type EvaluationStatus =
  | "Eligible"
  | "Eligibilite a confirmer"
  | "Potentiellement eligible"
  | "Non eligible";

/**
 * Booléens "système X présent dans le bâtiment" — utilisés par certaines
 * fiches qui exigent l'existence d'un équipement à remplacer/améliorer.
 */
export type ProjectSystemFlags = {
  projectSystemHeating?: boolean;
  projectSystemDhw?: boolean;
  projectSystemCooling?: boolean;
  projectSystemVentilation?: boolean;
  projectSystemLighting?: boolean;
  projectSystemFridge?: boolean;
  projectSystemRefrigeratedDisplay?: boolean;
  projectSystemDataCenter?: boolean;
  projectSystemHeatNetwork?: boolean;
  projectSystemColdNetwork?: boolean;
};

/**
 * Objet projet — entrée unique du moteur. Chaque fiche pioche les champs
 * qu'elle attend ; les champs manquants ajoutent des notes "missing"
 * sans casser le calcul.
 */
export interface Project extends ProjectSystemFlags {
  buildingType?: BuildingType | "";
  postalCode?: string;
  constructionYear?: number | string;
  buildingSurface?: number | string;
  // Résidentiel
  housingType?: HousingType | "";
  householdSize?: number | string;
  incomeBracket?: ResourceCategoryKey | "";
  // Tertiaire
  sector?: Sector | "";
  // Tarification CEE (€/MWh cumac) — laisser piloter par l'appelant.
  mwhCumacPrice?: number | string;
  mwhCumacPricePrecarious?: number | string;
}

/**
 * Coup de pouce — bonus appliqué à certaines fiches BAR-TH lourdes
 * (rénovation d'ampleur, PAC haute performance…). Multiplie le kWh cumac.
 */
export interface CoupDePouceInfo {
  name: string;
  status: EvaluationStatus;
  factor: number;
  notes: string[];
}

/**
 * Résultat d'évaluation d'une fiche unique.
 */
export interface Evaluation {
  code: string;
  title: string;
  enabled: boolean;
  status: EvaluationStatus;
  kwhCumac: number | null;
  euroAmount: number | null;
  standardKwhCumac: number | null;
  standardEuroAmount: number | null;
  blockers: string[];
  missing: string[];
  notes: string[];
  calculationLabel: string;
  standardCalculationLabel: string;
  calculationEngineMissing: boolean;
  coupDePouce: CoupDePouceInfo | null;
}

/**
 * Données dérivées du projet (climat, âge bâtiment).
 */
export interface DerivedProjectData {
  climateZone: ClimateZone | "";
  departmentCode: string;
  postalMessage: string;
  isOlderThanTwoYears: boolean;
  buildingAgeLabel: string;
}

/**
 * "Action" — paramètres spécifiques par fiche (surface des travaux, R installé,
 * type de PAC, etc.). La structure dépend de chaque fiche.
 */
export type Action = Record<string, unknown>;

/**
 * Catalogue d'une fiche CEE.
 */
export interface SheetDefinition {
  code: string;
  family: "Enveloppe" | "Thermique" | "Equipement" | "Services";
  buildingTypes: BuildingType[];
  title: string;
  pdfUrl?: string;
  /** Évaluateur de la fiche : reçoit le projet + action, retourne un Evaluation prêt. */
  evaluate: (project: Project, action: Action, enabled: boolean) => Evaluation;
}

/**
 * Options pour `applyCommonRules`.
 */
export interface CommonRulesOptions {
  supportedBuildingTypes?: BuildingType[];
  needsSector?: boolean;
  needsHousingType?: boolean;
  requiresExistingBuilding?: boolean;
  supportedSectors?: Sector[] | null;
  supportedHousingTypes?: HousingType[] | null;
  requiredProjectSystemsAll?: (keyof ProjectSystemFlags)[];
  requiredProjectSystemsAny?: (keyof ProjectSystemFlags)[];
}
