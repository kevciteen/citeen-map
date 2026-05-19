/**
 * Catalogue des champs UI par fiche CEE.
 *
 * Porté depuis `simulateur-cee-main/js/sheets.js`. Pour chaque code de fiche,
 * on liste les champs à afficher dans l'UI "Compléter la fiche". Les noms
 * (`name`) et options (`options`) sont conservés EXACTEMENT comme dans la
 * source (ils servent de keys pour le moteur de calcul). Les `label` ont été
 * réaccentués pour l'affichage.
 *
 * Ne porte PAS : requirements, compute, evaluate, notes — uniquement la
 * structure de saisie utilisateur.
 */

import type { Action, Project } from "./types";

export interface SheetField {
  type: "select" | "number" | "checkbox" | "text";
  name: string;
  label: string;
  options?: string[];
  /** Affiché seulement si la condition est vraie. */
  showWhen?: (action: Action, project: Project) => boolean;
  /** Si la value n'est pas dans options mais référencée comme option valide. */
  defaultValue?: string;
  /** Groupe optionnel (ex "sheet" / "coupDePouce") pour regroupement UI. */
  group?: "sheet" | "coupDePouce";
}

export const SHEET_FIELDS: Record<string, SheetField[]> = {
  // ============================================================
  // BAT-* (Tertiaire)
  // ============================================================

  "BAT-TH-116": [
    { type: "select", name: "operationMode", label: "Nature de l'opération", options: ["", "Achat d'un système neuf", "Amélioration d'un système existant"] },
    { type: "select", name: "installedClass", label: "Classe du système installé", options: ["", "A", "B"] },
    { type: "checkbox", name: "existingClassAtMostC", label: "En cas d'amélioration, le système existant est au plus de classe C", showWhen: (action) => action.operationMode === "Amelioration d'un systeme existant" },
    { type: "checkbox", name: "heatingPresent", label: "Le système pilote le chauffage" },
    { type: "number", name: "heatingSurface", label: "Surface gérée - chauffage (m²)", showWhen: (action) => Boolean(action.heatingPresent) },
    { type: "checkbox", name: "dhwPresent", label: "Le système pilote l'ECS" },
    { type: "number", name: "dhwSurface", label: "Surface gérée - ECS (m²)", showWhen: (action) => Boolean(action.dhwPresent) },
    { type: "checkbox", name: "coolingPresent", label: "Le système pilote le refroidissement / la climatisation" },
    { type: "number", name: "coolingSurface", label: "Surface gérée - refroidissement / climatisation (m²)", showWhen: (action) => Boolean(action.coolingPresent) },
    { type: "checkbox", name: "lightingPresent", label: "Le système pilote l'éclairage" },
    { type: "number", name: "lightingSurface", label: "Surface gérée - éclairage (m²)", showWhen: (action) => Boolean(action.lightingPresent) },
    { type: "checkbox", name: "auxiliariesPresent", label: "Le système pilote les auxiliaires" },
    { type: "number", name: "auxSurface", label: "Surface gérée - auxiliaires (m²)", showWhen: (action) => Boolean(action.auxiliariesPresent) },
  ],

  "BAT-TH-163": [
    { type: "number", name: "heatedSurface", label: "Surface totale chauffée (m²)" },
    { type: "select", name: "usage", label: "Usage", options: ["", "Chauffage", "Chauffage + ECS", "ECS uniquement"] },
    { type: "select", name: "powerBand", label: "Puissance de la PAC", options: ["", "Inferieure ou egale a 400 kW", "Superieure a 400 kW"] },
    { type: "select", name: "temperatureType", label: "Type PAC si puissance <= 400 kW", options: ["", "Basse temperature", "Moyenne ou haute temperature"], showWhen: (action) => action.powerBand === "Inferieure ou egale a 400 kW" },
    { type: "select", name: "etasBand", label: "Tranche d'Etas", options: ["", "Inferieur a 111 %", "De 111 % a moins de 126 %", "De 126 % a moins de 175 %", "175 % ou plus"], showWhen: (action) => action.powerBand === "Inferieure ou egale a 400 kW" },
    { type: "select", name: "copBand", label: "Tranche de COP", options: ["", "Inferieur a 3,4", "De 3,4 a moins de 4,5", "4,5 ou plus"], showWhen: (action) => action.powerBand === "Superieure a 400 kW" },
    // Coup de pouce (BAT-TH-163)
    { type: "select", name: "coupDePouceReplacedEquipment", label: "Équipement remplacé", options: ["", "Chaudiere charbon / fioul / gaz", "Autre"], group: "coupDePouce" },
    { type: "select", name: "coupDePouceHeatNetworkImpossible", label: "L'impossibilité technique ou économique de raccordement au réseau de chaleur est justifiée", options: ["", "Oui", "Non"], group: "coupDePouce" },
  ],

  "BAT-EN-101": [
    { type: "number", name: "insulatedSurface", label: "Surface à isoler (m²)" },
  ],

  "BAT-EN-102": [
    { type: "number", name: "insulatedWallSurface", label: "Surface de murs à isoler (m²)" },
    { type: "select", name: "heatingEnergy", label: "Énergie de chauffage", options: ["", "Electricite", "Combustible"] },
  ],

  "BAT-EN-103": [
    { type: "number", name: "insulatedFloorSurface", label: "Surface de plancher à isoler (m²)" },
  ],

  "BAT-EN-104": [
    { type: "number", name: "installedWindowSurface", label: "Surface de fenêtres / portes-fenêtres installées (m²)" },
  ],

  "BAT-EN-107": [
    { type: "number", name: "terraceInsulatedSurface", label: "Surface de toiture-terrasse à isoler (m²)" },
    { type: "select", name: "heatingEnergy", label: "Énergie de chauffage", options: ["", "Electricite", "Combustible"] },
  ],

  "BAT-EN-111": [
    { type: "number", name: "parietodynamicSurface", label: "Surface de fenêtres / portes-fenêtres posées (m²)" },
    { type: "select", name: "compatibleVentilation", label: "Les locaux sont équipés d'une ventilation compatible", options: ["", "Oui", "Non"] },
  ],

  "BAT-EN-112": [
    { type: "number", name: "reflectiveRoofSurface", label: "Surface de toiture couverte par le revêtement réflectif (m²)" },
    { type: "select", name: "heatCoolByPac", label: "La production de chaud et de froid est assurée par une PAC", options: ["", "Oui", "Non"] },
  ],

  "BAT-EN-113": [
    { type: "number", name: "curtainWallSurface", label: "Surface de façade rideau / semi-rideau installée (m²)" },
  ],

  "BAT-EQ-117": [
    { type: "select", name: "caseType", label: "Cas d'opération (voir fiche)", options: ["", "Cas 1", "Cas 2", "Cas 3"] },
    { type: "select", name: "case3Option", label: "Option de l'installation CO2 transcritique", options: ["", "Option 0", "Option 1", "Option 1 bis", "Option 2"], showWhen: (action) => action.caseType === "Cas 3" },
    { type: "select", name: "saturatedFeed", label: "Alimentation des évaporateurs en régime saturé (option 3)", options: ["", "Oui", "Non"], showWhen: (action) => action.caseType === "Cas 3" && Boolean(action.case3Option) },
    { type: "number", name: "positivePowerKw", label: "Puissance frigorifique utile positive (kW)", showWhen: (action) => action.caseType === "Cas 1" || action.caseType === "Cas 3" },
    { type: "number", name: "negativePowerKw", label: "Puissance frigorifique utile négative (kW)", showWhen: (action) => action.caseType === "Cas 2" || action.caseType === "Cas 3" },
  ],

  "BAT-EQ-123": [
    { type: "select", name: "eligibleTechnology", label: "Le moto-variateur est bien à aimants permanents ou à reluctance", options: ["", "Oui", "Non"] },
    { type: "select", name: "application", label: "Application principale", options: ["", "Chauffage, pompage", "Ventilation, renouvellement d'air", "Refrigeration", "Climatisation", "Autres applications"] },
    { type: "select", name: "unitPowerLimit", label: "Chaque moteur unitaire est inférieur ou égal à 1 MW", options: ["", "Oui", "Non"] },
    { type: "number", name: "totalPowerKw", label: "Somme des puissances installées (kW)" },
  ],

  "BAT-EQ-124": [
    { type: "select", name: "foodRetailOpenToPublic", label: "Le local est un commerce alimentaire ouvert au public", options: ["", "Oui", "Non"] },
    { type: "number", name: "linearGlassLength", label: "Longueur linéaire de portes en verre (m)" },
  ],

  "BAT-EQ-125": [
    { type: "select", name: "foodRetailOpenToPublic", label: "Le local est un commerce alimentaire ouvert au public", options: ["", "Oui", "Non"] },
    { type: "select", name: "meubleArchitecture", label: "Architecture du meuble", options: ["", "Simple", "Double", "Combine"] },
    { type: "number", name: "coverLength", label: "Longueur totale de couvercles installés (m)" },
  ],

  "BAT-EQ-129": [
    { type: "select", name: "lightingPilot", label: "L'éclairage électrique est piloté automatiquement", options: ["", "Oui", "Non"] },
    { type: "select", name: "withCurb", label: "Les lanterneaux sont tous avec costière", options: ["", "Oui", "Non"] },
    { type: "select", name: "lanternType", label: "Type de lanterneaux", options: ["", "Ponctuel fixe", "Ponctuel ouvrant", "Continu"] },
    { type: "number", name: "atFlatArea", label: "Aire At flat totale (m²)" },
  ],

  "BAT-EQ-130": [
    { type: "select", name: "condensationSystemType", label: "Type de système de condensation", options: ["", "Condenseur a eau seul", "Condenseur a air sec", "Condenseur evaporatif / tour"] },
    { type: "select", name: "coldApplicationType", label: "Application du groupe de froid", options: ["", "Climatisation de confort", "Climatisation en datacenter", "Refrigeration / ambiance hors confort"] },
    { type: "select", name: "newCe1Building", label: "Le bâtiment neuf relève de la catégorie CE1", options: ["", "Oui", "Non"], showWhen: (action) => action.coldApplicationType === "Climatisation de confort" },
    // Options dynamiques dans la source (dépendent de condensationSystemType) — non portées.
    { type: "select", name: "deltaT", label: "Delta T retenu pour le calcul (°C)", options: [""] },
    { type: "number", name: "nominalColdPowerKw", label: "Puissance électrique nominale totale du groupe de froid (kW)" },
  ],

  "BAT-EQ-131": [
    { type: "select", name: "lightingPilot", label: "L'éclairage électrique est piloté selon les apports de lumière naturelle", options: ["", "Oui", "Non"] },
    { type: "number", name: "totalTubeSection", label: "Somme des sections de conduits installés S (m²)" },
  ],

  "BAT-EQ-134": [
    { type: "select", name: "foodRetailOpenToPublic", label: "Le site est un commerce alimentaire ouvert au public", options: ["", "Oui", "Non"] },
    { type: "select", name: "fridgeType", label: "Type de meuble frigorifique", options: ["", "Armoire verticale / semi-verticale / mixte", "Armoire horizontale", "Congelateur vertical / mixte", "Congelateur horizontal"] },
    { type: "select", name: "efficiencyClass", label: "Classe d'efficacité énergétique", options: ["", "A", "B", "C", "D"] },
    { type: "number", name: "installedLength", label: "Longueur totale de meubles installés (m)" },
  ],

  "BAT-EQ-135": [
    { type: "select", name: "isDataCenter", label: "Le dispositif est installé pour un centre de données", options: ["", "Oui", "Non"] },
    { type: "select", name: "class1Asi", label: "Le dispositif ASI est de classe 1", options: ["", "Oui", "Non"] },
    { type: "select", name: "efficiency98", label: "Le rendement du dispositif est au moins de 98 %", options: ["", "Oui", "Non"] },
    { type: "number", name: "asiPowerKw", label: "Puissance active de sortie assignée (kW)" },
  ],

  "BAT-TH-142": [
    { type: "select", name: "heightBand", label: "Hauteur sous plafond / faîtage", options: ["", "Inferieure a 5 m", "Superieure ou egale a 5 m"] },
    { type: "select", name: "heatingMode", label: "Système de chauffage", options: ["", "Convectif", "Radiatif", "Mixte"] },
    { type: "number", name: "convectivePowerKw", label: "Puissance nominale convective (kW)", showWhen: (action) => action.heatingMode === "Convectif" || action.heatingMode === "Mixte" },
    { type: "number", name: "radiativePowerKw", label: "Puissance nominale radiative (kW)", showWhen: (action) => action.heatingMode === "Radiatif" || action.heatingMode === "Mixte" },
  ],

  "BAT-SE-103": [
    { type: "select", name: "collectiveHotWaterHeating", label: "Le bâtiment est équipé d'une installation collective de chauffage à eau chaude", options: ["", "Oui", "Non"] },
    { type: "select", name: "temperatureGapUnder2", label: "Après équilibrage, l'écart de température est inférieur à 2 °C", options: ["", "Oui", "Non"] },
    { type: "number", name: "heatedSurface", label: "Surface totale chauffée (m²)" },
  ],

  "BAT-SE-104": [
    { type: "select", name: "cpeServicesContract", label: "Le contrat est un CPE Services portant sur une installation collective de chauffage", options: ["", "Oui", "Non"] },
    { type: "number", name: "heatedSurface104", label: "Surface chauffée (m²)" },
    { type: "select", name: "contractDurationBand", label: "Durée du contrat", options: ["", "2", "3", "4", "5", "6", "7", "8", "9", "10 ou plus"] },
    { type: "select", name: "useEcs104", label: "Le contrat couvre l'ECS", options: ["", "Oui", "Non"] },
    { type: "select", name: "useComfortCooling104", label: "Le contrat couvre la climatisation pour le confort", options: ["", "Oui", "Non"] },
    { type: "select", name: "useSpecificElectricity104", label: "Le contrat couvre l'électricité spécifique", options: ["", "Oui", "Non"] },
  ],

  "BAT-SE-105": [
    { type: "select", name: "connectedToHeatNetwork", label: "Le bâtiment est raccordé à un réseau de chaleur", options: ["", "Oui", "Non"] },
    { type: "number", name: "heatedSurface105", label: "Surface chauffée (m²)" },
  ],

  "BAT-TH-103": [
    { type: "select", name: "lowTempHydraulicFloor", label: "Le système installé est bien un plancher chauffant hydraulique basse température", options: ["", "Oui", "Non"] },
    { type: "number", name: "heatedSurface", label: "Surface chauffée (m²)" },
  ],

  "BAT-TH-105": [
    { type: "select", name: "lowTempRadiators", label: "Les équipements installés sont des radiateurs basse température", options: ["", "Oui", "Non"] },
    { type: "number", name: "heatedSurface", label: "Surface concernée (m²)" },
  ],

  "BAT-TH-108": [
    { type: "select", name: "intermittentRegulation", label: "Le système installé assure une programmation d'intermittence", options: ["", "Oui", "Non"] },
    { type: "number", name: "heatedSurface", label: "Surface régulée (m²)" },
    { type: "select", name: "heatingEnergy", label: "Énergie de chauffage", options: ["", "Combustible", "Electricite"] },
  ],

  "BAT-TH-109": [
    { type: "select", name: "collectiveHeating", label: "L'installation de chauffage est collective", options: ["", "Oui", "Non"] },
    { type: "select", name: "autoAdaptive", label: "L'optimiseur comprend une fonction auto-adaptative", options: ["", "Oui", "Non"] },
  ],

  "BAT-TH-110": [
    { type: "select", name: "condensingRecovery", label: "L'équipement installé est un récupérateur de chaleur à condensation", options: ["", "Oui", "Non"] },
    { type: "select", name: "recoveredUse", label: "Usage couvert", options: ["", "Chauffage", "Chauffage et ECS"] },
    { type: "select", name: "replacesWholeBoilerRoom", label: "La nouvelle chaufferie ne comporte pas d'autres équipements à prendre en compte", options: ["", "Oui", "Non"] },
    { type: "number", name: "p1RecoveredEquipmentPowerKw", label: "P1 - Puissance des équipements remplacés (kW)", showWhen: (action) => action.replacesWholeBoilerRoom === "Non" },
    { type: "number", name: "p2OtherHeatingPowerKw", label: "P2 - Autres équipements de chauffage de la nouvelle chaufferie (kW)", showWhen: (action) => action.replacesWholeBoilerRoom === "Non" },
    { type: "number", name: "p3OtherEcsPowerKw", label: "P3 - Autres équipements ECS de la nouvelle chaufferie (kW)", showWhen: (action) => action.replacesWholeBoilerRoom === "Non" },
    { type: "number", name: "p4DhwPacPowerKw", label: "P4 - Puissance PAC ECS de la nouvelle chaufferie (kW)", showWhen: (action) => action.replacesWholeBoilerRoom === "Non" && action.recoveredUse === "Chauffage et ECS" },
  ],

  "BAT-TH-111": [
    { type: "select", name: "collectiveSolarWaterHeater", label: "L'équipement installé est un chauffe-eau solaire collectif", options: ["", "Oui", "Non"] },
    { type: "number", name: "annualDhwNeedKwh", label: "B - Besoin annuel en ECS à produire par l'énergie solaire (kWh/an)" },
    { type: "number", name: "solarCoveragePercent", label: "T - Taux de couverture solaire (%)" },
  ],

  "BAT-TH-112": [
    { type: "select", name: "asynchronousMotorVariation", label: "Le système installé est une variation électronique de vitesse sur moteur asynchrone", options: ["", "Oui", "Non"] },
    { type: "select", name: "motorApplication", label: "Application principale", options: ["", "Chauffage, pompage", "Ventilation, renouvellement d'air", "Refrigeration", "Climatisation", "Autres applications"] },
    { type: "number", name: "totalPowerKw", label: "Somme des puissances concernées (kW)" },
  ],

  "BAT-TH-125": [
    { type: "select", name: "simpleFlowVentilation", label: "Le système installé est une ventilation simple flux", options: ["", "Oui", "Non"] },
    { type: "select", name: "simpleFlowType", label: "Type de ventilation simple flux", options: ["", "Simple flux modulee proportionnelle", "Simple flux modulee a detection de presence", "Simple flux a debit d'air constant"] },
    { type: "select", name: "ventilationSector125", label: "Catégorie de locaux", options: ["", "Bureaux", "Enseignement", "Restauration", "Autres locaux"] },
    { type: "number", name: "ventilatedSurface", label: "Surface ventilée (m²)" },
  ],

  "BAT-TH-126": [
    { type: "select", name: "doubleFlowVentilation", label: "Le système installé est une ventilation double flux", options: ["", "Oui", "Non"] },
    { type: "select", name: "doubleFlowType", label: "Type de ventilation double flux", options: ["", "Double flux modulee proportionnelle", "Double flux modulee a detection de presence", "Double flux a debit d'air constant"] },
    { type: "select", name: "ventilationSector126", label: "Catégorie de locaux", options: ["", "Bureaux", "Enseignement", "Restauration", "Etablissement sportif", "Salles > 250 m3", "Autres locaux"] },
    { type: "number", name: "ventilatedSurface", label: "Surface ventilée (m²)" },
  ],

  "BAT-TH-127": [
    { type: "select", name: "heatNetworkConnection", label: "Le projet concerne un raccordement à un réseau de chaleur", options: ["", "Oui", "Non"] },
    { type: "number", name: "connectedSurface", label: "Surface concernée (m²)" },
    { type: "select", name: "connectionUse", label: "Type de raccordement", options: ["", "Chauffage", "Chauffage et ECS"] },
    { type: "select", name: "subscribedPowerBand", label: "Puissance souscrite", options: ["", "<= 400 kW", "> 400 kW"] },
    // Coup de pouce (BAT-TH-127)
    { type: "select", name: "coupDePouceReplacedEquipment", label: "Équipement remplacé", options: ["", "Chaudiere charbon / fioul / gaz", "Autre"], group: "coupDePouce" },
  ],

  "BAT-TH-134": [
    { type: "select", name: "highPressureFloating", label: "Le système permet une haute pression flottante", options: ["", "Oui", "Non"] },
    { type: "select", name: "coldApplication134", label: "Application du groupe de froid", options: ["", "Climatisation de confort", "Climatisation Data Center", "Refrigeration / ambiance hors confort"] },
    { type: "select", name: "newCe1Building134", label: "Le bâtiment neuf relève de la catégorie CE1", options: ["", "Oui", "Non"], showWhen: (action) => action.coldApplication134 === "Climatisation de confort" },
    { type: "select", name: "condensationFamily134", label: "Famille de condensation", options: ["", "Condensation par rapport a l'atmosphere", "Condensation a eau seule"] },
    { type: "number", name: "nominalColdPowerKw", label: "Puissance nominale du groupe de froid (kW)" },
  ],

  "BAT-TH-139": [
    { type: "select", name: "heatRecoveryColdGroup", label: "Le projet récupère la chaleur sur un groupe de froid", options: ["", "Oui", "Non"] },
    { type: "number", name: "recoveredHeatPowerKw", label: "Puissance thermique récupérée (kW)" },
    { type: "number", name: "heatRecoveryDurationHours", label: "Durée annuelle d'utilisation de la chaleur récupérée (h/an)" },
  ],

  "BAT-TH-143": [
    { type: "select", name: "efficientFanCoil", label: "L'équipement installé est un ventilo-convecteur performant", options: ["", "Oui", "Non"] },
    { type: "select", name: "usageCategory143", label: "Catégorie d'usage", options: ["", "Sante avec hebergement", "Hotels et autres hebergements", "Sante sans hebergement", "Bureaux, restauration, commerces", "Autres secteurs"] },
    { type: "number", name: "heatedSurface143", label: "Surface totale chauffée (m²)" },
    { type: "number", name: "cooledSurface143", label: "Surface totale rafraîchie (m²)" },
  ],

  "BAT-TH-145": [
    { type: "select", name: "lowPressureFloating", label: "Le système permet une basse pression flottante", options: ["", "Oui", "Non"] },
    { type: "select", name: "coldApplication145", label: "Application du groupe de froid", options: ["", "Refrigeration / ambiance hors confort", "Climatisation de confort"] },
    { type: "select", name: "newCe1Building145", label: "Le bâtiment neuf relève de la catégorie CE1", options: ["", "Oui", "Non"], showWhen: (action) => action.coldApplication145 === "Climatisation de confort" },
    { type: "number", name: "nominalColdPowerKw", label: "Puissance nominale du groupe de froid (kW)" },
  ],

  "BAT-TH-153": [
    { type: "select", name: "isDataCenter", label: "Le projet concerne un centre de données", options: ["", "Oui", "Non"] },
    { type: "select", name: "aisleContainmentType", label: "Type de confinement", options: ["", "Allee froide", "Allee chaude"] },
    { type: "number", name: "dataRoomArea", label: "Surface du centre de données (m²)" },
  ],

  "BAT-TH-154": [
    { type: "select", name: "greyWaterRecovery", label: "Le projet met en place une récupération de chaleur sur eaux grises", options: ["", "Oui", "Non"] },
    { type: "select", name: "usage154", label: "Usage du bâtiment", options: ["", "Hotellerie", "Etablissement sportif", "Sante", "Terrain de camping", "Salon de coiffure", "Piscine - renouvellement + lavages", "Piscine - renouvellement seul"] },
    { type: "select", name: "installationMode154", label: "Mode d'installation", options: ["", "Debits inegaux", "Debits egaux"] },
    { type: "number", name: "unitCount154", label: "Nombre d'unités concernées" },
    { type: "number", name: "efficiency154", label: "Efficacité du récupérateur (%)" },
  ],

  "BAT-TH-156": [
    { type: "select", name: "freeCooling", label: "Le système met en place un refroidissement gratuit par air extérieur", options: ["", "Oui", "Non"] },
    { type: "select", name: "application156", label: "Secteur d'application", options: ["", "Climatisation hors Data Center", "Climatisation Data Center"] },
    { type: "select", name: "temperatureBand156", label: "Plage de températures de consigne du réseau", options: ["", "[15C ; 18C[", "[18C ; 20C]"] },
    { type: "number", name: "networkPowerKw156", label: "Puissance thermique du réseau (kW)" },
  ],

  "BAT-TH-157": [
    { type: "select", name: "collectiveBiomassBoiler", label: "Le projet concerne une chaudière biomasse collective", options: ["", "Oui", "Non"] },
    { type: "select", name: "biomassFuel157", label: "La biomasse utilisée est une biomasse ligneuse éligible", options: ["", "Oui", "Non"] },
    { type: "select", name: "boilerPowerBand157", label: "Puissance nominale de la chaudière", options: ["", "<= 500 kW", "> 500 kW"] },
    { type: "number", name: "usefulHeatProducedKwh157", label: "Q - Quantité de chaleur nette utile produite (kWh/an)" },
  ],

  "BAT-TH-158": [
    { type: "select", name: "pacFamily", label: "Type de PAC", options: ["", "PAC <= 12 kW", "PAC > 12 kW", "PAC rooftop"] },
    { type: "number", name: "heatedSurface", label: "Surface totale chauffée / traitée (m²)" },
  ],

  "BAT-TH-159": [
    { type: "select", name: "coldNetworkConnection", label: "Le projet concerne un raccordement à un réseau de froid", options: ["", "Oui", "Non"] },
    { type: "select", name: "sector159", label: "Secteur d'usage du bâtiment", options: ["", "Data Center", "Cafes, hotels, restaurants", "Sante", "Enseignement, recherche", "Sport, loisirs, culture", "Bureaux", "Commerces", "Autres"] },
    { type: "number", name: "connectedColdPowerKw159", label: "Puissance thermique souscrite (kW)" },
  ],

  "BAT-TH-161": [
    { type: "select", name: "backupGenerators", label: "Le projet concerne des groupes électrogènes de secours", options: ["", "Oui", "Non"] },
    { type: "number", name: "generatorRoomPowerKw", label: "Puissance concernée (kW)" },
    { type: "number", name: "generatorCount161", label: "Nombre de groupes équipés" },
  ],

  "BAT-TH-162": [
    { type: "select", name: "sourcePacEligible", label: "Le projet concerne bien un système géothermique avec PAC eau/eau ou eau glycolée/eau", options: ["", "Oui", "Non"] },
    { type: "number", name: "heatedSurface", label: "Surface concernée (m²)" },
    { type: "select", name: "usage", label: "Usage couvert", options: ["", "Chauffage", "Chauffage et ECS", "ECS uniquement"] },
    { type: "select", name: "powerBand", label: "Puissance thermique nominale", options: ["", "<= 400 kW", "> 400 kW"] },
    { type: "select", name: "temperatureType", label: "Type de PAC si puissance <= 400 kW", options: ["", "Basse temperature", "Moyenne ou haute temperature"], showWhen: (action) => action.powerBand === "<= 400 kW" },
    { type: "select", name: "etasBand", label: "Tranche d'Etas", options: ["", "111 % a moins de 126 %", "126 % a moins de 175 %", "175 % ou plus"], showWhen: (action) => action.powerBand === "<= 400 kW" },
    { type: "select", name: "copBand", label: "Tranche de COP", options: ["", "4 a moins de 5", "5 ou plus"], showWhen: (action) => action.powerBand === "> 400 kW" },
    { type: "select", name: "replacesWholeBoilerRoom", label: "La nouvelle chaufferie ne comporte pas d'autres équipements à prendre en compte", options: ["", "Oui", "Non"] },
    { type: "number", name: "installedPacPowerKw", label: "Puissance nominale totale du système installé (kW)", showWhen: (action) => action.replacesWholeBoilerRoom === "Non" },
    { type: "number", name: "totalBoilerRoomPowerKw", label: "Puissance totale utile de la nouvelle chaufferie (kW)", showWhen: (action) => action.replacesWholeBoilerRoom === "Non" },
    // Coup de pouce (BAT-TH-162)
    { type: "select", name: "coupDePouceReplacedEquipment", label: "Équipement remplacé", options: ["", "Chaudiere charbon / fioul / gaz", "Autre"], group: "coupDePouce" },
    { type: "select", name: "coupDePouceHeatNetworkImpossible", label: "L'impossibilité technique ou économique de raccordement au réseau de chaleur est justifiée", options: ["", "Oui", "Non"], group: "coupDePouce" },
  ],

  "BAT-TH-164": [
    { type: "select", name: "sourcePacEligible", label: "La PAC installée est de type eau/eau ou eau glycolée/eau", options: ["", "Oui", "Non"] },
    { type: "number", name: "heatedSurface", label: "Surface totale chauffée (m²)" },
    { type: "select", name: "usage", label: "Usage couvert", options: ["", "Chauffage", "Chauffage et ECS", "ECS uniquement"] },
    { type: "select", name: "powerBand", label: "Puissance thermique nominale", options: ["", "<= 400 kW", "> 400 kW"] },
    { type: "select", name: "temperatureType", label: "Type de PAC si puissance <= 400 kW", options: ["", "Basse temperature", "Moyenne ou haute temperature"], showWhen: (action) => action.powerBand === "<= 400 kW" },
    { type: "select", name: "etasBand", label: "Tranche d'Etas", options: ["", "111 % a moins de 126 %", "126 % a moins de 175 %", "175 % ou plus"], showWhen: (action) => action.powerBand === "<= 400 kW" },
    { type: "select", name: "copBand", label: "Tranche de COP", options: ["", "3,4 a moins de 4,5", "4,5 ou plus"], showWhen: (action) => action.powerBand === "> 400 kW" },
    { type: "select", name: "replacesWholeBoilerRoom", label: "La nouvelle chaufferie ne comporte pas d'autres équipements à prendre en compte", options: ["", "Oui", "Non"] },
    { type: "number", name: "installedPacPowerKw", label: "Puissance nominale totale des PAC installées (kW)", showWhen: (action) => action.replacesWholeBoilerRoom === "Non" },
    { type: "number", name: "totalBoilerRoomPowerKw", label: "Puissance totale utile de la nouvelle chaufferie (kW)", showWhen: (action) => action.replacesWholeBoilerRoom === "Non" },
    // Coup de pouce (BAT-TH-164)
    { type: "select", name: "coupDePouceReplacedEquipment", label: "Équipement remplacé", options: ["", "Chaudiere charbon / fioul / gaz", "Autre"], group: "coupDePouce" },
    { type: "select", name: "coupDePouceHeatNetworkImpossible", label: "L'impossibilité technique ou économique de raccordement au réseau de chaleur est justifiée", options: ["", "Oui", "Non"], group: "coupDePouce" },
  ],

  // ============================================================
  // BAR-* (Résidentiel)
  // ============================================================

  "BAR-EN-101": [
    { type: "number", name: "insulatedSurface", label: "Surface à isoler (m²)" },
  ],

  "BAR-EN-102": [
    { type: "number", name: "insulatedWallSurface", label: "Surface de murs à isoler (m²)" },
  ],

  "BAR-EN-103": [
    { type: "number", name: "insulatedFloorSurface", label: "Surface de plancher à isoler (m²)" },
  ],

  "BAR-EN-104": [
    { type: "number", name: "installedWindowSurface", label: "Surface de menuiseries installées (m²)" },
  ],

  "BAR-EN-105": [
    { type: "number", name: "terraceInsulatedSurface", label: "Surface de toiture-terrasse à isoler (m²)" },
  ],

  "BAR-EN-108": [
    { type: "number", name: "closingSurface", label: "Surface équipée de fermetures isolantes (m²)" },
  ],

  "BAR-EN-110": [
    { type: "number", name: "parietodynamicCount", label: "Nombre de fenêtres ou portes-fenêtres posées" },
    { type: "select", name: "compatibleVentilation", label: "Le logement est équipé d'une ventilation compatible", options: ["", "Oui", "Non"] },
  ],

  "BAR-EQ-115": [
    { type: "select", name: "displayProvisionCompliant", label: "Le dispositif est acheté ou loué pour au moins 24 mois", options: ["", "Oui", "Non"] },
    { type: "select", name: "individualMetersIfCollectiveFuel", label: "En cas de chauffage collectif par combustible, le logement dispose de compteurs individuels ou de répartiteurs", options: ["", "Oui", "Non", "Sans objet"] },
    { type: "select", name: "comfortMonitoringOption", label: "Le dispositif inclut l'option suivi du confort", options: ["", "Oui", "Non"] },
  ],

  "BAR-SE-104": [
    { type: "select", name: "collectiveHotWaterHeating", label: "Le bâtiment est équipé d'un chauffage collectif à eau chaude", options: ["", "Oui", "Non"] },
    { type: "number", name: "apartmentCount", label: "Nombre d'appartements concernés" },
  ],

  "BAR-SE-105": [
    { type: "select", name: "cpeServicesContract", label: "Le contrat relève bien d'un CPE Services", options: ["", "Oui", "Non"] },
    { type: "number", name: "apartmentCount", label: "Nombre d'appartements concernés" },
    { type: "select", name: "guaranteeDurationYears", label: "Durée de garantie éligible (années pleines)", options: ["", "2", "3", "4", "5", "6", "7", "8", "9", "10 ou plus"] },
  ],

  "BAR-SE-106": [
    {
      type: "select",
      name: "followedEnergyScope",
      label: "Énergies couvertes par le service de suivi",
      options: [
        "",
        "Electricite specifique uniquement",
        "Electricite totale (chauffage electrique + electricite specifique)",
        "Gaz + electricite specifique",
      ],
    },
  ],

  "BAR-SE-107": [
    { type: "select", name: "connectedToHeatNetwork", label: "Le bâtiment est raccordé à un réseau de chaleur", options: ["", "Oui", "Non"] },
    { type: "number", name: "apartmentCount", label: "Nombre d'appartements concernés" },
  ],

  "BAR-SE-108": [
    { type: "select", name: "individualHydraulicHeating", label: "Le logement dispose d'un chauffage individuel hydraulique", options: ["", "Oui", "Non"] },
    { type: "select", name: "heatingPowerUnder70Kw", label: "La puissance thermique nominale est inférieure ou égale à 70 kW", options: ["", "Oui", "Non"] },
    { type: "select", name: "notHeatedByExcludedHeatPump", label: "La boucle d'eau n'est pas chauffée par une PAC air/eau, eau/eau, sol/eau ou hybride", options: ["", "Oui", "Non"] },
  ],

  "BAR-TH-101": [
    { type: "select", name: "individualSolarWaterHeater", label: "Le projet concerne un chauffe-eau solaire individuel", options: ["", "Oui", "Non"] },
    { type: "number", name: "collectorAreaM2", label: "Surface de capteurs solaires (m²)" },
    { type: "select", name: "coversAllDhwNeeds", label: "Le chauffe-eau couvre la totalité du besoin d'eau chaude sanitaire", options: ["", "Oui", "Non"] },
    { type: "select", name: "fluidTypeEligible", label: "Les capteurs sont à circulation d'eau ou d'eau glycolée", options: ["", "Oui", "Non"] },
    { type: "select", name: "backupEnergyType", label: "Énergie de l'appoint", options: ["", "Electrique a effet Joule", "Autre"] },
    { type: "select", name: "drawOffProfile", label: "Profil de soutirage déclaré", options: ["", "M", "L", "XL", "XXL"] },
    { type: "select", name: "waterHeatingEfficiencyCompliant", label: "L'efficacité énergétique respecte le seuil officiel du profil et de l'appoint", options: ["", "Oui", "Non"] },
  ],

  "BAR-TH-102": [
    { type: "select", name: "collectiveSolarWaterHeater", label: "Le projet concerne un chauffe-eau solaire collectif", options: ["", "Oui", "Non"] },
    { type: "number", name: "dhwSolarNeedKwh", label: "Besoin annuel en eau chaude sanitaire produit par l'énergie solaire B (kWh/an)" },
    { type: "number", name: "solarCoverageRate", label: "Taux de couverture solaire T (%)" },
  ],

  "BAR-TH-110": [
    { type: "select", name: "lowTempRadiators", label: "Les équipements installés sont des radiateurs basse température", options: ["", "Oui", "Non"] },
    { type: "number", name: "radiatorCount", label: "Nombre de radiateurs installés" },
    { type: "select", name: "apartmentHeatingMode", label: "Type de chauffage de l'appartement", options: ["", "Individuel", "Collectif"], showWhen: (_action, project) => project.housingType === "Appartement" },
  ],

  "BAR-TH-111": [
    { type: "select", name: "outsideTemperatureProbe", label: "Le chauffage est régulé par sonde de température extérieure", options: ["", "Oui", "Non"] },
    { type: "select", name: "heatingEnergy", label: "Énergie du chauffage", options: ["", "Electricite", "Combustible"] },
  ],

  "BAR-TH-112": [
    { type: "select", name: "independentWoodHeater", label: "L'équipement installé est un appareil indépendant de chauffage au bois", options: ["", "Oui", "Non"] },
    { type: "select", name: "woodHeaterType", label: "Type d'appareil", options: ["", "Poele", "Insert / foyer ferme", "Cuisiniere"] },
    { type: "select", name: "performanceCompliant", label: "L'appareil respecte les performances officielles de la fiche ou bénéficie du label Flamme verte 7*", options: ["", "Oui", "Non"] },
    // Coup de pouce (BAR-TH-112)
    { type: "select", name: "coupDePoucePrimaryResidence", label: "Le logement est occupé à titre de résidence principale", options: ["", "Oui", "Non"], group: "coupDePouce" },
    { type: "select", name: "coupDePouceReplacedIndependentCoalHeater", label: "Équipement remplacé", options: ["", "Equipement independant de chauffage au charbon", "Autre"], group: "coupDePouce" },
  ],

  "BAR-TH-113": [
    { type: "select", name: "biomassBoiler", label: "Le projet concerne une chaudière biomasse individuelle", options: ["", "Oui", "Non"] },
    { type: "select", name: "class5OrFlammeVerte", label: "La chaudière est de classe 5 NF EN 303.5 ou bénéficie du label Flamme verte", options: ["", "Oui", "Non"] },
    // Coup de pouce (BAR-TH-113)
    { type: "select", name: "coupDePoucePrimaryResidence", label: "Le logement est occupé à titre de résidence principale", options: ["", "Oui", "Non"], group: "coupDePouce" },
    { type: "select", name: "coupDePouceReplacedEquipment", label: "Équipement remplacé", options: ["", "Chaudiere charbon / fioul / gaz", "Autre"], group: "coupDePouce" },
  ],

  "BAR-TH-116": [
    { type: "select", name: "lowTempHydraulicFloor", label: "Le système installé est un plancher chauffant hydraulique basse température", options: ["", "Oui", "Non"] },
    { type: "number", name: "heatedSurface", label: "Surface chauffée (m²)" },
    { type: "select", name: "apartmentHeatingMode", label: "L'appartement est équipé d'un chauffage individuel", options: ["", "Oui", "Non"], showWhen: (_action, project) => project.housingType === "Appartement" },
  ],

  "BAR-TH-117": [
    { type: "select", name: "thermostaticValves", label: "Le projet installe des robinets thermostatiques", options: ["", "Oui", "Non"] },
    { type: "select", name: "centralFuelBoiler", label: "Les radiateurs sont raccordés à un chauffage central à combustible avec chaudière existante", options: ["", "Oui", "Non"] },
    { type: "number", name: "valveCount", label: "Nombre de robinets thermostatiques" },
    { type: "select", name: "apartmentHeatingMode", label: "Type de chauffage de l'appartement", options: ["", "Individuel", "Collectif"], showWhen: (_action, project) => project.housingType === "Appartement" },
  ],

  "BAR-TH-122": [
    { type: "select", name: "condensingRecovery", label: "Le projet installe un récupérateur de chaleur à condensation", options: ["", "Oui", "Non"] },
    { type: "number", name: "apartmentCount", label: "Nombre d'appartements concernés" },
    { type: "number", name: "equippedBoilerPowerKw", label: "Puissance des chaudières nouvellement équipées (kW)" },
    { type: "number", name: "newPlantPowerKw", label: "Puissance totale de la nouvelle chaufferie (kW)" },
  ],

  "BAR-TH-123": [
    { type: "select", name: "collectiveHeating", label: "L'installation de chauffage est collective", options: ["", "Oui", "Non"] },
    { type: "select", name: "autoAdaptive", label: "L'optimiseur comprend une fonction auto-adaptative", options: ["", "Oui", "Non"] },
    { type: "select", name: "summerWinterSwitch", label: "Le dispositif intègre une fonction commutateur été/hiver", options: ["", "Oui", "Non"] },
    { type: "select", name: "nightSetback", label: "Le dispositif intègre une fonction descente de température", options: ["", "Oui", "Non"] },
    { type: "number", name: "apartmentCount", label: "Nombre d'appartements" },
  ],

  "BAR-TH-125": [
    { type: "select", name: "doubleFlowVentilation", label: "Le logement reçoit une ventilation double flux", options: ["", "Oui", "Non"] },
    { type: "select", name: "installationMode", label: "Type d'installation", options: ["", "Collective", "Individuelle autoreglable", "Individuelle modulee hygroreglable"] },
    { type: "number", name: "dwellingCount", label: "Nombre de logements desservis", showWhen: (action) => action.installationMode === "Collective" },
  ],

  "BAR-TH-127": [
    { type: "select", name: "hygroVentilation", label: "Le logement reçoit une ventilation simple flux hygroréglable", options: ["", "Oui", "Non"] },
    { type: "select", name: "installationMode", label: "Type d'installation", options: ["", "Collective", "Individuelle"] },
    { type: "select", name: "vmcType", label: "Type de ventilation", options: ["", "Type A", "Type B"] },
    { type: "select", name: "fanType", label: "Type de caisson", options: ["", "Caisson basse consommation", "Caisson standard", "Caisson basse pression"] },
    { type: "number", name: "dwellingCount", label: "Nombre de logements desservis", showWhen: (action) => action.installationMode === "Collective" },
  ],

  "BAR-TH-129": [
    { type: "select", name: "airAirPac", label: "La PAC installée est de type air/air", options: ["", "Oui", "Non"] },
    { type: "number", name: "heatedSurface", label: "Surface exclusivement chauffée par la PAC (m²)" },
    { type: "select", name: "nominalPowerAtMost12", label: "La puissance nominale est inférieure ou égale à 12 kW", options: ["", "Oui", "Non"] },
    { type: "select", name: "scopEligible", label: "Le SCOP est supérieur ou égal à 3,9", options: ["", "Oui", "Non"] },
    { type: "select", name: "houseScopBand", label: "Tranche de SCOP", options: ["", "3,9 a moins de 4,3", "4,3 ou plus"], showWhen: (_action, project) => project.housingType === "Maison individuelle" },
  ],

  "BAR-TH-130": [
    { type: "select", name: "newResidentialBuilding", label: "Le projet concerne un bâtiment résidentiel neuf", options: ["", "Oui", "Non"] },
    { type: "number", name: "referenceSurface", label: "Surface de référence RE2020 (m²)" },
    { type: "number", name: "cefMax", label: "Consommation conventionnelle d'énergie finale maximale Cefmax (kWh/m².an)" },
    { type: "number", name: "cef", label: "Consommation conventionnelle d'énergie finale du bâtiment Cef (kWh/m².an)" },
    { type: "select", name: "bbioCompliant", label: "Le projet vérifie Bbio < 0,9 Bbiomax", options: ["", "Oui", "Non"] },
    { type: "select", name: "icenergyCompliant", label: "Le projet vérifie Icenergie < Icenergie_max", options: ["", "Oui", "Non"] },
  ],

  "BAR-TH-137": [
    { type: "select", name: "heatNetworkConnection", label: "Le projet concerne un raccordement à un réseau de chaleur", options: ["", "Oui", "Non"] },
    { type: "select", name: "neverConnectedLast5Years", label: "Le bâtiment n'a pas été raccordé à un réseau de chaleur dans les 5 dernières années", options: ["", "Oui", "Non"] },
    { type: "select", name: "noPriorCeeOnConnection", label: "Les raccordements précédents n'ont pas fait l'objet d'une demande de CEE", options: ["", "Oui", "Non"] },
    { type: "number", name: "apartmentCount", label: "Nombre d'appartements raccordés", showWhen: (_action, project) => project.housingType !== "Maison individuelle" },
    // Coup de pouce (BAR-TH-137)
    { type: "select", name: "coupDePoucePrimaryResidence", label: "Le logement est occupé à titre de résidence principale", options: ["", "Oui", "Non"], showWhen: (_action, project) => project.housingType === "Maison individuelle", group: "coupDePouce" },
    { type: "select", name: "coupDePouceReplacedEquipment", label: "Équipement remplacé", options: ["", "Chaudiere charbon / fioul / gaz", "Autre"], group: "coupDePouce" },
  ],

  "BAR-TH-139": [
    { type: "select", name: "pumpVariation", label: "Le projet installe une variation électronique de vitesse sur une pompe", options: ["", "Oui", "Non"] },
    { type: "number", name: "pumpPowerKw", label: "Puissance de la pompe (kW)" },
  ],

  "BAR-TH-143": [
    { type: "select", name: "solarCombinedSystem", label: "Le projet concerne un système solaire combiné", options: ["", "Oui", "Non"] },
    { type: "select", name: "lowTempEmitters", label: "Le système est couplé à des émetteurs de chauffage central basse température", options: ["", "Oui", "Non"] },
    { type: "select", name: "collectorProductivityEligible", label: "Les capteurs atteignent la productivité officielle minimale et ne sont pas hybrides", options: ["", "Oui", "Non"] },
    { type: "select", name: "collectorAreaEligible", label: "La surface de capteurs est supérieure ou égale à 8 m²", options: ["", "Oui", "Non"] },
    { type: "select", name: "storageEligible", label: "La capacité de stockage est strictement supérieure à 400 L", options: ["", "Oui", "Non"] },
    // Coup de pouce (BAR-TH-143)
    { type: "select", name: "coupDePoucePrimaryResidence", label: "Le logement est occupé à titre de résidence principale", options: ["", "Oui", "Non"], group: "coupDePouce" },
    { type: "select", name: "coupDePouceReplacedEquipment", label: "Équipement remplacé", options: ["", "Chaudiere charbon / fioul / gaz", "Autre"], group: "coupDePouce" },
  ],

  "BAR-TH-148": [
    { type: "select", name: "thermodynamicWaterHeater", label: "Le projet concerne un chauffe-eau thermodynamique à accumulation", options: ["", "Oui", "Non"] },
    { type: "select", name: "drawOffProfile", label: "Profil de soutirage", options: ["", "M", "L", "XL"] },
    { type: "select", name: "waterHeatingEfficiencyCompliant", label: "L'efficacité énergétique respecte le seuil officiel du profil déclaré", options: ["", "Oui", "Non"] },
  ],

  "BAR-TH-155": [
    { type: "select", name: "hybridVentilation", label: "Le projet met en place une ventilation hybride hygroréglable", options: ["", "Oui", "Non"] },
    { type: "select", name: "naturalOrNoVentilation", label: "L'appartement était équipé d'une ventilation naturelle ou sans système de ventilation", options: ["", "Oui", "Non"] },
    { type: "number", name: "apartmentCount", label: "Nombre d'appartements" },
    { type: "select", name: "installationType", label: "Type d'installation", options: ["", "Type A", "Type B"] },
    { type: "select", name: "extractorType", label: "Type d'extracteur", options: ["", "Basse consommation", "Standard"] },
    { type: "select", name: "technicalOpinionValid", label: "Le système bénéficie d'un avis technique du CCFAT en cours de validité ou équivalent", options: ["", "Oui", "Non"] },
  ],

  "BAR-TH-158": [
    { type: "select", name: "advancedElectricEmitter", label: "Le projet installe des émetteurs électriques à régulation avancée", options: ["", "Oui", "Non"] },
    { type: "number", name: "emitterCount", label: "Nombre d'émetteurs remplacés" },
  ],

  "BAR-TH-159": [
    { type: "select", name: "hybridPac", label: "Le projet concerne une PAC hybride individuelle", options: ["", "Oui", "Non"] },
    { type: "select", name: "usage", label: "Usage couvert", options: ["", "Chauffage", "Chauffage et ECS"] },
    { type: "select", name: "etasBand", label: "Tranche d'efficacité énergétique saisonnière (Etas)", options: ["", "111 % a moins de 120 %", "120 % a moins de 130 %", "130 % a moins de 140 %", "140 % a moins de 150 %", "150 % a moins de 160 %", "160 % ou plus"] },
    { type: "select", name: "coverRateAtLeast70", label: "Le taux de couverture de la PAC hors appoint est supérieur ou égal à 70 %", options: ["", "Oui", "Non"] },
    { type: "select", name: "regulatorEligible", label: "Le régulateur relève d'une classe IV à VIII", options: ["", "Oui", "Non"] },
    { type: "select", name: "mediumHighTemperaturePac", label: "La PAC est de type moyenne ou haute température", options: ["", "Oui", "Non"] },
    { type: "number", name: "heatedSurface", label: "Surface chauffée (m²)" },
  ],

  "BAR-TH-161": [
    { type: "select", name: "networkSingularPointInsulation", label: "Le projet isole des points singuliers d'un réseau", options: ["", "Oui", "Non"] },
    { type: "select", name: "dnBand", label: "Diamètre nominal (DN) de la canalisation", options: ["", "20 a 65", "65 a 100", "plus de 100"] },
    { type: "select", name: "fluidTemperatureBand", label: "Température du fluide caloporteur", options: ["", "50 a 120", "plus de 120"] },
    { type: "number", name: "singularPointCount", label: "Nombre de housses isolantes mises en place" },
    { type: "select", name: "insulationCompliant", label: "Les housses respectent les exigences officielles de résistance thermique et de composition", options: ["", "Oui", "Non"] },
  ],

  "BAR-TH-162": [
    { type: "select", name: "pvtSystem", label: "Le projet concerne un système énergétique photovoltaïque + thermique", options: ["", "Oui", "Non"] },
    { type: "select", name: "hybridCollectorsOnly", label: "Les capteurs sont exclusivement hybrides et leur productivité atteint le seuil officiel", options: ["", "Oui", "Non"] },
    { type: "select", name: "minimumAreaReached", label: "La surface totale de capteurs hybrides est au minimum de 6 m²", options: ["", "Oui", "Non"] },
  ],

  "BAR-TH-165": [
    { type: "select", name: "collectiveBiomassBoiler", label: "Le projet concerne une chaudière biomasse collective", options: ["", "Oui", "Non"] },
    { type: "select", name: "powerBand", label: "Puissance thermique nominale de la chaudière", options: ["", "500 ou moins", "plus de 500"] },
    { type: "number", name: "usefulHeatProducedKwh", label: "Chaleur nette utile annuelle produite Q (kWh/an)" },
  ],

  "BAR-TH-168": [
    { type: "select", name: "solarThermalDevice", label: "Le projet concerne un dispositif solaire thermique", options: ["", "Oui", "Non"] },
    { type: "select", name: "usage", label: "Usage du dispositif", options: ["", "ECS seule", "ECS et chauffage"] },
    { type: "select", name: "collectorAreaEligible", label: "La surface de capteurs respecte le seuil officiel selon l'usage", options: ["", "Oui", "Non"] },
    { type: "select", name: "collectorPerformanceEligible", label: "Les capteurs respectent la productivité officielle, ne sont pas hybrides et le dispositif est livré sans appoint", options: ["", "Oui", "Non"] },
    { type: "number", name: "collectorAreaM2", label: "Surface hors-tout de capteurs solaires mise en place (m²)" },
  ],

  "BAR-TH-169": [
    { type: "select", name: "collectiveDhwPac", label: "Le projet concerne une PAC collective pour l'ECS", options: ["", "Oui", "Non"] },
    { type: "number", name: "dwellingCount", label: "Nombre de logements concernés" },
    { type: "select", name: "copBand", label: "Tranche de COP de la PAC installée", options: ["", "2,8 a moins de 3,2", "3,2 a moins de 3,6", "3,6 a moins de 4", "4 a moins de 4,4", "4,4 a moins de 4,8", "4,8 ou plus"] },
    { type: "number", name: "factorR", label: "Facteur R" },
  ],

  "BAR-TH-170": [
    { type: "select", name: "serverHeatRecovery", label: "Le projet récupère la chaleur de serveurs pour l'ECS collective", options: ["", "Oui", "Non"] },
    { type: "number", name: "electricalPowerKw", label: "Puissance électrique de l'installation (kW)" },
  ],

  "BAR-TH-171": [
    { type: "select", name: "airWaterPac", label: "La PAC installée est de type air/eau", options: ["", "Oui", "Non"] },
    { type: "select", name: "temperatureType", label: "Application", options: ["", "Basse temperature", "Moyenne ou haute temperature"] },
    { type: "select", name: "usage", label: "Usage couvert", options: ["", "Chauffage", "Chauffage + ECS"] },
    { type: "select", name: "etasBand", label: "Tranche d'efficacité énergétique saisonnière (Etas)", options: ["", "111 % a moins de 140 %", "140 % ou plus"] },
    { type: "select", name: "regulatorEligible", label: "Le régulateur relève d'une classe IV à VIII", options: ["", "Oui", "Non"] },
    { type: "select", name: "apartmentHeatingMode", label: "L'appartement est équipé d'un chauffage individuel", options: ["", "Oui", "Non"], showWhen: (_action, project) => project.housingType === "Appartement" },
    { type: "number", name: "heatedSurface", label: "Surface chauffée (m²)" },
    // Coup de pouce (BAR-TH-171)
    { type: "select", name: "coupDePoucePrimaryResidence", label: "Le logement est occupé à titre de résidence principale", options: ["", "Oui", "Non"], group: "coupDePouce" },
    { type: "select", name: "coupDePouceReplacedEquipment", label: "Équipement remplacé", options: ["", "Chaudiere charbon / fioul / gaz", "Autre"], group: "coupDePouce" },
  ],

  "BAR-TH-172": [
    { type: "select", name: "waterWaterPac", label: "La PAC installée est de type eau/eau ou eau glycolée/eau", options: ["", "Oui", "Non"] },
    { type: "select", name: "temperatureType", label: "Application", options: ["", "Basse temperature", "Moyenne ou haute temperature"] },
    { type: "select", name: "usage", label: "Usage couvert", options: ["", "Chauffage", "Chauffage + ECS"] },
    { type: "select", name: "performanceBand", label: "Tranche de performance", options: ["", "111 % a moins de 170 %", "170 % ou plus"] },
    { type: "select", name: "regulatorEligible", label: "Le régulateur relève d'une classe IV à VIII", options: ["", "Oui", "Non"] },
    { type: "number", name: "heatedSurface", label: "Surface chauffée (m²)" },
    // Coup de pouce (BAR-TH-172)
    { type: "select", name: "coupDePoucePrimaryResidence", label: "Le logement est occupé à titre de résidence principale", options: ["", "Oui", "Non"], group: "coupDePouce" },
    { type: "select", name: "coupDePouceReplacedEquipment", label: "Équipement remplacé", options: ["", "Chaudiere charbon / fioul / gaz", "Autre"], group: "coupDePouce" },
  ],

  "BAR-TH-173": [
    { type: "select", name: "roomByRoomProgramming", label: "Le projet installe une programmation horaire pièce par pièce", options: ["", "Oui", "Non"] },
    { type: "select", name: "heatingType", label: "Type de chauffage", options: ["", "A emetteurs electriques", "A boucle d'eau chaude", "Mixte (hydraulique et electrique)"] },
    { type: "select", name: "apartmentHeatingMode", label: "L'appartement est équipé d'un chauffage individuel", options: ["", "Oui", "Non"], showWhen: (_action, project) => project.housingType === "Appartement" },
    { type: "number", name: "equippedEmitterCount", label: "Nombre d'émetteurs équipés" },
  ],

  "BAR-TH-174": [
    { type: "select", name: "classJumpCount", label: "Nombre de sauts de classe énergétique", options: ["", "2", "3", "4 ou plus"], group: "sheet" },
    { type: "select", name: "priorWorkStageDone", label: "Une première étape de travaux a déjà été réalisée", options: ["Non", "Oui"], group: "sheet" },
    { type: "select", name: "firstStageClassJumpCount", label: "Nombre de sauts de classe de la première étape", options: ["", "1", "2", "3", "4 ou plus"], showWhen: (action) => action.priorWorkStageDone === "Oui", group: "sheet" },
    { type: "select", name: "secondaryResidence", label: "Le logement est une résidence secondaire", options: ["", "Non", "Oui"], group: "coupDePouce" },
    { type: "select", name: "anahRenovationRoute", label: "Le projet relève d'un parcours Anah / MaPrimeRénov' rénovation d'ampleur", options: ["", "Non", "Oui"], group: "coupDePouce" },
  ],

  "BAR-TH-175": [
    { type: "select", name: "classJumpCount", label: "Nombre de sauts de classe énergétique", options: ["", "2", "3", "4 ou plus"], group: "sheet" },
    { type: "select", name: "priorWorkStageDone", label: "Une première étape de travaux a déjà été réalisée", options: ["Non", "Oui"], group: "sheet" },
    { type: "select", name: "firstStageClassJumpCount", label: "Nombre de sauts de classe de la première étape", options: ["", "1", "2", "3", "4 ou plus"], showWhen: (action) => action.priorWorkStageDone === "Oui", group: "sheet" },
    { type: "select", name: "secondaryResidence", label: "Le logement est une résidence secondaire", options: ["", "Non", "Oui"], group: "coupDePouce" },
    { type: "select", name: "anahRenovationRoute", label: "Le projet relève d'un parcours Anah / MaPrimeRénov' rénovation d'ampleur", options: ["", "Non", "Oui"], group: "coupDePouce" },
  ],

  "BAR-TH-176": [
    { type: "select", name: "jouleWaterHeaterControl", label: "Le projet installe un système de régulation sur chauffe-eau électrique à effet Joule", options: ["", "Oui", "Non"] },
    { type: "select", name: "classACompliantSystem", label: "Le système répond aux fonctionnalités requises de classe A", options: ["", "Oui", "Non"] },
    { type: "select", name: "waterHeaterTankSize", label: "Taille du chauffe-eau", options: ["", "10 L a 150 L", "Plus de 150 L"] },
  ],

  "BAR-TH-177": [
    { type: "select", name: "globalCollectiveRenovation", label: "Le projet relève d'une rénovation thermique globale du bâtiment", options: ["", "Oui", "Non"] },
    { type: "select", name: "atLeastThreeDistinctTaxHouseholds", label: "Le bâtiment comprend au moins 3 foyers fiscaux distincts rattachés à des logements distincts", options: ["", "Oui", "Non"] },
    { type: "select", name: "energyAuditDone", label: "Un audit énergétique préalable a bien été réalisé", options: ["", "Oui", "Non"] },
    { type: "select", name: "cepBelow331", label: "Le Cep projet est inférieur à 331 kWh/m².an", options: ["", "Oui", "Non"] },
    { type: "select", name: "energyGainAtLeast35", label: "Le gain énergétique du projet est d'au moins 35 %", options: ["", "Oui", "Non"] },
    { type: "select", name: "ghgReduced", label: "Les émissions de GES après rénovation sont inférieures ou égales à l'état initial", options: ["", "Oui", "Non"] },
    { type: "select", name: "fossilHeatingBlocked", label: "Le projet évite les cas d'exclusion sur charbon, fioul ou gaz > 30 % hors réseau de chaleur", options: ["", "Oui", "Non"] },
  ],

  "BAR-TH-178": [
    { type: "select", name: "geothermalSystem", label: "Le projet concerne un système géothermique", options: ["", "Oui", "Non"] },
    { type: "number", name: "dwellingCount", label: "Nombre d'appartements chauffés" },
    { type: "select", name: "usage", label: "Usage couvert", options: ["", "Chauffage", "Chauffage et ECS"] },
    { type: "select", name: "powerBand", label: "Puissance thermique nominale totale des PAC", options: ["", "400 ou moins", "plus de 400"] },
    { type: "select", name: "performanceBand", label: "Tranche de performance", options: ["", "111 % a moins de 126 %", "126 % a moins de 150 %", "150 % a moins de 175 %", "175 % a moins de 190 %", "190 % ou plus", "4 a moins de 4,5", "4,5 a moins de 5", "5 a moins de 5,5", "5,5 ou plus"] },
    { type: "number", name: "pacPowerSharePercent", label: "Part de puissance PAC dans la nouvelle chaufferie (%)" },
    // Coup de pouce (BAR-TH-178)
    { type: "select", name: "coupDePouceReplacedEquipment", label: "Équipement remplacé", options: ["", "Chaudiere charbon / fioul / gaz", "Autre"], group: "coupDePouce" },
    { type: "select", name: "coupDePouceHeatNetworkImpossible", label: "L'impossibilité technique ou économique de raccordement au réseau de chaleur est justifiée", options: ["", "Oui", "Non"], group: "coupDePouce" },
  ],

  "BAR-TH-179": [
    { type: "select", name: "collectiveAirWaterPac", label: "Le projet concerne une PAC collective air/eau", options: ["", "Oui", "Non"] },
    { type: "number", name: "dwellingCount", label: "Nombre de logements desservis" },
    { type: "select", name: "usage", label: "Usage couvert", options: ["", "Chauffage", "Chauffage et ECS"] },
    { type: "select", name: "etasBand", label: "Tranche d'efficacité énergétique saisonnière (Etas)", options: ["", "111 % a moins de 126 %", "126 % a moins de 150 %", "150 % a moins de 175 %", "175 % a moins de 190 %", "190 % ou plus"] },
    { type: "number", name: "pacPowerSharePercent", label: "Part de puissance PAC dans la nouvelle chaufferie (%)" },
    // Coup de pouce (BAR-TH-179)
    { type: "select", name: "coupDePouceReplacedEquipment", label: "Équipement remplacé", options: ["", "Chaudiere charbon / fioul / gaz", "Autre"], group: "coupDePouce" },
    { type: "select", name: "coupDePouceHeatNetworkImpossible", label: "L'impossibilité technique ou économique de raccordement au réseau de chaleur est justifiée", options: ["", "Oui", "Non"], group: "coupDePouce" },
  ],

  "BAR-TH-180": [
    { type: "select", name: "collectiveWaterWaterPac", label: "Le projet concerne une PAC collective eau/eau ou eau glycolée/eau", options: ["", "Oui", "Non"] },
    { type: "number", name: "dwellingCount", label: "Nombre de logements desservis" },
    { type: "select", name: "usage", label: "Usage couvert", options: ["", "Chauffage", "Chauffage et ECS"] },
    { type: "select", name: "powerBand", label: "Puissance thermique nominale totale des PAC", options: ["", "Inferieure ou egale a 400 kW", "Superieure a 400 kW"] },
    { type: "select", name: "performanceBand", label: "Tranche de performance", options: ["", "111 % a moins de 126 %", "126 % a moins de 150 %", "150 % a moins de 175 %", "175 % a moins de 190 %", "190 % ou plus", "4 a moins de 4,5", "4,5 a moins de 5", "5 a moins de 5,5", "5,5 ou plus"] },
    { type: "number", name: "pacPowerSharePercent", label: "Part de puissance PAC dans la nouvelle chaufferie (%)" },
    // Coup de pouce (BAR-TH-180)
    { type: "select", name: "coupDePouceReplacedEquipment", label: "Équipement remplacé", options: ["", "Chaudiere charbon / fioul / gaz", "Autre"], group: "coupDePouce" },
    { type: "select", name: "coupDePouceHeatNetworkImpossible", label: "L'impossibilité technique ou économique de raccordement au réseau de chaleur est justifiée", options: ["", "Oui", "Non"], group: "coupDePouce" },
  ],
};
