/**
 * Catalogue des fiches CEE.
 *
 * Mappe chaque code (ex: "BAR-TH-174") à son titre officiel + sa fonction
 * évaluateur + sa famille (déduite du préfixe) + son type de bâtiment.
 *
 * Source des titres : `simulateur-cee-main/js/sheets.js` (équipe Citeen).
 */
import * as R from "./evaluators/residential";
import * as T from "./evaluators/tertiary";
import { evaluateBarTh174, evaluateBarTh175 } from "./engine";
import type { Action, BuildingType, Evaluation, Project } from "./types";

export type SheetFamily = "Enveloppe" | "Thermique" | "Equipement" | "Services";

export interface SheetDefinition {
  code: string;
  title: string;
  family: SheetFamily;
  buildingTypes: BuildingType[];
  evaluate: (project: Project, action: Action, enabled: boolean) => Evaluation;
}

export function familyFromCode(code: string): SheetFamily {
  if (code.includes("-EN-")) return "Enveloppe";
  if (code.includes("-EQ-")) return "Equipement";
  if (code.includes("-SE-")) return "Services";
  return "Thermique";
}

export function buildingTypesFromCode(code: string): BuildingType[] {
  if (code.startsWith("BAR-")) return ["Habitation"];
  if (code.startsWith("BAT-")) return ["Tertiaire"];
  return [];
}

type Evaluator = (
  project: Project,
  action: Action,
  enabled: boolean,
) => Evaluation;

// Mapping code → { title, evaluate }
const CATALOG_RAW: Record<string, { title: string; evaluate: Evaluator }> = {
  // === Résidentiel : Bâtiment Ancien Résidentiel (BAR-*) ===============
  "BAR-EN-101": { title: "Isolation de combles ou de toitures", evaluate: R.evaluateBarEn101 },
  "BAR-EN-102": { title: "Isolation des murs", evaluate: R.evaluateBarEn102 },
  "BAR-EN-103": { title: "Isolation d'un plancher", evaluate: R.evaluateBarEn103 },
  "BAR-EN-104": { title: "Fenêtre ou porte-fenêtre complète avec vitrage isolant", evaluate: R.evaluateBarEn104 },
  "BAR-EN-105": { title: "Isolation des toitures-terrasses", evaluate: R.evaluateBarEn105 },
  "BAR-EN-108": { title: "Fermeture isolante", evaluate: R.evaluateBarEn108 },
  "BAR-EN-110": { title: "Fenêtre ou porte-fenêtre complète avec vitrage pariétodynamique", evaluate: R.evaluateBarEn110 },

  "BAR-EQ-115": { title: "Dispositif d'affichage et d'interprétation des consommations d'énergie", evaluate: R.evaluateBarEq115 },

  "BAR-SE-104": { title: "Réglage des organes d'équilibrage d'une installation de chauffage à eau chaude", evaluate: R.evaluateBarSe104 },
  "BAR-SE-105": { title: "Contrat de Performance Energétique Services (CPE Services)", evaluate: R.evaluateBarSe105 },
  "BAR-SE-106": { title: "Service de suivi des consommations d'énergie", evaluate: R.evaluateBarSe106 },
  "BAR-SE-107": { title: "Abaissement de la température de retour vers un réseau de chaleur", evaluate: R.evaluateBarSe107 },
  "BAR-SE-108": { title: "Désembouage d'un réseau hydraulique individuel de chauffage", evaluate: R.evaluateBarSe108 },

  "BAR-TH-101": { title: "Chauffe-eau solaire individuel", evaluate: R.evaluateBarTh101 },
  "BAR-TH-102": { title: "Chauffe-eau solaire collectif", evaluate: R.evaluateBarTh102 },
  "BAR-TH-110": { title: "Radiateur basse température pour un chauffage central", evaluate: R.evaluateBarTh110 },
  "BAR-TH-111": { title: "Régulation par sonde de température extérieure", evaluate: R.evaluateBarTh111 },
  "BAR-TH-112": { title: "Appareil indépendant de chauffage au bois", evaluate: R.evaluateBarTh112 },
  "BAR-TH-113": { title: "Chaudière biomasse individuelle", evaluate: R.evaluateBarTh113 },
  "BAR-TH-116": { title: "Plancher chauffant hydraulique à basse température", evaluate: R.evaluateBarTh116 },
  "BAR-TH-117": { title: "Robinet thermostatique", evaluate: R.evaluateBarTh117 },
  "BAR-TH-122": { title: "Récupérateur de chaleur à condensation", evaluate: R.evaluateBarTh122 },
  "BAR-TH-123": { title: "Optimiseur de relance en chauffage collectif", evaluate: R.evaluateBarTh123 },
  "BAR-TH-125": { title: "Ventilation mécanique double flux à haute performance", evaluate: R.evaluateBarTh125 },
  "BAR-TH-127": { title: "Ventilation mécanique simple flux hygroréglable", evaluate: R.evaluateBarTh127 },
  "BAR-TH-129": { title: "Pompe à chaleur de type air/air", evaluate: R.evaluateBarTh129 },
  "BAR-TH-130": { title: "Surperformance énergétique pour un bâtiment neuf", evaluate: R.evaluateBarTh130 },
  "BAR-TH-137": { title: "Raccordement d'un bâtiment résidentiel existant à un réseau de chaleur", evaluate: R.evaluateBarTh137 },
  "BAR-TH-139": { title: "Système de variation électronique de vitesse sur une pompe", evaluate: R.evaluateBarTh139 },
  "BAR-TH-143": { title: "Système solaire combiné", evaluate: R.evaluateBarTh143 },
  "BAR-TH-148": { title: "Chauffe-eau thermodynamique à accumulation", evaluate: R.evaluateBarTh148 },
  "BAR-TH-155": { title: "Ventilation hybride hygroréglable", evaluate: R.evaluateBarTh155 },
  "BAR-TH-158": { title: "Émetteur électrique à régulation électronique à fonctions avancées", evaluate: R.evaluateBarTh158 },
  "BAR-TH-159": { title: "Pompe à chaleur hybride individuelle", evaluate: R.evaluateBarTh159 },
  "BAR-TH-161": { title: "Isolation de points singuliers d'un réseau", evaluate: R.evaluateBarTh161 },
  "BAR-TH-162": { title: "Système énergétique comportant des capteurs solaires photovoltaïques et thermiques à circulation d'eau", evaluate: R.evaluateBarTh162 },
  "BAR-TH-165": { title: "Chaudière biomasse collective", evaluate: R.evaluateBarTh165 },
  "BAR-TH-168": { title: "Dispositif solaire thermique", evaluate: R.evaluateBarTh168 },
  "BAR-TH-169": { title: "Pompe à chaleur collective pour l'eau chaude sanitaire", evaluate: R.evaluateBarTh169 },
  "BAR-TH-170": { title: "Récupération de chaleur fatale issue de serveurs informatiques pour l'ECS collective", evaluate: R.evaluateBarTh170 },
  "BAR-TH-171": { title: "Pompe à chaleur de type air/eau", evaluate: R.evaluateBarTh171 },
  "BAR-TH-172": { title: "Pompe à chaleur de type eau/eau ou eau glycolée/eau", evaluate: R.evaluateBarTh172 },
  "BAR-TH-173": { title: "Système de régulation par programmation horaire pièce par pièce", evaluate: R.evaluateBarTh173 },
  "BAR-TH-174": { title: "Rénovation d'ampleur d'une maison individuelle", evaluate: evaluateBarTh174 },
  "BAR-TH-175": { title: "Rénovation d'ampleur d'un appartement", evaluate: evaluateBarTh175 },
  "BAR-TH-176": { title: "Système de régulation de la consommation d'un chauffe-eau électrique à effet Joule", evaluate: R.evaluateBarTh176 },
  "BAR-TH-177": { title: "Rénovation thermique globale d'un bâtiment résidentiel collectif existant", evaluate: R.evaluateBarTh177 },
  "BAR-TH-178": { title: "Système géothermique", evaluate: R.evaluateBarTh178 },
  "BAR-TH-179": { title: "Pompe à chaleur collective de type air/eau", evaluate: R.evaluateBarTh179 },
  "BAR-TH-180": { title: "Pompe à chaleur collective de type eau/eau ou eau glycolée/eau", evaluate: R.evaluateBarTh180 },

  // === Tertiaire : Bâtiment Tertiaire (BAT-*) ===========================
  "BAT-EN-101": { title: "Isolation de combles ou toitures", evaluate: T.evaluate101 },
  "BAT-EN-102": { title: "Isolation des murs", evaluate: T.evaluate102 },
  "BAT-EN-103": { title: "Isolation d'un plancher", evaluate: T.evaluate103 },
  "BAT-EN-104": { title: "Fenêtre ou porte-fenêtre complète avec vitrage isolant", evaluate: T.evaluate104 },
  "BAT-EN-107": { title: "Isolation des toitures-terrasses", evaluate: T.evaluate107 },
  "BAT-EN-111": { title: "Fenêtre ou porte-fenêtre complète avec vitrage pariétodynamique", evaluate: T.evaluate111 },
  "BAT-EN-112": { title: "Revêtements réflectifs en toiture", evaluate: T.evaluate112 },
  "BAT-EN-113": { title: "Façade rideau ou semi-rideau avec vitrage isolant", evaluate: T.evaluate113 },

  "BAT-EQ-117": { title: "Installation frigorifique utilisant du CO2 subcritique ou transcritique", evaluate: T.evaluateEq117 },
  "BAT-EQ-123": { title: "Moto-variateur synchrone à aimants permanents ou à réluctance", evaluate: T.evaluateEq123 },
  "BAT-EQ-124": { title: "Fermeture des meubles frigorifiques de vente à température positive", evaluate: T.evaluateEq124 },
  "BAT-EQ-125": { title: "Fermeture des meubles frigorifiques de vente à température négative", evaluate: T.evaluateEq125 },
  "BAT-EQ-129": { title: "Lanterneaux d'éclairage zénithal", evaluate: T.evaluateEq129 },
  "BAT-EQ-130": { title: "Système de condensation frigorifique à haute efficacité", evaluate: T.evaluateEq130 },
  "BAT-EQ-131": { title: "Conduits de lumière naturelle", evaluate: T.evaluateEq131 },
  "BAT-EQ-134": { title: "Meuble frigorifique de vente performant avec groupe de production de froid intégré", evaluate: T.evaluateEq134 },
  "BAT-EQ-135": { title: "Dispositif performant d'alimentation sans interruption", evaluate: T.evaluateEq135 },

  "BAT-SE-103": { title: "Réglage des organes d'équilibrage d'une installation de chauffage à eau chaude", evaluate: T.evaluateSe103 },
  "BAT-SE-104": { title: "Contrat de Performance Energétique Services (CPE Services) Chauffage", evaluate: T.evaluateBatSe104 },
  "BAT-SE-105": { title: "Abaissement de la température de retour vers un réseau de chaleur", evaluate: T.evaluateBatSe105 },

  "BAT-TH-103": { title: "Plancher chauffant hydraulique à basse température", evaluate: T.evaluateBatTh103 },
  "BAT-TH-105": { title: "Radiateur basse température pour un chauffage central", evaluate: T.evaluateBatTh105 },
  "BAT-TH-108": { title: "Système de régulation par programmation d'intermittence", evaluate: T.evaluateBatTh108 },
  "BAT-TH-109": { title: "Optimiseur de relance en chauffage collectif comprenant une fonction auto-adaptative", evaluate: T.evaluateBatTh109 },
  "BAT-TH-110": { title: "Récupérateur de chaleur à condensation", evaluate: T.evaluateBatTh110 },
  "BAT-TH-111": { title: "Chauffe-eau solaire collectif", evaluate: T.evaluateBatTh111 },
  "BAT-TH-112": { title: "Système de variation électronique de vitesse sur un moteur asynchrone", evaluate: T.evaluateBatTh112 },
  "BAT-TH-116": { title: "Système de gestion technique du bâtiment", evaluate: T.evaluate116 },
  "BAT-TH-125": { title: "Ventilation mécanique simple flux à débit d'air constant ou modulé", evaluate: T.evaluateBatTh125 },
  "BAT-TH-126": { title: "Ventilation mécanique double flux avec échangeur à débit d'air constant ou modulé", evaluate: T.evaluateBatTh126 },
  "BAT-TH-127": { title: "Raccordement d'un bâtiment tertiaire à un réseau de chaleur", evaluate: T.evaluateBatTh127 },
  "BAT-TH-134": { title: "Système de régulation sur un groupe de production de froid (haute pression flottante)", evaluate: T.evaluateBatTh134 },
  "BAT-TH-139": { title: "Récupération de chaleur sur un groupe de production de froid", evaluate: T.evaluateBatTh139 },
  "BAT-TH-142": { title: "Destratification d'air", evaluate: T.evaluate142 },
  "BAT-TH-143": { title: "Ventilo-convecteur performant", evaluate: T.evaluateBatTh143 },
  "BAT-TH-145": { title: "Système de régulation sur un groupe de production de froid (basse pression flottante)", evaluate: T.evaluateBatTh145 },
  "BAT-TH-153": { title: "Confinement des allées dans un centre de données", evaluate: T.evaluateBatTh153 },
  "BAT-TH-154": { title: "Système de récupération de chaleur sur eaux grises", evaluate: T.evaluateBatTh154 },
  "BAT-TH-156": { title: "Système de refroidissement gratuit par air extérieur", evaluate: T.evaluateBatTh156 },
  "BAT-TH-157": { title: "Chaudière biomasse collective", evaluate: T.evaluateBatTh157 },
  "BAT-TH-158": { title: "Pompe à chaleur réversible de type air/air", evaluate: T.evaluateTh158 },
  "BAT-TH-159": { title: "Raccordement d'un bâtiment tertiaire à un réseau de froid", evaluate: T.evaluateBatTh159 },
  "BAT-TH-161": { title: "Maintien en température des groupes électrogènes par PAC air/eau", evaluate: T.evaluateBatTh161 },
  "BAT-TH-162": { title: "Système géothermique", evaluate: T.evaluateBatTh162 },
  "BAT-TH-163": { title: "Pompe à chaleur air/eau", evaluate: T.evaluate163 },
  "BAT-TH-164": { title: "Pompe à chaleur eau/eau ou eau glycolée/eau", evaluate: T.evaluateBatTh164 },
};

// === Catalogue final enrichi ============================================

export const SHEETS: Record<string, SheetDefinition> = Object.fromEntries(
  Object.entries(CATALOG_RAW).map(([code, { title, evaluate }]) => [
    code,
    {
      code,
      title,
      family: familyFromCode(code),
      buildingTypes: buildingTypesFromCode(code),
      evaluate,
    },
  ]),
);

export const SHEET_CODES = Object.keys(SHEETS);
export const SHEET_CODES_HABITATION = SHEET_CODES.filter((c) =>
  SHEETS[c].buildingTypes.includes("Habitation"),
);
export const SHEET_CODES_TERTIAIRE = SHEET_CODES.filter((c) =>
  SHEETS[c].buildingTypes.includes("Tertiaire"),
);
