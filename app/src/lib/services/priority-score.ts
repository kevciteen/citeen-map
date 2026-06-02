/**
 * Score de priorité copro — heuristique 0–100 calculée à la volée en SQL.
 *
 * Objectif : trier/filtrer les copros par "qui devrait faire l'objet d'une
 * relance maintenant". Pas de table dédiée — recompute = pas de drift, et
 * les variables (DPE, syndic, stage prospect) changent souvent.
 *
 * Formule (cumulative, plafonnée à 100) :
 *   - PASSOIRE  : +30 si classe_finale ∈ (F,G)   (motivation primaire)
 *   - VOLUME    : +min(20, nb_lots / 2)           (taille = enjeu)
 *   - SYNDIC    : +15 si syndic déjà gagné ailleurs (capital social)
 *               : +8  si syndic en cours dans pipeline
 *   - FRAÎCHEUR : +5  si aucune note/tag depuis 6 mois ("dormante à réveiller")
 *   - PÉNALITÉS : -100 si prospect déjà gagné/perdu (= exclu)
 *                 -25  si refus récent (note avec mot "refus" < 90j)
 *
 * Le détail (breakdown) est renvoyé pour expliciter le "pourquoi" du score
 * dans l'UI — c'est essentiel pour qu'un commercial ne fasse pas confiance
 * aveuglément à un nombre.
 */

export type ScoreBreakdown = {
  score: number;
  components: Array<{ label: string; value: number; reason: string }>;
  excluded: boolean;
  excludedReason?: string;
};

/**
 * SQL fragment qui calcule le score pour une copro donnée.
 * À utiliser dans un SELECT avec `c` aliasé sur copros.
 *
 * Retourne un INTEGER de 0 à 100.
 */
export const SCORE_SQL_FRAGMENT = `
  CASE
    WHEN EXISTS (
      SELECT 1 FROM prospects p
      WHERE p.copro_id = c.id AND p.stage IN ('won', 'lost')
    ) THEN 0
    ELSE
      MIN(100,
        -- PASSOIRE (30 pts) : F ou G
        (CASE WHEN e.classe_finale IN ('F', 'G') THEN 30 ELSE 0 END)
        -- VOLUME (max 20 pts) : nb_lots / 2 plafonné
        + MIN(20, COALESCE(c.nb_lots, 0) / 2)
        -- SYNDIC GAGNÉ ailleurs (15 pts)
        + (CASE WHEN EXISTS (
            SELECT 1 FROM copros c2
            JOIN prospects p2 ON p2.copro_id = c2.id
            WHERE c2.syndic = c.syndic AND c.syndic IS NOT NULL
              AND c2.id != c.id AND p2.stage = 'won'
          ) THEN 15 ELSE 0 END)
        -- SYNDIC EN COURS (8 pts, exclusif avec gagné)
        + (CASE WHEN EXISTS (
            SELECT 1 FROM copros c2
            JOIN prospects p2 ON p2.copro_id = c2.id
            WHERE c2.syndic = c.syndic AND c.syndic IS NOT NULL
              AND c2.id != c.id
              AND p2.stage IN ('contacted', 'meeting', 'proposal')
          ) AND NOT EXISTS (
            SELECT 1 FROM copros c2
            JOIN prospects p2 ON p2.copro_id = c2.id
            WHERE c2.syndic = c.syndic AND c.syndic IS NOT NULL
              AND c2.id != c.id AND p2.stage = 'won'
          ) THEN 8 ELSE 0 END)
        -- DORMANTE (5 pts) : aucune activité (note/tag) depuis 6 mois
        + (CASE WHEN NOT EXISTS (
            SELECT 1 FROM entity_notes n
            WHERE n.entity_type = 'copro' AND n.entity_ref = CAST(c.id AS TEXT)
              AND n.created_at > unixepoch() - (180 * 86400)
          ) AND NOT EXISTS (
            SELECT 1 FROM entity_tags t
            WHERE t.entity_type = 'copro' AND t.entity_ref = CAST(c.id AS TEXT)
              AND t.created_at > unixepoch() - (180 * 86400)
          ) THEN 5 ELSE 0 END)
        -- REFUS RÉCENT (-25 pts) : note contenant "refus" dans les 90j
        - (CASE WHEN EXISTS (
            SELECT 1 FROM entity_notes n
            WHERE n.entity_type = 'copro' AND n.entity_ref = CAST(c.id AS TEXT)
              AND n.created_at > unixepoch() - (90 * 86400)
              AND LOWER(n.body) LIKE '%refus%'
          ) THEN 25 ELSE 0 END)
      )
  END
`;

/**
 * Calcule le score + breakdown pour une copro précise.
 * Utilisé par /api/copros/:id/score pour l'affichage détaillé.
 */
type DbLike = {
  get: <T>(q: string, args?: (string | number | null)[]) => Promise<T | undefined>;
};

export async function computeScoreBreakdown(
  db: DbLike,
  coproId: number,
): Promise<ScoreBreakdown> {
  const row = await db.get<{
    nb_lots: number | null;
    classe_finale: string | null;
    syndic: string | null;
    excluded: number;
    syndic_won: number;
    syndic_inprogress: number;
    has_recent_activity: number;
    has_recent_refusal: number;
  }>(
    `
    SELECT
      c.nb_lots,
      e.classe_finale,
      c.syndic,
      CASE WHEN EXISTS (
        SELECT 1 FROM prospects p
        WHERE p.copro_id = c.id AND p.stage IN ('won', 'lost')
      ) THEN 1 ELSE 0 END AS excluded,
      CASE WHEN EXISTS (
        SELECT 1 FROM copros c2
        JOIN prospects p2 ON p2.copro_id = c2.id
        WHERE c2.syndic = c.syndic AND c.syndic IS NOT NULL
          AND c2.id != c.id AND p2.stage = 'won'
      ) THEN 1 ELSE 0 END AS syndic_won,
      CASE WHEN EXISTS (
        SELECT 1 FROM copros c2
        JOIN prospects p2 ON p2.copro_id = c2.id
        WHERE c2.syndic = c.syndic AND c.syndic IS NOT NULL
          AND c2.id != c.id
          AND p2.stage IN ('contacted', 'meeting', 'proposal')
      ) THEN 1 ELSE 0 END AS syndic_inprogress,
      CASE WHEN EXISTS (
        SELECT 1 FROM entity_notes n
        WHERE n.entity_type = 'copro' AND n.entity_ref = CAST(c.id AS TEXT)
          AND n.created_at > unixepoch() - (180 * 86400)
      ) OR EXISTS (
        SELECT 1 FROM entity_tags t
        WHERE t.entity_type = 'copro' AND t.entity_ref = CAST(c.id AS TEXT)
          AND t.created_at > unixepoch() - (180 * 86400)
      ) THEN 1 ELSE 0 END AS has_recent_activity,
      CASE WHEN EXISTS (
        SELECT 1 FROM entity_notes n
        WHERE n.entity_type = 'copro' AND n.entity_ref = CAST(c.id AS TEXT)
          AND n.created_at > unixepoch() - (90 * 86400)
          AND LOWER(n.body) LIKE '%refus%'
      ) THEN 1 ELSE 0 END AS has_recent_refusal
    FROM copros c
    LEFT JOIN dpe_estimates e ON e.copro_id = c.id
    WHERE c.id = ?
    `,
    [coproId],
  );

  if (!row) {
    return { score: 0, components: [], excluded: true, excludedReason: "Copro introuvable" };
  }

  if (row.excluded) {
    return {
      score: 0,
      components: [],
      excluded: true,
      excludedReason: "Prospect déjà gagné ou perdu pour cette copro",
    };
  }

  const components: ScoreBreakdown["components"] = [];

  if (row.classe_finale === "F" || row.classe_finale === "G") {
    components.push({
      label: "Passoire énergétique",
      value: 30,
      reason: `Classe DPE ${row.classe_finale} — obligation de rénovation`,
    });
  }

  const volume = Math.min(20, Math.floor((row.nb_lots ?? 0) / 2));
  if (volume > 0) {
    components.push({
      label: "Volume de lots",
      value: volume,
      reason: `${row.nb_lots} lots — enjeu financier ${volume === 20 ? "maximal" : "significatif"}`,
    });
  }

  if (row.syndic_won) {
    components.push({
      label: "Syndic déjà acquis",
      value: 15,
      reason: `Syndic « ${row.syndic} » a déjà signé ailleurs — capital social`,
    });
  } else if (row.syndic_inprogress) {
    components.push({
      label: "Syndic en cours",
      value: 8,
      reason: `Syndic « ${row.syndic} » est déjà en discussion sur d'autres copros`,
    });
  }

  if (!row.has_recent_activity) {
    components.push({
      label: "Copro dormante",
      value: 5,
      reason: "Aucune note ou tag depuis 6 mois — relance opportune",
    });
  }

  if (row.has_recent_refusal) {
    components.push({
      label: "Refus récent",
      value: -25,
      reason: "Note mentionnant un refus dans les 90 derniers jours",
    });
  }

  const score = Math.max(0, Math.min(100, components.reduce((s, c) => s + c.value, 0)));

  return { score, components, excluded: false };
}
