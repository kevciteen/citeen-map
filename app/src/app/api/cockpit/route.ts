/**
 * GET /api/cockpit
 *
 * Renvoie en un seul appel toutes les données du Cockpit pilotage :
 *  - kpis : passoires actives, relances jour, pipeline value, taux conversion
 *  - tasks : à faire aujourd'hui (next_actions <= today)
 *  - kanban : prospects par stage (compact)
 *  - topSyndics : syndics F+G non en pipeline (potentiel)
 *  - activity : notes/tags récents cross-entité
 *  - prospectsMap : points pour la mini-carte (lat, lon, dpe)
 */
import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import { ensureAuth } from "@/lib/auth/guards";
import { ensureEntityOverrides } from "@/lib/db/ensure-entity-overrides";
import { ensureProspectExtras } from "@/lib/db/ensure-prospect-extras";

export const runtime = "nodejs";

type Stage =
  | "lead"
  | "to_contact"
  | "contacted"
  | "meeting"
  | "proposal"
  | "won"
  | "lost";

export async function GET(req: NextRequest) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  await Promise.all([ensureEntityOverrides(), ensureProspectExtras()]);

  // Filtre "Mes prospects" : si ?mine=1, on filtre toutes les requêtes
  // prospects pour ne garder que ceux assignés à l'utilisateur courant
  // (assigned_user_id = FK users.id — convention en place via ensure-prospect-extras).
  const mine = req.nextUrl.searchParams.get("mine") === "1";
  const meId = Number(guard.id);
  const mineP = mine ? `AND p.assigned_user_id = ${meId}` : "";
  const mineNoAlias = mine ? `AND assigned_user_id = ${meId}` : "";

  // === KPIs ===
  // 1. Passoires F+G en pipeline actif (non won/lost)
  const passoiresRow = await db.get<{ n: number }>(`
    SELECT COUNT(DISTINCT p.id) AS n
    FROM prospects p
    LEFT JOIN copros c ON c.id = p.copro_id
    LEFT JOIN dpe_estimates e ON e.copro_id = c.id
    WHERE p.stage NOT IN ('won', 'lost')
      AND (e.classe_finale IN ('F','G'))
      ${mineP}
  `);
  // 2. Relances dues aujourd'hui
  const relancesRow = await db.get<{ n: number }>(`
    SELECT COUNT(*) AS n FROM prospects
    WHERE next_action_at IS NOT NULL
      AND next_action_at <= unixepoch()
      AND stage NOT IN ('won', 'lost')
      ${mineNoAlias}
  `);
  // 3. Pipeline value (somme estimated_value des stages actifs)
  const pipelineRow = await db.get<{ v: number }>(`
    SELECT COALESCE(SUM(estimated_value), 0) AS v FROM prospects
    WHERE stage IN ('meeting', 'proposal')
    ${mineNoAlias}
  `);
  // 4. Taux conversion (won / total non-lead)
  const convRow = await db.get<{ total: number; won: number }>(`
    SELECT
      SUM(CASE WHEN stage != 'lead' THEN 1 ELSE 0 END) AS total,
      SUM(CASE WHEN stage = 'won' THEN 1 ELSE 0 END) AS won
    FROM prospects
    WHERE 1=1
    ${mineNoAlias}
  `);
  const conv = convRow && convRow.total > 0
    ? (convRow.won / convRow.total) * 100 : 0;

  // === À FAIRE AUJOURD'HUI ===
  const tasks = await db.all<{
    id: number;
    label: string | null;
    stage: Stage;
    next_action_at: number | null;
    next_action_label: string | null;
    copro_id: number | null;
    copro_nom: string | null;
    copro_adresse: string | null;
    copro_commune: string | null;
    classe_finale: string | null;
    assigned_user_id: number | null;
    assigned_user_name: string | null;
  }>(`
    SELECT
      p.id, p.next_action_at, p.next_action_label, p.stage,
      COALESCE(c.nom_copro, p.custom_label, c.adresse) AS label,
      c.id AS copro_id, c.nom_copro AS copro_nom,
      c.adresse AS copro_adresse, c.commune AS copro_commune,
      e.classe_finale,
      p.assigned_user_id,
      COALESCE(u.name, u.email) AS assigned_user_name
    FROM prospects p
    LEFT JOIN copros c ON c.id = p.copro_id
    LEFT JOIN dpe_estimates e ON e.copro_id = c.id
    LEFT JOIN users u ON u.id = p.assigned_user_id
    WHERE p.next_action_at IS NOT NULL
      AND p.next_action_at <= unixepoch() + 86400
      AND p.stage NOT IN ('won', 'lost')
      ${mineP}
    ORDER BY p.next_action_at ASC
    LIMIT 20
  `);

  // === KANBAN COMPACT (count par stage) ===
  const stageCounts = await db.all<{ stage: Stage; n: number }>(`
    SELECT stage, COUNT(*) AS n FROM prospects
    WHERE stage NOT IN ('won', 'lost')
    ${mineNoAlias}
    GROUP BY stage
  `);

  // Sample prospects (3 par stage actif) pour preview kanban
  const sampleStages: Stage[] = ["lead", "to_contact", "contacted", "meeting", "proposal"];
  const kanbanSamples = await db.all<{
    id: number;
    stage: Stage;
    label: string | null;
    classe_finale: string | null;
  }>(`
    WITH ranked AS (
      SELECT
        p.id, p.stage,
        COALESCE(c.nom_copro, p.custom_label, c.adresse) AS label,
        e.classe_finale,
        ROW_NUMBER() OVER (PARTITION BY p.stage ORDER BY p.updated_at DESC) AS rn
      FROM prospects p
      LEFT JOIN copros c ON c.id = p.copro_id
      LEFT JOIN dpe_estimates e ON e.copro_id = c.id
      WHERE p.stage IN (${sampleStages.map(() => "?").join(",")})
      ${mineP}
    )
    SELECT id, stage, label, classe_finale FROM ranked WHERE rn <= 3
  `, sampleStages);

  // === TOP SYNDICS À RELANCER (heuristique) ===
  // Syndics avec beaucoup de F+G mais peu/pas en pipeline
  const topSyndics = await db.all<{
    syndic: string;
    nb_fg: number;
    nb_pipeline: number;
    potentiel: number;
  }>(`
    SELECT
      TRIM(c.syndic) AS syndic,
      COUNT(*) AS nb_fg,
      COUNT(DISTINCT p.id) AS nb_pipeline,
      (COUNT(*) - COUNT(DISTINCT p.id)) AS potentiel
    FROM copros c
    LEFT JOIN dpe_estimates e ON e.copro_id = c.id
    LEFT JOIN prospects p ON p.copro_id = c.id AND p.stage NOT IN ('won','lost')
    WHERE c.syndic IS NOT NULL AND c.syndic != ''
      AND e.classe_finale IN ('F', 'G')
    GROUP BY TRIM(c.syndic)
    HAVING potentiel >= 3
    ORDER BY potentiel DESC
    LIMIT 10
  `);

  // === ACTIVITÉ RÉCENTE (notes + tags des 7 derniers jours) ===
  const recentActivity = await db.all<{
    kind: string;
    entity_type: string;
    entity_ref: string;
    body_or_tag: string;
    created_at: number;
  }>(`
    SELECT 'note' AS kind, entity_type, entity_ref, body AS body_or_tag, created_at
    FROM entity_notes
    WHERE created_at >= unixepoch() - (7 * 86400)
    UNION ALL
    SELECT 'tag' AS kind, entity_type, entity_ref, tag AS body_or_tag, created_at
    FROM entity_tags
    WHERE created_at >= unixepoch() - (7 * 86400)
    ORDER BY created_at DESC
    LIMIT 15
  `);

  // === PROSPECTS POUR MINI-CARTE ===
  const prospectsMap = await db.all<{
    id: number;
    lat: number;
    lon: number;
    stage: Stage;
    classe_finale: string | null;
  }>(`
    SELECT
      p.id,
      COALESCE(c.lat, p.custom_lat) AS lat,
      COALESCE(c.lon, p.custom_lon) AS lon,
      p.stage,
      e.classe_finale
    FROM prospects p
    LEFT JOIN copros c ON c.id = p.copro_id
    LEFT JOIN dpe_estimates e ON e.copro_id = c.id
    WHERE p.stage NOT IN ('won', 'lost')
      AND COALESCE(c.lat, p.custom_lat) IS NOT NULL
      AND COALESCE(c.lon, p.custom_lon) IS NOT NULL
      ${mineP}
    LIMIT 500
  `);

  return NextResponse.json({
    kpis: {
      passoiresActives: passoiresRow?.n ?? 0,
      relancesJour: relancesRow?.n ?? 0,
      pipelineValue: pipelineRow?.v ?? 0,
      tauxConversion: conv,
    },
    tasks,
    kanban: {
      counts: stageCounts,
      samples: kanbanSamples,
    },
    topSyndics,
    recentActivity,
    prospectsMap,
  }, {
    headers: { "Cache-Control": "private, max-age=30" },
  });
}
