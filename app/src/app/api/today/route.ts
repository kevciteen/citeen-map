import { NextResponse } from "next/server";
import { requireUser } from "@/lib/auth/session";
import { db } from "@/lib/db/client";
import { ensureProspectExtras } from "@/lib/db/ensure-prospect-extras";

export const runtime = "nodejs";

/**
 * Vue "Aujourd'hui" du commercial : agrège tout ce qu'il doit voir au démarrage
 * de sa journée. Filtré sur les prospects qui lui sont assignés.
 *
 *  - prospects en retard (next_action_at < now)
 *  - prospects à relancer aujourd'hui
 *  - prospects à relancer cette semaine
 *  - tâches à faire aujourd'hui
 *  - répartition par étape + valeur pipeline
 */
export async function GET() {
  let me;
  try {
    me = await requireUser();
  } catch {
    return NextResponse.json({ error: "Non authentifié" }, { status: 401 });
  }
  await ensureProspectExtras();

  const now = Math.floor(Date.now() / 1000);
  const startOfDay = Math.floor(new Date().setHours(0, 0, 0, 0) / 1000);
  const endOfDay = startOfDay + 86400;
  const endOfWeek = startOfDay + 86400 * 7;

  const baseSelect = `
    SELECT p.id, p.stage, p.priority, p.estimated_value, p.next_action_at,
           p.next_action_label, p.custom_label, p.custom_address,
           c.nom_copro, c.adresse, c.code_postal, c.commune, c.syndic,
           e.classe_finale
    FROM prospects p
    LEFT JOIN copros c ON c.id = p.copro_id
    LEFT JOIN dpe_estimates e ON e.copro_id = p.copro_id
    WHERE p.assigned_user_id = ?
      AND p.stage NOT IN ('won', 'lost')
  `;

  const [overdue, today, thisWeek, byStage, tasksToday, tasksOverdue] = await Promise.all([
    db.all(
      `${baseSelect}
       AND p.next_action_at IS NOT NULL
       AND p.next_action_at < ?
       ORDER BY p.next_action_at ASC LIMIT 50`,
      [me.id, startOfDay],
    ),
    db.all(
      `${baseSelect}
       AND p.next_action_at IS NOT NULL
       AND p.next_action_at >= ? AND p.next_action_at < ?
       ORDER BY p.priority DESC, p.next_action_at ASC LIMIT 50`,
      [me.id, startOfDay, endOfDay],
    ),
    db.all(
      `${baseSelect}
       AND p.next_action_at IS NOT NULL
       AND p.next_action_at >= ? AND p.next_action_at < ?
       ORDER BY p.next_action_at ASC LIMIT 50`,
      [me.id, endOfDay, endOfWeek],
    ),
    db.all<{ stage: string; n: number; total: number }>(
      `SELECT p.stage, COUNT(*) AS n, COALESCE(SUM(p.estimated_value), 0) AS total
       FROM prospects p
       WHERE p.assigned_user_id = ?
       GROUP BY p.stage`,
      [me.id],
    ),
    db.all(
      `SELECT t.id, t.title, t.kind, t.due_at, t.done_at, t.prospect_id,
              p.custom_label, c.nom_copro, c.adresse
       FROM tasks t
       LEFT JOIN prospects p ON p.id = t.prospect_id
       LEFT JOIN copros c ON c.id = p.copro_id
       WHERE t.done_at IS NULL
         AND t.due_at IS NOT NULL
         AND t.due_at >= ? AND t.due_at < ?
         AND (p.assigned_user_id = ? OR p.id IS NULL)
       ORDER BY t.due_at ASC`,
      [startOfDay, endOfDay, me.id],
    ),
    db.all(
      `SELECT t.id, t.title, t.kind, t.due_at, t.done_at, t.prospect_id,
              p.custom_label, c.nom_copro, c.adresse
       FROM tasks t
       LEFT JOIN prospects p ON p.id = t.prospect_id
       LEFT JOIN copros c ON c.id = p.copro_id
       WHERE t.done_at IS NULL
         AND t.due_at IS NOT NULL
         AND t.due_at < ?
         AND (p.assigned_user_id = ? OR p.id IS NULL)
       ORDER BY t.due_at ASC LIMIT 50`,
      [startOfDay, me.id],
    ),
  ]);

  return NextResponse.json({
    user: { id: me.id, name: me.name, email: me.email },
    now,
    overdue,
    today,
    thisWeek,
    byStage,
    tasksToday,
    tasksOverdue,
  });
}
