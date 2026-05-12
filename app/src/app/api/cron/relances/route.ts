/**
 * Cron Vercel — exécuté quotidiennement (voir vercel.json crons).
 *
 * Notifie les commerciaux des prospects en retard / à relancer aujourd'hui
 * en créant une notification in-app pour chaque user concerné. Pas d'email.
 *
 * Sécurité : on vérifie le header x-cron-secret pour empêcher l'appel
 * externe. Vercel ajoute automatiquement Authorization: Bearer $CRON_SECRET
 * quand on déclare le path dans vercel.json crons.
 */
import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import { ensureCommentsAndNotificationsTables } from "@/lib/db/ensure-comments";

export const runtime = "nodejs";
export const maxDuration = 60;

function assertCronAuth(req: NextRequest): NextResponse | null {
  const expected = process.env.CRON_SECRET;
  if (!expected) return null; // no-op en dev local
  const auth = req.headers.get("authorization");
  if (auth !== `Bearer ${expected}`) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }
  return null;
}

export async function GET(req: NextRequest) {
  const unauthorized = assertCronAuth(req);
  if (unauthorized) return unauthorized;
  await ensureCommentsAndNotificationsTables();

  const now = Math.floor(Date.now() / 1000);
  const startOfToday = Math.floor(new Date().setHours(0, 0, 0, 0) / 1000);
  const endOfToday = startOfToday + 86400;

  // Agrège par user_id les prospects en retard ET à relancer aujourd'hui
  const rows = await db.all<{
    user_id: number;
    overdue: number;
    today: number;
  }>(
    `SELECT p.assigned_user_id AS user_id,
            SUM(CASE WHEN p.next_action_at < ? THEN 1 ELSE 0 END) AS overdue,
            SUM(CASE WHEN p.next_action_at >= ? AND p.next_action_at < ? THEN 1 ELSE 0 END) AS today
     FROM prospects p
     WHERE p.assigned_user_id IS NOT NULL
       AND p.next_action_at IS NOT NULL
       AND p.stage NOT IN ('won', 'lost')
       AND p.next_action_at < ?
     GROUP BY p.assigned_user_id
     HAVING overdue + today > 0`,
    [startOfToday, startOfToday, endOfToday, endOfToday],
  );

  let created = 0;
  for (const r of rows) {
    const parts: string[] = [];
    if (r.overdue > 0) parts.push(`${r.overdue} en retard`);
    if (r.today > 0) parts.push(`${r.today} à relancer aujourd'hui`);
    const body = parts.join(" · ");
    await db.run(
      `INSERT INTO notifications
         (user_id, kind, title, body, link, from_user_name, entity_type)
       VALUES (?, 'system', ?, ?, '/today', 'Citeen', 'prospect')`,
      [r.user_id, "Tes relances du jour", body],
    );
    created++;
  }

  return NextResponse.json({ ok: true, notificationsCreated: created, now });
}
