import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import type { InValue } from "@libsql/client";

export const runtime = "nodejs";

/**
 * Liste agrégée des syndics avec, pour chacun :
 *  - nombre de copros gérées
 *  - lots habitation cumulés
 *  - distribution DPE (A→G + NC) en absolu
 *  - communes principales
 *  - % déjà dans le pipeline
 */
export async function GET(req: NextRequest) {
  const sp = req.nextUrl.searchParams;
  const q = sp.get("q")?.trim();
  const dept = sp.get("dept")?.trim();
  const limit = Math.min(Math.max(Number(sp.get("limit") ?? 100), 1), 500);
  const offset = Math.max(Number(sp.get("offset") ?? 0), 0);
  const sort = sp.get("sort") ?? "nb_copros_desc";

  const where: string[] = ["c.syndic IS NOT NULL", "LOWER(c.syndic) != 'non connu'"];
  const params: InValue[] = [];

  if (q) {
    where.push("LOWER(c.syndic) LIKE ?");
    params.push(`%${q.toLowerCase()}%`);
  }
  if (dept) {
    where.push("c.departement = ?");
    params.push(dept);
  }

  const whereSql = "WHERE " + where.join(" AND ");

  const SORT_MAP: Record<string, string> = {
    nb_copros_desc: "nb_copros DESC",
    nb_copros_asc: "nb_copros ASC",
    lots_desc: "lots_total DESC",
    name_asc: "syndic ASC",
    name_desc: "syndic DESC",
  };
  const orderBy = SORT_MAP[sort] ?? SORT_MAP.nb_copros_desc;

  const totalRow = await db.get<{ c: number }>(
    `SELECT COUNT(DISTINCT TRIM(c.syndic)) AS c FROM copros c ${whereSql}`,
    params,
  );
  const total = totalRow?.c ?? 0;

  const items = await db.all(
    `SELECT
       TRIM(c.syndic) AS syndic,
       COUNT(*) AS nb_copros,
       COALESCE(SUM(c.nb_lots_habitation), 0) AS lots_total,
       COUNT(DISTINCT c.commune) AS nb_communes,
       COUNT(DISTINCT c.departement) AS nb_departements,
       SUM(CASE WHEN e.classe_finale = 'A' THEN 1 ELSE 0 END) AS dpe_a,
       SUM(CASE WHEN e.classe_finale = 'B' THEN 1 ELSE 0 END) AS dpe_b,
       SUM(CASE WHEN e.classe_finale = 'C' THEN 1 ELSE 0 END) AS dpe_c,
       SUM(CASE WHEN e.classe_finale = 'D' THEN 1 ELSE 0 END) AS dpe_d,
       SUM(CASE WHEN e.classe_finale = 'E' THEN 1 ELSE 0 END) AS dpe_e,
       SUM(CASE WHEN e.classe_finale = 'F' THEN 1 ELSE 0 END) AS dpe_f,
       SUM(CASE WHEN e.classe_finale = 'G' THEN 1 ELSE 0 END) AS dpe_g,
       SUM(CASE WHEN e.classe_finale IS NULL OR e.classe_finale = 'NC' THEN 1 ELSE 0 END) AS dpe_nc,
       SUM(CASE WHEN EXISTS (SELECT 1 FROM prospects p WHERE p.copro_id = c.id) THEN 1 ELSE 0 END) AS in_pipeline,
       AVG(e.conso_moyenne) AS avg_conso,
       GROUP_CONCAT(DISTINCT c.departement) AS dept_list
     FROM copros c
     LEFT JOIN dpe_estimates e ON e.copro_id = c.id
     ${whereSql}
     GROUP BY TRIM(c.syndic)
     ORDER BY ${orderBy}
     LIMIT ? OFFSET ?`,
    [...params, limit, offset],
  );

  return NextResponse.json({ total, limit, offset, items });
}
