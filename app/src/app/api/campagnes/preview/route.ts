/**
 * GET /api/campagnes/preview?dpe=F,G&dept=75&minLots=20&secteur=...&onlyNew=1
 *
 * Donne un aperçu d'une campagne de prospection ciblée :
 *  - count total de copros matching
 *  - eligibleCount = copros NON déjà en pipeline (onlyNew=1)
 *  - alreadyInPipeline = celles déjà avec prospect
 *  - dpeDistribution
 *  - sample (200 max) pour preview UI + carte
 *
 * Ce endpoint ne crée rien — le launch passe par POST /api/prospects/bulk
 * avec les coproIds retournés.
 */
import { NextRequest, NextResponse } from "next/server";
import type { InValue } from "@libsql/client";
import { db } from "@/lib/db/client";
import { ensureAuth } from "@/lib/auth/guards";

export const runtime = "nodejs";

type CoproRow = {
  id: number;
  numero_immatriculation: string;
  nom_copro: string | null;
  adresse: string | null;
  code_postal: string | null;
  commune: string | null;
  departement: string | null;
  syndic: string | null;
  lat: number | null;
  lon: number | null;
  nb_lots: number | null;
  nb_lots_habitation: number | null;
  classe_finale: string | null;
  has_prospect: number;
};

function num(v: string | null): number | null {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

export async function GET(req: NextRequest) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const sp = req.nextUrl.searchParams;

  const dpeParam = sp.get("dpe")?.trim()?.toUpperCase();
  const dept = sp.get("dept")?.trim();
  const cp = sp.get("cp")?.trim();
  const syndic = sp.get("syndic")?.trim();
  const minLots = num(sp.get("minLots"));
  const periode = sp.get("periode")?.trim();
  const onlyNew = sp.get("onlyNew") === "1";
  const sampleLimit = Math.min(Math.max(num(sp.get("sample")) ?? 200, 1), 1000);

  const where: string[] = ["c.lat IS NOT NULL", "c.lon IS NOT NULL"];
  const params: InValue[] = [];

  if (dpeParam) {
    const dpeClasses = dpeParam.split(",").map((c) => c.trim().toUpperCase())
      .filter((c) => /^[A-G]$/.test(c) || c === "NC");
    if (dpeClasses.length > 0) {
      where.push(`COALESCE(e.classe_finale, 'NC') IN (${dpeClasses.map(() => "?").join(",")})`);
      params.push(...dpeClasses);
    }
  }
  if (dept) {
    where.push("c.departement = ?");
    params.push(dept);
  }
  if (cp) {
    where.push("c.code_postal LIKE ?");
    params.push(`${cp}%`);
  }
  if (syndic) {
    where.push("c.syndic = ?");
    params.push(syndic);
  }
  if (minLots != null) {
    where.push("COALESCE(c.nb_lots_habitation, c.nb_lots, 0) >= ?");
    params.push(minLots);
  }
  if (periode) {
    where.push("c.periode_construction = ?");
    params.push(periode);
  }
  if (onlyNew) {
    where.push("NOT EXISTS (SELECT 1 FROM prospects p WHERE p.copro_id = c.id)");
  }

  // 1. Counts globaux (rapide)
  const total = await db.get<{ n: number }>(
    `SELECT COUNT(*) AS n FROM copros c
     LEFT JOIN dpe_estimates e ON e.copro_id = c.id
     WHERE ${where.join(" AND ")}`,
    params,
  );

  const inPipeline = await db.get<{ n: number }>(
    `SELECT COUNT(*) AS n FROM copros c
     LEFT JOIN dpe_estimates e ON e.copro_id = c.id
     WHERE ${where.join(" AND ")}
       AND EXISTS (SELECT 1 FROM prospects p WHERE p.copro_id = c.id)`,
    params,
  );

  // 2. Distribution DPE des copros matching
  const dpeDistribution = await db.all<{ classe: string; n: number }>(
    `SELECT COALESCE(e.classe_finale, 'NC') AS classe, COUNT(*) AS n
     FROM copros c
     LEFT JOIN dpe_estimates e ON e.copro_id = c.id
     WHERE ${where.join(" AND ")}
     GROUP BY COALESCE(e.classe_finale, 'NC')
     ORDER BY classe`,
    params,
  );

  // 3. Sample pour preview UI/carte
  const items = await db.all<CoproRow>(
    `SELECT c.id, c.numero_immatriculation, c.nom_copro, c.adresse, c.code_postal,
            c.commune, c.departement, c.syndic, c.lat, c.lon,
            c.nb_lots, c.nb_lots_habitation,
            e.classe_finale,
            CASE WHEN EXISTS (SELECT 1 FROM prospects p WHERE p.copro_id = c.id) THEN 1 ELSE 0 END AS has_prospect
     FROM copros c
     LEFT JOIN dpe_estimates e ON e.copro_id = c.id
     WHERE ${where.join(" AND ")}
     ORDER BY
       CASE COALESCE(e.classe_finale, 'NC')
         WHEN 'G' THEN 1 WHEN 'F' THEN 2 WHEN 'E' THEN 3
         WHEN 'D' THEN 4 WHEN 'C' THEN 5 WHEN 'B' THEN 6 WHEN 'A' THEN 7
         ELSE 8 END,
       COALESCE(c.nb_lots_habitation, c.nb_lots, 0) DESC
     LIMIT ?`,
    [...params, sampleLimit],
  );

  return NextResponse.json(
    {
      count: total?.n ?? 0,
      inPipeline: inPipeline?.n ?? 0,
      eligible: (total?.n ?? 0) - (inPipeline?.n ?? 0),
      dpeDistribution,
      items,
    },
    {
      headers: {
        "Cache-Control": "private, max-age=30",
      },
    },
  );
}
