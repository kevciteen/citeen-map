/**
 * GET /api/syndics/[slug]/copros?name=...&dpe=E,F,G&minLots=20
 *
 * Renvoie la liste des copropriétés gérées par un syndic, avec leur
 * lat/lon + classe DPE finale (réel ou simulé) + nb_lots.
 *
 * On utilise le slug comme clé d'entrée mais on doit matcher `copros.syndic`
 * sur le nom canonique (stocké dans syndic_contacts.name après la 1re
 * édition, sinon dérivé du query param `name`).
 *
 * Pour la perf : essai d'abord `c.syndic = ?` (indexé), fallback `TRIM(c.syndic) = ?`.
 */
import { NextRequest, NextResponse } from "next/server";
import type { InValue } from "@libsql/client";
import { db } from "@/lib/db/client";
import { ensureAuth } from "@/lib/auth/guards";
import { getSyndicRecord } from "@/lib/services/syndic-storage";

export const runtime = "nodejs";

type CoproRow = {
  id: number;
  numero_immatriculation: string;
  nom_copro: string | null;
  adresse: string | null;
  code_postal: string | null;
  commune: string | null;
  departement: string | null;
  lat: number | null;
  lon: number | null;
  nb_lots: number | null;
  nb_lots_habitation: number | null;
  classe_finale: string | null;
};

function num(v: string | null): number | null {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

async function fetchCoprosForName(
  name: string,
  filters: {
    dpeClasses: string[];
    minLots: number | null;
    limit: number;
  },
): Promise<CoproRow[]> {
  const params: InValue[] = [];
  const where: string[] = [];

  // Tentative indexée d'abord (chemin chaud)
  where.push("c.syndic = ?");
  params.push(name);

  if (filters.dpeClasses.length > 0) {
    const placeholders = filters.dpeClasses.map(() => "?").join(",");
    where.push(`COALESCE(e.classe_finale, 'NC') IN (${placeholders})`);
    params.push(...filters.dpeClasses);
  }
  if (filters.minLots != null) {
    where.push("COALESCE(c.nb_lots_habitation, c.nb_lots, 0) >= ?");
    params.push(filters.minLots);
  }

  const sql = `
    SELECT c.id, c.numero_immatriculation, c.nom_copro, c.adresse, c.code_postal,
           c.commune, c.departement, c.lat, c.lon,
           c.nb_lots, c.nb_lots_habitation,
           e.classe_finale
    FROM copros c
    LEFT JOIN dpe_estimates e ON e.copro_id = c.id
    WHERE ${where.join(" AND ")}
    ORDER BY
      CASE COALESCE(e.classe_finale, 'NC')
        WHEN 'G' THEN 1 WHEN 'F' THEN 2 WHEN 'E' THEN 3
        WHEN 'D' THEN 4 WHEN 'C' THEN 5 WHEN 'B' THEN 6 WHEN 'A' THEN 7
        ELSE 8 END,
      COALESCE(c.nb_lots_habitation, c.nb_lots, 0) DESC
    LIMIT ?
  `;
  let rows = await db.all<CoproRow>(sql, [...params, filters.limit]);

  // Fallback TRIM si data contient des espaces (rare)
  if (rows.length === 0) {
    const fallbackSql = sql.replace("c.syndic = ?", "TRIM(c.syndic) = ?");
    rows = await db.all<CoproRow>(fallbackSql, [
      name.trim(),
      ...filters.dpeClasses,
      ...(filters.minLots != null ? [filters.minLots] : []),
      filters.limit,
    ]);
  }
  return rows;
}

export async function GET(
  req: NextRequest,
  { params }: { params: Promise<{ slug: string }> },
) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;

  const { slug } = await params;
  const sp = req.nextUrl.searchParams;
  const fallbackName = sp.get("name")?.trim();
  const dpeParam = sp.get("dpe")?.trim()?.toUpperCase();
  const minLots = num(sp.get("minLots"));
  const limit = Math.min(Math.max(num(sp.get("limit")) ?? 500, 1), 2000);

  // Résoud le nom canonique : DB > query param
  const stored = await getSyndicRecord(slug);
  const name = (stored?.name ?? fallbackName ?? "").trim();
  if (!name) {
    return NextResponse.json(
      { error: "Nom du syndic requis (?name=...)" },
      { status: 400 },
    );
  }

  const dpeClasses = dpeParam
    ? dpeParam.split(",").map((c) => c.trim().toUpperCase())
        .filter((c) => /^[A-G]$/.test(c) || c === "NC")
    : [];

  const items = await fetchCoprosForName(name, { dpeClasses, minLots, limit });

  // Distribution DPE pour les charts (toujours sur la totalité des copros
  // du syndic, indépendamment des filtres pour donner le contexte)
  const distrib = await db.all<{ classe: string; n: number }>(
    `SELECT COALESCE(e.classe_finale, 'NC') AS classe, COUNT(*) AS n
     FROM copros c
     LEFT JOIN dpe_estimates e ON e.copro_id = c.id
     WHERE c.syndic = ?
     GROUP BY COALESCE(e.classe_finale, 'NC')`,
    [name],
  );

  return NextResponse.json(
    {
      name,
      count: items.length,
      items,
      dpeDistribution: distrib,
    },
    {
      headers: {
        "Cache-Control": "private, max-age=60, stale-while-revalidate=300",
      },
    },
  );
}
