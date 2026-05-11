import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import type { InValue } from "@libsql/client";

export const runtime = "nodejs";

function num(v: string | null): number | null {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

export async function GET(req: NextRequest) {
  const sp = req.nextUrl.searchParams;
  const q = sp.get("q")?.trim();
  const cp = sp.get("cp")?.trim();
  const commune = sp.get("commune")?.trim();
  const syndic = sp.get("syndic")?.trim();
  const dept = sp.get("dept")?.trim();
  const dpeClass = sp.get("dpe")?.trim().toUpperCase();
  const minLots = num(sp.get("minLots"));
  const maxLots = num(sp.get("maxLots"));
  const periode = sp.get("periode")?.trim();
  const consoMin = num(sp.get("consoMin"));
  const consoMax = num(sp.get("consoMax"));
  const hasCollectif = sp.get("hasCollectif"); // "1" / "0"
  const dpeComputed = sp.get("dpeComputed");   // "1" / "0"
  const quality = sp.get("quality");           // "verified" | "approximate" | ...
  const inPipeline = sp.get("inPipeline");     // "1" / "0" / null
  const ids = sp.get("ids");                   // "1,2,3..." pour bulk
  const sort = sp.get("sort") ?? "default";    // default | conso_desc | conso_asc | lots_desc
  const limit = Math.min(Math.max(num(sp.get("limit")) ?? 50, 1), 500);
  const offset = Math.max(num(sp.get("offset")) ?? 0, 0);

  const where: string[] = [];
  const params: InValue[] = [];

  if (cp) {
    where.push("c.code_postal LIKE ?");
    params.push(`${cp}%`);
  }
  if (commune) {
    where.push("LOWER(c.commune) LIKE ?");
    params.push(`%${commune.toLowerCase()}%`);
  }
  if (syndic) {
    where.push("LOWER(c.syndic) LIKE ?");
    params.push(`%${syndic.toLowerCase()}%`);
  }
  if (dept) {
    where.push("c.departement = ?");
    params.push(dept);
  }
  if (minLots != null) {
    where.push("COALESCE(c.nb_lots_habitation, c.nb_lots, 0) >= ?");
    params.push(minLots);
  }
  if (maxLots != null) {
    where.push("COALESCE(c.nb_lots_habitation, c.nb_lots, 0) <= ?");
    params.push(maxLots);
  }
  if (periode) {
    where.push("c.periode_construction = ?");
    params.push(periode);
  }
  if (consoMin != null) {
    where.push("e.conso_moyenne >= ?");
    params.push(consoMin);
  }
  if (consoMax != null) {
    where.push("e.conso_moyenne <= ?");
    params.push(consoMax);
  }
  if (hasCollectif === "1") {
    where.push("e.classe_reelle IS NOT NULL");
  } else if (hasCollectif === "0") {
    where.push("e.classe_reelle IS NULL");
  }
  if (dpeComputed === "1") {
    where.push("e.classe_finale IS NOT NULL");
  } else if (dpeComputed === "0") {
    where.push("e.classe_finale IS NULL");
  }
  if (quality) {
    const allowed = ["verified", "approximate", "uncertain", "no_data"];
    const wanted = quality.split(",").filter((q) => allowed.includes(q));
    if (wanted.length) {
      const placeholders = wanted.map(() => "?").join(",");
      where.push(`e.quality_level IN (${placeholders})`);
      params.push(...wanted);
    }
  }
  if (inPipeline === "1") {
    where.push("EXISTS (SELECT 1 FROM prospects p WHERE p.copro_id = c.id)");
  } else if (inPipeline === "0") {
    where.push("NOT EXISTS (SELECT 1 FROM prospects p WHERE p.copro_id = c.id)");
  }
  if (ids) {
    const idList = ids
      .split(",")
      .map((s) => Number(s.trim()))
      .filter((n) => Number.isFinite(n));
    if (idList.length) {
      const placeholders = idList.map(() => "?").join(",");
      where.push(`c.id IN (${placeholders})`);
      params.push(...idList);
    }
  }
  /* Full-text search via FTS5 (instantané vs full-scan LIKE).
     Chaque terme devient un préfixe (sab → sab*), et tous les termes sont
     requis (AND). On échappe les caractères FTS5 spéciaux. */
  let useFts = false;
  let ftsQuery = "";
  if (q) {
    const tokens = q
      .replace(/["()*]/g, " ")
      .trim()
      .split(/\s+/)
      .filter((t) => t.length >= 2);
    if (tokens.length > 0) {
      useFts = true;
      ftsQuery = tokens.map((t) => `"${t}"*`).join(" AND ");
    }
  }
  if (dpeClass) {
    const classes = dpeClass
      .split(",")
      .map((c) => c.trim().toUpperCase())
      .filter((c) => /^[A-G]$/.test(c) || c === "NC");
    if (classes.length) {
      const placeholders = classes.map(() => "?").join(",");
      // NC handles "no estimate yet" too (LEFT JOIN nulls)
      if (classes.includes("NC")) {
        where.push(
          `(COALESCE(e.classe_finale, 'NC') IN (${placeholders}) OR e.classe_finale IS NULL)`,
        );
      } else {
        where.push(`e.classe_finale IN (${placeholders})`);
      }
      params.push(...classes);
    }
  }

  const whereSql = where.length ? "WHERE " + where.join(" AND ") : "";
  const fromSql = useFts
    ? `FROM copros_fts JOIN copros c ON c.id = copros_fts.rowid LEFT JOIN dpe_estimates e ON e.copro_id = c.id`
    : `FROM copros c LEFT JOIN dpe_estimates e ON e.copro_id = c.id`;
  const ftsWhere = useFts ? `copros_fts MATCH ?` : "";
  const fullWhere = [ftsWhere, whereSql.replace(/^WHERE /, "")]
    .filter(Boolean)
    .join(" AND ");
  const fullWhereSql = fullWhere ? `WHERE ${fullWhere}` : "";
  const ftsParams: InValue[] = useFts ? [ftsQuery] : [];

  const countSql = `SELECT COUNT(*) AS c ${fromSql} ${fullWhereSql}`;
  const countRow = await db.get<{ c: number }>(countSql, [...ftsParams, ...params]);
  const total = countRow?.c ?? 0;

  const SORT_MAP: Record<string, string> = {
    default: "c.commune, c.code_postal, c.adresse",
    name_asc: "c.nom_copro ASC NULLS LAST, c.id",
    name_desc: "c.nom_copro DESC NULLS LAST, c.id",
    commune_asc: "c.commune ASC NULLS LAST, c.code_postal",
    commune_desc: "c.commune DESC NULLS LAST, c.code_postal",
    lots_desc: "COALESCE(c.nb_lots_habitation, c.nb_lots, 0) DESC, c.id",
    lots_asc: "COALESCE(c.nb_lots_habitation, c.nb_lots, 0) ASC, c.id",
    periode_asc: "c.periode_construction ASC NULLS LAST, c.id",
    periode_desc: "c.periode_construction DESC NULLS LAST, c.id",
    syndic_asc: "c.syndic ASC NULLS LAST, c.id",
    syndic_desc: "c.syndic DESC NULLS LAST, c.id",
    dpe_asc: "e.classe_finale ASC NULLS LAST, c.id",
    dpe_desc: "e.classe_finale DESC NULLS LAST, c.id",
    conso_asc: "e.conso_moyenne ASC NULLS LAST, c.id",
    conso_desc: "e.conso_moyenne DESC NULLS LAST, c.id",
  };
  const orderBy = SORT_MAP[sort] ?? SORT_MAP.default;

  const listSql = `
    SELECT c.id, c.numero_immatriculation, c.nom_copro, c.adresse, c.code_postal,
           c.commune, c.departement, c.syndic,
           c.nb_lots, c.nb_lots_habitation, c.periode_construction,
           c.lat, c.lon,
           e.classe_finale, e.classe_reelle, e.conso_moyenne, e.nb_dpe_individuels,
           e.quality_level,
           (SELECT p.id FROM prospects p WHERE p.copro_id = c.id LIMIT 1) AS prospect_id,
           (SELECT p.stage FROM prospects p WHERE p.copro_id = c.id LIMIT 1) AS prospect_stage
    ${fromSql}
    ${fullWhereSql}
    ORDER BY ${orderBy}
    LIMIT ? OFFSET ?
  `;
  const items = await db.all(listSql, [...ftsParams, ...params, limit, offset]);

  return NextResponse.json({ total, limit, offset, items });
}
