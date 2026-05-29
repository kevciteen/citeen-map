/**
 * GET /api/directory/export?format=csv&...filtres
 *
 * Reprend exactement les filtres de /api/directory et renvoie un CSV
 * UTF-8 BOM (compatible Excel). Limite étendue à 10 000 lignes pour les
 * exports de prospection larges.
 */
import { NextRequest, NextResponse } from "next/server";
import type { InValue } from "@libsql/client";
import { db } from "@/lib/db/client";
import { ensureAuth } from "@/lib/auth/guards";
import { ensureDirectory } from "@/lib/db/ensure-directory";

export const runtime = "nodejs";
export const maxDuration = 60;

type DirectoryRow = {
  entity_type: string;
  display_name: string;
  display_subtitle: string | null;
  address: string | null;
  postcode: string | null;
  city: string | null;
  departement: string | null;
  lat: number | null;
  lon: number | null;
  coords_source: string | null;
  phone: string | null;
  email: string | null;
  website: string | null;
  dpe_class: string | null;
  nb_lots: number | null;
  secteur: string | null;
};

function num(v: string | null): number | null {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function csvCell(v: unknown): string {
  if (v === null || v === undefined) return "";
  const s = String(v);
  if (/[",;\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

export async function GET(req: NextRequest) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  await ensureDirectory();

  const sp = req.nextUrl.searchParams;
  const limit = Math.min(Math.max(num(sp.get("limit")) ?? 10000, 1), 10000);
  const q = sp.get("q")?.trim();
  const cp = sp.get("cp")?.trim();
  const dept = sp.get("dept")?.trim();
  const typesParam = sp.get("types")?.trim();
  const minLat = num(sp.get("minLat"));
  const maxLat = num(sp.get("maxLat"));
  const minLon = num(sp.get("minLon"));
  const maxLon = num(sp.get("maxLon"));
  const onlyWithContact = sp.get("onlyWithContact") === "1";
  const onlyWithCoords = sp.get("onlyWithCoords") === "1";
  const dpeParam = sp.get("dpe")?.trim()?.toUpperCase();
  const minLots = num(sp.get("minLots"));
  const secteur = sp.get("secteur")?.trim();

  const where: string[] = ["1=1"];
  const params: InValue[] = [];

  if (q) {
    const ftsQuery = q
      .replace(/["()*]/g, " ")
      .trim()
      .split(/\s+/)
      .filter(Boolean)
      .map((t) => `${t}*`)
      .join(" ");
    if (ftsQuery.length > 0) {
      where.push(
        "id IN (SELECT rowid FROM directory_fts WHERE directory_fts MATCH ?)",
      );
      params.push(ftsQuery);
    }
  }
  if (cp) {
    where.push("postcode LIKE ?");
    params.push(`${cp}%`);
  }
  if (dept) {
    where.push("departement = ?");
    params.push(dept);
  }
  if (typesParam) {
    const types = typesParam.split(",").map((t) => t.trim()).filter((t) =>
      ["occupant", "copro", "syndic", "prospect_custom"].includes(t),
    );
    if (types.length) {
      where.push(`entity_type IN (${types.map(() => "?").join(",")})`);
      params.push(...types);
    }
  }
  if (minLat != null && maxLat != null && minLon != null && maxLon != null) {
    where.push("lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?");
    params.push(minLat, maxLat, minLon, maxLon);
  }
  if (onlyWithContact) {
    where.push("(phone IS NOT NULL OR email IS NOT NULL OR website IS NOT NULL)");
  }
  if (onlyWithCoords) {
    where.push("lat IS NOT NULL AND lon IS NOT NULL");
  }
  if (dpeParam) {
    const dpeClasses = dpeParam.split(",").map((c) => c.trim().toUpperCase())
      .filter((c) => /^[A-G]$/.test(c) || c === "NC");
    if (dpeClasses.length > 0) {
      where.push(`COALESCE(dpe_class, 'NC') IN (${dpeClasses.map(() => "?").join(",")})`);
      params.push(...dpeClasses);
    }
  }
  if (minLots != null) {
    where.push("nb_lots IS NOT NULL AND nb_lots >= ?");
    params.push(minLots);
  }
  if (secteur) {
    where.push("secteur = ?");
    params.push(secteur);
  }

  const rows = await db.all<DirectoryRow>(
    `SELECT entity_type, display_name, display_subtitle,
            address, postcode, city, departement,
            lat, lon, coords_source,
            phone, email, website,
            dpe_class, nb_lots, secteur
     FROM directory
     WHERE ${where.join(" AND ")}
     ORDER BY entity_type, display_name
     LIMIT ?`,
    [...params, limit],
  );

  const headers = [
    "type", "nom", "sous_titre",
    "adresse", "code_postal", "commune", "departement",
    "lat", "lon", "source_coords",
    "telephone", "email", "site_web",
    "dpe", "nb_lots", "secteur",
  ];
  const lines = [
    headers.join(";"),
    ...rows.map((r) =>
      [
        r.entity_type, r.display_name, r.display_subtitle,
        r.address, r.postcode, r.city, r.departement,
        r.lat, r.lon, r.coords_source,
        r.phone, r.email, r.website,
        r.dpe_class, r.nb_lots, r.secteur,
      ].map(csvCell).join(";"),
    ),
  ];
  // BOM UTF-8 pour qu'Excel détecte l'encodage tout seul
  const csv = "﻿" + lines.join("\r\n");

  const filename = `annuaire-${new Date().toISOString().slice(0, 10)}.csv`;
  return new NextResponse(csv, {
    status: 200,
    headers: {
      "content-type": "text/csv; charset=utf-8",
      "content-disposition": `attachment; filename="${filename}"`,
      "cache-control": "private, no-store",
    },
  });
}
