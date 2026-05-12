/**
 * Export campagne mailing — CSV prêt à imprimer (publipostage Word/La Poste).
 *
 * Cible :
 *   - "copros"  → 1 ligne par copro avec l'adresse du SYNDIC résolue via Sirene
 *   - "maisons" → 1 ligne par maison avec l'adresse du logement (occupant
 *     anonyme), DPE + arguments commerciaux
 *   - "both"    → les deux concaténés
 *
 * Format CSV "compatible publipostage" :
 *   civilite ; nom ; societe ; adresse1 ; adresse2 ; cp ; ville ;
 *   ref_internal ; classe_dpe ; argument ; ...
 *
 * Throttling Sirene : on bulk-résout par chunks de 5 en parallèle pour rester
 * sous la limite Vercel (60s) et l'API publique.
 */
import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import type { InValue } from "@libsql/client";
import { resolveSyndicByName, type SyndicContact } from "@/lib/services/syndic-contact";
import { searchMaisonsByZone } from "@/lib/services/maison";
import { ensureAuth } from "@/lib/auth/guards";
import { rateLimit } from "@/lib/rate-limit";

export const runtime = "nodejs";
export const maxDuration = 60;

const MAX_COPROS = 300;   // borne pour rester sous 60s avec résolution syndic
const MAX_MAISONS = 1000;

type CoproRow = {
  id: number;
  numero_immatriculation: string;
  nom_copro: string | null;
  adresse: string | null;
  code_postal: string | null;
  commune: string | null;
  departement: string | null;
  syndic: string | null;
  nb_lots: number | null;
  classe_finale: string | null;
};

function num(v: string | null): number | null {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

async function bulkResolveSyndics(
  names: string[],
  concurrency = 5,
): Promise<Map<string, SyndicContact | null>> {
  const cache = new Map<string, SyndicContact | null>();
  const unique = [...new Set(names.filter((n) => n && n.trim().length >= 3))];
  for (let i = 0; i < unique.length; i += concurrency) {
    const batch = unique.slice(i, i + concurrency);
    await Promise.all(
      batch.map(async (name) => {
        try {
          cache.set(name, await resolveSyndicByName(name));
        } catch {
          cache.set(name, null);
        }
      }),
    );
    // petite pause pour rester gentil avec l'API publique
    if (i + concurrency < unique.length) await new Promise((r) => setTimeout(r, 150));
  }
  return cache;
}

function csvEscape(v: string | number | null | undefined): string {
  if (v == null) return "";
  const s = String(v);
  if (/[",;\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

const CSV_HEADER = [
  "type",
  "civilite",
  "nom",
  "societe",
  "adresse1",
  "adresse2",
  "cp",
  "ville",
  "departement",
  "siren",
  "ref_interne",
  "objet",
  "classe_dpe",
  "nb_lots",
  "argument",
];

function coproToRow(c: CoproRow, sc: SyndicContact | null): string[] {
  const objet = c.nom_copro
    ? `Copropriété "${c.nom_copro}"`
    : `Copropriété ${c.adresse ?? ""}`;
  const adresseCopro = [c.adresse, c.code_postal, c.commune].filter(Boolean).join(" ");
  const argument = c.classe_finale && /^[F|G]$/.test(c.classe_finale)
    ? `Logement classé ${c.classe_finale} (passoire) — éligible MaPrimeRénov' Copro`
    : c.classe_finale
      ? `Logement classé ${c.classe_finale} — opportunité audit énergétique`
      : "Audit énergétique et accompagnement rénovation";

  if (sc) {
    return [
      "syndic",
      "",
      sc.dirigeant ?? "",
      sc.nomComplet,
      sc.adresse ?? "",
      "",
      sc.codePostal ?? "",
      sc.commune ?? "",
      sc.departement ?? "",
      sc.siren,
      c.numero_immatriculation,
      `${objet} — ${adresseCopro}`,
      c.classe_finale ?? "",
      String(c.nb_lots ?? ""),
      argument,
    ].map(csvEscape) as string[];
  }
  // Pas de syndic résolu — on retourne l'adresse de la copro pour
  // mailing au gardien / boîte aux lettres immeuble
  return [
    "copro_directe",
    "",
    "Conseil syndical",
    c.syndic ?? "",
    c.adresse ?? "",
    "",
    c.code_postal ?? "",
    c.commune ?? "",
    c.departement ?? "",
    "",
    c.numero_immatriculation,
    objet,
    c.classe_finale ?? "",
    String(c.nb_lots ?? ""),
    argument,
  ].map(csvEscape) as string[];
}

type MaisonExportRow = {
  numero_dpe: string;
  classe: string;
  surface: number | null;
  cout_chauffage: number | null;
  address: { housenumber: string | null; street: string | null; postcode: string | null; city: string | null; label: string };
};

function maisonToRow(m: MaisonExportRow): string[] {
  const adresse1 = [m.address.housenumber, m.address.street].filter(Boolean).join(" ");
  const isPassoire = m.classe === "F" || m.classe === "G";
  const argument = isPassoire
    ? `Logement classé ${m.classe} (passoire thermique) — MaPrimeRénov' jusqu'à 70 000 €`
    : `DPE ${m.classe}${m.cout_chauffage ? ` · ${Math.round(m.cout_chauffage)} €/an de chauffage` : ""}`;
  return [
    "maison",
    "",
    "Propriétaire occupant",
    "",
    adresse1 || m.address.label,
    "",
    m.address.postcode ?? "",
    m.address.city ?? "",
    "",
    "",
    m.numero_dpe,
    `Rénovation énergétique${m.surface ? ` (${m.surface} m²)` : ""}`,
    m.classe,
    "",
    argument,
  ].map(csvEscape) as string[];
}

async function fetchCopros(sp: URLSearchParams): Promise<CoproRow[]> {
  const where: string[] = ["c.lat IS NOT NULL"];
  const params: InValue[] = [];

  const cp = sp.get("cp")?.trim();
  const syndic = sp.get("syndic")?.trim();
  const dept = sp.get("dept")?.trim();
  const dpeRaw = sp.get("dpe")?.trim().toUpperCase();
  const minLots = num(sp.get("minLots"));

  if (cp) {
    where.push("c.code_postal LIKE ?");
    params.push(`${cp}%`);
  }
  if (syndic) {
    where.push("LOWER(c.syndic) LIKE ?");
    params.push(`%${syndic.toLowerCase()}%`);
  }
  if (dept) {
    where.push("c.departement = ?");
    params.push(dept);
  }
  if (dpeRaw) {
    const arr = dpeRaw.split(",").map((c) => c.trim()).filter(Boolean);
    if (arr.length) {
      where.push(`COALESCE(e.classe_finale, 'NC') IN (${arr.map(() => "?").join(",")})`);
      params.push(...arr);
    }
  }
  if (minLots != null) {
    where.push("c.nb_lots_habitation >= ?");
    params.push(minLots);
  }

  const sql = `
    SELECT c.id, c.numero_immatriculation, c.nom_copro, c.adresse, c.code_postal,
           c.commune, c.departement, c.syndic, c.nb_lots, e.classe_finale
    FROM copros c
    LEFT JOIN dpe_estimates e ON e.copro_id = c.id
    WHERE ${where.join(" AND ")}
    LIMIT ${MAX_COPROS}
  `;
  return (await db.all<CoproRow>(sql, params)) ?? [];
}

export async function GET(req: NextRequest) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const limited = await rateLimit("heavy", `user:${guard.id}`);
  if (limited) return limited;
  const sp = req.nextUrl.searchParams;
  const cible = sp.get("cible") ?? "copros"; // copros | maisons | both
  const wantsCopros = cible === "copros" || cible === "both";
  const wantsMaisons = cible === "maisons" || cible === "both";
  const resolveSyndic = sp.get("resolveSyndic") !== "0";

  const rows: string[][] = [CSV_HEADER];

  if (wantsCopros) {
    const copros = await fetchCopros(sp);
    if (copros.length > 0) {
      const syndicNames = copros
        .map((c) => c.syndic)
        .filter((s): s is string => Boolean(s));
      const sirenCache = resolveSyndic
        ? await bulkResolveSyndics(syndicNames, 5)
        : new Map<string, SyndicContact | null>();
      for (const c of copros) {
        const sc = c.syndic ? sirenCache.get(c.syndic) ?? null : null;
        rows.push(coproToRow(c, sc));
      }
    }
  }

  if (wantsMaisons) {
    const cp = sp.get("cp")?.trim();
    const commune = sp.get("commune")?.trim();
    const dpeRaw = sp.get("dpe")?.trim();
    if (cp || commune) {
      const result = await searchMaisonsByZone({
        cp,
        commune,
        dpeClasses: dpeRaw ? dpeRaw.split(",").map((c) => c.trim().toUpperCase()) : undefined,
        limit: MAX_MAISONS,
        size: 2000,
      });
      for (const m of result.items) {
        rows.push(maisonToRow(m as unknown as MaisonExportRow));
      }
    }
  }

  // CSV avec BOM UTF-8 pour Excel
  const csv = "﻿" + rows.map((r) => r.join(";")).join("\r\n");
  const stamp = new Date().toISOString().slice(0, 10);
  const filename = `campagne-mailing-${cible}-${stamp}.csv`;

  return new NextResponse(csv, {
    status: 200,
    headers: {
      "content-type": "text/csv; charset=utf-8",
      "content-disposition": `attachment; filename="${filename}"`,
    },
  });
}
