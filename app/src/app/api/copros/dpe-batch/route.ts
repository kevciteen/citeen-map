import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import { estimateDpeForCopro } from "@/lib/services/dpe";
import { z } from "zod";
import { ensureAuth } from "@/lib/auth/guards";

export const runtime = "nodejs";
// Allow long-running batch (Vercel default is 10s, we set higher for local)
export const maxDuration = 120;

const schema = z.object({
  coproIds: z.array(z.number().int().positive()).min(1).max(100),
  // forceRefresh: ignore cache (refresh DPE for already-computed copros)
  forceRefresh: z.boolean().optional(),
  // concurrency: number of parallel estimations
  concurrency: z.number().int().min(1).max(20).optional(),
});

type CoproRow = {
  id: number;
  lat: number | null;
  lon: number | null;
  adresse: string | null;
  code_postal: string | null;
  commune: string | null;
  numero_immatriculation: string | null;
  code_insee_commune: string | null;
  section: string | null;
  numero_parcelle: string | null;
};

async function estimateOne(c: CoproRow) {
  if (c.lat == null || c.lon == null) return { id: c.id, ok: false, reason: "no_coords" };
  try {
    const est = await estimateDpeForCopro({
      lat: c.lat,
      lon: c.lon,
      address: c.adresse,
      codePostal: c.code_postal,
      commune: c.commune,
      numeroImmatriculation: c.numero_immatriculation,
      codeInseeCommune: c.code_insee_commune,
      section: c.section,
      numeroParcelle: c.numero_parcelle,
    });
    await db.run(
      `INSERT INTO dpe_estimates
       (copro_id, classe_reelle, classe_simulee, classe_finale, conso_moyenne,
        nb_dpe_individuels, rayon_recherche, quality_level, computed_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, unixepoch())
       ON CONFLICT(copro_id) DO UPDATE SET
         classe_reelle = excluded.classe_reelle,
         classe_simulee = excluded.classe_simulee,
         classe_finale = excluded.classe_finale,
         conso_moyenne = excluded.conso_moyenne,
         nb_dpe_individuels = excluded.nb_dpe_individuels,
         rayon_recherche = excluded.rayon_recherche,
         quality_level = excluded.quality_level,
         computed_at = unixepoch()`,
      [
        c.id,
        est.collectifReel?.classe ?? null,
        est.immeubleSimule?.classe ?? null,
        est.immeubleFinal.classe,
        est.immeubleFinal.conso ?? est.immeubleSimule?.consoMoyenne ?? null,
        est.totalDpeMatched,
        est.rayonM,
        est.quality.level,
      ],
    );
    return {
      id: c.id,
      ok: true,
      classe: est.immeubleFinal.classe,
      conso: est.immeubleFinal.conso,
      matched: est.totalDpeMatched,
      quality: est.quality.level,
    };
  } catch (err) {
    return { id: c.id, ok: false, reason: (err as Error).message };
  }
}

// Process items in parallel batches of size `concurrency`
async function runWithConcurrency<T, R>(
  items: T[],
  concurrency: number,
  fn: (item: T) => Promise<R>,
): Promise<R[]> {
  const results: R[] = [];
  let idx = 0;
  const workers = Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    while (idx < items.length) {
      const myIdx = idx++;
      results[myIdx] = await fn(items[myIdx]);
    }
  });
  await Promise.all(workers);
  return results;
}

export async function POST(req: NextRequest) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  const body = await req.json();
  const parsed = schema.safeParse(body);
  if (!parsed.success) {
    return NextResponse.json({ error: parsed.error.format() }, { status: 400 });
  }
  const { coproIds, forceRefresh, concurrency = 8 } = parsed.data;

  // Filter out copros that already have an estimate (unless forceRefresh)
  let toProcess: number[] = coproIds;
  if (!forceRefresh) {
    const placeholders = coproIds.map(() => "?").join(",");
    const alreadyDone = await db.all<{ copro_id: number }>(
      `SELECT copro_id FROM dpe_estimates WHERE copro_id IN (${placeholders})`,
      coproIds,
    );
    const doneSet = new Set(alreadyDone.map((r) => r.copro_id));
    toProcess = coproIds.filter((id) => !doneSet.has(id));
  }

  if (toProcess.length === 0) {
    return NextResponse.json({
      totalRequested: coproIds.length,
      processed: 0,
      skipped: coproIds.length,
      results: [],
    });
  }

  const placeholders = toProcess.map(() => "?").join(",");
  const copros = await db.all<CoproRow>(
    `SELECT id, lat, lon, adresse, code_postal, commune,
            numero_immatriculation, code_insee_commune, section, numero_parcelle
     FROM copros WHERE id IN (${placeholders})`,
    toProcess,
  );

  const t0 = Date.now();
  const results = await runWithConcurrency(copros, concurrency, estimateOne);
  const ok = results.filter((r) => r.ok).length;

  return NextResponse.json({
    totalRequested: coproIds.length,
    skipped: coproIds.length - toProcess.length,
    processed: toProcess.length,
    ok,
    failed: toProcess.length - ok,
    durationMs: Date.now() - t0,
    results,
  });
}
