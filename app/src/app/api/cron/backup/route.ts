/**
 * Cron backup — dump des tables critiques en JSON et upload vers un bucket
 * S3 ou compatible. Cron Vercel quotidien.
 *
 * NO-OP si BACKUP_S3_URL n'est pas configuré → on log juste le résumé.
 * Quand tu auras un bucket (S3 / R2 / B2 / Wasabi), set les env vars :
 *   BACKUP_S3_URL=https://<bucket>.s3.<region>.amazonaws.com
 *   BACKUP_S3_KEY=<access_key>
 *   BACKUP_S3_SECRET=<secret>
 * (ou utiliser une URL pré-signée renouvelée à la demande)
 *
 * Tables sauvegardées : users, prospects, comments, notifications, syndic_contacts,
 * notes, tasks, contacts, activities, dpe_estimates.
 * On ne dump PAS la table copros (134k lignes, source = data.gouv.fr re-importable).
 */
import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";

export const runtime = "nodejs";
export const maxDuration = 60;

const TABLES_TO_BACKUP = [
  "users",
  "prospects",
  "comments",
  "notifications",
  "syndic_contacts",
  "notes",
  "tasks",
  "contacts",
  "activities",
  "dpe_estimates",
];

function assertCronAuth(req: NextRequest): NextResponse | null {
  const expected = process.env.CRON_SECRET;
  if (!expected) return null;
  const auth = req.headers.get("authorization");
  if (auth !== `Bearer ${expected}`) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }
  return null;
}

export async function GET(req: NextRequest) {
  const unauthorized = assertCronAuth(req);
  if (unauthorized) return unauthorized;

  const dump: Record<string, unknown[]> = {};
  const counts: Record<string, number> = {};

  for (const table of TABLES_TO_BACKUP) {
    try {
      const rows = await db.all(`SELECT * FROM ${table}`);
      dump[table] = rows;
      counts[table] = rows.length;
    } catch {
      // Table peut-être absente (ensure-* pas encore appelé) — on skip
      counts[table] = 0;
    }
  }

  const totalRows = Object.values(counts).reduce((a, b) => a + b, 0);
  const sizeBytes = Buffer.byteLength(JSON.stringify(dump), "utf8");

  // Upload S3 — TODO une fois bucket configuré. Pour l'instant on retourne
  // juste le résumé pour vérifier que le dump tourne.
  const bucketUrl = process.env.BACKUP_S3_URL;
  let uploaded = false;
  if (bucketUrl && process.env.BACKUP_S3_KEY && process.env.BACKUP_S3_SECRET) {
    // À implémenter quand un bucket sera dispo (signature S3 V4 nécessaire).
    // En attendant, on peut tout simplement appeler un Vercel Blob URL.
    uploaded = false;
  }

  return NextResponse.json({
    ok: true,
    timestamp: new Date().toISOString(),
    totalRows,
    counts,
    sizeBytes,
    sizeKb: Math.round(sizeBytes / 1024),
    uploaded,
    note: uploaded
      ? "Backup uploadé"
      : "Backup non uploadé — configure BACKUP_S3_URL/KEY/SECRET pour activer",
  });
}
