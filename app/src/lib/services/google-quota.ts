/**
 * Compteur d'appels Google Places — un compteur par jour UTC.
 * Permet de surveiller la consommation vs le free tier (~1000/mois) et
 * d'alerter avant épuisement silencieux.
 */
import { db } from "@/lib/db/client";

export type GoogleQuotaCallKind = "find" | "details";

function todayKey(): string {
  return new Date().toISOString().slice(0, 10);
}

export async function recordGooglePlacesCall(kind: GoogleQuotaCallKind): Promise<void> {
  const day = todayKey();
  const col = kind === "find" ? "find_count" : "details_count";
  await db.run(
    `INSERT INTO google_places_usage (day, ${col}, updated_at)
     VALUES (?, 1, unixepoch())
     ON CONFLICT(day) DO UPDATE SET
       ${col} = ${col} + 1,
       updated_at = unixepoch()`,
    [day],
  );
}

export type GoogleQuotaSnapshot = {
  day: string;
  find: number;
  details: number;
  total: number;
  monthFind: number;
  monthDetails: number;
  monthTotal: number;
};

export async function getGoogleQuotaSnapshot(): Promise<GoogleQuotaSnapshot> {
  const day = todayKey();
  const monthPrefix = day.slice(0, 7); // YYYY-MM
  const today = await db.get<{ find_count: number; details_count: number }>(
    `SELECT find_count, details_count FROM google_places_usage WHERE day = ?`,
    [day],
  );
  const month = await db.get<{ f: number; d: number }>(
    `SELECT COALESCE(SUM(find_count), 0) AS f, COALESCE(SUM(details_count), 0) AS d
     FROM google_places_usage WHERE day LIKE ?`,
    [`${monthPrefix}%`],
  );
  const find = today?.find_count ?? 0;
  const details = today?.details_count ?? 0;
  const monthFind = month?.f ?? 0;
  const monthDetails = month?.d ?? 0;
  return {
    day,
    find,
    details,
    total: find + details,
    monthFind,
    monthDetails,
    monthTotal: monthFind + monthDetails,
  };
}
