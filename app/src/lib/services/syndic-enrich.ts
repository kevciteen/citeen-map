/**
 * Enrichissement automatique des coordonnées syndic :
 *   1. Résolution Sirene (adresse du siège social via Recherche Entreprises)
 *   2. Géocodage BAN du siège → lat/lon
 *   3. findContactInfo (OSM → Google Places fallback) sur le siège
 *   4. Persiste dans les colonnes auto_* de syndic_contacts (jamais les
 *      colonnes éditables manuelles).
 *
 * Le résultat est ré-utilisable autant de fois qu'on veut : grâce au cache
 * contact_cache (Étape 1), les appels Google ne consomment du quota que la
 * première fois.
 */
import { db } from "@/lib/db/client";
import { ensureSyndicContactsTable } from "@/lib/db/ensure-syndic-contacts";
import { resolveSyndicByName, type SyndicContact } from "./syndic-contact";
import { geocodeAddress } from "./ban";
import { findContactInfo } from "./coords";
import { syncDirectorySyndic } from "./directory-sync";

export type SyndicEnrichResult = {
  resolved: boolean;
  source: "osm" | "google" | "none";
  phone: string | null;
  website: string | null;
  email: string | null;
  hours: string | null;
  lat: number | null;
  lon: number | null;
  sirene: SyndicContact | null;
  reason?: string;
};

export async function enrichSyndicAuto(
  slug: string,
  name: string,
): Promise<SyndicEnrichResult> {
  await ensureSyndicContactsTable();

  // 1. Sirene (cache 24h via fetch Next.js)
  const sirene = await resolveSyndicByName(name);
  if (!sirene || !sirene.adresse) {
    await persistAuto(slug, name, {
      phone: null, website: null, email: null, hours: null,
      source: "none", lat: null, lon: null,
    }, sirene);
    await syncDirectorySyndic(slug).catch(() => {});
    return {
      resolved: false,
      source: "none",
      phone: null, website: null, email: null, hours: null,
      lat: null, lon: null,
      sirene,
      reason: sirene ? "Pas d'adresse siège" : "Pas de match Sirene",
    };
  }

  // 2. Géocode BAN
  const adresseComplete = [sirene.adresse, sirene.codePostal, sirene.commune]
    .filter(Boolean)
    .join(" ");
  const banResults = await geocodeAddress(adresseComplete, { limit: 1, postcode: sirene.codePostal ?? undefined });
  const ban = banResults[0];
  const lat = ban?.score && ban.score >= 0.5 ? ban.lat : null;
  const lon = ban?.score && ban.score >= 0.5 ? ban.lon : null;

  // 3. OSM + Google fallback (compteur quota Google déjà câblé dans findContactInfo)
  const contact = await findContactInfo({
    denomination: sirene.nomComplet,
    address: adresseComplete,
    lat: lat ?? undefined,
    lon: lon ?? undefined,
  });

  await persistAuto(slug, name, {
    phone: contact.phone,
    website: contact.website,
    email: contact.email,
    hours: contact.hours,
    source: contact.source,
    lat,
    lon,
  }, sirene);

  // Double-write : sync immédiat dans l'annuaire unifié
  await syncDirectorySyndic(slug).catch(() => {
    // Le sync best-effort ; on ne fait pas échouer l'enrichissement si
    // l'annuaire est temporairement indisponible (CRON sync rattrapera).
  });

  return {
    resolved: contact.source !== "none",
    source: contact.source,
    phone: contact.phone,
    website: contact.website,
    email: contact.email,
    hours: contact.hours,
    lat,
    lon,
    sirene,
  };
}

async function persistAuto(
  slug: string,
  name: string,
  data: {
    phone: string | null;
    website: string | null;
    email: string | null;
    hours: string | null;
    source: string;
    lat: number | null;
    lon: number | null;
  },
  sirene: SyndicContact | null,
): Promise<void> {
  await db.run(
    `INSERT INTO syndic_contacts
       (slug, name, sirene_json,
        auto_phone, auto_website, auto_email, auto_hours,
        auto_source, auto_lat, auto_lon, auto_fetched_at,
        created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, unixepoch(), unixepoch(), unixepoch())
     ON CONFLICT(slug) DO UPDATE SET
       name = excluded.name,
       sirene_json = COALESCE(excluded.sirene_json, syndic_contacts.sirene_json),
       auto_phone = excluded.auto_phone,
       auto_website = excluded.auto_website,
       auto_email = excluded.auto_email,
       auto_hours = excluded.auto_hours,
       auto_source = excluded.auto_source,
       auto_lat = excluded.auto_lat,
       auto_lon = excluded.auto_lon,
       auto_fetched_at = unixepoch(),
       updated_at = unixepoch()`,
    [
      slug,
      name,
      sirene ? JSON.stringify(sirene) : null,
      data.phone,
      data.website,
      data.email,
      data.hours,
      data.source,
      data.lat,
      data.lon,
    ],
  );
}
