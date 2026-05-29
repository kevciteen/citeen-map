/**
 * Synchronise l'annuaire `directory` depuis les tables canoniques.
 *
 * Idempotent : `INSERT ... ON CONFLICT(entity_type, entity_ref) DO UPDATE`.
 * Préserve la PK + les timestamps `created_at` ; met à jour `synced_at` à
 * chaque passe.
 *
 * Chaque fonction sync<X> retourne le nombre de rows traitées. À appeler
 * via /api/directory/sync (admin) ou ponctuellement après un import.
 */
import { db } from "@/lib/db/client";
import { ensureDirectory } from "@/lib/db/ensure-directory";

export type DirectorySyncResult = {
  copros: number;
  occupants: number;
  syndics: number;
  prospectsCustom: number;
  total: number;
};

const UPSERT_SQL = `
  INSERT INTO directory (
    entity_type, entity_ref, display_name, display_subtitle,
    address, postcode, city, departement, lat, lon, coords_source, coords_score,
    phone, phone_source, email, email_source, website, website_source, hours,
    parent_copro_id, parent_building_id, enriched_at,
    synced_at, created_at, updated_at
  ) VALUES (
    ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?,
    unixepoch(), unixepoch(), unixepoch()
  )
  ON CONFLICT(entity_type, entity_ref) DO UPDATE SET
    display_name = excluded.display_name,
    display_subtitle = excluded.display_subtitle,
    address = excluded.address,
    postcode = excluded.postcode,
    city = excluded.city,
    departement = excluded.departement,
    lat = excluded.lat,
    lon = excluded.lon,
    coords_source = excluded.coords_source,
    coords_score = excluded.coords_score,
    phone = excluded.phone,
    phone_source = excluded.phone_source,
    email = excluded.email,
    email_source = excluded.email_source,
    website = excluded.website,
    website_source = excluded.website_source,
    hours = excluded.hours,
    parent_copro_id = excluded.parent_copro_id,
    parent_building_id = excluded.parent_building_id,
    enriched_at = excluded.enriched_at,
    synced_at = unixepoch(),
    updated_at = unixepoch()
`;

type Args = (string | number | null)[];

async function upsert(args: Args): Promise<void> {
  await db.run(UPSERT_SQL, args);
}

/* -------------------------------- COPROS -------------------------------- */

type CoproRow = {
  id: number;
  numero_immatriculation: string | null;
  nom_copro: string | null;
  adresse: string | null;
  code_postal: string | null;
  commune: string | null;
  departement: string | null;
  lat: number | null;
  lon: number | null;
  coords_source: string | null;
  coords_score: number | null;
  nb_lots: number | null;
  nb_lots_habitation: number | null;
};

async function syncCopros(): Promise<number> {
  const rows = await db.all<CoproRow>(
    `SELECT id, numero_immatriculation, nom_copro, adresse, code_postal, commune,
            departement, lat, lon, coords_source, coords_score,
            nb_lots, nb_lots_habitation
     FROM copros`,
  );
  for (const r of rows) {
    const name =
      r.nom_copro?.trim() ||
      (r.adresse ? `Copropriété ${r.adresse}` : `Copro ${r.numero_immatriculation ?? r.id}`);
    const lots = r.nb_lots_habitation ?? r.nb_lots;
    const subtitle = `Copropriété${lots ? ` — ${lots} lots` : ""}`;
    await upsert([
      "copro", String(r.id), name, subtitle,
      r.adresse, r.code_postal, r.commune, r.departement,
      r.lat, r.lon, r.coords_source, r.coords_score,
      null, null, null, null, null, null, null,
      null, null, null,
    ]);
  }
  return rows.length;
}

/* ---------------------------- OCCUPANTS TERTIAIRE ---------------------------- */

type OccupantRow = {
  id: number;
  building_id: number | null;
  siret: string | null;
  denomination: string | null;
  naf_label: string | null;
  tranche_effectif: string | null;
  phone: string | null;
  website: string | null;
  email: string | null;
  hours: string | null;
  contact_source: string | null;
  contact_fetched_at: number | null;
  b_adresse: string | null;
  b_code_postal: string | null;
  b_commune: string | null;
  b_departement: string | null;
  b_lat: number | null;
  b_lon: number | null;
};

async function syncOccupants(): Promise<number> {
  const rows = await db.all<OccupantRow>(
    `SELECT o.id, o.building_id, o.siret, o.denomination, o.naf_label,
            o.tranche_effectif, o.phone, o.website, o.email, o.hours,
            o.contact_source, o.contact_fetched_at,
            b.adresse AS b_adresse, b.code_postal AS b_code_postal,
            b.commune AS b_commune, b.departement AS b_departement,
            b.lat AS b_lat, b.lon AS b_lon
     FROM tertiary_occupants o
     LEFT JOIN tertiary_buildings b ON b.id = o.building_id`,
  );
  for (const r of rows) {
    const name = r.denomination?.trim() || `SIRET ${r.siret ?? r.id}`;
    const naf = r.naf_label?.trim();
    const eff = r.tranche_effectif?.trim();
    const subtitleParts: string[] = ["Société tertiaire"];
    if (naf) subtitleParts.push(naf);
    if (eff && eff !== "NN" && eff !== "00") subtitleParts.push(`eff. ${eff}`);
    const subtitle = subtitleParts.join(" — ");
    const channelSource = r.contact_source && r.contact_source !== "none" ? r.contact_source : null;
    await upsert([
      "occupant", String(r.id), name, subtitle,
      r.b_adresse, r.b_code_postal, r.b_commune, r.b_departement,
      r.b_lat, r.b_lon, r.b_lat != null ? "sirene" : null, null,
      r.phone, r.phone ? channelSource : null,
      r.email, r.email ? channelSource : null,
      r.website, r.website ? channelSource : null,
      r.hours,
      null, r.building_id, r.contact_fetched_at,
    ]);
  }
  return rows.length;
}

/* --------------------------------- SYNDICS --------------------------------- */

type SyndicRow = {
  slug: string;
  name: string;
  email: string | null;
  phone: string | null;
  website: string | null;
  address_override: string | null;
  auto_phone: string | null;
  auto_website: string | null;
  auto_email: string | null;
  auto_hours: string | null;
  auto_source: string | null;
  auto_lat: number | null;
  auto_lon: number | null;
  auto_fetched_at: number | null;
  sirene_json: string | null;
};

type SireneAddress = {
  adresse?: string | null;
  codePostal?: string | null;
  commune?: string | null;
  departement?: string | null;
};

function parseSirene(json: string | null): SireneAddress | null {
  if (!json) return null;
  try {
    return JSON.parse(json) as SireneAddress;
  } catch {
    return null;
  }
}

async function syncSyndics(): Promise<number> {
  const rows = await db.all<SyndicRow>(`SELECT * FROM syndic_contacts`);
  for (const r of rows) {
    const sirene = parseSirene(r.sirene_json);
    const adresse = r.address_override?.trim() || sirene?.adresse || null;
    const cp = sirene?.codePostal ?? null;
    const commune = sirene?.commune ?? null;
    const dept = sirene?.departement ?? null;
    // Channels : manuel > auto
    const phone = r.phone?.trim() || r.auto_phone || null;
    const phoneSrc = r.phone?.trim() ? "manual" : (phone ? r.auto_source : null);
    const email = r.email?.trim() || r.auto_email || null;
    const emailSrc = r.email?.trim() ? "manual" : (email ? r.auto_source : null);
    const website = r.website?.trim() || r.auto_website || null;
    const websiteSrc = r.website?.trim() ? "manual" : (website ? r.auto_source : null);
    await upsert([
      "syndic", r.slug, r.name, "Syndic",
      adresse, cp, commune, dept,
      r.auto_lat, r.auto_lon, r.auto_lat != null ? "ban" : null, null,
      phone, phoneSrc, email, emailSrc, website, websiteSrc, r.auto_hours,
      null, null, r.auto_fetched_at,
    ]);
  }
  return rows.length;
}

/* ----------------------------- PROSPECTS CUSTOM ----------------------------- */

type ProspectCustomRow = {
  id: number;
  custom_label: string | null;
  custom_address: string | null;
  custom_lat: number | null;
  custom_lon: number | null;
};

async function syncProspectsCustom(): Promise<number> {
  const rows = await db.all<ProspectCustomRow>(
    `SELECT id, custom_label, custom_address, custom_lat, custom_lon
     FROM prospects
     WHERE custom_label IS NOT NULL AND custom_label != ''`,
  );
  for (const r of rows) {
    await upsert([
      "prospect_custom", String(r.id), r.custom_label ?? `Prospect ${r.id}`, "Saisie libre",
      r.custom_address, null, null, null,
      r.custom_lat, r.custom_lon, r.custom_lat != null ? "manual" : null, null,
      null, null, null, null, null, null, null,
      null, null, null,
    ]);
  }
  return rows.length;
}

/* ---------------------------------- API ---------------------------------- */

export async function syncDirectoryAll(): Promise<DirectorySyncResult> {
  await ensureDirectory();
  const copros = await syncCopros();
  const occupants = await syncOccupants();
  const syndics = await syncSyndics();
  const prospectsCustom = await syncProspectsCustom();
  return {
    copros,
    occupants,
    syndics,
    prospectsCustom,
    total: copros + occupants + syndics + prospectsCustom,
  };
}
