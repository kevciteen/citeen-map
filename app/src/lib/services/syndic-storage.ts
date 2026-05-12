import { db } from "@/lib/db/client";
import { ensureSyndicContactsTable } from "@/lib/db/ensure-syndic-contacts";
import { resolveSyndicByName, type SyndicContact } from "@/lib/services/syndic-contact";

export type SyndicContactRecord = {
  slug: string;
  name: string;
  email: string | null;
  phone: string | null;
  contact_person: string | null;
  website: string | null;
  address_override: string | null;
  notes: string | null;
  sirene_json: string | null;
  created_at: number;
  updated_at: number;
};

export type SyndicAggregate = {
  syndic: string;
  nb_copros: number;
  lots_total: number;
  nb_communes: number;
  nb_departements: number;
  dpe_a: number;
  dpe_b: number;
  dpe_c: number;
  dpe_d: number;
  dpe_e: number;
  dpe_f: number;
  dpe_g: number;
  dpe_nc: number;
  in_pipeline: number;
  dept_list: string | null;
  commune_list: string | null;
};

export type SyndicFullDetail = {
  slug: string;
  name: string;
  aggregate: SyndicAggregate | null;
  sirene: SyndicContact | null;
  editable: {
    email: string | null;
    phone: string | null;
    contact_person: string | null;
    website: string | null;
    address_override: string | null;
    notes: string | null;
  };
};

export async function getSyndicRecord(
  slug: string,
): Promise<SyndicContactRecord | null> {
  await ensureSyndicContactsTable();
  const row = await db.get<SyndicContactRecord>(
    `SELECT * FROM syndic_contacts WHERE slug = ?`,
    [slug],
  );
  return row ?? null;
}

export async function upsertSyndicContact(input: {
  slug: string;
  name: string;
  email?: string | null;
  phone?: string | null;
  contactPerson?: string | null;
  website?: string | null;
  addressOverride?: string | null;
  notes?: string | null;
  sireneJson?: string | null;
}): Promise<void> {
  await ensureSyndicContactsTable();
  await db.run(
    `INSERT INTO syndic_contacts
       (slug, name, email, phone, contact_person, website, address_override, notes, sirene_json, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, unixepoch(), unixepoch())
     ON CONFLICT(slug) DO UPDATE SET
       name = excluded.name,
       email = COALESCE(excluded.email, syndic_contacts.email),
       phone = COALESCE(excluded.phone, syndic_contacts.phone),
       contact_person = COALESCE(excluded.contact_person, syndic_contacts.contact_person),
       website = COALESCE(excluded.website, syndic_contacts.website),
       address_override = COALESCE(excluded.address_override, syndic_contacts.address_override),
       notes = COALESCE(excluded.notes, syndic_contacts.notes),
       sirene_json = COALESCE(excluded.sirene_json, syndic_contacts.sirene_json),
       updated_at = unixepoch()`,
    [
      input.slug,
      input.name,
      input.email ?? null,
      input.phone ?? null,
      input.contactPerson ?? null,
      input.website ?? null,
      input.addressOverride ?? null,
      input.notes ?? null,
      input.sireneJson ?? null,
    ],
  );
}

/**
 * Patch partial — null met explicitement à null (utilisé pour effacer un champ).
 */
export async function patchSyndicContact(
  slug: string,
  patch: Partial<{
    email: string | null;
    phone: string | null;
    contact_person: string | null;
    website: string | null;
    address_override: string | null;
    notes: string | null;
  }>,
): Promise<void> {
  await ensureSyndicContactsTable();
  const fields: string[] = [];
  const args: (string | number | null)[] = [];
  for (const [k, v] of Object.entries(patch)) {
    fields.push(`${k} = ?`);
    args.push(v ?? null);
  }
  if (fields.length === 0) return;
  fields.push("updated_at = unixepoch()");
  args.push(slug);
  await db.run(
    `UPDATE syndic_contacts SET ${fields.join(", ")} WHERE slug = ?`,
    args,
  );
}

/**
 * Agrégats : copros gérées, lots, distribution DPE, communes…
 */
export async function getSyndicAggregate(
  name: string,
): Promise<SyndicAggregate | null> {
  const row = await db.get<SyndicAggregate>(
    `SELECT
       TRIM(c.syndic) AS syndic,
       COUNT(*) AS nb_copros,
       COALESCE(SUM(c.nb_lots_habitation), 0) AS lots_total,
       COUNT(DISTINCT c.commune) AS nb_communes,
       COUNT(DISTINCT c.departement) AS nb_departements,
       SUM(CASE WHEN e.classe_finale = 'A' THEN 1 ELSE 0 END) AS dpe_a,
       SUM(CASE WHEN e.classe_finale = 'B' THEN 1 ELSE 0 END) AS dpe_b,
       SUM(CASE WHEN e.classe_finale = 'C' THEN 1 ELSE 0 END) AS dpe_c,
       SUM(CASE WHEN e.classe_finale = 'D' THEN 1 ELSE 0 END) AS dpe_d,
       SUM(CASE WHEN e.classe_finale = 'E' THEN 1 ELSE 0 END) AS dpe_e,
       SUM(CASE WHEN e.classe_finale = 'F' THEN 1 ELSE 0 END) AS dpe_f,
       SUM(CASE WHEN e.classe_finale = 'G' THEN 1 ELSE 0 END) AS dpe_g,
       SUM(CASE WHEN e.classe_finale IS NULL OR e.classe_finale = 'NC' THEN 1 ELSE 0 END) AS dpe_nc,
       SUM(CASE WHEN EXISTS (SELECT 1 FROM prospects p WHERE p.copro_id = c.id) THEN 1 ELSE 0 END) AS in_pipeline,
       GROUP_CONCAT(DISTINCT c.departement) AS dept_list,
       GROUP_CONCAT(DISTINCT c.commune) AS commune_list
     FROM copros c
     LEFT JOIN dpe_estimates e ON e.copro_id = c.id
     WHERE TRIM(c.syndic) = ?
     GROUP BY TRIM(c.syndic)`,
    [name],
  );
  return row ?? null;
}

/**
 * Récupère la fiche complète d'un syndic identifié par son slug.
 * Le slug ne suffit pas à retrouver le nom canonique côté SQLite : on doit
 * passer par une lookup intermédiaire (on stocke aussi name en DB une fois
 * la fiche éditée pour la première fois).
 */
export async function getSyndicFullDetail(
  slug: string,
  fallbackName?: string,
): Promise<SyndicFullDetail | null> {
  await ensureSyndicContactsTable();

  // 1. Récupère le nom canonique
  const stored = await getSyndicRecord(slug);
  const name = stored?.name ?? fallbackName ?? null;
  if (!name) return null;

  // 2. Agrégats SQL
  const aggregate = await getSyndicAggregate(name);

  // 3. Sirene live (cached 24h)
  let sirene: SyndicContact | null = null;
  try {
    sirene = await resolveSyndicByName(name);
  } catch {
    // ignore — on retourne le reste
  }

  return {
    slug,
    name,
    aggregate,
    sirene,
    editable: {
      email: stored?.email ?? null,
      phone: stored?.phone ?? null,
      contact_person: stored?.contact_person ?? null,
      website: stored?.website ?? null,
      address_override: stored?.address_override ?? null,
      notes: stored?.notes ?? null,
    },
  };
}
