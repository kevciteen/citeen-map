import { db } from "@/lib/db/client";
import { ensureSyndicContactsTable } from "@/lib/db/ensure-syndic-contacts";
import { resolveSyndicByName, type SyndicContact } from "@/lib/services/syndic-contact";

/**
 * Durée de fraîcheur des données Sirene cachées en DB. Au-delà, on
 * refetch live (les sociétés ne changent pas tous les jours, 7j est OK
 * pour un CRM de prospection).
 */
const SIRENE_TTL_SEC = 7 * 24 * 3600;

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
  auto_phone: string | null;
  auto_website: string | null;
  auto_email: string | null;
  auto_hours: string | null;
  auto_source: string | null;
  auto_lat: number | null;
  auto_lon: number | null;
  auto_fetched_at: number | null;
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
  auto: {
    phone: string | null;
    website: string | null;
    email: string | null;
    hours: string | null;
    source: string | null;
    lat: number | null;
    lon: number | null;
    fetched_at: number | null;
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

/**
 * Renvoie le SyndicContact (Sirene) en lisant d'abord le cache DB
 * (sirene_json sur syndic_contacts), avec fallback live + re-persist.
 * Évite ~200-400 ms d'HTTPS vers recherche-entreprises.api.gouv.fr à
 * chaque vue de fiche syndic / copro.
 */
export async function getOrFetchSirene(
  slug: string,
  name: string,
): Promise<SyndicContact | null> {
  await ensureSyndicContactsTable();
  const stored = await db.get<{
    sirene_json: string | null;
    updated_at: number | null;
  }>(
    `SELECT sirene_json, updated_at FROM syndic_contacts WHERE slug = ?`,
    [slug],
  );

  const nowSec = Math.floor(Date.now() / 1000);
  if (stored?.sirene_json && stored.updated_at !== null) {
    const age = nowSec - stored.updated_at;
    if (age >= 0 && age < SIRENE_TTL_SEC) {
      try {
        return JSON.parse(stored.sirene_json) as SyndicContact;
      } catch {
        // JSON invalide → on tombera sur le live
      }
    }
  }

  const fresh = await resolveSyndicByName(name).catch(() => null);
  if (fresh) {
    // Upsert juste sirene_json (préserve les autres champs édités)
    await db.run(
      `INSERT INTO syndic_contacts (slug, name, sirene_json, created_at, updated_at)
       VALUES (?, ?, ?, unixepoch(), unixepoch())
       ON CONFLICT(slug) DO UPDATE SET
         name = excluded.name,
         sirene_json = excluded.sirene_json,
         updated_at = unixepoch()`,
      [slug, name, JSON.stringify(fresh)],
    );
  }
  return fresh;
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
  // Optim perf : on n'utilise PLUS `WHERE TRIM(c.syndic) = ?` (kill l'index
  // idx_copros_syndic, full scan 100k rows). On essaie d'abord l'exact
  // match indexé, et si rien, on tente avec TRIM (fallback safety).
  // La correlated EXISTS sur prospects est remplacée par un LEFT JOIN sur
  // une sous-requête DISTINCT qui scanne prospects 1 seule fois.
  const baseSql = (whereSyndic: string, groupBy: string) => `
    SELECT
       ${groupBy} AS syndic,
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
       SUM(CASE WHEN p.copro_id IS NOT NULL THEN 1 ELSE 0 END) AS in_pipeline,
       GROUP_CONCAT(DISTINCT c.departement) AS dept_list,
       GROUP_CONCAT(DISTINCT c.commune) AS commune_list
     FROM copros c
     LEFT JOIN dpe_estimates e ON e.copro_id = c.id
     LEFT JOIN (SELECT DISTINCT copro_id FROM prospects WHERE copro_id IS NOT NULL) p
       ON p.copro_id = c.id
     WHERE ${whereSyndic}
     GROUP BY ${groupBy}
  `;

  // 1. Tentative indexée (chemin chaud)
  let row = await db.get<SyndicAggregate>(
    baseSql("c.syndic = ?", "c.syndic"),
    [name],
  );
  // 2. Fallback safety (rare) si données contiennent des espaces autour
  if (!row || row.nb_copros === 0) {
    row = await db.get<SyndicAggregate>(
      baseSql("TRIM(c.syndic) = ?", "TRIM(c.syndic)"),
      [name.trim()],
    );
  }
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

  // 3. Sirene (DB-first, TTL 7j → fallback live + persist)
  const sirene = await getOrFetchSirene(slug, name).catch(() => null);

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
    auto: {
      phone: stored?.auto_phone ?? null,
      website: stored?.auto_website ?? null,
      email: stored?.auto_email ?? null,
      hours: stored?.auto_hours ?? null,
      source: stored?.auto_source ?? null,
      lat: stored?.auto_lat ?? null,
      lon: stored?.auto_lon ?? null,
      fetched_at: stored?.auto_fetched_at ?? null,
    },
  };
}
