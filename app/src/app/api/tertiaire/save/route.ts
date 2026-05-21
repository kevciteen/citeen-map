/**
 * POST /api/tertiaire/save — persiste en DB un résultat de lookup
 * (issu de /api/tertiaire/lookup) pour le rendre adressable par un prospect.
 *
 * Body : LookupResult complet (au minimum geocode + bdnb || dpeTertiaire).
 * Renvoie : { buildingId, created: boolean }.
 *
 * Stratégie : utilise numero_dpe (ADEME) ou batimentGroupeId (BDNB) comme
 * clé externe. Si déjà existant pour cette adresse, retourne l'id existant.
 */
import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import { ensureTertiary } from "@/lib/db/ensure-tertiary";
import { ensureAuth } from "@/lib/auth/guards";
import type { TertiaireLookupResult } from "@/lib/services/tertiaire";

export const runtime = "nodejs";

type Sector =
  | "Bureaux"
  | "Commerces"
  | "Hotellerie / Restauration"
  | "Sante"
  | "Enseignement"
  | "Autres secteurs";

function mapTypeUsageToSecteur(typeUsage: unknown): Sector {
  const s = String(typeUsage ?? "").toLowerCase();
  if (!s) return "Autres secteurs";
  if (s.includes("bureau")) return "Bureaux";
  if (s.includes("commerce") || s.includes("magasin")) return "Commerces";
  if (s.includes("hotel") || s.includes("hôtel") || s.includes("restaur")) return "Hotellerie / Restauration";
  if (s.includes("sante") || s.includes("santé") || s.includes("hopit") || s.includes("hôpit") || s.includes("clinique") || s.includes("ehpad")) return "Sante";
  if (s.includes("enseign") || s.includes("ecole") || s.includes("école") || s.includes("scolaire")) return "Enseignement";
  return "Autres secteurs";
}

function parseDateMs(v: unknown): number | null {
  if (!v) return null;
  const t = Date.parse(String(v));
  return Number.isFinite(t) ? Math.floor(t / 1000) : null;
}

export async function POST(req: NextRequest) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  await ensureTertiary();

  let payload: TertiaireLookupResult;
  try {
    payload = (await req.json()) as TertiaireLookupResult;
  } catch {
    return NextResponse.json({ error: "JSON invalide" }, { status: 400 });
  }

  if (!payload.geocode) {
    return NextResponse.json(
      { error: "Lookup incomplet : adresse non géocodée" },
      { status: 400 },
    );
  }

  // Détermine la clé externe : préférence DPE numero, sinon BDNB id
  const dpeNumero = payload.dpeTertiaire?.numero_dpe ? String(payload.dpeTertiaire.numero_dpe) : null;
  const bdnbId = payload.bdnb?.batimentGroupeId ?? null;
  const externalId = dpeNumero ?? bdnbId;
  const source = dpeNumero ? "dpe-tertiaire" : bdnbId ? "bdnb" : "user";

  // Cherche un bâtiment existant via la clé la plus précise
  let existing: { id: number } | undefined;
  if (externalId) {
    existing = await db.get<{ id: number }>(
      `SELECT id FROM tertiary_buildings WHERE source = ? AND external_id = ?`,
      [source, externalId],
    );
  }
  // Fallback : même adresse + ±10m
  if (!existing) {
    existing = await db.get<{ id: number }>(
      `SELECT id FROM tertiary_buildings
       WHERE adresse = ? AND lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?
       LIMIT 1`,
      [
        payload.geocode.label,
        payload.geocode.lat - 0.0001,
        payload.geocode.lat + 0.0001,
        payload.geocode.lon - 0.0001,
        payload.geocode.lon + 0.0001,
      ],
    );
  }

  let buildingId: number;
  let created = false;

  const adresse = payload.geocode.label;
  const codePostal = payload.geocode.postcode ?? null;
  const commune = payload.geocode.city ?? null;
  const codeInsee = payload.geocode.citycode ?? null;
  const departement = codePostal ? codePostal.substring(0, 2) : null;
  const typeUsage = payload.bdnb?.typeUsage ?? payload.dpeTertiaire?.type_usage_principal ?? null;
  const secteur = mapTypeUsageToSecteur(typeUsage);
  const surface = payload.bdnb?.surfaceUtileTertiaire ?? Number(payload.dpeTertiaire?.surface_utile ?? 0) ?? null;
  const anneeConstruction = Number(payload.dpeTertiaire?.annee_construction ?? 0) || null;
  const section = payload.parcelle?.section ?? null;
  const numeroParcelle = payload.parcelle?.numero ?? null;
  const refCadastrale = payload.parcelle?.idu ?? null;

  if (existing) {
    buildingId = existing.id;
    await db.run(
      `UPDATE tertiary_buildings SET
         label = COALESCE(?, label),
         adresse = COALESCE(?, adresse),
         code_postal = COALESCE(?, code_postal),
         commune = COALESCE(?, commune),
         code_insee_commune = COALESCE(?, code_insee_commune),
         departement = COALESCE(?, departement),
         lat = COALESCE(?, lat),
         lon = COALESCE(?, lon),
         section = COALESCE(?, section),
         numero_parcelle = COALESCE(?, numero_parcelle),
         reference_cadastrale = COALESCE(?, reference_cadastrale),
         secteur = COALESCE(?, secteur),
         type_usage = COALESCE(?, type_usage),
         surface_m2 = COALESCE(?, surface_m2),
         annee_construction = COALESCE(?, annee_construction)
       WHERE id = ?`,
      [
        adresse,
        adresse,
        codePostal,
        commune,
        codeInsee,
        departement,
        payload.geocode.lat,
        payload.geocode.lon,
        section,
        numeroParcelle,
        refCadastrale,
        secteur,
        typeUsage,
        surface,
        anneeConstruction,
        buildingId,
      ],
    );
  } else {
    const res = await db.run(
      `INSERT INTO tertiary_buildings (
         source, external_id, label, adresse, code_postal, commune,
         code_insee_commune, departement, lat, lon, section, numero_parcelle,
         reference_cadastrale, secteur, type_usage, surface_m2, annee_construction
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        source,
        externalId,
        adresse,
        adresse,
        codePostal,
        commune,
        codeInsee,
        departement,
        payload.geocode.lat,
        payload.geocode.lon,
        section,
        numeroParcelle,
        refCadastrale,
        secteur,
        typeUsage,
        surface,
        anneeConstruction,
      ],
    );
    buildingId = res.lastInsertRowid;
    created = true;
  }

  // Upsert DPE si présent
  if (payload.dpeTertiaire) {
    const d = payload.dpeTertiaire;
    await db.run(
      `INSERT INTO tertiary_dpe (
         building_id, numero_dpe, etiquette_dpe, etiquette_ges,
         conso_energie_primaire, conso_energie_finale, emissions_ges,
         surface_utile, type_usage_dpe, date_etablissement, date_modification
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(building_id) DO UPDATE SET
         numero_dpe = excluded.numero_dpe,
         etiquette_dpe = excluded.etiquette_dpe,
         etiquette_ges = excluded.etiquette_ges,
         conso_energie_primaire = excluded.conso_energie_primaire,
         conso_energie_finale = excluded.conso_energie_finale,
         emissions_ges = excluded.emissions_ges,
         surface_utile = excluded.surface_utile,
         type_usage_dpe = excluded.type_usage_dpe,
         date_etablissement = excluded.date_etablissement,
         date_modification = excluded.date_modification,
         cached_at = unixepoch()`,
      [
        buildingId,
        String(d.numero_dpe ?? ""),
        String(d.etiquette_dpe ?? "") || null,
        String(d.etiquette_ges ?? "") || null,
        Number(d.conso_kwhep_m2_an ?? 0) || null,
        Number(d.conso_kwhef_m2_an ?? 0) || null,
        Number(d.emission_ges_kgco2_m2_an ?? 0) || null,
        Number(d.surface_utile ?? 0) || null,
        String(d.type_usage_principal ?? "") || null,
        parseDateMs(d.date_etablissement_dpe),
        parseDateMs(d.date_derniere_modification_dpe),
      ],
    );
  }

  // Persiste les occupants (delete + insert)
  if (payload.occupants && payload.occupants.length > 0) {
    await db.run(`DELETE FROM tertiary_occupants WHERE building_id = ?`, [buildingId]);
    for (const o of payload.occupants) {
      await db.run(
        `INSERT INTO tertiary_occupants (
           building_id, siret, siren, denomination, naf_code, naf_label,
           tranche_effectif, adresse_enregistree, est_siege, est_actif
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          buildingId,
          o.siret,
          o.siren,
          o.denomination,
          o.nafCode,
          o.nafLabel,
          o.trancheEffectif,
          o.adresseEnregistree,
          o.estSiege ? 1 : 0,
          o.estActif ? 1 : 0,
        ],
      );
    }
  }

  return NextResponse.json({ buildingId, created });
}
