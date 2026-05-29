/**
 * GET /api/tertiaire/[id] — fiche détaillée d'un bâtiment tertiaire.
 *
 * Renvoie : building + DPE + occupants (depuis DB) + diagnostic synthétique
 * pour le moteur CEE (secteur, surface, année, code postal, classe DPE).
 *
 * Si `?refresh=1`, ré-interroge la Recherche d'entreprises pour rafraîchir
 * les occupants (sinon retourne ceux en cache DB).
 */
import { NextRequest, NextResponse } from "next/server";
import { db } from "@/lib/db/client";
import { ensureTertiary } from "@/lib/db/ensure-tertiary";
import { ensureAuth } from "@/lib/auth/guards";
import { searchEntreprisesAtAddress } from "@/lib/services/entreprises";
import { syncDirectoryBuilding } from "@/lib/services/directory-sync";

export const runtime = "nodejs";

type Building = {
  id: number;
  source: string;
  external_id: string | null;
  label: string | null;
  adresse: string | null;
  code_postal: string | null;
  commune: string | null;
  code_insee_commune: string | null;
  departement: string | null;
  lat: number | null;
  lon: number | null;
  secteur: string | null;
  type_usage: string | null;
  surface_m2: number | null;
  annee_construction: number | null;
};

type Dpe = {
  numero_dpe: string | null;
  etiquette_dpe: string | null;
  etiquette_ges: string | null;
  conso_energie_primaire: number | null;
  conso_energie_finale: number | null;
  emissions_ges: number | null;
  surface_utile: number | null;
  type_usage_dpe: string | null;
  date_etablissement: number | null;
};

type Occupant = {
  id: number;
  siret: string | null;
  siren: string | null;
  denomination: string | null;
  naf_code: string | null;
  naf_label: string | null;
  tranche_effectif: string | null;
  est_siege: number | null;
  dirigeants_json: string | null;
  phone: string | null;
  website: string | null;
  email: string | null;
  hours: string | null;
  contact_source: string | null;
};

export async function GET(
  req: NextRequest,
  ctx: { params: Promise<{ id: string }> },
) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  await ensureTertiary();

  const { id } = await ctx.params;
  const buildingId = Number(id);
  if (!Number.isFinite(buildingId)) {
    return NextResponse.json({ error: "id invalide" }, { status: 400 });
  }

  const building = await db.get<Building>(
    `SELECT * FROM tertiary_buildings WHERE id = ?`,
    [buildingId],
  );
  if (!building) {
    return NextResponse.json({ error: "Bâtiment introuvable" }, { status: 404 });
  }

  const dpe = await db.get<Dpe>(
    `SELECT numero_dpe, etiquette_dpe, etiquette_ges, conso_energie_primaire,
            conso_energie_finale, emissions_ges, surface_utile, type_usage_dpe, date_etablissement
     FROM tertiary_dpe WHERE building_id = ?`,
    [buildingId],
  );

  const refresh = req.nextUrl.searchParams.get("refresh") === "1";
  let occupants = await db.all<Occupant>(
    `SELECT id, siret, siren, denomination, naf_code, naf_label, tranche_effectif, est_siege, dirigeants_json, phone, website, email, hours, contact_source
     FROM tertiary_occupants WHERE building_id = ? ORDER BY est_siege DESC, denomination`,
    [buildingId],
  );

  if (refresh || occupants.length === 0) {
    if (building.adresse || (building.lat && building.lon)) {
      const fresh = await searchEntreprisesAtAddress({
        q: building.adresse ?? "",
        codeInsee: building.code_insee_commune ?? undefined,
        codePostal: building.code_postal ?? undefined,
        lat: building.lat ?? undefined,
        lon: building.lon ?? undefined,
        limit: 50,
      });

      // UPSERT par (building_id, siret) : préserve phone/website/email/hours/
      // contact_source/contact_fetched_at quand l'occupant a déjà été enrichi.
      const freshSirets: string[] = [];
      for (const o of fresh) {
        const dirJson =
          o.dirigeants && o.dirigeants.length > 0 ? JSON.stringify(o.dirigeants) : null;
        if (o.siret) {
          freshSirets.push(o.siret);
          await db.run(
            `INSERT INTO tertiary_occupants (
               building_id, siret, siren, denomination, naf_code, naf_label,
               tranche_effectif, adresse_enregistree, est_siege, est_actif,
               dirigeants_json, cached_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, unixepoch())
             ON CONFLICT(building_id, siret) WHERE siret IS NOT NULL DO UPDATE SET
               siren = excluded.siren,
               denomination = excluded.denomination,
               naf_code = excluded.naf_code,
               naf_label = excluded.naf_label,
               tranche_effectif = excluded.tranche_effectif,
               adresse_enregistree = excluded.adresse_enregistree,
               est_siege = excluded.est_siege,
               est_actif = excluded.est_actif,
               dirigeants_json = excluded.dirigeants_json,
               cached_at = unixepoch()`,
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
              dirJson,
            ],
          );
        } else {
          // Sans SIRET : pas de clé d'unicité, simple INSERT
          await db.run(
            `INSERT INTO tertiary_occupants (
               building_id, siret, siren, denomination, naf_code, naf_label,
               tranche_effectif, adresse_enregistree, est_siege, est_actif, dirigeants_json
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [
              buildingId,
              null,
              o.siren,
              o.denomination,
              o.nafCode,
              o.nafLabel,
              o.trancheEffectif,
              o.adresseEnregistree,
              o.estSiege ? 1 : 0,
              o.estActif ? 1 : 0,
              dirJson,
            ],
          );
        }
      }

      // Nettoyage : supprime les occupants disparus du résultat SIRENE
      // SAUF ceux déjà enrichis (contact_fetched_at non NULL — travail utilisateur).
      if (freshSirets.length > 0) {
        const placeholders = freshSirets.map(() => "?").join(",");
        await db.run(
          `DELETE FROM tertiary_occupants
           WHERE building_id = ?
             AND contact_fetched_at IS NULL
             AND (siret IS NULL OR siret NOT IN (${placeholders}))`,
          [buildingId, ...freshSirets],
        );
      } else {
        // SIRENE n'a rien renvoyé : on garde tout ce qui est enrichi, on nettoie le reste
        await db.run(
          `DELETE FROM tertiary_occupants
           WHERE building_id = ? AND contact_fetched_at IS NULL`,
          [buildingId],
        );
      }

      occupants = await db.all<Occupant>(
        `SELECT id, siret, siren, denomination, naf_code, naf_label, tranche_effectif, est_siege, dirigeants_json, phone, website, email, hours, contact_source
         FROM tertiary_occupants WHERE building_id = ? ORDER BY est_siege DESC, denomination`,
        [buildingId],
      );

      // Double-write : resync de tous les occupants de ce bâtiment dans
      // l'annuaire (les anciennes lignes supprimées sont écrasées au
      // prochain /sync complet, mais ne polluent pas la vue immédiate).
      await syncDirectoryBuilding(buildingId).catch(() => {});
    }
  }

  // Désérialise dirigeants_json pour les renvoyer en array direct
  const occupantsWithDirigeants = occupants.map((o) => ({
    ...o,
    dirigeants: o.dirigeants_json ? JSON.parse(o.dirigeants_json) : [],
  }));

  // Synthèse pour le moteur CEE
  const ceeProject = {
    buildingType: "Tertiaire" as const,
    sector: building.secteur ?? "",
    postalCode: building.code_postal ?? "",
    constructionYear: building.annee_construction ?? "",
    buildingSurface: building.surface_m2 ?? dpe?.surface_utile ?? "",
  };

  // Vérifier s'il y a déjà un prospect lié
  const prospect = await db.get<{ id: number; stage: string }>(
    `SELECT id, stage FROM prospects WHERE tertiary_building_id = ? LIMIT 1`,
    [buildingId],
  );

  return NextResponse.json({
    building,
    dpe,
    occupants: occupantsWithDirigeants,
    occupantsCount: occupants.length,
    prospect,
    ceeProject,
  });
}
