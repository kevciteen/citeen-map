/**
 * POST /api/email-templates/render
 *
 * Prend un templateId + un contexte (coproId, prospectId, contactId optionnels)
 * et renvoie { subject, body, to } prêts à passer à mailto:.
 *
 * On garde la logique de rendu côté server pour avoir accès à la DB
 * et appliquer les overrides éventuels (entity_overrides) sur la copro.
 */
import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";
import { db } from "@/lib/db/client";
import { ensureAuth } from "@/lib/auth/guards";
import { ensureEmailTemplates } from "@/lib/db/ensure-email-templates";
import {
  buildVarTable, renderTemplate, type RenderContext,
} from "@/lib/services/email-template-render";

export const runtime = "nodejs";

const schema = z.object({
  templateId: z.number().int().positive(),
  coproId: z.number().int().positive().optional(),
  prospectId: z.number().int().positive().optional(),
  contactId: z.number().int().positive().optional(),
  toOverride: z.string().email().optional(),
});

export async function POST(req: NextRequest) {
  const guard = await ensureAuth();
  if (guard instanceof NextResponse) return guard;
  await ensureEmailTemplates();

  const json = await req.json().catch(() => null);
  const parsed = schema.safeParse(json);
  if (!parsed.success) {
    return NextResponse.json({ error: parsed.error.flatten() }, { status: 400 });
  }
  const d = parsed.data;

  const tpl = await db.get<{
    subject: string; body: string; is_shared: number; created_by: number | null;
  }>(`SELECT subject, body, is_shared, created_by FROM email_templates WHERE id = ?`, [d.templateId]);
  if (!tpl) return NextResponse.json({ error: "template_not_found" }, { status: 404 });
  if (!tpl.is_shared && tpl.created_by !== guard.id && guard.role !== "admin") {
    return NextResponse.json({ error: "forbidden" }, { status: 403 });
  }

  const ctx: RenderContext = {
    expediteur: {
      prenom: guard.name?.split(" ")[0] ?? null,
      nom: guard.name?.split(" ").slice(1).join(" ") ?? null,
      email: guard.email,
    },
    baseUrl: req.nextUrl.origin,
  };

  // Charger copro si demandé
  let coproId = d.coproId;
  if (!coproId && d.prospectId) {
    const p = await db.get<{ copro_id: number | null }>(
      `SELECT copro_id FROM prospects WHERE id = ?`, [d.prospectId],
    );
    coproId = p?.copro_id ?? undefined;
    ctx.prospect = { id: d.prospectId };
  }
  if (coproId) {
    const c = await db.get<{
      nom_copro: string | null; adresse: string | null;
      commune: string | null; code_postal: string | null;
      syndic: string | null; nb_lots: number | null;
      numero_immatriculation: string | null;
      classe_finale: string | null;
    }>(
      `SELECT c.nom_copro, c.adresse, c.commune, c.code_postal, c.syndic, c.nb_lots,
              c.numero_immatriculation, e.classe_finale
       FROM copros c
       LEFT JOIN dpe_estimates e ON e.copro_id = c.id
       WHERE c.id = ?`,
      [coproId],
    );
    if (c) {
      ctx.copro = {
        id: coproId,
        nom_copro: c.nom_copro,
        adresse: c.adresse,
        commune: c.commune,
        code_postal: c.code_postal,
        syndic: c.syndic,
        nb_lots: c.nb_lots,
        numero_immatriculation: c.numero_immatriculation,
        classe_dpe: c.classe_finale,
      };
    }
  }

  // Destinataire : contactId ou prospects.contacts ou rien
  let to: string | null = d.toOverride ?? null;
  if (d.contactId) {
    const contact = await db.get<{
      full_name: string | null; email: string | null;
    }>(`SELECT full_name, email FROM contacts WHERE id = ?`, [d.contactId]);
    if (contact) {
      const [prenom, ...rest] = (contact.full_name ?? "").trim().split(/\s+/);
      ctx.destinataire = {
        prenom: prenom || null,
        nom: rest.join(" ") || null,
        email: contact.email,
      };
      if (!to) to = contact.email ?? null;
    }
  }

  const vars = buildVarTable(ctx);
  const subject = renderTemplate(tpl.subject, vars);
  const body = renderTemplate(tpl.body, vars);

  return NextResponse.json({ subject, body, to });
}
