import { sqliteTable, text, integer, real, index } from "drizzle-orm/sqlite-core";
import { sql } from "drizzle-orm";

// ============================================================
// REFERENCE DATA — copropriétés (registre national, data.gouv.fr)
// ============================================================
export const copros = sqliteTable(
  "copros",
  {
    id: integer("id").primaryKey({ autoIncrement: true }),
    numeroImmatriculation: text("numero_immatriculation").notNull().unique(),
    nomCopro: text("nom_copro"),
    adresse: text("adresse"),
    codePostal: text("code_postal"),
    commune: text("commune"),
    departement: text("departement"),
    syndic: text("syndic"),
    nbLots: integer("nb_lots"),
    nbLotsHabitation: integer("nb_lots_habitation"),
    periodeConstruction: text("periode_construction"),
    lat: real("lat"),
    lon: real("lon"),
    codeInseeCommune: text("code_insee_commune"),
    section: text("section"),
    numeroParcelle: text("numero_parcelle"),
    referenceCadastrale: text("reference_cadastrale"),
    importedAt: integer("imported_at", { mode: "timestamp" }).default(
      sql`(unixepoch())`,
    ),
  },
  (t) => ({
    coproBbox: index("idx_copros_bbox").on(t.lat, t.lon),
    coproCp: index("idx_copros_cp").on(t.codePostal),
    coproCommune: index("idx_copros_commune").on(t.commune),
    coproDept: index("idx_copros_dept").on(t.departement),
    coproSyndic: index("idx_copros_syndic").on(t.syndic),
  }),
);

// ============================================================
// CACHE — DPE collectif ADEME (synthèse par copro)
// On stocke les estimations calculées pour éviter de re-frapper ADEME
// ============================================================
export const dpeEstimates = sqliteTable("dpe_estimates", {
  coproId: integer("copro_id")
    .primaryKey()
    .references(() => copros.id, { onDelete: "cascade" }),
  classeReelle: text("classe_reelle"),     // si DPE immeuble existant
  classeSimulee: text("classe_simulee"),   // calculée à partir des DPE individuels
  classeFinale: text("classe_finale"),     // réel sinon simulé
  geScoreMoyen: real("ge_score_moyen"),    // GES kg CO2/m²/an
  consoMoyenne: real("conso_moyenne"),     // kWhep/m²/an
  nbDpeIndividuels: integer("nb_dpe_individuels"),
  rayonRecherche: integer("rayon_recherche"),
  computedAt: integer("computed_at", { mode: "timestamp" }).default(
    sql`(unixepoch())`,
  ),
});

// ============================================================
// CRM — Pipeline commercial
// ============================================================
export type PipelineStage =
  | "lead"
  | "to_contact"
  | "contacted"
  | "meeting"
  | "proposal"
  | "won"
  | "lost";

export const prospects = sqliteTable(
  "prospects",
  {
    id: integer("id").primaryKey({ autoIncrement: true }),
    coproId: integer("copro_id").references(() => copros.id, {
      onDelete: "cascade",
    }),
    // Le prospect peut aussi être une adresse libre (recherche Google sans copro)
    customLabel: text("custom_label"),
    customAddress: text("custom_address"),
    customLat: real("custom_lat"),
    customLon: real("custom_lon"),
    stage: text("stage").$type<PipelineStage>().notNull().default("lead"),
    priority: integer("priority").notNull().default(2), // 1=low 2=medium 3=high
    estimatedValue: real("estimated_value"),
    expectedCloseDate: integer("expected_close_date", { mode: "timestamp" }),
    nextActionAt: integer("next_action_at", { mode: "timestamp" }),
    nextActionLabel: text("next_action_label"),
    assignedTo: text("assigned_to"),
    tags: text("tags"), // JSON array as string
    createdAt: integer("created_at", { mode: "timestamp" })
      .notNull()
      .default(sql`(unixepoch())`),
    updatedAt: integer("updated_at", { mode: "timestamp" })
      .notNull()
      .default(sql`(unixepoch())`),
  },
  (t) => ({
    prospectStage: index("idx_prospects_stage").on(t.stage),
    prospectCopro: index("idx_prospects_copro").on(t.coproId),
    prospectNextAction: index("idx_prospects_next_action").on(t.nextActionAt),
  }),
);

export const contacts = sqliteTable(
  "contacts",
  {
    id: integer("id").primaryKey({ autoIncrement: true }),
    prospectId: integer("prospect_id")
      .notNull()
      .references(() => prospects.id, { onDelete: "cascade" }),
    role: text("role"), // syndic, président conseil syndical, gardien, copropriétaire
    fullName: text("full_name"),
    email: text("email"),
    phone: text("phone"),
    company: text("company"),
    notes: text("notes"),
    createdAt: integer("created_at", { mode: "timestamp" })
      .notNull()
      .default(sql`(unixepoch())`),
  },
  (t) => ({
    contactProspect: index("idx_contacts_prospect").on(t.prospectId),
  }),
);

export const notes = sqliteTable(
  "notes",
  {
    id: integer("id").primaryKey({ autoIncrement: true }),
    prospectId: integer("prospect_id")
      .notNull()
      .references(() => prospects.id, { onDelete: "cascade" }),
    body: text("body").notNull(),
    author: text("author"),
    createdAt: integer("created_at", { mode: "timestamp" })
      .notNull()
      .default(sql`(unixepoch())`),
  },
  (t) => ({
    noteProspect: index("idx_notes_prospect").on(t.prospectId),
  }),
);

export const tasks = sqliteTable(
  "tasks",
  {
    id: integer("id").primaryKey({ autoIncrement: true }),
    prospectId: integer("prospect_id").references(() => prospects.id, {
      onDelete: "cascade",
    }),
    title: text("title").notNull(),
    kind: text("kind"), // call, email, visit, meeting, other
    dueAt: integer("due_at", { mode: "timestamp" }),
    doneAt: integer("done_at", { mode: "timestamp" }),
    createdAt: integer("created_at", { mode: "timestamp" })
      .notNull()
      .default(sql`(unixepoch())`),
  },
  (t) => ({
    taskProspect: index("idx_tasks_prospect").on(t.prospectId),
    taskDue: index("idx_tasks_due").on(t.dueAt),
  }),
);

export const activities = sqliteTable(
  "activities",
  {
    id: integer("id").primaryKey({ autoIncrement: true }),
    prospectId: integer("prospect_id")
      .notNull()
      .references(() => prospects.id, { onDelete: "cascade" }),
    type: text("type").notNull(), // stage_change, note_added, task_done, contact_added, email_sent, call_made
    payload: text("payload"), // JSON
    author: text("author"),
    createdAt: integer("created_at", { mode: "timestamp" })
      .notNull()
      .default(sql`(unixepoch())`),
  },
  (t) => ({
    activityProspect: index("idx_activities_prospect").on(t.prospectId),
  }),
);

export const documents = sqliteTable(
  "documents",
  {
    id: integer("id").primaryKey({ autoIncrement: true }),
    prospectId: integer("prospect_id")
      .notNull()
      .references(() => prospects.id, { onDelete: "cascade" }),
    name: text("name").notNull(),
    url: text("url").notNull(),
    kind: text("kind"), // devis, audit, photo, plan, courrier, autre
    description: text("description"),
    createdAt: integer("created_at", { mode: "timestamp" })
      .notNull()
      .default(sql`(unixepoch())`),
    createdBy: text("created_by"),
  },
  (t) => ({
    documentProspect: index("idx_documents_prospect").on(t.prospectId),
  }),
);

export type Copro = typeof copros.$inferSelect;
export type Prospect = typeof prospects.$inferSelect;
export type Contact = typeof contacts.$inferSelect;
export type Note = typeof notes.$inferSelect;
export type Task = typeof tasks.$inferSelect;
export type Activity = typeof activities.$inferSelect;
export type DpeEstimate = typeof dpeEstimates.$inferSelect;
export type Document = typeof documents.$inferSelect;
