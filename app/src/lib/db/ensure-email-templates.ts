import { db } from "./client";

/**
 * Table email_templates : modèles de mail réutilisables pour la prospection.
 *
 *  - subject : objet du mail (peut contenir des variables {{xxx}})
 *  - body    : corps texte (variables {{xxx}} aussi)
 *  - scope   : 'prospect' | 'syndic' | 'copro' | 'generic'
 *              indique sur quelle entité ce template a du sens
 *  - is_shared : 1 si visible par toute l'équipe, 0 si privé à l'auteur
 *
 * Pas de WYSIWYG : texte brut pour pouvoir l'envoyer en mailto: sans
 * conversion HTML→texte. Si plus tard on intègre Resend on aura la possibilité
 * d'ajouter un body_html.
 */
let ensured = false;

export async function ensureEmailTemplates(): Promise<void> {
  if (ensured) return;
  await db.exec(`
    CREATE TABLE IF NOT EXISTS email_templates (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      subject TEXT NOT NULL,
      body TEXT NOT NULL,
      scope TEXT NOT NULL DEFAULT 'generic',
      is_shared INTEGER NOT NULL DEFAULT 1,
      created_by INTEGER,
      created_at INTEGER NOT NULL DEFAULT (unixepoch()),
      updated_at INTEGER NOT NULL DEFAULT (unixepoch())
    );
    CREATE INDEX IF NOT EXISTS idx_email_templates_scope ON email_templates(scope);
  `);
  ensured = true;
}
