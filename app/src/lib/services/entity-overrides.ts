/**
 * Service de gestion des overrides / notes / tags par entité.
 *
 * Tout est append-only ou upsert — pas de soft delete ni d'historique
 * pour rester simple. À évoluer si on a besoin de "qui a modifié quoi quand".
 */
import { db } from "@/lib/db/client";
import {
  ensureEntityOverrides,
  type OverrideEntityType,
} from "@/lib/db/ensure-entity-overrides";

export type EntityKey = {
  entityType: OverrideEntityType;
  entityRef: string;
};

export type Override = {
  field_name: string;
  value: string | null;
  author_id: number | null;
  updated_at: number;
};

export type EntityNote = {
  id: number;
  body: string;
  author_id: number | null;
  created_at: number;
};

export type EntityTag = {
  id: number;
  tag: string;
  author_id: number | null;
  created_at: number;
};

export type EntityOverlay = {
  overrides: Override[];
  notes: EntityNote[];
  tags: EntityTag[];
};

/** Fetch tout l'overlay (overrides + notes + tags) pour une entité. */
export async function getEntityOverlay(key: EntityKey): Promise<EntityOverlay> {
  await ensureEntityOverrides();
  const [overrides, notes, tags] = await Promise.all([
    db.all<Override>(
      `SELECT field_name, value, author_id, updated_at
       FROM entity_overrides
       WHERE entity_type = ? AND entity_ref = ?
       ORDER BY field_name`,
      [key.entityType, key.entityRef],
    ),
    db.all<EntityNote>(
      `SELECT id, body, author_id, created_at
       FROM entity_notes
       WHERE entity_type = ? AND entity_ref = ?
       ORDER BY created_at DESC`,
      [key.entityType, key.entityRef],
    ),
    db.all<EntityTag>(
      `SELECT id, tag, author_id, created_at
       FROM entity_tags
       WHERE entity_type = ? AND entity_ref = ?
       ORDER BY tag`,
      [key.entityType, key.entityRef],
    ),
  ]);
  return { overrides, notes, tags };
}

/** Upsert un override pour un champ. Si `value` est null/empty, supprime. */
export async function setOverride(
  key: EntityKey,
  fieldName: string,
  value: string | null,
  authorId: number | null,
): Promise<void> {
  await ensureEntityOverrides();
  if (value === null || (typeof value === "string" && value.trim() === "")) {
    await db.run(
      `DELETE FROM entity_overrides
       WHERE entity_type = ? AND entity_ref = ? AND field_name = ?`,
      [key.entityType, key.entityRef, fieldName],
    );
    return;
  }
  await db.run(
    `INSERT INTO entity_overrides (entity_type, entity_ref, field_name, value, author_id)
     VALUES (?, ?, ?, ?, ?)
     ON CONFLICT(entity_type, entity_ref, field_name) DO UPDATE SET
       value = excluded.value,
       author_id = excluded.author_id,
       updated_at = unixepoch()`,
    [key.entityType, key.entityRef, fieldName, value.trim(), authorId],
  );
}

export async function addNote(
  key: EntityKey,
  body: string,
  authorId: number | null,
): Promise<EntityNote | null> {
  const clean = body.trim();
  if (!clean) return null;
  await ensureEntityOverrides();
  const res = await db.run(
    `INSERT INTO entity_notes (entity_type, entity_ref, body, author_id)
     VALUES (?, ?, ?, ?)`,
    [key.entityType, key.entityRef, clean, authorId],
  );
  const row = await db.get<EntityNote>(
    `SELECT id, body, author_id, created_at FROM entity_notes WHERE id = ?`,
    [res.lastInsertRowid],
  );
  return row ?? null;
}

export async function deleteNote(noteId: number): Promise<void> {
  await ensureEntityOverrides();
  await db.run(`DELETE FROM entity_notes WHERE id = ?`, [noteId]);
}

export async function addTag(
  key: EntityKey,
  tag: string,
  authorId: number | null,
): Promise<EntityTag | null> {
  const clean = tag.trim().toLowerCase().replace(/\s+/g, "-").slice(0, 40);
  if (!clean) return null;
  await ensureEntityOverrides();
  await db.run(
    `INSERT INTO entity_tags (entity_type, entity_ref, tag, author_id)
     VALUES (?, ?, ?, ?)
     ON CONFLICT(entity_type, entity_ref, tag) DO NOTHING`,
    [key.entityType, key.entityRef, clean, authorId],
  );
  return await db.get<EntityTag>(
    `SELECT id, tag, author_id, created_at FROM entity_tags
     WHERE entity_type = ? AND entity_ref = ? AND tag = ?`,
    [key.entityType, key.entityRef, clean],
  ) ?? null;
}

export async function removeTag(key: EntityKey, tag: string): Promise<void> {
  const clean = tag.trim().toLowerCase().replace(/\s+/g, "-");
  if (!clean) return;
  await ensureEntityOverrides();
  await db.run(
    `DELETE FROM entity_tags
     WHERE entity_type = ? AND entity_ref = ? AND tag = ?`,
    [key.entityType, key.entityRef, clean],
  );
}

/**
 * Helper : applique les overrides à un objet source de données.
 * Les fields override "écrasent" les champs du même nom.
 * Renvoie aussi la liste des champs surchargés (pour afficher un badge "édité").
 */
export function applyOverridesToData<T extends Record<string, unknown>>(
  source: T,
  overrides: Override[],
): { data: T; overriddenFields: Set<string> } {
  const data = { ...source };
  const overriddenFields = new Set<string>();
  for (const o of overrides) {
    if (o.value !== null) {
      (data as Record<string, unknown>)[o.field_name] = o.value;
      overriddenFields.add(o.field_name);
    }
  }
  return { data, overriddenFields };
}
