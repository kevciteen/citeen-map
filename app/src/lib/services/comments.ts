import { db } from "@/lib/db/client";
import { ensureCommentsAndNotificationsTables } from "@/lib/db/ensure-comments";

/**
 * Format @mention : `@[Nom Complet](user:42)`
 * Permet d'afficher le nom et de garder l'ID de l'utilisateur cible
 * (pas de collision même si plusieurs users portent le même nom).
 */
const MENTION_REGEX = /@\[([^\]]+)\]\(user:(\d+)\)/g;

export function parseMentions(body: string): { userId: number; name: string }[] {
  const found = new Map<number, string>();
  let m: RegExpExecArray | null;
  const re = new RegExp(MENTION_REGEX.source, "g");
  while ((m = re.exec(body)) !== null) {
    const userId = Number(m[2]);
    if (Number.isFinite(userId) && !found.has(userId)) {
      found.set(userId, m[1]);
    }
  }
  return [...found.entries()].map(([userId, name]) => ({ userId, name }));
}

export type CommentRow = {
  id: number;
  entity_type: string;
  entity_id: string;
  author_id: number;
  author_email: string;
  author_name: string | null;
  body: string;
  parent_id: number | null;
  created_at: number;
  updated_at: number;
};

export async function listComments(
  entityType: string,
  entityId: string,
): Promise<CommentRow[]> {
  await ensureCommentsAndNotificationsTables();
  const rows = await db.all<CommentRow>(
    `SELECT c.id, c.entity_type, c.entity_id, c.author_id, c.body,
            c.parent_id, c.created_at, c.updated_at,
            u.email AS author_email, u.name AS author_name
     FROM comments c
     LEFT JOIN users u ON u.id = c.author_id
     WHERE c.entity_type = ? AND c.entity_id = ?
     ORDER BY c.created_at ASC`,
    [entityType, entityId],
  );
  return rows;
}

export async function createComment(input: {
  entityType: string;
  entityId: string;
  authorId: number;
  authorName: string | null;
  body: string;
  parentId?: number | null;
  link: string; // URL where to navigate when clicking the notif
}): Promise<{ id: number; mentionsCreated: number }> {
  await ensureCommentsAndNotificationsTables();
  const body = input.body.trim();
  if (!body) throw new Error("body empty");

  const res = await db.run(
    `INSERT INTO comments (entity_type, entity_id, author_id, body, parent_id)
     VALUES (?, ?, ?, ?, ?)`,
    [
      input.entityType,
      input.entityId,
      input.authorId,
      body,
      input.parentId ?? null,
    ],
  );
  const commentId = Number(res.lastInsertRowid);

  // Parse @mentions, créer une notification pour chaque user mentionné
  // (sauf si on s'auto-mentionne)
  const mentions = parseMentions(body);
  let created = 0;
  for (const { userId, name } of mentions) {
    if (userId === input.authorId) continue;
    await db.run(
      `INSERT INTO notifications
         (user_id, kind, title, body, link, from_user_id, from_user_name,
          entity_type, entity_id, comment_id)
       VALUES (?, 'mention', ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        userId,
        `${input.authorName ?? "Quelqu'un"} t'a mentionné`,
        body.slice(0, 200),
        input.link,
        input.authorId,
        input.authorName,
        input.entityType,
        input.entityId,
        commentId,
      ],
    );
    created++;
    void name; // unused but kept for future use
  }
  return { id: commentId, mentionsCreated: created };
}

export type NotificationRow = {
  id: number;
  user_id: number;
  kind: string;
  title: string;
  body: string | null;
  link: string | null;
  from_user_id: number | null;
  from_user_name: string | null;
  entity_type: string | null;
  entity_id: string | null;
  comment_id: number | null;
  read_at: number | null;
  created_at: number;
};

export async function listNotifications(
  userId: number,
  options: { limit?: number; onlyUnread?: boolean } = {},
): Promise<NotificationRow[]> {
  await ensureCommentsAndNotificationsTables();
  const limit = Math.min(options.limit ?? 50, 200);
  const where = options.onlyUnread
    ? "WHERE user_id = ? AND read_at IS NULL"
    : "WHERE user_id = ?";
  return await db.all<NotificationRow>(
    `SELECT * FROM notifications ${where}
     ORDER BY created_at DESC LIMIT ?`,
    [userId, limit],
  );
}

export async function countUnreadNotifications(userId: number): Promise<number> {
  await ensureCommentsAndNotificationsTables();
  const row = await db.get<{ c: number }>(
    "SELECT COUNT(*) AS c FROM notifications WHERE user_id = ? AND read_at IS NULL",
    [userId],
  );
  return row?.c ?? 0;
}

export async function markNotificationRead(
  userId: number,
  notifId: number,
): Promise<void> {
  await ensureCommentsAndNotificationsTables();
  await db.run(
    "UPDATE notifications SET read_at = unixepoch() WHERE id = ? AND user_id = ? AND read_at IS NULL",
    [notifId, userId],
  );
}

export async function markAllNotificationsRead(userId: number): Promise<void> {
  await ensureCommentsAndNotificationsTables();
  await db.run(
    "UPDATE notifications SET read_at = unixepoch() WHERE user_id = ? AND read_at IS NULL",
    [userId],
  );
}
