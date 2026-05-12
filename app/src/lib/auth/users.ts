import { db } from "@/lib/db/client";
import { ensureUsersTable } from "@/lib/db/ensure-users";
import { hashPassword, verifyPassword } from "./password";

export type UserRow = {
  id: number;
  email: string;
  password_hash: string;
  role: "admin" | "member";
  name: string | null;
  active: number;
  must_change_password: number;
  created_at: number;
  created_by: number | null;
  last_login_at: number | null;
};

export async function findUserByEmail(email: string): Promise<UserRow | null> {
  await ensureUsersTable();
  const row = await db.get<UserRow>(
    "SELECT * FROM users WHERE LOWER(email) = LOWER(?) LIMIT 1",
    [email.trim()],
  );
  return row ?? null;
}

export async function findUserById(id: number): Promise<UserRow | null> {
  await ensureUsersTable();
  const row = await db.get<UserRow>("SELECT * FROM users WHERE id = ?", [id]);
  return row ?? null;
}

export async function listUsers(): Promise<UserRow[]> {
  await ensureUsersTable();
  const rows = await db.all<UserRow>(
    "SELECT * FROM users ORDER BY role DESC, created_at DESC",
  );
  return rows;
}

export async function createUser(input: {
  email: string;
  password: string;
  role?: "admin" | "member";
  name?: string;
  createdBy?: number;
  mustChangePassword?: boolean;
}): Promise<number> {
  await ensureUsersTable();
  const hash = await hashPassword(input.password);
  const res = await db.run(
    `INSERT INTO users (email, password_hash, role, name, must_change_password, created_by)
     VALUES (?, ?, ?, ?, ?, ?)`,
    [
      input.email.trim().toLowerCase(),
      hash,
      input.role ?? "member",
      input.name ?? null,
      input.mustChangePassword ? 1 : 0,
      input.createdBy ?? null,
    ],
  );
  return Number(res.lastInsertRowid);
}

export async function updateUser(
  id: number,
  patch: Partial<{
    name: string | null;
    role: "admin" | "member";
    active: boolean;
  }>,
): Promise<void> {
  await ensureUsersTable();
  const fields: string[] = [];
  const args: (string | number | null)[] = [];
  if ("name" in patch) {
    fields.push("name = ?");
    args.push(patch.name ?? null);
  }
  if (patch.role) {
    fields.push("role = ?");
    args.push(patch.role);
  }
  if ("active" in patch) {
    fields.push("active = ?");
    args.push(patch.active ? 1 : 0);
  }
  if (fields.length === 0) return;
  args.push(id);
  await db.run(`UPDATE users SET ${fields.join(", ")} WHERE id = ?`, args);
}

export async function setUserPassword(
  id: number,
  plain: string,
  clearMustChange = true,
): Promise<void> {
  await ensureUsersTable();
  const hash = await hashPassword(plain);
  await db.run(
    `UPDATE users SET password_hash = ?, must_change_password = ? WHERE id = ?`,
    [hash, clearMustChange ? 0 : 1, id],
  );
}

export async function authenticateUser(
  email: string,
  password: string,
): Promise<UserRow | null> {
  const user = await findUserByEmail(email);
  if (!user || !user.active) return null;
  const ok = await verifyPassword(password, user.password_hash);
  if (!ok) return null;
  await db.run("UPDATE users SET last_login_at = unixepoch() WHERE id = ?", [
    user.id,
  ]);
  return user;
}

/**
 * Seed initial admin si la base est vide. Idempotent.
 * Lit ADMIN_EMAIL / ADMIN_INITIAL_PASSWORD en env vars.
 */
export async function ensureAdminUser(): Promise<void> {
  await ensureUsersTable();
  const adminEmail = process.env.ADMIN_EMAIL?.trim();
  const adminPassword = process.env.ADMIN_INITIAL_PASSWORD;
  if (!adminEmail || !adminPassword) return; // pas configuré, no-op
  const existing = await findUserByEmail(adminEmail);
  if (existing) return;
  await createUser({
    email: adminEmail,
    password: adminPassword,
    role: "admin",
    name: "Admin",
    mustChangePassword: true,
  });
}
