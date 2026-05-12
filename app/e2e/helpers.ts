import { type Page, expect } from "@playwright/test";

const EMAIL = process.env.E2E_ADMIN_EMAIL ?? "kf@citeen.fr";
const PASSWORD = process.env.E2E_ADMIN_PASSWORD ?? "";

/**
 * Connecte l'utilisateur admin et attend qu'on soit redirigé sur /today
 * (ou /settings/password si first login forcé — dans ce cas la suite skip).
 */
export async function loginAdmin(page: Page): Promise<void> {
  if (!PASSWORD) {
    throw new Error(
      "E2E_ADMIN_PASSWORD env var manquante — fournis-la pour run les tests",
    );
  }
  await page.goto("/login");
  await page.getByPlaceholder("vous@citeen.fr").fill(EMAIL);
  await page.getByPlaceholder("••••••••").fill(PASSWORD);
  await page.getByRole("button", { name: "Se connecter" }).click();
  // Attend la redirection (today ou settings/password)
  await page.waitForURL((url) =>
    url.pathname === "/today" ||
    url.pathname === "/settings/password" ||
    url.pathname === "/dashboard",
  );
}

export async function expectAuthenticated(page: Page): Promise<void> {
  // L'avatar utilisateur en haut à droite doit être visible
  await expect(page.locator('button[title*="Mon compte"], button[title*="@"]')).toBeVisible({
    timeout: 8000,
  });
}
