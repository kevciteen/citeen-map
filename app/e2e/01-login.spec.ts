import { test, expect } from "@playwright/test";
import { loginAdmin } from "./helpers";

test.describe("Auth", () => {
  test("redirige vers /login quand non connecté", async ({ page }) => {
    await page.goto("/");
    await expect(page).toHaveURL(/\/login/);
  });

  test("/today nécessite une session, redirige vers login", async ({ page }) => {
    await page.context().clearCookies();
    await page.goto("/today");
    await expect(page).toHaveURL(/\/login/);
  });

  test("login admin valide → redirection app", async ({ page }) => {
    await loginAdmin(page);
    // On accepte today, dashboard ou settings/password (first login)
    await expect(page).toHaveURL(/\/(today|dashboard|settings\/password)/);
  });

  test("login avec mauvais mdp → message d'erreur", async ({ page }) => {
    await page.goto("/login");
    await page.getByPlaceholder("vous@citeen.fr").fill("kf@citeen.fr");
    await page.getByPlaceholder("••••••••").fill("definitely-wrong");
    await page.getByRole("button", { name: "Se connecter" }).click();
    await expect(page.getByText(/identifiants invalides/i)).toBeVisible({
      timeout: 5000,
    });
  });
});
