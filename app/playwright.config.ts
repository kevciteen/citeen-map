import { defineConfig, devices } from "@playwright/test";

/**
 * Tests E2E Playwright — couvrent les 3 parcours critiques :
 *   1. Login + redirection vers /today
 *   2. Création d'un prospect (manuel, custom_label)
 *   3. Commentaire avec @mention sur un prospect
 *
 * Variables env attendues côté CI :
 *   - E2E_BASE_URL (par défaut http://localhost:3100)
 *   - E2E_ADMIN_EMAIL (par défaut kf@citeen.fr)
 *   - E2E_ADMIN_PASSWORD (requis — pas de default pour pas commiter de pwd)
 *
 * Pour run en local :
 *   npm run dev   # dans un terminal
 *   npm run e2e   # dans un autre
 */
export default defineConfig({
  testDir: "./e2e",
  fullyParallel: false,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: 1,
  reporter: process.env.CI ? "github" : "list",
  use: {
    baseURL: process.env.E2E_BASE_URL ?? "http://localhost:3100",
    trace: "on-first-retry",
    screenshot: "only-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
});
