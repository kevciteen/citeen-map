import { test, expect } from "@playwright/test";
import { loginAdmin } from "./helpers";

test.describe("Prospect", () => {
  test("création d'un prospect manuel via /prospects/new", async ({ page }) => {
    await loginAdmin(page);

    // Navigation vers le formulaire de création
    await page.goto("/prospects/new");

    // Champ libellé custom (label dépend du form actuel, on tâtonne)
    const label = `Test E2E ${Date.now()}`;
    const labelInput = page.getByPlaceholder(/libellé|label|nom/i).first();
    await labelInput.fill(label);

    // Soumettre — bouton de création (on cherche le bouton final du form)
    const submit = page.getByRole("button", { name: /créer|ajouter|enregistrer/i }).first();
    await submit.click();

    // Doit rediriger vers la fiche
    await page.waitForURL(/\/prospects\/\d+/, { timeout: 8000 });
    await expect(page.getByText(label)).toBeVisible();
  });

  test("le pipeline tableau affiche les prospects existants", async ({ page }) => {
    await loginAdmin(page);
    await page.goto("/prospects");
    // Le toggle Tableau doit être par défaut
    await expect(page.getByRole("button", { name: /tableau/i })).toBeVisible();
    // Le total pipeline ouvert doit s'afficher
    await expect(page.getByText(/pipeline ouvert/i)).toBeVisible({ timeout: 8000 });
  });
});
