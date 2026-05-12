import { test, expect } from "@playwright/test";
import { loginAdmin } from "./helpers";

test.describe("Comments + @mention", () => {
  test("création d'un commentaire avec @mention sur un prospect existant", async ({
    page,
  }) => {
    await loginAdmin(page);

    // Crée d'abord un prospect pour avoir un id stable
    await page.goto("/prospects/new");
    const label = `E2E Mention ${Date.now()}`;
    await page.getByPlaceholder(/libellé|label|nom/i).first().fill(label);
    await page.getByRole("button", { name: /créer|ajouter|enregistrer/i }).first().click();
    await page.waitForURL(/\/prospects\/\d+/, { timeout: 8000 });

    // La section commentaires doit être visible
    await expect(page.getByText(/commentaires/i)).toBeVisible({ timeout: 8000 });

    // Tape un commentaire avec @ pour déclencher l'autocomplete
    const textarea = page.locator('textarea').last();
    await textarea.fill("Hello @");
    // L'autocomplete doit apparaître
    await expect(page.getByText(/mentionner/i)).toBeVisible({ timeout: 5000 });
    // On valide la première suggestion avec Enter
    await textarea.press("Enter");
    // Le texte doit maintenant contenir @[...](user:...)
    await expect(textarea).toHaveValue(/@\[.+\]\(user:\d+\)/);

    // Ajout d'un mot et envoi
    await textarea.fill(`${await textarea.inputValue()} message E2E`);
    await page.getByRole("button", { name: /envoyer/i }).click();

    // Le commentaire doit apparaître dans le fil
    await expect(page.getByText("message E2E")).toBeVisible({ timeout: 5000 });
  });
});
