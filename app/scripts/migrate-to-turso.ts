/* eslint-disable no-console */
/**
 * Génère un dump SQL prêt à être poussé sur Turso (cloud SQLite).
 *
 * Usage :
 *   1. Créer un compte sur turso.tech (gratuit, free tier 500 DB / 1GB)
 *   2. Installer la CLI : `curl -sSfL https://get.tur.so/install.sh | bash`
 *      ou via scoop / brew selon l'OS.
 *   3. Login : `turso auth login`
 *   4. Créer la DB : `turso db create citeen-prod`
 *   5. Récupérer l'URL : `turso db show citeen-prod --url`
 *   6. Récupérer un token : `turso db tokens create citeen-prod`
 *   7. Lancer ce script : `npx tsx scripts/migrate-to-turso.ts`
 *      → produit data/citeen.dump.sql
 *   8. Pousser : `turso db shell citeen-prod < ./data/citeen.dump.sql`
 *   9. Vérifier : `turso db shell citeen-prod "SELECT COUNT(*) FROM copros;"`
 *   10. Ajouter les variables d'env sur Vercel :
 *       TURSO_DATABASE_URL = libsql://citeen-prod-<org>.turso.io
 *       TURSO_AUTH_TOKEN   = <le token de l'étape 6>
 *
 * Note : ce script utilise la commande `sqlite3` CLI (présente sur la plupart
 * des systèmes). Il fait un .dump dans data/citeen.dump.sql.
 */
import { execSync } from "node:child_process";
import { existsSync, statSync } from "node:fs";
import { resolve } from "node:path";

const dbPath = resolve(process.cwd(), "data", "citeen.db");
const outPath = resolve(process.cwd(), "data", "citeen.dump.sql");

if (!existsSync(dbPath)) {
  console.error(`[ERROR] DB locale introuvable: ${dbPath}`);
  process.exit(1);
}
const size = statSync(dbPath).size;
console.log(`[INFO] DB locale: ${dbPath} (${(size / 1024 / 1024).toFixed(1)} MB)`);

console.log(`[INFO] Génération du dump SQL → ${outPath}`);
console.log(`[INFO] Cela peut prendre 30s-2min selon la taille de la base.`);

try {
  // Force checkpoint du WAL pour avoir une DB cohérente
  execSync(`sqlite3 "${dbPath}" "PRAGMA wal_checkpoint(FULL);"`, {
    stdio: "inherit",
  });
  // Dump
  execSync(`sqlite3 "${dbPath}" ".dump" > "${outPath}"`, {
    shell: process.platform === "win32" ? "cmd.exe" : undefined,
    stdio: "inherit",
  });
} catch (err) {
  console.error(`[ERROR] Le dump a échoué.`);
  console.error(
    `Vérifiez que la commande "sqlite3" est installée et dans le PATH.`,
  );
  console.error(`Erreur: ${(err as Error).message}`);
  process.exit(1);
}

const outSize = statSync(outPath).size;
console.log(
  `\n✓ Dump généré: ${outPath} (${(outSize / 1024 / 1024).toFixed(1)} MB)`,
);
console.log(`\n=== ÉTAPES SUIVANTES ===\n`);
console.log(`1. turso db create citeen-prod`);
console.log(`2. turso db shell citeen-prod < "${outPath}"`);
console.log(`3. turso db tokens create citeen-prod  # copier le token`);
console.log(`4. turso db show citeen-prod --url     # copier l'URL libsql://`);
console.log(`5. Ajouter sur Vercel :`);
console.log(`     TURSO_DATABASE_URL = libsql://citeen-prod-<org>.turso.io`);
console.log(`     TURSO_AUTH_TOKEN   = <token de l'étape 3>`);
console.log(
  `6. Le refactor async du code reste à faire (tour suivant) : demander à Claude de "passer le code à libsql async".\n`,
);
