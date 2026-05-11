# Déploiement Citeen CRM sur Vercel

Ce document décrit le déploiement de l'app Next.js (dossier `app/`) sur Vercel,
avec base de données **Turso** (SQLite cloud, free tier 500 DB / 1 GB).

## Architecture cible

| Composant | Outil |
|---|---|
| Code (frontend + API) | **Vercel** (Next.js) |
| Base SQLite | **Turso** (libsql cloud) |
| Source de vérité copros | Registre national copropriétés (data.gouv.fr) |
| Cadastre | API IGN apicarto |
| BAN | api-adresse.data.gouv.fr |
| DPE | data.ademe.fr |

## Pré-requis

1. **Compte Vercel** lié au repo GitHub `kevciteen/citeen-map`
2. **Compte Turso** : <https://turso.tech> (gratuit)
3. **Turso CLI** installée :
   - Windows : `scoop install turso` (ou `winget install Turso.TursoCLI`)
   - macOS : `brew install tursodatabase/tap/turso`
   - Linux : `curl -sSfL https://get.tur.so/install.sh | bash`

## Étape 1 — Migration de la base vers Turso

```bash
cd app
turso auth login
turso db create citeen-prod
npx tsx scripts/migrate-to-turso.ts   # crée data/citeen.dump.sql
turso db shell citeen-prod < ./data/citeen.dump.sql
turso db shell citeen-prod "SELECT COUNT(*) FROM copros;"   # → 134286
```

Récupérez l'URL et un token :
```bash
turso db show citeen-prod --url    # libsql://citeen-prod-<org>.turso.io
turso db tokens create citeen-prod # token long
```

## Étape 2 — Refactor du code pour libsql

Le code utilise actuellement `better-sqlite3` (synchrone, fichier local).
Pour Vercel + Turso, il faut passer à `@libsql/client` (async, libsql remote).

**Ce refactor n'est pas encore fait** — demander à Claude :
> « Passe tout le code à @libsql/client async pour pouvoir déployer sur Vercel + Turso »

Le refactor consiste à :
- Installer `@libsql/client`
- Remplacer `sqlite.prepare(sql).get/all/run(args)` par `await db.execute({ sql, args })`
- Adapter `sqlite.transaction()` → `db.batch([...])`
- Adapter les Server Components à `async function ServerComp(){}`

## Étape 3 — Configuration Vercel

1. Sur <https://vercel.com> : **Add New… → Project → Import** depuis GitHub `citeen-map`
2. Framework Preset : **Next.js**
3. Root Directory : **`app`** (important — l'app est dans le sous-dossier)
4. **Environment Variables** :
   ```
   TURSO_DATABASE_URL = libsql://citeen-prod-<org>.turso.io
   TURSO_AUTH_TOKEN   = <token Turso>
   NEXT_PUBLIC_GOOGLE_MAPS_API_KEY = <optionnel, pour Street View>
   ```
5. **Deploy** — la première build prend ~5 min (Next.js + dépendances)

## Étape 4 — Vérification

URL fournie par Vercel : `https://citeen-map.vercel.app` (ou personnalisé)

Tester :
- `/dashboard` : statistiques générales
- `/map` : carte avec 134k copros visibles
- `/copros` : tableau avec filtres
- `/syndics` : 1237 syndics
- `/copros/107638` : fiche GREENSIDE (test cas réel)

## Limites du free tier

| Service | Limite gratuite | Suffisant pour MVP ? |
|---|---|---|
| Vercel | 100 GB bandwidth / mois | ✅ |
| Vercel functions | 100 GB-heures / mois | ✅ |
| Turso | 500 DB, 1 GB total, 1B reads/mois | ✅ |
| OpenFreeMap (carte) | gratuit illimité | ✅ |
| API BAN / IGN / ADEME | gratuit | ✅ (sans abus) |

## Custom domain

Vercel → Project Settings → Domains → Add `citeen.fr` (ou autre)
Configurer les DNS chez le registrar selon les instructions Vercel.

## Sauvegarde

- Turso a un PITR (point-in-time recovery) sur 24h en free tier
- Pour une sauvegarde complète : `turso db shell citeen-prod ".dump" > backup.sql`
