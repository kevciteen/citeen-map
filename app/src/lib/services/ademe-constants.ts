/**
 * Identifiant canonique du dataset DPE Logements existants (depuis juillet 2021)
 * sur data.ademe.fr.
 *
 * On utilise l'ID interne (meg-...) plutôt que l'alias `dpe03existant` car ce
 * dernier est désormais filtré (HTTP 403) sur l'endpoint /lines. L'API meta
 * `/datasets/dpe03existant` reste 200 mais les requêtes data passent par l'ID.
 *
 * Si data.gouv migre encore le dataset, recheck via :
 *   curl -I https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant
 *   → header `x-resource` contient le nouvel ID
 */
export const ADEME_DPE_DATASET_ID = "meg-83tjwtg8dyz4vv7h1dqe";

export const ADEME_DPE_LINES_URL = `https://data.ademe.fr/data-fair/api/v1/datasets/${ADEME_DPE_DATASET_ID}/lines`;
