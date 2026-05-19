/**
 * GET /api/appartements/by-dpe/[numero]
 *
 * Identique à /api/maisons/by-dpe/[numero] (l'ADEME ne distingue pas le
 * type à la consommation par numéro), mais l'endpoint dédié permet à la
 * page /appartements/[numero] de garder ses URLs propres.
 */
export { GET } from "../../../maisons/by-dpe/[numero]/route";
export const runtime = "nodejs";
export const maxDuration = 30;
