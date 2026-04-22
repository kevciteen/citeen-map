import { createRegexHandler } from "../../../_dispatch.js";

export default createRegexHandler(
  /^\/api\/copros\/([^/?]+)\/dpe\/export_all\.csv(?:\?|$)/,
  (match) => `/copros/${match[1]}/dpe/export_all.csv`
);
