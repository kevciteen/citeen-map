import { createRegexHandler } from "../../_dispatch.js";

export default createRegexHandler(
  /^\/api\/copros\/([^/?]+)\/dpe(?:\?|$)/,
  (match) => `/copros/${match[1]}/dpe`
);
