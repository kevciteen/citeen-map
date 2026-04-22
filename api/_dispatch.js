import { buildApp } from "../backend/app.js";

const app = buildApp();
const ready = app.ready();

function extractQueryString(url) {
  const idx = String(url || "").indexOf("?");
  return idx >= 0 ? String(url).slice(idx) : "";
}

async function dispatchToFastify(req, res, targetPath) {
  await ready;
  req.url = `${targetPath}${extractQueryString(req.url)}`;
  app.server.emit("request", req, res);
}

export function createHandler(targetPath) {
  return async function handler(req, res) {
    await dispatchToFastify(req, res, targetPath);
  };
}

export function createRegexHandler(pattern, toTargetPath) {
  return async function handler(req, res) {
    const match = String(req.url || "").match(pattern);
    if (!match) {
      res.statusCode = 404;
      res.end("not_found");
      return;
    }

    await dispatchToFastify(req, res, toTargetPath(match));
  };
}
