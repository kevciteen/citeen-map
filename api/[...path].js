import { buildApp } from "../backend/app.js";

const app = buildApp();
const ready = app.ready();

export default async function handler(req, res) {
  await ready;

  const originalUrl = req.url || "/";
  if (originalUrl === "/api") {
    req.url = "/";
  } else if (originalUrl.startsWith("/api/")) {
    req.url = originalUrl.slice(4);
  }

  app.server.emit("request", req, res);
}
