/**
 * Next.js instrumentation hook (auto-appelé au démarrage). Charge Sentry
 * uniquement si le runtime correspond et si la DSN est configurée.
 */
export async function register() {
  if (process.env.NEXT_RUNTIME === "nodejs") {
    await import("../sentry.server.config");
  }
  if (process.env.NEXT_RUNTIME === "edge") {
    await import("../sentry.edge.config");
  }
}
