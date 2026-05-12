/**
 * Init Sentry côté client. NOOP si SENTRY_DSN n'est pas configuré (dev local
 * ou compte Sentry pas encore créé). On évite ainsi de logger des erreurs
 * fantômes pendant le développement.
 */
import * as Sentry from "@sentry/nextjs";

const DSN = process.env.NEXT_PUBLIC_SENTRY_DSN;

if (DSN) {
  Sentry.init({
    dsn: DSN,
    tracesSampleRate: 0.1,
    replaysSessionSampleRate: 0,
    replaysOnErrorSampleRate: 1.0,
    environment: process.env.VERCEL_ENV ?? process.env.NODE_ENV,
    // Ignore les erreurs network bénignes (network reset, abort, etc.)
    ignoreErrors: [
      "ResizeObserver loop limit exceeded",
      "Non-Error promise rejection captured",
      "AbortError",
    ],
  });
}
