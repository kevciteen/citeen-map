import { fileURLToPath } from "node:url";
import { dirname } from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));

/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  // Pin workspace root to app/ — there is a stale package-lock.json at the
  // repo root left over from the old Vite scaffold, which confuses Next 16's
  // auto-detection and breaks webpack module resolution.
  outputFileTracingRoot: __dirname,
  turbopack: { root: __dirname },
  serverExternalPackages: [
    "better-sqlite3",
    // Sentry's server runtime + its OpenTelemetry tracing tree must be resolved
    // at runtime by Node — bundling it via webpack breaks (cf. node-fetch
    // vendored/undici.js requiring @opentelemetry/core through an opaque path).
    "@sentry/nextjs",
    "@sentry/node",
    "@sentry/core",
    "@opentelemetry/api",
    "@opentelemetry/core",
    "@opentelemetry/instrumentation",
    "@opentelemetry/resources",
    "@opentelemetry/sdk-trace-base",
    "@opentelemetry/semantic-conventions",
  ],
  experimental: {
    optimizePackageImports: ["lucide-react"],
  },
  webpack: (config, { isServer, webpack }) => {
    // Sentry 10 / @sentry/node pulls many @opentelemetry/instrumentation-*
    // tracing integrations (amqplib, mysql, redis, graphql, mongo, kafka…).
    // We don't use any of them — and some peers (e.g. @opentelemetry/core)
    // aren't installed, which crashes the webpack build on Windows.
    // We keep only the integrations relevant to a fetch/http server.
    if (isServer) {
      config.plugins.push(
        new webpack.IgnorePlugin({
          resourceRegExp:
            /^@opentelemetry\/instrumentation-(?!http|fetch|undici)/,
        }),
      );
    }
    return config;
  },
};

export default nextConfig;
