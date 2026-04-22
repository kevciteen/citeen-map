import { buildApp } from "./app.js";

const app = buildApp();

async function main() {
  await app.ready();
  await app.listen({ host: "0.0.0.0", port: 3002 });
}

main().catch((err) => {
  app.log.error(err);
  process.exit(1);
});
