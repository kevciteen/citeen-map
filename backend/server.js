import Fastify from "fastify";
import cors from "@fastify/cors";
import pg from "pg";
import "dotenv/config";
import sensible from "@fastify/sensible";

import dpeRoutes from "./routes/dpe.js";
import exportRoutes from "./routes/export.js";

const app = Fastify({ logger: true });

const { Pool } = pg;
const pool = new Pool({ connectionString: process.env.DATABASE_URL });

// expose db au projet
app.decorate("db", pool);

async function main() {
  await app.register(cors, { origin: true });
  await app.register(sensible);

  await app.register(dpeRoutes);
  await app.register(exportRoutes);

  app.get("/health", async () => ({ ok: true }));

// BBOX query: ?bbox=minLon,minLat,maxLon,maxLat&limit=5000&syndic=...&q=...
app.get("/copros", async (req, reply) => {
  const { bbox, limit = 5000, syndic, q, departement } = req.query;

  if (!bbox) return reply.code(400).send({ error: "bbox requis" });

  const [minLon, minLat, maxLon, maxLat] = String(bbox).split(",").map(Number);
  if (![minLon, minLat, maxLon, maxLat].every(Number.isFinite)) {
    return reply.code(400).send({ error: "bbox invalide" });
  }

  const lim = Math.min(Number(limit) || 5000, 20000);

  const where = [];
  const params = [];
  let i = 1;

  // spatial filter
  where.push(`geom && ST_MakeEnvelope($${i++}, $${i++}, $${i++}, $${i++}, 4326)`);
  params.push(minLon, minLat, maxLon, maxLat);

  if (departement) {
    where.push(`departement = $${i++}`);
    params.push(String(departement));
  }

  if (syndic) {
    where.push(`syndic ILIKE $${i++}`);
    params.push(`%${syndic}%`);
  }

  if (q) {
    where.push(`(
      COALESCE(nom_copro,'') ILIKE $${i}
      OR COALESCE(adresse,'') ILIKE $${i}
      OR COALESCE(commune,'') ILIKE $${i}
      OR COALESCE(code_postal,'') ILIKE $${i}
    )`);
    params.push(`%${q}%`);
    i++;
  }

  const sql = `
    SELECT
      id,
      nom_copro,
      adresse,
      code_postal,
      commune,
      syndic,
      departement,
      lat,
      lon
    FROM copros
    WHERE ${where.join(" AND ")}
    LIMIT ${lim};
  `;

  const { rows } = await pool.query(sql, params);

  return {
    type: "FeatureCollection",
    features: rows.map((r) => ({
      type: "Feature",
      geometry: { type: "Point", coordinates: [r.lon, r.lat] },
      properties: {
        id: r.id,
        nom_copro: r.nom_copro,
        adresse: r.adresse,
        code_postal: r.code_postal,
        commune: r.commune,
        syndic: r.syndic,
        departement: r.departement,
      },
    })),
  };
});

// Fiche immeuble
app.get("/copros/:id", async (req, reply) => {
  const id = Number(req.params.id);
  if (!Number.isFinite(id)) return reply.code(400).send({ error: "id invalide" });

  const { rows } = await pool.query(`SELECT * FROM copros WHERE id = $1`, [id]);
  if (rows.length === 0) return reply.code(404).send({ error: "not found" });
  return rows[0];
});

await app.ready(); // <-- IMPORTANT
  await app.listen({ host: "0.0.0.0", port: 3002 });
}

main().catch((err) => {
  app.log.error(err);
  process.exit(1);
});