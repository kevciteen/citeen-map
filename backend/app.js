import Fastify from "fastify";
import cors from "@fastify/cors";
import pg from "pg";
import sensible from "@fastify/sensible";
import dotenv from "dotenv";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import dpeRoutes from "./routes/dpe.js";
import exportRoutes from "./routes/export.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
dotenv.config();
if (!process.env.DATABASE_URL && !process.env.DB_HOST) {
  const backendEnvPath = join(__dirname, ".env");
  if (existsSync(backendEnvPath)) {
    dotenv.config({ path: backendEnvPath, override: false });
  }
}

const { Pool } = pg;

function buildDbConfig() {
  if (process.env.DATABASE_URL) {
    return { connectionString: process.env.DATABASE_URL };
  }

  const host = process.env.DB_HOST;
  const database = process.env.DB_NAME;
  const user = process.env.DB_USER;

  if (host && database && user) {
    return {
      host,
      port: Number(process.env.DB_PORT || 5432),
      database,
      user,
      password: process.env.DB_PASSWORD || "",
    };
  }

  throw new Error(
    "Database configuration missing. Set DATABASE_URL or DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD."
  );
}

const pool = new Pool(buildDbConfig());

function parseOptionalNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function buildLonLatBbox(minLon, minLat, maxLon, maxLat, startIndex = 1) {
  return {
    clause: `lon BETWEEN $${startIndex} AND $${startIndex + 1} AND lat BETWEEN $${startIndex + 2} AND $${startIndex + 3}`,
    params: [minLon, maxLon, minLat, maxLat],
  };
}

function buildRadiusBounds(lat, lon, radiusMeters) {
  const latDelta = radiusMeters / 111320;
  const cosLat = Math.cos((lat * Math.PI) / 180);
  const lonDelta = radiusMeters / (111320 * Math.max(Math.abs(cosLat), 0.2));

  return {
    minLon: lon - lonDelta,
    maxLon: lon + lonDelta,
    minLat: lat - latDelta,
    maxLat: lat + latDelta,
  };
}

function haversineDistanceSql(latParamIndex, lonParamIndex) {
  return `
    6371000 * acos(
      LEAST(
        1,
        GREATEST(
          -1,
          cos(radians($${latParamIndex})) * cos(radians(lat)) *
          cos(radians(lon) - radians($${lonParamIndex})) +
          sin(radians($${latParamIndex})) * sin(radians(lat))
        )
      )
    )
  `;
}

export function buildApp() {
  const app = Fastify({ logger: true });

  app.decorate("db", pool);

  app.register(cors, { origin: true });
  app.register(sensible);

  app.register(dpeRoutes);
  app.register(exportRoutes);

  app.get("/health", async () => ({ ok: true }));

  // BBOX query: ?bbox=minLon,minLat,maxLon,maxLat&limit=5000&syndic=...&q=...
  app.get("/copros", async (req, reply) => {
    const {
      bbox,
      limit = 5000,
      syndic,
      q,
      copro,
      commune,
      code_postal,
      numero_immatriculation,
      departement,
    } = req.query;

    const hasBbox = bbox !== undefined && bbox !== null && String(bbox).trim() !== "";
    let minLon = null;
    let minLat = null;
    let maxLon = null;
    let maxLat = null;

    if (hasBbox) {
      [minLon, minLat, maxLon, maxLat] = String(bbox).split(",").map(Number);
      if (![minLon, minLat, maxLon, maxLat].every(Number.isFinite)) {
        return reply.code(400).send({ error: "bbox invalide" });
      }
    }

    const hasTextFilter = [syndic, q, copro, commune, code_postal, numero_immatriculation, departement]
      .some((value) => value !== undefined && value !== null && String(value).trim() !== "");

    if (!hasBbox && !hasTextFilter) {
      return reply.code(400).send({ error: "bbox ou filtre requis" });
    }

    const lim = Math.min(Number(limit) || 5000, hasBbox ? 20000 : 3000);

    const where = [];
    const params = [];
    let i = 1;

    if (hasBbox) {
      const bboxSql = buildLonLatBbox(minLon, minLat, maxLon, maxLat, i);
      where.push(bboxSql.clause);
      params.push(...bboxSql.params);
      i += 4;
    }

    if (departement) {
      where.push(`departement = $${i++}`);
      params.push(String(departement));
    }

    if (code_postal) {
      where.push(`code_postal ILIKE $${i++}`);
      params.push(`${String(code_postal).trim()}%`);
    }

    if (commune) {
      where.push(`commune ILIKE $${i++}`);
      params.push(`%${commune}%`);
    }

    if (syndic) {
      where.push(`syndic ILIKE $${i++}`);
      params.push(`%${syndic}%`);
    }

    if (copro) {
      where.push(`nom_copro ILIKE $${i++}`);
      params.push(`%${copro}%`);
    }

    if (numero_immatriculation) {
      where.push(`numero_immatriculation ILIKE $${i++}`);
      params.push(`%${numero_immatriculation}%`);
    }

    if (q) {
      where.push(`(
        COALESCE(nom_copro,'') ILIKE $${i}
        OR COALESCE(adresse,'') ILIKE $${i}
        OR COALESCE(commune,'') ILIKE $${i}
        OR COALESCE(code_postal,'') ILIKE $${i}
        OR COALESCE(numero_immatriculation,'') ILIKE $${i}
      )`);
      params.push(`%${q}%`);
      i++;
    }

    const sql = `
      SELECT
        id,
        numero_immatriculation,
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
      ORDER BY commune NULLS LAST, code_postal NULLS LAST, adresse NULLS LAST, id
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
          numero_immatriculation: r.numero_immatriculation,
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

  app.get("/copros/nearby", async (req, reply) => {
    const lat = parseOptionalNumber(req.query.lat);
    const lon = parseOptionalNumber(req.query.lon);
    const r = Math.min(parseOptionalNumber(req.query.r) ?? 120, 500);
    const limit = Math.min(parseOptionalNumber(req.query.limit) ?? 8, 20);

    if (lat === null || lon === null) {
      return reply.code(400).send({ error: "lat/lon invalides" });
    }

    const bounds = buildRadiusBounds(lat, lon, r);
    const distanceSql = haversineDistanceSql(1, 2);
    const sql = `
      SELECT
        id,
        numero_immatriculation,
        nom_copro,
        adresse,
        code_postal,
        commune,
        syndic,
        departement,
        lat,
        lon,
        ROUND(${distanceSql})::int AS distance_m
      FROM copros
      WHERE lon BETWEEN $3 AND $4
        AND lat BETWEEN $5 AND $6
        AND ${distanceSql} <= $7
      ORDER BY ${distanceSql} ASC
      LIMIT $8;
    `;

    const { rows } = await pool.query(sql, [
      lat,
      lon,
      bounds.minLon,
      bounds.maxLon,
      bounds.minLat,
      bounds.maxLat,
      r,
      limit,
    ]);
    return { items: rows };
  });

  app.get("/copros/:id", async (req, reply) => {
    const id = Number(req.params.id);
    if (!Number.isFinite(id)) return reply.code(400).send({ error: "id invalide" });

    const { rows } = await pool.query(`SELECT * FROM copros WHERE id = $1`, [id]);
    if (rows.length === 0) return reply.code(404).send({ error: "not found" });
    return rows[0];
  });

  return app;
}
