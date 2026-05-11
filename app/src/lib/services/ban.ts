import { cacheGet, cacheSet } from "./cache";

const BAN_API_BASE = "https://api-adresse.data.gouv.fr/search/";

export type GeocodeResult = {
  lat: number;
  lon: number;
  label: string;
  citycode?: string;
  postcode?: string;
  city?: string;
  street?: string;
  housenumber?: string;
  score: number;
};

function normalizeKey(value: string): string {
  return value
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "")
    .trim();
}

export async function geocodeAddress(
  query: string,
  opts?: { limit?: number; postcode?: string; autocomplete?: boolean },
): Promise<GeocodeResult[]> {
  const q = query?.trim();
  if (!q) return [];

  const limit = opts?.limit ?? 5;
  const cacheKey = `ban:${normalizeKey(q)}|${limit}|${opts?.postcode ?? ""}|${opts?.autocomplete ? "1" : "0"}`;
  const hit = cacheGet<GeocodeResult[]>(cacheKey);
  if (hit) return hit;

  const url = new URL(BAN_API_BASE);
  url.searchParams.set("q", q);
  url.searchParams.set("limit", String(limit));
  url.searchParams.set("autocomplete", opts?.autocomplete ? "1" : "0");
  if (opts?.postcode) url.searchParams.set("postcode", opts.postcode);

  const res = await fetch(url, {
    headers: { accept: "application/json" },
    next: { revalidate: 3600 },
  });
  if (!res.ok) return [];

  const json = (await res.json()) as {
    features?: Array<{
      geometry?: { coordinates?: [number, number] };
      properties?: {
        label?: string;
        score?: number;
        citycode?: string;
        postcode?: string;
        city?: string;
        street?: string;
        housenumber?: string;
      };
    }>;
  };

  const results: GeocodeResult[] = (json.features ?? [])
    .map((f) => {
      const c = f.geometry?.coordinates;
      if (!c || c.length !== 2) return null;
      return {
        lon: Number(c[0]),
        lat: Number(c[1]),
        label: f.properties?.label ?? q,
        score: Number(f.properties?.score ?? 0),
        citycode: f.properties?.citycode,
        postcode: f.properties?.postcode,
        city: f.properties?.city,
        street: f.properties?.street,
        housenumber: f.properties?.housenumber,
      } as GeocodeResult;
    })
    .filter((x): x is GeocodeResult => x !== null);

  cacheSet(cacheKey, results, 60 * 60 * 1000);
  return results;
}
