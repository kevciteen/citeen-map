/**
 * Fetch JSON typé avec auto-abort (TanStack Query passe un AbortSignal via
 * le QueryFunctionContext qu'on chaîne sur le fetch). Si l'utilisateur tape
 * "Pari", "Pariis", "Paris", seules le dernier garde ses ressources : les
 * deux premiers sont annulés côté navigateur ET côté Vercel.
 */
export async function jsonFetcher<T>(
  url: string,
  signal?: AbortSignal,
): Promise<T> {
  const res = await fetch(url, {
    headers: { accept: "application/json" },
    signal,
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`${res.status} ${res.statusText}${body ? ` — ${body.slice(0, 120)}` : ""}`);
  }
  return res.json() as Promise<T>;
}
