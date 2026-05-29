import { useEffect, useState } from "react";

/**
 * Renvoie une version "delayed" de `value` qui ne change qu'après `delayMs`
 * d'inactivité. Utile pour debouncer les inputs de recherche avant de
 * déclencher un fetch côté serveur.
 */
export function useDebouncedValue<T>(value: T, delayMs = 250): T {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const t = window.setTimeout(() => setDebounced(value), delayMs);
    return () => window.clearTimeout(t);
  }, [value, delayMs]);
  return debounced;
}
