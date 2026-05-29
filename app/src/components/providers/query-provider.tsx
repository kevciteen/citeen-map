"use client";
/**
 * Provider TanStack Query global. Singleton QueryClient avec config tunée
 * pour un CRM intensif :
 *   - staleTime 30s : les données restent fraîches 30s, pas de refetch inutile
 *   - gcTime 5min : on garde les résultats 5min en mémoire pour navigation rapide
 *   - retry 1 : on ne reflood pas Turso si une requête fail
 *   - refetchOnWindowFocus false : pas de refetch auto au switch d'onglet
 *     (le user reste sur les mêmes données entre deux switches)
 */
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useState } from "react";

export function QueryProvider({ children }: { children: React.ReactNode }) {
  const [client] = useState(
    () =>
      new QueryClient({
        defaultOptions: {
          queries: {
            staleTime: 30 * 1000,
            gcTime: 5 * 60 * 1000,
            retry: 1,
            refetchOnWindowFocus: false,
          },
        },
      }),
  );
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
}
