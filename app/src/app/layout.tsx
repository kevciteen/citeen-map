import type { Metadata } from "next";
import "./globals.css";
import { Toaster } from "sonner";
import { QueryProvider } from "@/components/providers/query-provider";

export const metadata: Metadata = {
  title: "Citeen — CRM Prospection Copropriétés",
  description:
    "Outil de prospection commerciale premium pour la rénovation énergétique en copropriété. Carte interactive, DPE ADEME, registre national des copropriétés.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="fr" suppressHydrationWarning>
      <body
        className="bg-background font-sans antialiased"
        style={{ height: "100vh", margin: 0, overflow: "hidden" }}
      >
        <QueryProvider>{children}</QueryProvider>
        <Toaster richColors position="top-right" />
      </body>
    </html>
  );
}
