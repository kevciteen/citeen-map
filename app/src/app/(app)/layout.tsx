import { Sidebar } from "@/components/layout/sidebar";
import { GlobalSearch } from "@/components/global-search/global-search";

export default function AppLayout({ children }: { children: React.ReactNode }) {
  return (
    <div
      className="flex w-screen overflow-hidden"
      style={{ height: "100vh" }}
    >
      <Sidebar />
      <main
        className="flex min-w-0 flex-1 flex-col overflow-hidden"
        style={{ height: "100vh" }}
      >
        {children}
      </main>
      <GlobalSearch />
    </div>
  );
}
