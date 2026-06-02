"use client";
/**
 * Dropdown d'assignement compact d'un prospect à un membre de l'équipe.
 * Aligné sur la convention en place : prospects.assigned_user_id (FK users.id).
 * Pour la fiche prospect détaillée, voir AssignmentPicker dans prospect-detail.tsx.
 */
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Loader2, User, UserCheck, X } from "lucide-react";
import { jsonFetcher } from "@/lib/fetcher";
import { toast } from "sonner";

type TeamMember = {
  id: number;
  email: string;
  name: string | null;
  role: "admin" | "member";
};

type TeamResp = {
  team: TeamMember[];
  me: { id: number; email: string; name: string | null };
};

export function AssignDropdown({
  prospectId,
  currentUserId,
}: {
  prospectId: number;
  currentUserId: number | null;
}) {
  const qc = useQueryClient();

  const { data: teamData } = useQuery({
    queryKey: ["team"],
    queryFn: ({ signal }) => jsonFetcher<TeamResp>("/api/team", signal),
    staleTime: 5 * 60 * 1000,
  });

  const assignMut = useMutation({
    mutationFn: async (userId: number | null) => {
      const r = await fetch(`/api/prospects/${prospectId}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ assignedUserId: userId }),
      });
      if (!r.ok) throw new Error("Erreur assignement");
    },
    onSuccess: () => {
      toast.success("Assignement mis à jour");
      qc.invalidateQueries({ queryKey: ["prospect", prospectId] });
      qc.invalidateQueries({ queryKey: ["cockpit"] });
    },
    onError: (e) => toast.error((e as Error).message),
  });

  return (
    <div
      className="inline-flex items-center gap-1.5 rounded-md border border-border bg-card px-2 py-1 text-xs"
      onClick={(e) => e.stopPropagation()}
    >
      <User className="h-3 w-3 text-muted-foreground" />
      <select
        value={currentUserId ?? ""}
        onChange={(e) =>
          assignMut.mutate(e.target.value ? Number(e.target.value) : null)
        }
        disabled={assignMut.isPending}
        className="border-0 bg-transparent text-xs focus:outline-none"
      >
        <option value="">Non assigné</option>
        {teamData?.team.map((m) => (
          <option key={m.id} value={m.id}>
            {teamData.me?.id === m.id ? "🧑 " : ""}
            {m.name ?? m.email}
            {m.role === "admin" ? " (admin)" : ""}
          </option>
        ))}
      </select>
      {currentUserId ? (
        <button
          onClick={() => assignMut.mutate(null)}
          className="rounded p-0.5 text-muted-foreground hover:bg-secondary hover:text-foreground"
          title="Retirer l'assignement"
        >
          <X className="h-2.5 w-2.5" />
        </button>
      ) : null}
      {assignMut.isPending ? (
        <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />
      ) : currentUserId ? (
        <UserCheck className="h-3 w-3 text-emerald-700" />
      ) : null}
    </div>
  );
}
