"use client";
import { useState } from "react";
import { useRouter } from "next/navigation";
import { Topbar } from "@/components/layout/topbar";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { AddressSearch } from "@/components/map/address-search";
import { toast } from "sonner";

export default function NewProspectPage() {
  const router = useRouter();
  const [label, setLabel] = useState("");
  const [picked, setPicked] = useState<{ lat: number; lon: number; label: string } | null>(
    null,
  );
  const [saving, setSaving] = useState(false);

  const submit = async () => {
    if (!picked && !label.trim()) {
      toast.error("Sélectionnez une adresse ou un libellé");
      return;
    }
    setSaving(true);
    const r = await fetch("/api/prospects", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        customLabel: label.trim() || picked?.label,
        customAddress: picked?.label,
        customLat: picked?.lat,
        customLon: picked?.lon,
        stage: "lead",
        priority: 2,
      }),
    });
    setSaving(false);
    if (r.ok) {
      const j = await r.json();
      toast.success("Prospect créé");
      router.push(`/prospects/${j.id}`);
    } else {
      toast.error("Erreur");
    }
  };

  return (
    <div className="flex h-full flex-col">
      <Topbar title="Nouveau prospect" subtitle="Adresse libre — sans copro du registre" />
      <div className="flex-1 overflow-y-auto bg-secondary/30 p-6">
        <div className="mx-auto max-w-xl">
          <Card>
            <CardHeader>
              <CardTitle className="text-sm">Détails</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div>
                <label className="mb-1 block text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                  Adresse (recherche BAN)
                </label>
                <AddressSearch
                  onSelect={(r) =>
                    setPicked({ lat: r.lat, lon: r.lon, label: r.label })
                  }
                />
                {picked ? (
                  <p className="mt-1 text-[11px] text-muted-foreground">
                    Sélection : {picked.label} ({picked.lat.toFixed(5)},{" "}
                    {picked.lon.toFixed(5)})
                  </p>
                ) : null}
              </div>
              <div>
                <label className="mb-1 block text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                  Libellé interne (optionnel)
                </label>
                <Input
                  value={label}
                  onChange={(e) => setLabel(e.target.value)}
                  placeholder="Ex. Résidence Les Lilas"
                />
              </div>
              <Button onClick={submit} disabled={saving} className="w-full">
                Créer le prospect
              </Button>
              <p className="text-[11px] text-muted-foreground">
                Astuce : pour les copros du registre, utilisez la carte → clic sur un immeuble → "Ajouter au pipeline".
              </p>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
