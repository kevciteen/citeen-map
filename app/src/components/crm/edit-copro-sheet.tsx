"use client";
import { useState } from "react";
import * as Dialog from "@radix-ui/react-dialog";
import { Loader2, MapPin, Save, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { AddressSearch } from "@/components/map/address-search";
import { toast } from "sonner";
import { useRouter } from "next/navigation";

type Props = {
  copro: {
    id: number;
    nom_copro: string | null;
    adresse: string | null;
    code_postal: string | null;
    commune: string | null;
    syndic: string | null;
    lat: number | null;
    lon: number | null;
  };
  trigger: React.ReactNode;
};

export function EditCoproSheet({ copro, trigger }: Props) {
  const router = useRouter();
  const [open, setOpen] = useState(false);
  const [saving, setSaving] = useState(false);
  const [nomCopro, setNomCopro] = useState(copro.nom_copro ?? "");
  const [adresse, setAdresse] = useState(copro.adresse ?? "");
  const [codePostal, setCodePostal] = useState(copro.code_postal ?? "");
  const [commune, setCommune] = useState(copro.commune ?? "");
  const [syndic, setSyndic] = useState(copro.syndic ?? "");
  const [lat, setLat] = useState(copro.lat?.toString() ?? "");
  const [lon, setLon] = useState(copro.lon?.toString() ?? "");

  const useBanResult = (r: {
    lat: number;
    lon: number;
    label: string;
    postcode?: string;
    city?: string;
    housenumber?: string;
    street?: string;
  }) => {
    setLat(r.lat.toFixed(6));
    setLon(r.lon.toFixed(6));
    if (r.housenumber && r.street) setAdresse(`${r.housenumber} ${r.street}`);
    else if (r.street) setAdresse(r.street);
    else setAdresse(r.label);
    if (r.postcode) setCodePostal(r.postcode);
    if (r.city) setCommune(r.city);
    toast.success("Position et adresse récupérées de la BAN");
  };

  const submit = async () => {
    setSaving(true);
    try {
      const body: Record<string, unknown> = {
        nom_copro: nomCopro.trim() || null,
        adresse: adresse.trim() || null,
        code_postal: codePostal.trim() || null,
        commune: commune.trim() || null,
        syndic: syndic.trim() || null,
      };
      const latN = Number(lat);
      const lonN = Number(lon);
      if (Number.isFinite(latN)) body.lat = latN;
      if (Number.isFinite(lonN)) body.lon = lonN;

      const r = await fetch(`/api/copros/${copro.id}/update`, {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!r.ok) {
        const j = await r.json().catch(() => ({}));
        toast.error(j?.error ? "Erreur de validation" : "Erreur");
      } else {
        toast.success("Copro mise à jour — le matching DPE va être recalculé");
        setOpen(false);
        router.refresh();
      }
    } finally {
      setSaving(false);
    }
  };

  return (
    <Dialog.Root open={open} onOpenChange={setOpen}>
      <Dialog.Trigger asChild>{trigger}</Dialog.Trigger>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-50 bg-black/60 backdrop-blur-sm" />
        <Dialog.Content className="fixed left-1/2 top-1/2 z-50 max-h-[90vh] w-[640px] max-w-[calc(100vw-2rem)] -translate-x-1/2 -translate-y-1/2 overflow-y-auto rounded-xl border border-border bg-card p-6 shadow-2xl">
          <div className="mb-4 flex items-start justify-between">
            <div>
              <Dialog.Title className="text-lg font-bold">
                Corriger la copropriété
              </Dialog.Title>
              <Dialog.Description className="text-xs text-muted-foreground">
                Les valeurs du registre national peuvent être erronées
                (typos, adresse approximative, mauvaise lat/lon). Saisissez la
                bonne adresse — la BAN renverra la position exacte.
              </Dialog.Description>
            </div>
            <Dialog.Close asChild>
              <button className="flex h-7 w-7 items-center justify-center rounded-md text-muted-foreground hover:bg-secondary">
                <X className="h-4 w-4" />
              </button>
            </Dialog.Close>
          </div>

          <div className="space-y-4">
            <div className="rounded-lg border border-primary/30 bg-primary/5 p-3">
              <p className="mb-1.5 text-xs font-bold text-primary">
                Recherche BAN (recommandé)
              </p>
              <AddressSearch onSelect={useBanResult} />
              <p className="mt-1 text-[11px] text-muted-foreground">
                Tapez l'adresse réelle, sélectionnez‑la, et tous les champs
                ci‑dessous se mettent à jour automatiquement avec les valeurs
                canoniques BAN.
              </p>
            </div>

            <div className="grid grid-cols-2 gap-3">
              <Field label="Nom d'usage">
                <Input value={nomCopro} onChange={(e) => setNomCopro(e.target.value)} />
              </Field>
              <Field label="Syndic">
                <Input value={syndic} onChange={(e) => setSyndic(e.target.value)} />
              </Field>
              <Field label="Adresse" full>
                <Input value={adresse} onChange={(e) => setAdresse(e.target.value)} />
              </Field>
              <Field label="Code postal">
                <Input
                  value={codePostal}
                  onChange={(e) => setCodePostal(e.target.value)}
                  inputMode="numeric"
                />
              </Field>
              <Field label="Commune">
                <Input value={commune} onChange={(e) => setCommune(e.target.value)} />
              </Field>
              <Field label="Latitude">
                <Input
                  value={lat}
                  onChange={(e) => setLat(e.target.value)}
                  inputMode="decimal"
                />
              </Field>
              <Field label="Longitude">
                <Input
                  value={lon}
                  onChange={(e) => setLon(e.target.value)}
                  inputMode="decimal"
                />
              </Field>
            </div>

            {lat && lon ? (
              <a
                href={`https://www.google.com/maps/search/?api=1&query=${lat},${lon}`}
                target="_blank"
                rel="noreferrer"
                className="flex items-center gap-1 text-xs text-primary hover:underline"
              >
                <MapPin className="h-3 w-3" />
                Vérifier sur Google Maps
              </a>
            ) : null}
          </div>

          <div className="mt-6 flex items-center justify-end gap-2 border-t border-border pt-4">
            <Dialog.Close asChild>
              <Button variant="ghost">Annuler</Button>
            </Dialog.Close>
            <Button onClick={submit} disabled={saving}>
              {saving ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
              Enregistrer
            </Button>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

function Field({
  label,
  children,
  full,
}: {
  label: string;
  children: React.ReactNode;
  full?: boolean;
}) {
  return (
    <div className={full ? "col-span-2" : ""}>
      <label className="mb-1 block text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
        {label}
      </label>
      {children}
    </div>
  );
}
