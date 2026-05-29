"use client";
import { useCallback, useEffect, useState } from "react";
import { Loader2, MapPin, Globe, Network, RefreshCw, PlayCircle } from "lucide-react";
import { Button } from "@/components/ui/button";
import { toast } from "sonner";

type CoordsStats = {
  total: number;
  withCoords: number;
  withoutCoords: number;
  pendingBackfill: number;
  bySource: Array<{ source: string; count: number }>;
};

type GoogleQuota = {
  google: {
    day: string;
    find: number;
    details: number;
    total: number;
    monthFind: number;
    monthDetails: number;
    monthTotal: number;
  };
  cache: { total: number; valid: number };
};

type DirectoryStats = {
  total: number;
  byType: Array<{ entity_type: string; count: number }>;
  lastSyncedAt: number | null;
};

const MONTHLY_FREE_TIER = 1000;

export function CoordsHealthBrowser() {
  const [coords, setCoords] = useState<CoordsStats | null>(null);
  const [quota, setQuota] = useState<GoogleQuota | null>(null);
  const [directory, setDirectory] = useState<DirectoryStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState<string | null>(null);

  const loadAll = useCallback(async () => {
    setLoading(true);
    try {
      const [coordsRes, quotaRes, dirRes] = await Promise.all([
        fetch("/api/copros/coords-stats").then((r) => r.json()).catch(() => null),
        fetch("/api/tertiaire/quota").then((r) => r.json()).catch(() => null),
        fetch("/api/directory/stats").then((r) => r.json()).catch(() => null),
      ]);
      if (coordsRes && !coordsRes.error) setCoords(coordsRes);
      if (quotaRes && !quotaRes.error) setQuota(quotaRes);
      if (dirRes && !dirRes.error) setDirectory(dirRes);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadAll();
  }, [loadAll]);

  const runBackfillCopros = async (limit: number) => {
    setBusy("backfill-copros");
    try {
      const r = await fetch("/api/copros/backfill-coords", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ limit }),
      });
      const j = await r.json();
      if (!r.ok) {
        toast.error(j.error ?? "Erreur backfill copros");
        return;
      }
      toast.success(
        `Backfill : ${j.byBan} BAN + ${j.byCadastre} cadastre, ${j.unresolved} non résolus${j.errors ? ` (${j.errors} erreurs)` : ""}`,
      );
      await loadAll();
    } catch (e) {
      toast.error(`Erreur : ${(e as Error).message}`);
    } finally {
      setBusy(null);
    }
  };

  const runSyncDirectory = async () => {
    setBusy("sync-directory");
    try {
      const r = await fetch("/api/directory/sync", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
      });
      const j = await r.json();
      if (!r.ok) {
        toast.error(j.error ?? "Erreur sync directory");
        return;
      }
      toast.success(
        `Sync : ${j.copros} copros + ${j.occupants} occupants + ${j.syndics} syndics + ${j.prospectsCustom} prospects (total ${j.total})`,
      );
      await loadAll();
    } catch (e) {
      toast.error(`Erreur : ${(e as Error).message}`);
    } finally {
      setBusy(null);
    }
  };

  if (loading) {
    return (
      <div className="flex h-40 items-center justify-center">
        <Loader2 className="h-5 w-5 animate-spin text-primary" />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <p className="text-sm text-muted-foreground">
          État global du géocodage et de l&apos;enrichissement contacts.
        </p>
        <Button variant="outline" size="sm" onClick={loadAll} disabled={busy !== null}>
          <RefreshCw className="h-4 w-4" />
          Rafraîchir
        </Button>
      </div>

      <CoordsCopros stats={coords} busy={busy} onBackfill={runBackfillCopros} />
      <GoogleQuotaCard quota={quota} />
      <DirectoryCard
        stats={directory}
        busy={busy}
        onSync={runSyncDirectory}
      />
    </div>
  );
}

/* --------------------------------- COPROS --------------------------------- */

function CoordsCopros({
  stats,
  busy,
  onBackfill,
}: {
  stats: CoordsStats | null;
  busy: string | null;
  onBackfill: (limit: number) => void;
}) {
  if (!stats) {
    return (
      <SectionCard title="Coordonnées copros" icon={MapPin}>
        <p className="text-sm text-muted-foreground">Données indisponibles.</p>
      </SectionCard>
    );
  }
  const ratio = stats.total > 0 ? (stats.withCoords / stats.total) * 100 : 0;
  const running = busy === "backfill-copros";
  return (
    <SectionCard title="Coordonnées copros" icon={MapPin}>
      <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
        <Metric label="Total" value={stats.total.toLocaleString("fr-FR")} />
        <Metric
          label="Géocodées"
          value={stats.withCoords.toLocaleString("fr-FR")}
          hint={`${ratio.toFixed(1)} %`}
          accent="primary"
        />
        <Metric
          label="Sans coords"
          value={stats.withoutCoords.toLocaleString("fr-FR")}
          accent={stats.withoutCoords > 0 ? "warn" : "ok"}
        />
        <Metric
          label="À traiter"
          value={stats.pendingBackfill.toLocaleString("fr-FR")}
          hint="non encore tenté"
        />
      </div>

      {stats.bySource.length > 0 ? (
        <div className="mt-4 flex flex-wrap gap-2">
          {stats.bySource.map((s) => (
            <span
              key={s.source}
              className="rounded-full border border-border bg-secondary/40 px-3 py-1 text-xs text-muted-foreground"
            >
              <strong className="text-foreground">{s.count.toLocaleString("fr-FR")}</strong>{" "}
              · {s.source}
            </span>
          ))}
        </div>
      ) : null}

      <div className="mt-4 flex flex-wrap gap-2">
        <Button
          size="sm"
          onClick={() => onBackfill(100)}
          disabled={running || stats.pendingBackfill === 0}
        >
          {running ? <Loader2 className="h-4 w-4 animate-spin" /> : <PlayCircle className="h-4 w-4" />}
          Lancer 100
        </Button>
        <Button
          size="sm"
          variant="secondary"
          onClick={() => onBackfill(500)}
          disabled={running || stats.pendingBackfill === 0}
        >
          {running ? <Loader2 className="h-4 w-4 animate-spin" /> : <PlayCircle className="h-4 w-4" />}
          Lancer 500
        </Button>
        {stats.pendingBackfill === 0 ? (
          <span className="self-center text-xs text-muted-foreground">
            ✓ Tout est traité
          </span>
        ) : null}
      </div>
    </SectionCard>
  );
}

/* --------------------------------- QUOTA --------------------------------- */

function GoogleQuotaCard({ quota }: { quota: GoogleQuota | null }) {
  if (!quota) {
    return (
      <SectionCard title="Quota Google Places" icon={Globe}>
        <p className="text-sm text-muted-foreground">Données indisponibles.</p>
      </SectionCard>
    );
  }
  const month = quota.google.monthTotal;
  const ratioMonth = (month / MONTHLY_FREE_TIER) * 100;
  const warn = ratioMonth >= 80;
  return (
    <SectionCard title="Quota Google Places" icon={Globe}>
      <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
        <Metric label="Find (jour)" value={quota.google.find.toLocaleString("fr-FR")} />
        <Metric label="Details (jour)" value={quota.google.details.toLocaleString("fr-FR")} />
        <Metric
          label="Mois en cours"
          value={`${month.toLocaleString("fr-FR")} / ${MONTHLY_FREE_TIER}`}
          hint={`${ratioMonth.toFixed(1)} %`}
          accent={warn ? "warn" : month > 0 ? "primary" : "ok"}
        />
        <Metric
          label="Cache contacts"
          value={`${quota.cache.valid.toLocaleString("fr-FR")} / ${quota.cache.total.toLocaleString("fr-FR")}`}
          hint="valides / total"
        />
      </div>
      {warn ? (
        <p className="mt-3 rounded-md border border-amber-300 bg-amber-50 p-2 text-xs text-amber-900">
          ⚠️ Quota mensuel à {ratioMonth.toFixed(0)} % — réduire la fréquence
          d&apos;enrichissement Google ou augmenter le free tier.
        </p>
      ) : null}
    </SectionCard>
  );
}

/* ------------------------------- DIRECTORY ------------------------------- */

function DirectoryCard({
  stats,
  busy,
  onSync,
}: {
  stats: DirectoryStats | null;
  busy: string | null;
  onSync: () => void;
}) {
  const running = busy === "sync-directory";
  return (
    <SectionCard title="Annuaire unifié" icon={Network}>
      <p className="text-sm text-muted-foreground">
        Table <code className="rounded bg-secondary/60 px-1 text-xs">directory</code> qui
        agrège copros + occupants + syndics + prospects custom. À resync après
        chaque backfill ou enrichissement massif.
      </p>
      {stats ? (
        <>
          <p className="mt-3 text-sm">
            Lignes actuelles indexées :{" "}
            <strong>{stats.total.toLocaleString("fr-FR")}</strong>
            {stats.total === 0 ? (
              <span className="ml-2 text-xs text-muted-foreground">
                (pas encore synchronisé — clique sur Sync)
              </span>
            ) : null}
          </p>
          {stats.byType.length > 0 ? (
            <div className="mt-2 flex flex-wrap gap-2">
              {stats.byType.map((t) => (
                <span
                  key={t.entity_type}
                  className="rounded-full border border-border bg-secondary/40 px-3 py-1 text-xs text-muted-foreground"
                >
                  <strong className="text-foreground">
                    {t.count.toLocaleString("fr-FR")}
                  </strong>{" "}
                  · {t.entity_type}
                </span>
              ))}
            </div>
          ) : null}
          {stats.lastSyncedAt ? (
            <p className="mt-2 text-[11px] text-muted-foreground">
              Dernière sync :{" "}
              {new Date(stats.lastSyncedAt * 1000).toLocaleString("fr-FR")}
            </p>
          ) : null}
        </>
      ) : null}
      <div className="mt-4">
        <Button size="sm" onClick={onSync} disabled={running}>
          {running ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
          Sync directory
        </Button>
      </div>
    </SectionCard>
  );
}

/* ------------------------------- PRIMITIVES ------------------------------- */

function SectionCard({
  title,
  icon: Icon,
  children,
}: {
  title: string;
  icon: React.ComponentType<{ className?: string }>;
  children: React.ReactNode;
}) {
  return (
    <section className="rounded-xl border border-border bg-card p-5 shadow-sm">
      <div className="mb-4 flex items-center gap-2 text-sm font-semibold">
        <Icon className="h-4 w-4 text-primary" />
        {title}
      </div>
      {children}
    </section>
  );
}

function Metric({
  label,
  value,
  hint,
  accent,
}: {
  label: string;
  value: string;
  hint?: string;
  accent?: "primary" | "warn" | "ok";
}) {
  const accentClass =
    accent === "primary"
      ? "text-primary"
      : accent === "warn"
        ? "text-amber-700"
        : accent === "ok"
          ? "text-emerald-700"
          : "text-foreground";
  return (
    <div>
      <p className="text-[10px] uppercase tracking-wider text-muted-foreground">
        {label}
      </p>
      <p className={`text-xl font-semibold ${accentClass}`}>{value}</p>
      {hint ? <p className="text-[11px] text-muted-foreground">{hint}</p> : null}
    </div>
  );
}
