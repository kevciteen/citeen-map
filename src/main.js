import "./style.css";
import maplibregl from "maplibre-gl";

const API_BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:3002";
const GOOGLE_MAPS_API_KEY = import.meta.env.VITE_GOOGLE_MAPS_API_KEY || "";
const MAX_DPE_SUMMARY_IDS = 120;

document.querySelector("#app").innerHTML = `
  <div id="map" style="position:fixed; inset:0;"></div>

  <div id="panel" class="panel">
    <div class="panel__header">
      <div class="brand">
        <div class="brand__dot"></div>
        <div class="brand__title">Citeen</div>
      </div>
      <div id="status" class="status">—</div>
    </div>

    <div class="panel__body">
      <label class="field">
        <div class="field__label">Adresse / lieu</div>
        <input id="q" class="input" placeholder="Adresse, rue, point d'intérêt…" autocomplete="off" />
        <div id="qHint" class="field__hint">Autocomplete Google + repérage des copropriétés proches</div>
      </label>

      <div class="grid2">
        <label class="field">
          <div class="field__label">Nom de copro</div>
          <input id="copro" class="input" placeholder="Ex: Résidence Victor Hugo" />
        </label>

        <label class="field">
          <div class="field__label">Syndic</div>
          <input id="syndic" class="input" placeholder="Ex: Foncia, Nexity…" />
        </label>
      </div>

      <div class="grid3">
        <label class="field">
          <div class="field__label">Commune</div>
          <input id="commune" class="input" placeholder="Paris, Montreuil…" />
        </label>

        <label class="field">
          <div class="field__label">Code postal</div>
          <input id="cp" class="input" placeholder="75018" inputmode="numeric" />
        </label>

        <label class="field">
          <div class="field__label">Département</div>
          <input id="dep" class="input" placeholder="75" inputmode="numeric" />
        </label>
      </div>

      <label class="field">
        <div class="field__label">Immatriculation copro</div>
        <input id="immat" class="input" placeholder="AB1234567" autocomplete="off" />
      </label>

      <div class="actions">
        <button id="refresh" class="btn btn--primary">Rechercher</button>
        <button id="clear" class="btn btn--ghost">Réinitialiser</button>
        <button id="export" class="btn btn--ghost">Exporter CSV</button>
      </div>
    </div>
  </div>

  <div id="sidebar" class="sidebar" style="display:none;"></div>
`;

const map = new maplibregl.Map({
  container: "map",
  style: {
    version: 8,
    sources: {
      osm: {
        type: "raster",
        tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
        tileSize: 256,
        attribution: "© OpenStreetMap contributors",
      },
    },
    layers: [{ id: "osm", type: "raster", source: "osm" }],
  },
  center: [2.3522, 48.8566],
  zoom: 11,
});

map.addControl(new maplibregl.NavigationControl(), "top-right");

const COPROS_SOURCE_ID = "copros-src";
const ADDRESS_SOURCE_ID = "address-src";

const FALLBACK_POINT_COLOR = "rgba(148,163,184,0.35)"; // neutre (pas "NC")
const FALLBACK_HALO_COLOR = "rgba(96,165,250,0.18)";

// Cache DPE: on stocke la promesse pour dédupliquer (évite multi-fetch au clic/enrich)
const dpePromiseCache = new Map(); // key -> Promise<json>
const dpeSummaryCache = new Map(); // key -> summary
let addressPopup = null;
let coproPopup = null;
let googleScriptPromise = null;
let lastSearchFeatures = [];
let activeColorHydrationToken = 0;

let miniMap = null;
let miniMapMarker = null;

let sidebarMode = "overview"; // "overview" | "detail"
let lastSidebarModel = null;  // mémorise la dernière fiche pour toggle


// -------------------- Utils --------------------
function setStatus(text) {
  document.getElementById("status").textContent = text;
}

function escapeHtml(s) {
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function bboxFromMap() {
  const b = map.getBounds();
  return [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()].join(",");
}

async function mapLimit(items, limit, fn) {
  const ret = [];
  const executing = [];
  for (const item of items) {
    const p = Promise.resolve().then(() => fn(item));
    ret.push(p);
    executing.push(p);
    p.finally(() => {
      const idx = executing.indexOf(p);
      if (idx >= 0) executing.splice(idx, 1);
    });
    if (executing.length >= limit) await Promise.race(executing);
  }
  return Promise.all(ret);
}

function getSearchFilters() {
  return {
    q: document.getElementById("q").value.trim(),
    copro: document.getElementById("copro").value.trim(),
    syndic: document.getElementById("syndic").value.trim(),
    commune: document.getElementById("commune").value.trim(),
    code_postal: document.getElementById("cp").value.trim(),
    departement: document.getElementById("dep").value.trim(),
    numero_immatriculation: document.getElementById("immat").value.trim(),
  };
}

function buildSearchParams({ limit }) {
  const filters = getSearchFilters();
  const params = new URLSearchParams({ bbox: bboxFromMap(), limit: String(limit) });

  for (const [key, value] of Object.entries(filters)) {
    if (value) params.set(key, value);
  }

  return params;
}

function distanceScoreFromCenter(feature, center) {
  const coords = feature?.geometry?.coordinates;
  if (!Array.isArray(coords) || coords.length !== 2) return Number.POSITIVE_INFINITY;
  const dx = Number(coords[0]) - Number(center.lng);
  const dy = Number(coords[1]) - Number(center.lat);
  return dx * dx + dy * dy;
}

function ensureGooglePlacesLoaded() {
  if (window.google?.maps?.places) return Promise.resolve(window.google);
  if (!GOOGLE_MAPS_API_KEY) return Promise.resolve(null);
  if (googleScriptPromise) return googleScriptPromise;

  googleScriptPromise = new Promise((resolve, reject) => {
    const script = document.createElement("script");
    script.src = `https://maps.googleapis.com/maps/api/js?key=${encodeURIComponent(
      GOOGLE_MAPS_API_KEY
    )}&libraries=places`;
    script.async = true;
    script.defer = true;
    script.onload = () => resolve(window.google || null);
    script.onerror = () => reject(new Error("google_places_load_failed"));
    document.head.appendChild(script);
  });

  return googleScriptPromise;
}

// -------------------- Sidebar helpers --------------------
function sidebarSet(html) {
  const sidebar = document.getElementById("sidebar");
  sidebar.style.display = "block";
  sidebar.innerHTML = html;
}
function sidebarClose() {
  const sidebar = document.getElementById("sidebar");
  sidebar.style.display = "none";
  sidebar.innerHTML = "";
  destroyMiniMap();
}

function sidebarHeader(title) {
  return `
    <div class="sb__top">
      <div class="sb__title">${escapeHtml(title)}</div>
      <button id="closeSide" class="sb__close">Fermer</button>
    </div>
  `;
}

function sidebarCard(title, innerHtml) {
  return `
    <div class="sb__card">
      <div class="sb__cardTitle">${escapeHtml(title)}</div>
      <div class="sb__cardBody">${innerHtml}</div>
    </div>
  `;
}

function wireSidebarClose() {
  const btn = document.getElementById("closeSide");
  if (btn) btn.onclick = sidebarClose;
}

function wireSidebarActionButtons() {
  const buttons = document.querySelectorAll("[data-open-copro-id]");
  for (const btn of buttons) {
    btn.onclick = async () => {
      const coproId = Number(btn.getAttribute("data-open-copro-id"));
      const lon = Number(btn.getAttribute("data-lon"));
      const lat = Number(btn.getAttribute("data-lat"));
      const coords =
        Number.isFinite(lon) && Number.isFinite(lat) ? [lon, lat] : undefined;

      if (coords) {
        map.easeTo({ center: coords, zoom: Math.max(map.getZoom(), 17) });
      }

      if (Number.isFinite(coproId)) {
        await openCoproDetails(coproId, coords);
      }
    };
  }
}

function sidebarLoading(label = "Chargement…") {
  sidebarSet(`
    ${sidebarHeader("Fiche immeuble")}
    ${sidebarCard("Données DPE", `<div class="sb__loading"><div class="spinner"></div><div>${escapeHtml(label)}</div></div>`)}
  `);
  wireSidebarClose();
}

// -------------------- Popup premium --------------------
function popupHtml({ title, classe, color, statut, conso, ges, confScore, confLabel, source, date, loading }) {
  if (loading) {
    return `
      <div class="popup-card">
        <div class="popup-title">${escapeHtml(title || "Sélection")}</div>
        <div class="popup-loading"><div class="spinner"></div><div>Chargement du DPE…</div></div>
      </div>
    `;
  }

  const meta = [
    statut ? escapeHtml(statut) : "—",
    date ? escapeHtml(date) : null,
  ].filter(Boolean).join(" · ");

  const confLine = [
    confScore !== "" && confScore !== null && confScore !== undefined ? escapeHtml(String(confScore)) : "—",
    confLabel ? escapeHtml(confLabel) : null,
  ].filter(Boolean).join(" · ");

  return `
    <div class="popup-card">
      <div class="popup-title">${escapeHtml(title)}</div>

      <div class="popup-row">
        <div class="popup-badge" style="background:${color};">${escapeHtml(classe)}</div>
        <div class="popup-meta">${meta}</div>
      </div>

      <div class="popup-info">
        <div><b>Conso:</b> ${escapeHtml(conso ?? "—")} kWh/m²/an</div>
        <div><b>GES:</b> ${escapeHtml(ges ?? "NC")}</div>
        <div><b>Confiance:</b> ${confLine}</div>
      </div>

      <div class="popup-source">${escapeHtml(source || "Source: ADEME")}</div>
    </div>
  `;
}

function openPremiumPopup({ lngLat, kind, payload }) {
  if (kind === "address" && addressPopup) addressPopup.remove();
  if (kind === "copro" && coproPopup) coproPopup.remove();

  const pop = new maplibregl.Popup({
    closeButton: true,
    closeOnClick: false,
    className: "premium-popup",
    maxWidth: "360px",
  })
    .setLngLat(lngLat)
    .setHTML(popupHtml(payload))
    .addTo(map);

  if (kind === "address") addressPopup = pop;
  else coproPopup = pop;

  return pop;
}

function updatePremiumPopup(popup, payload) {
  if (!popup) return;
  popup.setHTML(popupHtml(payload));
}

// -------------------- GoRenov renderer (unique adresse + copro) --------------------
function prettifyLabel(k) {
  return String(k || "")
    .replaceAll("_", " ")
    .replace(/\b\w/g, (m) => m.toUpperCase());
}

function renderRowsFromObject(obj) {
  const keys = Object.keys(obj || {});
  const rows = keys
    .map((k) => {
      const v = obj?.[k];
      if (v === null || v === undefined || v === "" || (typeof v === "number" && !Number.isFinite(v))) return null;
      return `
        <div class="sb__row">
          <div class="sb__k">${escapeHtml(prettifyLabel(k))}</div>
          <div class="sb__v">${escapeHtml(String(v))}</div>
        </div>
      `;
    })
    .filter(Boolean)
    .join("");

  if (!rows) return `<div class="sb__muted">—</div>`;
  return `<div class="sb__rows">${rows}</div>`;
}

function destroyMiniMap() {
  try {
    if (miniMap) miniMap.remove();
  } catch {}
  miniMap = null;
  miniMapMarker = null;
}

function ensureMiniMap({ lon, lat, label }) {
  const el = document.getElementById("sbMiniMap");
  if (!el) return;

  // (re)create map if needed
  if (!miniMap) {
    miniMap = new maplibregl.Map({
      container: el,
      interactive: false,
      attributionControl: false,
      style: {
        version: 8,
        sources: {
          osm: {
            type: "raster",
            tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
            tileSize: 256,
            attribution: "© OpenStreetMap contributors",
          },
        },
        layers: [{ id: "osm", type: "raster", source: "osm" }],
      },
      center: [lon, lat],
      zoom: 16,
    });

    miniMap.on("load", () => {
      miniMapMarker = new maplibregl.Marker({ color: "#60a5fa" })
        .setLngLat([lon, lat])
        .addTo(miniMap);
    });
  } else {
    miniMap.jumpTo({ center: [lon, lat], zoom: 16 });
    if (miniMapMarker) miniMapMarker.setLngLat([lon, lat]);
  }

  const badge = document.getElementById("sbMiniMapBadge");
  if (badge) badge.textContent = label || "Contexte";
}

function dpePalette(letter) {
  const map = {
    A: "#009A44",
    B: "#52B54A",
    C: "#C8D400",
    D: "#F4E500",
    E: "#F9B233",
    F: "#EA5B0C",
    G: "#C00000",
    NC: "#6B7280",
  };
  return map[String(letter || "NC").toUpperCase()] || map.NC;
}

function pickDpeRef(goRenov, dpeJson) {
  // 1) dpe représentatif (goRenov)
  const ref = goRenov?.sections?.dpe_representatif?.identifiant;
  if (ref) return String(ref);

  // 2) final / collectif
  const fin = dpeJson?.dpeImmeubleFinal?.numero_dpe || null;
  if (fin) return String(fin);

  const col = dpeJson?.dpeCollectifReel?.numero_dpe || null;
  if (col) return String(col);

  return "";
}

function buildHeaderPremium({ title, exportUrl }) {
  return `
    <div class="sbHead">
      <div class="sbHead__row">
        <div class="sbHead__title">${escapeHtml(title || "Fiche immeuble")}</div>
        <div class="sbHead__actions">
          ${exportUrl ? `<a class="sbBtn sbBtn--ghost" href="${exportUrl}" target="_blank" rel="noopener">Exporter tous les DPE</a>` : ""}
          <button id="sbOpenDetail" class="sbBtn sbBtn--ghost">Ouvrir la fiche détaillée</button>
          <button id="closeSide" class="sbBtn">Fermer</button>
        </div>
      </div>
      <div class="sbTabs">
        <button id="sbTabOverview" class="sbTab ${sidebarMode === "overview" ? "isActive" : ""}">Aperçu</button>
        <button id="sbTabDetail" class="sbTab ${sidebarMode === "detail" ? "isActive" : ""}">Détails</button>
      </div>
    </div>
  `;
}


function cardPremium(title, inner) {
  return `
    <div class="sbCard">
      <div class="sbCard__title">${escapeHtml(title)}</div>
      <div>${inner}</div>
    </div>
  `;
}

function renderDpeLadder({ activeClasse, metaRight }) {
  const classes = ["A","B","C","D","E","F","G"];
  const active = String(activeClasse || "NC").toUpperCase();

  const rows = classes.map((c) => {
    const isActive = c === active;
    return `
      <div class="sbDpeRow ${isActive ? "isActive" : ""}">
        <div class="sbDpeRow__left">
          <div class="sbDpeChip" style="background:${dpePalette(c)};">${c}</div>
          <div>${isActive ? "Classe actuelle" : " "}</div>
        </div>
        <div class="sbDpeMeta">${isActive ? escapeHtml(metaRight || "") : ""}</div>
      </div>
    `;
  }).join("");

  return `<div class="sbDpeLadder">${rows}</div>`;
}

function renderGoRenovSidebar({
  title,
  subtitle,
  goRenov,
  extraTopCardHtml,
  dpeJson,
  coords,
  exportUrl,
  extraOverviewCardsHtml = "",
  extraDetailCardsHtml = "",
}) {
  // persist for toggle (IMPORTANT: garder exportUrl)
  lastSidebarModel = {
    title,
    subtitle,
    goRenov,
    extraTopCardHtml,
    dpeJson,
    coords,
    exportUrl,
    extraOverviewCardsHtml,
    extraDetailCardsHtml,
  };

  const gr = goRenov || {};
  const grHeader = gr.header || {};
  const dpeBlock = gr.dpe || {};
  const sections = gr.sections || {};
  const meta = dpeBlock.meta || {};
  const final = dpeBlock.final || null;

  const classe = String(final?.classe || dpeJson?.dpeImmeubleFinal?.classe || "NC").toUpperCase();
  const conso = final?.conso_kwh_m2_an ?? dpeJson?.dpeImmeubleFinal?.conso_kwh_m2_an ?? null;
  const ges = final?.ges ?? dpeJson?.dpeImmeubleFinal?.ges ?? "NC";
  const confScore = final?.confiance?.score ?? dpeJson?.dpeImmeubleFinal?.confiance?.score ?? null;
  const confLabel = final?.confiance?.label ?? dpeJson?.dpeImmeubleFinal?.confiance?.label ?? null;

  const stats = dpeBlock?.stats || dpeJson?.stats || {};
  const rayon = stats?.rayon_m ?? null;
  const total = stats?.dpe_total ?? null;

  const addrLine = grHeader?.adresse || subtitle || "—";
  const titleLine = grHeader?.titre || "Immeuble";

  const dpeRef = pickDpeRef(goRenov, dpeJson);
  const ademePortal = "https://observatoire-dpe-audit.ademe.fr/";

  // Header (exportUrl peut être vide/undefined => ok)
  const header = buildHeaderPremium({ title, exportUrl });

  // Pills
  const pillsHtml = `
    <div class="sbPills">
      <div class="sbPill">${escapeHtml(classe)}<span>Classe DPE</span></div>
      <div class="sbPill">${escapeHtml(conso ?? "—")}<span>kWh/m²/an</span></div>
      <div class="sbPill">${escapeHtml(ges ?? "NC")}<span>GES</span></div>
      <div class="sbPill">${escapeHtml(confScore ?? "—")}<span>Confiance</span></div>
      <div class="sbPill">${escapeHtml(rayon ?? "—")}<span>Rayon (m)</span></div>
      <div class="sbPill">${escapeHtml(total ?? "—")}<span>DPE trouvés</span></div>
    </div>
  `;

  const recap = cardPremium(
    "Résumé",
    `
      <div style="font-weight:950; font-size:15px; line-height:1.35;">${escapeHtml(titleLine)}</div>
      <div class="sbCard__sub" style="margin-top:6px;">${escapeHtml(addrLine)}</div>
      ${extraTopCardHtml ? `<div style="margin-top:12px;">${extraTopCardHtml}</div>` : ""}
    `
  );

  const dpeCard = cardPremium(
    "DPE officiel ADEME",
    `
      ${renderDpeLadder({
        activeClasse: classe,
        metaRight: confLabel ? `${confLabel}` : "",
      })}
      <div style="margin-top:12px;">${pillsHtml}</div>

      <div class="sbActionsRow" style="margin-top:12px;">
        <a class="sbLinkBtn" href="${ademePortal}" target="_blank" rel="noopener">
          Consulter le site de l’ADEME
        </a>
        <button id="sbCopyRef" class="sbBtn sbBtn--ghost">
          Copier la référence DPE
        </button>
      </div>

      ${dpeRef
        ? `<div class="sbMuted" style="margin-top:10px;">Référence : <b>${escapeHtml(dpeRef)}</b></div>`
        : `<div class="sbMuted" style="margin-top:10px;">Référence DPE : —</div>`
      }
      <div class="sbMuted" style="margin-top:6px;">
        Astuce : ouvre le portail ADEME puis colle la référence ci-dessus.
      </div>
    `
  );

  // Sections details (sans conso_reelle / identification déjà supprimées)
  const sectionOrder = [
    ["caracteristiques", "Caractéristiques"],
    ["systemes", "Systèmes (chauffage / ECS / ventilation / clim)"],
    ["enveloppe", "Enveloppe"],
    ["vitrage", "Vitrage"],
    ["simulations", "Simulations"],
    ["dpe_representatif", "DPE représentatif"],
  ];

  const detailCards = sectionOrder
    .map(([k, label]) => {
      const obj = sections?.[k] || null;
      if (!obj) return null;
      return cardPremium(label, renderRowsFromObject(obj));
    })
    .filter(Boolean)
    .join("");

  const detailsEmpty = !detailCards
    ? cardPremium("Détails", `<div class="sbMuted">Aucune section GoRenov disponible.</div>`)
    : "";

  const overviewBody = `
    <div class="sbHero">
      ${dpeCard}
    </div>
    ${extraOverviewCardsHtml}
    ${recap}
  `;

  const detailBody = `
    ${recap}
    ${extraDetailCardsHtml}
    ${detailCards}
    ${detailsEmpty}
  `;

  sidebarSet(`${header}${sidebarMode === "overview" ? overviewBody : detailBody}`);
  wireSidebarClose();

  // Tabs + bouton "ouvrir fiche détaillée"
  const btnOpenDetail = document.getElementById("sbOpenDetail");
  if (btnOpenDetail) {
    btnOpenDetail.onclick = () => {
      sidebarMode = "detail";
      if (lastSidebarModel) renderGoRenovSidebar(lastSidebarModel);
    };
  }

  const t1 = document.getElementById("sbTabOverview");
  const t2 = document.getElementById("sbTabDetail");
  if (t1) t1.onclick = () => { sidebarMode = "overview"; if (lastSidebarModel) renderGoRenovSidebar(lastSidebarModel); };
  if (t2) t2.onclick = () => { sidebarMode = "detail"; if (lastSidebarModel) renderGoRenovSidebar(lastSidebarModel); };

  // Copier ref
  const copyBtn = document.getElementById("sbCopyRef");
  if (copyBtn) {
    copyBtn.onclick = async () => {
      if (!dpeRef) return;
      try {
        await navigator.clipboard.writeText(dpeRef);
        copyBtn.textContent = "Référence copiée";
        setTimeout(() => (copyBtn.textContent = "Copier la référence DPE"), 1200);
      } catch {
        window.prompt("Copie la référence DPE :", dpeRef);
      }
    };
  }

  // Mini-map supprimée comme demandé
  destroyMiniMap();
  wireSidebarActionButtons();
}



function buildDpeContextCards(dpeJson) {
  const cards = [];
  const reel = dpeJson?.dpeCollectifReel || null;
  const sim = dpeJson?.dpeImmeubleSimule || null;
  const stats = dpeJson?.stats || {};
  const meta = dpeJson?.meta || {};

  if (reel) {
    cards.push(
      cardPremium(
        "Dernier DPE collectif",
        renderRowsFromObject({
          statut: reel.statut,
          classe: reel.classe,
          conso_kwh_m2_an: reel.conso_kwh_m2_an,
          ges: reel.ges,
          date: reel.date,
          numero_dpe: reel.numero_dpe,
        })
      )
    );
  }

  if (sim) {
    cards.push(
      cardPremium(
        reel ? "Simulation immeuble disponible" : "Simulation immeuble",
        renderRowsFromObject({
          statut: sim.statut,
          classe: sim.classe,
          conso_kwh_m2_an: sim.conso_kwh_m2_an,
          methode: sim.methode,
        })
      )
    );
  }

  cards.push(
    cardPremium(
      "Base de calcul",
      renderRowsFromObject({
        dpe_total: stats.dpe_total,
        rayon_m: stats.rayon_m,
        final_source: meta.final_source,
        indiv_used_for_simulation: meta.indiv_used_for_simulation,
        indiv_eligibles: meta.indiv_eligibles,
      })
    )
  );

  return cards.join("");
}

function renderNearbyCoprosCard(candidates) {
  if (!Array.isArray(candidates) || candidates.length === 0) {
    return cardPremium(
      "Copros proches de l'adresse",
      `<div class="sbMuted">Aucune copropriÃ©tÃ© du registre n'a Ã©tÃ© trouvÃ©e Ã  proximitÃ© immÃ©diate de cette adresse.</div>`
    );
  }

  const best = candidates[0];
  const hint = Number.isFinite(best?.distance_m)
    ? `Meilleure correspondance actuelle Ã  ${best.distance_m} m.`
    : "Plusieurs copropriÃ©tÃ©s sont proches de l'adresse.";

  const items = candidates
    .map((copro) => {
      const title =
        copro.nom_copro ||
        [copro.adresse, copro.code_postal, copro.commune].filter(Boolean).join(" ") ||
        `Copro #${copro.id}`;
      const meta = [
        Number.isFinite(copro.distance_m) ? `${copro.distance_m} m` : null,
        copro.syndic ? `Syndic: ${copro.syndic}` : null,
        copro.numero_immatriculation ? `Immat.: ${copro.numero_immatriculation}` : null,
      ]
        .filter(Boolean)
        .join(" · ");

      return `
        <button
          class="sbCandidate"
          data-open-copro-id="${Number(copro.id)}"
          data-lon="${Number(copro.lon)}"
          data-lat="${Number(copro.lat)}"
        >
          <span class="sbCandidate__title">${escapeHtml(title)}</span>
          <span class="sbCandidate__meta">${escapeHtml(meta || "Ouvrir la fiche")}</span>
        </button>
      `;
    })
    .join("");

  return cardPremium(
    "Copros proches de l'adresse",
    `
      <div class="sbMuted" style="margin-bottom:10px;">${escapeHtml(hint)}</div>
      <div class="sbCandidateList">${items}</div>
    `
  );
}

// -------------------- API helpers (DPE) --------------------
async function fetchJsonCached(key, url) {
  if (dpePromiseCache.has(key)) return dpePromiseCache.get(key);
  const p = (async () => {
    const r = await fetch(url);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return r.json();
  })();
  dpePromiseCache.set(key, p);
  try {
    return await p;
  } catch (e) {
    dpePromiseCache.delete(key);
    throw e;
  }
}

async function fetchDpeForCoproIdFull(id) {
  return fetchJsonCached(`copro:${id}`, `${API_BASE}/copros/${id}/dpe`);
}

async function fetchDpeForLatLonFull({ lat, lon, r = 30 }) {
  return fetchJsonCached(`addr:${lat.toFixed(6)},${lon.toFixed(6)},${r}`, `${API_BASE}/dpe/around?lat=${lat}&lon=${lon}&r=${r}`);
}

async function fetchNearbyCopros({ lat, lon, r = 120, limit = 8 }) {
  const res = await fetch(`${API_BASE}/copros/nearby?lat=${lat}&lon=${lon}&r=${r}&limit=${limit}`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const json = await res.json();
  return Array.isArray(json?.items) ? json.items : [];
}

async function fetchCoproDpeSummaries(ids) {
  if (!Array.isArray(ids) || ids.length === 0) return [];
  const res = await fetch(`${API_BASE}/copros/dpe-summaries?ids=${ids.join(",")}`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const json = await res.json();
  return Array.isArray(json?.items) ? json.items : [];
}

function applyDpeSummaryToMap(summary) {
  if (!summary?.id) return;
  map.setFeatureState(
    { source: COPROS_SOURCE_ID, id: Number(summary.id) },
    {
      dpe_color: summary.classe_color || FALLBACK_POINT_COLOR,
      dpe_halo: summary.classe_color || FALLBACK_HALO_COLOR,
    }
  );
}

async function hydrateCoproColors(features = lastSearchFeatures) {
  if (!Array.isArray(features) || features.length === 0 || !map.getSource(COPROS_SOURCE_ID)) {
    return;
  }

  const center = map.getCenter();
  const ids = [];
  const seen = new Set();
  const sorted = features
    .slice()
    .sort((a, b) => distanceScoreFromCenter(a, center) - distanceScoreFromCenter(b, center));

  for (const feature of sorted) {
    const id = Number(feature?.properties?.id);
    if (!Number.isFinite(id) || seen.has(id)) continue;
    seen.add(id);
    ids.push(id);
  }

  for (const id of ids) {
    const cached = dpeSummaryCache.get(id);
    if (cached) applyDpeSummaryToMap(cached);
  }

  const missingIds = ids.filter((id) => !dpeSummaryCache.has(id)).slice(0, MAX_DPE_SUMMARY_IDS);
  if (missingIds.length === 0) return;

  const token = ++activeColorHydrationToken;
  try {
    const items = await fetchCoproDpeSummaries(missingIds);
    if (token !== activeColorHydrationToken) return;

    for (const item of items) {
      dpeSummaryCache.set(Number(item.id), item);
      applyDpeSummaryToMap(item);
    }
  } catch (err) {
    console.warn("Coloration DPE impossible", err);
  }
}

// -------------------- Map layers --------------------
function ensureSourcesAndBaseLayers() {
  if (!map.getSource(COPROS_SOURCE_ID)) {
    map.addSource(COPROS_SOURCE_ID, {
      type: "geojson",
      data: { type: "FeatureCollection", features: [] },
      cluster: true,
      clusterRadius: 55,
      clusterMaxZoom: 14,
      promoteId: "id", // IMPORTANT pour feature-state
    });
  }

  if (!map.getSource(ADDRESS_SOURCE_ID)) {
    map.addSource(ADDRESS_SOURCE_ID, {
      type: "geojson",
      data: { type: "FeatureCollection", features: [] },
    });
  }

  // Layers copros
  if (!map.getLayer("clusters")) {
    map.addLayer({
      id: "clusters",
      type: "circle",
      source: COPROS_SOURCE_ID,
      filter: ["has", "point_count"],
      paint: {
        "circle-radius": ["step", ["get", "point_count"], 18, 50, 24, 200, 30, 1000, 38],
        "circle-color": "rgba(15, 23, 42, 0.72)",
        "circle-stroke-color": "rgba(255,255,255,0.30)",
        "circle-stroke-width": 2,
        "circle-opacity": 0.92,
      },
    });

    map.addLayer({
      id: "cluster-count",
      type: "symbol",
      source: COPROS_SOURCE_ID,
      filter: ["has", "point_count"],
      layout: {
        "text-field": ["to-string", ["get", "point_count_abbreviated"]],
        "text-size": 12,
      },
      paint: {
        "text-color": "#ffffff",
        "text-opacity": 0.95,
      },
    });

    map.addLayer({
      id: "copro-halo",
      type: "circle",
      source: COPROS_SOURCE_ID,
      filter: ["!", ["has", "point_count"]],
      paint: {
        "circle-radius": 12,
        "circle-color": ["coalesce", ["feature-state", "dpe_halo"], FALLBACK_HALO_COLOR],
        "circle-opacity": 0.22,
      },
    });

    map.addLayer({
      id: "copro-dot",
      type: "circle",
      source: COPROS_SOURCE_ID,
      filter: ["!", ["has", "point_count"]],
      paint: {
        "circle-radius": 6,
        "circle-color": ["coalesce", ["feature-state", "dpe_color"], FALLBACK_POINT_COLOR],
        "circle-opacity": 0.95,
        "circle-stroke-width": 2,
        "circle-stroke-color": "rgba(15,23,42,0.55)",
      },
    });

    map.addLayer({
      id: "copro-hit",
      type: "circle",
      source: COPROS_SOURCE_ID,
      filter: ["!", ["has", "point_count"]],
      paint: {
        "circle-radius": 18,
        "circle-color": "rgba(0,0,0,0)",
        "circle-opacity": 0.01,
      },
    });

    map.on("click", "clusters", (e) => {
      const features = map.queryRenderedFeatures(e.point, { layers: ["clusters"] });
      const cluster = features?.[0];
      if (!cluster) return;

      const clusterId = cluster.properties.cluster_id;
      const src = map.getSource(COPROS_SOURCE_ID);

      src.getClusterExpansionZoom(clusterId, (err, zoom) => {
        if (err) return;
        map.easeTo({ center: cluster.geometry.coordinates, zoom });
      });
    });

    map.on("click", "copro-hit", async (e) => {
      const f = e.features?.[0];
      const id = f?.properties?.id;
      if (!id) return;
      const coords = f?.geometry?.coordinates;
      await openCoproDetails(id, coords);
    });

    map.on("mouseenter", "clusters", () => (map.getCanvas().style.cursor = "pointer"));
    map.on("mouseleave", "clusters", () => (map.getCanvas().style.cursor = ""));
    map.on("mouseenter", "copro-hit", () => (map.getCanvas().style.cursor = "pointer"));
    map.on("mouseleave", "copro-hit", () => (map.getCanvas().style.cursor = ""));
  }

  // Layers address (au-dessus)
  if (!map.getLayer("address-halo")) {
    map.addLayer({
      id: "address-halo",
      type: "circle",
      source: ADDRESS_SOURCE_ID,
      paint: {
        "circle-radius": 18,
        "circle-color": ["coalesce", ["get", "dpe_color"], FALLBACK_POINT_COLOR],
        "circle-opacity": 0.25,
      },
    });

    map.addLayer({
      id: "address-dot",
      type: "circle",
      source: ADDRESS_SOURCE_ID,
      paint: {
        "circle-radius": 10,
        "circle-color": ["coalesce", ["get", "dpe_color"], FALLBACK_POINT_COLOR],
        "circle-opacity": 1,
        "circle-stroke-width": 3,
        "circle-stroke-color": "#ffffff",
      },
    });

    map.moveLayer("address-halo");
    map.moveLayer("address-dot");
  }
}

function bringAddressToFront() {
  if (map.getLayer("address-halo")) map.moveLayer("address-halo");
  if (map.getLayer("address-dot")) map.moveLayer("address-dot");
}

// -------------------- Open details (copro) --------------------
let activeCoproRequestToken = 0;

async function openCoproDetails(coproId, coords) {
  const token = ++activeCoproRequestToken;

  // coordonnées sûres (évite crash si coords undefined)
  const safeLngLat =
    Array.isArray(coords) &&
    coords.length === 2 &&
    Number.isFinite(Number(coords[0])) &&
    Number.isFinite(Number(coords[1]))
      ? [Number(coords[0]), Number(coords[1])]
      : map.getCenter().toArray();

  // Loader immédiat (pas de "NC")
  sidebarLoading("Chargement DPE (copro)…");

  const popup = openPremiumPopup({
    lngLat: safeLngLat,
    kind: "copro",
    payload: { title: "Copropriété", loading: true },
  });

  let dpeJson;
  try {
    dpeJson = await fetchDpeForCoproIdFull(coproId);
    if (token !== activeCoproRequestToken) return;
  } catch (e) {
    if (token !== activeCoproRequestToken) return;

    updatePremiumPopup(popup, {
      title: "Copropriété",
      loading: false,
      classe: "—",
      color: FALLBACK_POINT_COLOR,
      statut: "—",
      conso: "—",
      ges: "—",
      confScore: "—",
      confLabel: "",
      source: "Erreur DPE",
    });

    sidebarSet(
      `${sidebarHeader("Fiche immeuble")}${sidebarCard(
        "Erreur",
        `<div class="sb__muted">Impossible de charger le DPE (copro).</div>`
      )}`
    );
    wireSidebarClose();
    return;
  }

  const copro = dpeJson?.copro || null;

  // Coloration point dès qu'on a l'info
  const fin = dpeJson?.dpeImmeubleFinal || null;
  const classe = String(fin?.classe || "NC").toUpperCase();
  const color = fin?.classe_color || FALLBACK_POINT_COLOR;
  dpeSummaryCache.set(Number(coproId), {
    id: Number(coproId),
    classe,
    classe_color: color,
  });

  // NOTE: promoteId: "id" => id numérique ok
  map.setFeatureState(
    { source: COPROS_SOURCE_ID, id: Number(coproId) },
    { dpe_color: color, dpe_halo: color }
  );

  // popup final
  updatePremiumPopup(popup, {
    title: copro?.nom_copro || "Copropriété",
    loading: false,
    classe,
    color,
    statut: fin?.statut || "—",
    conso: fin?.conso_kwh_m2_an ?? "—",
    ges: fin?.ges || "NC",
    date: fin?.date || "",
    confScore: fin?.confiance?.score ?? "—",
    confLabel: fin?.confiance?.label ?? "",
    source: "Source: ADEME (autour du point)",
  });

  // sidebar
  const extraTop = copro
    ? `
      <div class="sb__big">${escapeHtml(copro?.nom_copro || "—")}</div>
      <div class="sb__muted" style="margin-top:6px;">
        ${escapeHtml([copro?.adresse, copro?.code_postal, copro?.commune].filter(Boolean).join(" "))}
      </div>
      <div class="sb__muted" style="margin-top:10px;">
        Département: <b>${escapeHtml(copro?.departement || "—")}</b> · Syndic: <b>${escapeHtml(copro?.syndic || "—")}</b>
      </div>
    `
    : `<div class="sb__muted">Copro #${escapeHtml(String(coproId))}</div>`;

  const exportUrl = `${API_BASE}/copros/${Number(coproId)}/dpe/export_all.csv`;
  const dpeContextCards = buildDpeContextCards(dpeJson);

  renderGoRenovSidebar({
    title: "Fiche immeuble",
    subtitle: copro ? [copro?.adresse, copro?.code_postal, copro?.commune].filter(Boolean).join(" ") : "",
    goRenov: dpeJson?.goRenov || null,
    extraTopCardHtml: extraTop,
    dpeJson,
    coords: safeLngLat,
    exportUrl,
    extraOverviewCardsHtml: dpeContextCards,
    extraDetailCardsHtml: dpeContextCards,
  });


  // Bouton maps optionnel
  if (copro?.lat && copro?.lon) {
    const mapsBtn = `
      <a class="sb__btn" target="_blank" rel="noopener"
        href="https://www.google.com/maps?q=${encodeURIComponent(String(copro.lat) + "," + String(copro.lon))}">
        Ouvrir sur Google Maps
      </a>
    `;
    document.getElementById("sidebar")?.insertAdjacentHTML("beforeend", mapsBtn);
  }

  bringAddressToFront();
}



// -------------------- Address marker + popup + sidebar --------------------
async function setAddressMarker({ lat, lon, label }) {
  // Marker immédiat en neutre (pas NC)
  const geo = {
    type: "FeatureCollection",
    features: [
      {
        type: "Feature",
        geometry: { type: "Point", coordinates: [lon, lat] },
        properties: {
          dpe_color: FALLBACK_POINT_COLOR,
          label: label || "Adresse",
        },
      },
    ],
  };
  map.getSource(ADDRESS_SOURCE_ID)?.setData(geo);
  bringAddressToFront();

sidebarLoading("Chargement DPE (adresse)…");

const popup = openPremiumPopup({
  lngLat: [lon, lat],
  kind: "address",
  payload: { title: label || "Adresse", loading: true },
});



  let dpeJson = null;
  try {
    dpeJson = await fetchDpeForLatLonFull({ lat, lon, r: 30 });
  } catch (e) {
    updatePremiumPopup(popup, { title: label || "Adresse", loading: false, classe: "—", color: FALLBACK_POINT_COLOR, statut: "—", conso: "—", ges: "—", confScore: "—", confLabel: "", source: "Erreur DPE" });
    sidebarSet(`${sidebarHeader("Adresse")}${sidebarCard("Erreur", `<div class="sb__muted">Impossible de charger le DPE (adresse).</div>`)}`);
    wireSidebarClose();
    return null;
  }

  const fin = dpeJson?.dpeImmeubleFinal || null;
  const classe = String(fin?.classe || "NC").toUpperCase();
  const color = fin?.classe_color || FALLBACK_POINT_COLOR;

  // Mise à jour couleur marker adresse
  geo.features[0].properties.dpe_color = color;
  map.getSource(ADDRESS_SOURCE_ID)?.setData(geo);
  bringAddressToFront();

  updatePremiumPopup(popup, {
    title: label || "Adresse",
    loading: false,
    classe,
    color,
    statut: fin?.statut || "—",
    conso: fin?.conso_kwh_m2_an ?? "—",
    ges: fin?.ges || "NC",
    date: fin?.date || "",
    confScore: fin?.confiance?.score ?? "—",
    confLabel: fin?.confiance?.label ?? "",
    source: "Source: ADEME (autour du point)",
  });

  renderGoRenovSidebar({
  title: "Adresse",
  subtitle: label || "",
  goRenov: dpeJson?.goRenov || null,
  extraTopCardHtml: `<div style="font-weight:950; font-size:15px;">${escapeHtml(label || "—")}</div>`,
  dpeJson,                 // <-- AJOUT
  coords: [lon, lat],       // <-- AJOUT
});


  return dpeJson;
}

async function setAddressMarkerEnhanced({ lat, lon, label }) {
  const geo = {
    type: "FeatureCollection",
    features: [
      {
        type: "Feature",
        geometry: { type: "Point", coordinates: [lon, lat] },
        properties: {
          dpe_color: FALLBACK_POINT_COLOR,
          label: label || "Adresse",
        },
      },
    ],
  };

  map.getSource(ADDRESS_SOURCE_ID)?.setData(geo);
  bringAddressToFront();

  sidebarLoading("Chargement DPE (adresse)…");

  const popup = openPremiumPopup({
    lngLat: [lon, lat],
    kind: "address",
    payload: { title: label || "Adresse", loading: true },
  });

  let dpeJson = null;
  try {
    dpeJson = await fetchDpeForLatLonFull({ lat, lon, r: 30 });
  } catch (e) {
    updatePremiumPopup(popup, {
      title: label || "Adresse",
      loading: false,
      classe: "—",
      color: FALLBACK_POINT_COLOR,
      statut: "—",
      conso: "—",
      ges: "—",
      confScore: "—",
      confLabel: "",
      source: "Erreur DPE",
    });
    sidebarSet(
      `${sidebarHeader("Adresse")}${sidebarCard(
        "Erreur",
        `<div class="sb__muted">Impossible de charger le DPE (adresse).</div>`
      )}`
    );
    wireSidebarClose();
    return null;
  }

  const fin = dpeJson?.dpeImmeubleFinal || null;
  const classe = String(fin?.classe || "NC").toUpperCase();
  const color = fin?.classe_color || FALLBACK_POINT_COLOR;

  geo.features[0].properties.dpe_color = color;
  map.getSource(ADDRESS_SOURCE_ID)?.setData(geo);
  bringAddressToFront();

  updatePremiumPopup(popup, {
    title: label || "Adresse",
    loading: false,
    classe,
    color,
    statut: fin?.statut || "—",
    conso: fin?.conso_kwh_m2_an ?? "—",
    ges: fin?.ges || "NC",
    date: fin?.date || "",
    confScore: fin?.confiance?.score ?? "—",
    confLabel: fin?.confiance?.label ?? "",
    source: "Source: ADEME (autour du point)",
  });

  let nearbyCopros = [];
  try {
    nearbyCopros = await fetchNearbyCopros({ lat, lon, r: 120, limit: 8 });
  } catch (err) {
    console.warn("Recherche de copro proche impossible", err);
  }

  const addressContextCards =
    renderNearbyCoprosCard(nearbyCopros) +
    buildDpeContextCards(dpeJson);

  renderGoRenovSidebar({
    title: "Adresse",
    subtitle: label || "",
    goRenov: dpeJson?.goRenov || null,
    extraTopCardHtml: `<div style="font-weight:950; font-size:15px;">${escapeHtml(label || "—")}</div>`,
    dpeJson,
    coords: [lon, lat],
    extraOverviewCardsHtml: addressContextCards,
    extraDetailCardsHtml: addressContextCards,
  });

  return dpeJson;
}

// -------------------- Fetch copros (action utilisateur) --------------------
async function fetchCopros() {
  if (!map.loaded()) {
    setStatus("Carte en chargement…");
    return;
  }

  const q = document.getElementById("q").value.trim();
  const syndic = document.getElementById("syndic").value.trim();
  const departement = document.getElementById("dep").value.trim();

  if (!q && !syndic && !departement) {
    setStatus("Renseigne au moins un critère (adresse, syndic ou département).");
    return;
  }

  const bbox = bboxFromMap();
  const params = new URLSearchParams({ bbox, limit: "12000" });
  if (q) params.set("q", q);
  if (syndic) params.set("syndic", syndic);
  if (departement) params.set("departement", departement);

  const url = `${API_BASE}/copros?${params.toString()}`;
  const t0 = performance.now();

  setStatus("Recherche…");
  const res = await fetch(url);
  if (!res.ok) {
    setStatus(`Erreur HTTP ${res.status}`);
    return;
  }

  const geojson = await res.json();
  const ms = Math.round(performance.now() - t0);
  const features = Array.isArray(geojson?.features) ? geojson.features : [];
  setStatus(`${features.length.toLocaleString()} points · ${ms} ms`);

  // Affichage immédiat (couleur neutre via feature-state fallback)
  map.getSource(COPROS_SOURCE_ID)?.setData(geojson);
  bringAddressToFront();

  if (features.length === 0) sidebarClose();
}

async function runCoproSearch() {
  if (!map.loaded()) {
    setStatus("Carte en chargement…");
    return;
  }

  const filters = getSearchFilters();
  const hasFilters = Object.values(filters).some(Boolean);
  if (!hasFilters) {
    setStatus("Renseigne au moins un critère de recherche.");
    return;
  }

  const url = `${API_BASE}/copros?${buildSearchParams({ limit: 12000 }).toString()}`;
  const t0 = performance.now();

  setStatus("Recherche…");
  const res = await fetch(url);
  if (!res.ok) {
    setStatus(`Erreur HTTP ${res.status}`);
    return;
  }

  const geojson = await res.json();
  const ms = Math.round(performance.now() - t0);
  const features = Array.isArray(geojson?.features) ? geojson.features : [];
  lastSearchFeatures = features;

  setStatus(`${features.length.toLocaleString()} points · ${ms} ms`);
  map.getSource(COPROS_SOURCE_ID)?.setData(geojson);
  bringAddressToFront();

  if (features.length === 0) {
    sidebarClose();
    return;
  }

  await hydrateCoproColors(features);
}

function exportCurrentSearchCsv() {
  if (!map.loaded()) return;
  window.open(`${API_BASE}/export/copros_dpe.csv?${buildSearchParams({ limit: 5000 }).toString()}`, "_blank");
}

// -------------------- Export CSV --------------------
function exportCsv() {
  if (!map.loaded()) return;

  const q = document.getElementById("q").value.trim();
  const syndic = document.getElementById("syndic").value.trim();
  const departement = document.getElementById("dep").value.trim();

  const bbox = bboxFromMap();
  const params = new URLSearchParams({ bbox, limit: "5000" });
  if (q) params.set("q", q);
  if (syndic) params.set("syndic", syndic);
  if (departement) params.set("departement", departement);

  window.open(`${API_BASE}/export/copros_dpe.csv?${params.toString()}`, "_blank");
}

// -------------------- Google Autocomplete --------------------
function initGoogleAutocomplete() {
  if (!window.google?.maps?.places) {
    console.warn("Google Places non chargé (script manquant ou clé invalide).");
    return;
  }

  const input = document.getElementById("q");
  const ac = new google.maps.places.Autocomplete(input, {
    types: ["geocode"],
    componentRestrictions: { country: "fr" },
    fields: ["geometry", "formatted_address"],
  });

  ac.addListener("place_changed", async () => {
    const place = ac.getPlace();
    const loc = place?.geometry?.location;
    if (!loc) return;

    const lat = loc.lat();
    const lon = loc.lng();

    if (place.formatted_address) input.value = place.formatted_address;

    map.easeTo({ center: [lon, lat], zoom: 16 });

    await setAddressMarker({
      lat,
      lon,
      label: place.formatted_address || input.value || "Adresse",
    });

        // IMPORTANT: pas de recherche copros automatique sur sélection adresse.
    // L'utilisateur clique "Rechercher" s'il veut afficher les copros.

  });
}

async function initGoogleAutocompleteEnhanced() {
  const hint = document.getElementById("qHint");

  let googleApi = null;
  try {
    googleApi = await ensureGooglePlacesLoaded();
  } catch (err) {
    console.warn("Google Places non chargÃ©", err);
  }

  if (!googleApi?.maps?.places) {
    if (hint) {
      hint.textContent = GOOGLE_MAPS_API_KEY
        ? "Autocomplete Google indisponible pour le moment"
        : "Renseigne VITE_GOOGLE_MAPS_API_KEY pour activer l'autocomplete Google";
    }
    return;
  }

  if (hint) {
    hint.textContent = "Autocomplete Google active + repÃ©rage des copropriÃ©tÃ©s proches";
  }

  const input = document.getElementById("q");
  const ac = new googleApi.maps.places.Autocomplete(input, {
    types: ["geocode"],
    componentRestrictions: { country: "fr" },
    fields: ["geometry", "formatted_address"],
  });

  ac.addListener("place_changed", async () => {
    const place = ac.getPlace();
    const loc = place?.geometry?.location;
    if (!loc) return;

    const lat = loc.lat();
    const lon = loc.lng();

    if (place.formatted_address) input.value = place.formatted_address;

    map.easeTo({ center: [lon, lat], zoom: 16 });

    await setAddressMarkerEnhanced({
      lat,
      lon,
      label: place.formatted_address || input.value || "Adresse",
    });
  });
}

// -------------------- UI events --------------------
document.getElementById("refresh").addEventListener("click", runCoproSearch);

document.getElementById("clear").addEventListener("click", () => {
  document.getElementById("q").value = "";
  document.getElementById("copro").value = "";
  document.getElementById("syndic").value = "";
  document.getElementById("commune").value = "";
  document.getElementById("cp").value = "";
  document.getElementById("dep").value = "";
  document.getElementById("immat").value = "";
  sidebarClose();
  lastSearchFeatures = [];

  map.getSource(COPROS_SOURCE_ID)?.setData({ type: "FeatureCollection", features: [] });
  map.getSource(ADDRESS_SOURCE_ID)?.setData({ type: "FeatureCollection", features: [] });

  if (addressPopup) addressPopup.remove();
  addressPopup = null;
  if (coproPopup) coproPopup.remove();
  coproPopup = null;

  setStatus("Fais une recherche pour afficher les résultats.");
});

document.getElementById("export").addEventListener("click", exportCurrentSearchCsv);

for (const id of ["q", "copro", "syndic", "commune", "cp", "dep", "immat"]) {
  const el = document.getElementById(id);
  el.addEventListener("keydown", (e) => {
    if (e.key === "Enter") runCoproSearch();
  });
}

// -------------------- Init --------------------
map.on("load", () => {
  ensureSourcesAndBaseLayers();
  setStatus("Fais une recherche pour afficher les résultats.");
  initGoogleAutocompleteEnhanced();
});
