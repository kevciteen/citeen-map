import ExcelJS from "exceljs";

export const DPE_COLORS: Record<string, { bg: string; fg: string }> = {
  A: { bg: "FF1F9D55", fg: "FFFFFFFF" },
  B: { bg: "FF7CB342", fg: "FFFFFFFF" },
  C: { bg: "FFCDDC39", fg: "FF1F2937" },
  D: { bg: "FFFFEB3B", fg: "FF1F2937" },
  E: { bg: "FFFFB300", fg: "FFFFFFFF" },
  F: { bg: "FFFB8C00", fg: "FFFFFFFF" },
  G: { bg: "FFE53935", fg: "FFFFFFFF" },
  NC: { bg: "FF94A3B8", fg: "FFFFFFFF" },
};

export const QUALITY_COLORS: Record<string, { bg: string; fg: string }> = {
  verified: { bg: "FF22C55E", fg: "FFFFFFFF" },
  approximate: { bg: "FFF59E0B", fg: "FFFFFFFF" },
  uncertain: { bg: "FFF87171", fg: "FFFFFFFF" },
  no_data: { bg: "FF94A3B8", fg: "FFFFFFFF" },
};

export function applyHeaderStyle(row: ExcelJS.Row) {
  row.height = 22;
  row.eachCell((cell) => {
    cell.font = { bold: true, color: { argb: "FFFFFFFF" }, size: 11 };
    cell.fill = {
      type: "pattern",
      pattern: "solid",
      fgColor: { argb: "FF2563EB" }, // primary blue
    };
    cell.alignment = { vertical: "middle", horizontal: "left" };
    cell.border = {
      bottom: { style: "thin", color: { argb: "FF1E40AF" } },
    };
  });
}

export function styleDpeCell(cell: ExcelJS.Cell, classe: string | null | undefined) {
  const k = String(classe ?? "NC").toUpperCase();
  const c = DPE_COLORS[k] ?? DPE_COLORS.NC;
  cell.value = k === "NC" || !classe ? "—" : k;
  cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: c.bg } };
  cell.font = { bold: true, color: { argb: c.fg }, size: 12 };
  cell.alignment = { vertical: "middle", horizontal: "center" };
}

export function styleQualityCell(cell: ExcelJS.Cell, quality: string | null | undefined) {
  if (!quality) {
    cell.value = "—";
    return;
  }
  const c = QUALITY_COLORS[quality] ?? QUALITY_COLORS.no_data;
  const labels: Record<string, string> = {
    verified: "Vérifié",
    approximate: "Approximatif",
    uncertain: "Incertain",
    no_data: "Aucune donnée",
  };
  cell.value = labels[quality] ?? quality;
  cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: c.bg } };
  cell.font = { bold: true, color: { argb: c.fg }, size: 10 };
  cell.alignment = { vertical: "middle", horizontal: "center" };
}

export function freezeAndZebra(ws: ExcelJS.Worksheet) {
  ws.views = [{ state: "frozen", ySplit: 1 }];
  // Zebra striping
  for (let i = 2; i <= ws.rowCount; i++) {
    if (i % 2 === 0) {
      ws.getRow(i).eachCell((cell) => {
        if (!cell.fill || (cell.fill as ExcelJS.FillPattern).fgColor == null) {
          cell.fill = {
            type: "pattern",
            pattern: "solid",
            fgColor: { argb: "FFF8FAFC" },
          };
        }
      });
    }
  }
}

export function autoSizeColumns(ws: ExcelJS.Worksheet, minWidth = 10, maxWidth = 55) {
  ws.columns.forEach((col) => {
    let max = minWidth;
    col.eachCell?.({ includeEmpty: false }, (cell) => {
      const val = cell.value;
      const len =
        typeof val === "string"
          ? val.length
          : val == null
            ? 0
            : String(val).length;
      if (len > max) max = len;
    });
    col.width = Math.min(max + 2, maxWidth);
  });
}
