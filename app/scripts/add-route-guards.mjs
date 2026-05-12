// One-shot : injecte le guard ensureAuth() en début de chaque handler API
// métier qui n'est pas déjà protégé. À run une seule fois, puis le commit fait foi.
import fs from "node:fs";
import path from "node:path";

const apiRoot = path.resolve(import.meta.dirname, "../src/app/api");

const SKIP = new Set([
  "auth/login",
  "auth/logout",
  "auth/me",
  "auth/change-password",
  "admin/users",
  "admin/users/[id]",
  "comments",
  "notifications",
  "notifications/[id]",
  "notifications/mark-all-read",
  "users/mention-search",
  "today",
  "syndics/[slug]",
]);

function walk(dir) {
  const out = [];
  for (const name of fs.readdirSync(dir)) {
    const full = path.join(dir, name);
    if (fs.statSync(full).isDirectory()) out.push(...walk(full));
    else if (name === "route.ts") out.push(full);
  }
  return out;
}

const files = walk(apiRoot);
let modified = 0;
let already = 0;
let skipped = 0;

for (const file of files) {
  const rel = path
    .relative(apiRoot, file)
    .replace(/\\/g, "/")
    .replace(/\/route\.ts$/, "");
  if (SKIP.has(rel)) {
    skipped++;
    continue;
  }

  let src = fs.readFileSync(file, "utf8");

  if (/ensureAuth\b|requireUser\b|requireAdmin\b/.test(src)) {
    already++;
    continue;
  }

  if (!/from\s+"@\/lib\/auth\/guards"/.test(src)) {
    const lastImport = src.match(/^(?:import[^\n]*\n)+/m);
    if (lastImport) {
      const at = lastImport[0].length;
      src =
        src.slice(0, at) +
        'import { ensureAuth } from "@/lib/auth/guards";\n' +
        src.slice(at);
    } else {
      src =
        'import { NextResponse } from "next/server";\nimport { ensureAuth } from "@/lib/auth/guards";\n' +
        src;
    }
  }

  if (!/\bNextResponse\b/.test(src)) {
    if (/from\s+"next\/server"/.test(src)) {
      src = src.replace(
        /import\s+\{([^}]*)\}\s+from\s+"next\/server"/,
        (m, names) => {
          if (/\bNextResponse\b/.test(names)) return m;
          return `import {${names.trim()}, NextResponse} from "next/server"`;
        },
      );
    } else {
      src = 'import { NextResponse } from "next/server";\n' + src;
    }
  }

  const before = src;
  src = src.replace(
    /(export\s+async\s+function\s+(?:GET|POST|PATCH|PUT|DELETE)\s*\([^)]*\)\s*\{)/g,
    "$1\n  const guard = await ensureAuth();\n  if (guard instanceof NextResponse) return guard;",
  );
  if (src === before) continue;

  fs.writeFileSync(file, src, "utf8");
  modified++;
  console.log("MODIFIED:", rel);
}

console.log(
  `\nDone. modified=${modified} already=${already} skipped=${skipped}`,
);
