import { NextRequest, NextResponse } from "next/server";
import {
  getSyndicFullDetail,
  patchSyndicContact,
  upsertSyndicContact,
} from "@/lib/services/syndic-storage";

export const runtime = "nodejs";
export const maxDuration = 20;

export async function GET(
  req: NextRequest,
  { params }: { params: Promise<{ slug: string }> },
) {
  const { slug } = await params;
  const fallbackName = req.nextUrl.searchParams.get("name") ?? undefined;
  const detail = await getSyndicFullDetail(slug, fallbackName);
  if (!detail) {
    return NextResponse.json(
      { error: "Syndic introuvable. Fournissez ?name= en query la 1re fois." },
      { status: 404 },
    );
  }
  return NextResponse.json(detail, {
    headers: {
      "Cache-Control": "private, max-age=60, stale-while-revalidate=300",
    },
  });
}

export async function PATCH(
  req: NextRequest,
  { params }: { params: Promise<{ slug: string }> },
) {
  const { slug } = await params;
  const body = (await req.json().catch(() => null)) as Record<
    string,
    unknown
  > | null;
  if (!body) {
    return NextResponse.json({ error: "JSON body requis" }, { status: 400 });
  }
  const name = typeof body.name === "string" ? body.name : null;
  // Champs éditables — null = effacer, undefined = ne touche pas
  const patch: Record<string, string | null> = {};
  const fields = [
    "email",
    "phone",
    "contact_person",
    "website",
    "address_override",
    "notes",
  ] as const;
  for (const f of fields) {
    if (f in body) {
      const v = body[f];
      patch[f] = typeof v === "string" && v.trim() !== "" ? v.trim() : null;
    }
  }

  // Premier appel : crée la ligne avec le nom canonique. Suivants : patch.
  if (name) {
    await upsertSyndicContact({
      slug,
      name,
      email: patch.email,
      phone: patch.phone,
      contactPerson: patch.contact_person,
      website: patch.website,
      addressOverride: patch.address_override,
      notes: patch.notes,
    });
  } else {
    await patchSyndicContact(slug, patch);
  }
  return NextResponse.json({ ok: true });
}
