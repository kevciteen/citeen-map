import { NextRequest, NextResponse } from "next/server";
import { resolveSyndicByName } from "@/lib/services/syndic-contact";

export const runtime = "nodejs";
export const maxDuration = 15;

export async function GET(req: NextRequest) {
  const name = req.nextUrl.searchParams.get("name")?.trim();
  if (!name) {
    return NextResponse.json({ error: "?name= requis" }, { status: 400 });
  }
  try {
    const contact = await resolveSyndicByName(name);
    return NextResponse.json({ contact });
  } catch (err) {
    return NextResponse.json(
      { error: (err as Error).message, contact: null },
      { status: 500 },
    );
  }
}
