import { NextRequest } from "next/server";
import { handleMaisonsAround } from "@/lib/api/maisons-around";

export const runtime = "nodejs";
export const maxDuration = 60;

export async function GET(req: NextRequest) {
  return handleMaisonsAround(req, "appartement");
}
