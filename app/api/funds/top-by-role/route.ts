import { NextResponse } from "next/server";
import { buildTopByRoleResponse } from "@/lib/fund-data";
import type { TopByRoleRequest } from "@/lib/types";

export async function POST(request: Request) {
  try {
    const body = ((await request.json().catch(() => ({}))) ?? {}) as TopByRoleRequest;
    const response = await buildTopByRoleResponse(body);
    return NextResponse.json(response);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unable to build fund rankings.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
