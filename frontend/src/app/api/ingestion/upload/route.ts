import { NextRequest, NextResponse } from "next/server";

const backendBaseUrl = process.env.SPRINGBOOT_BASE_URL;

const safeParseJson = (payload: string) => {
  try {
    return JSON.parse(payload);
  } catch {
    return payload;
  }
};

export async function POST(request: NextRequest) {
  if (!backendBaseUrl) {
    return NextResponse.json(
      { error: "SPRINGBOOT_BASE_URL is not configured." },
      { status: 500 },
    );
  }

  const formData = await request.formData();

  try {
    const upstream = await fetch(
      `${backendBaseUrl}/api/extract-cleanse-enrich-and-store`,
      {
        method: "POST",
        body: formData,
      },
    );

    const rawBody = await upstream.text();
    const body = safeParseJson(rawBody);

    return NextResponse.json(
      {
        upstreamStatus: upstream.status,
        upstreamOk: upstream.ok,
        body,
        rawBody,
      },
      { status: upstream.status },
    );
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "Unable to reach Spring Boot backend.",
      },
      { status: 502 },
    );
  }
}
