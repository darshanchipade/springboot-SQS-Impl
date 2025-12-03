import { NextRequest, NextResponse } from "next/server";

const backendBaseUrl = process.env.SPRINGBOOT_BASE_URL;

const safeStringify = (input: unknown) => {
  try {
    return JSON.stringify(input);
  } catch {
    return null;
  }
};

const safeParse = (payload: string) => {
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

  let incoming: unknown;
  try {
    incoming = await request.json();
  } catch {
    return NextResponse.json(
      { error: "Request body must be valid JSON." },
      { status: 400 },
    );
  }

  const payload =
    typeof incoming === "object" && incoming !== null
      ? (incoming as Record<string, unknown>).payload
      : undefined;

  if (payload === undefined) {
    return NextResponse.json(
      { error: "Missing `payload` attribute." },
      { status: 400 },
    );
  }

  const serialized = safeStringify(payload);
  if (serialized === null) {
    return NextResponse.json(
      { error: "Payload could not be serialized." },
      { status: 400 },
    );
  }

  try {
    const upstream = await fetch(
      `${backendBaseUrl}/api/ingest-json-payload`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: serialized,
      },
    );

    const rawBody = await upstream.text();
    const body = safeParse(rawBody);

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
