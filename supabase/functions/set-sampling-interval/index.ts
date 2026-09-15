// Sets a device's sampling interval: validates the caller and the request,
// resolves the device's MAC under the caller's own RLS (no service_role),
// persists the request, then publishes it through the mqtt module — which
// is the only place that can reach the broker.
import { createClient } from "npm:@supabase/supabase-js@2";
import { parseMac, parseRequestBody } from "./parsers.ts";
import { publishSamplingInterval as defaultPublishSamplingInterval } from "./mqtt.ts";

interface DeviceRow {
  mac_address: string;
}

interface SupabaseLike {
  from(table: "devices"): {
    select(columns: string): {
      eq(column: string, value: string): {
        maybeSingle(): Promise<
          { data: DeviceRow | null; error: { message: string } | null }
        >;
      };
    };
  };
  from(table: "device_configs"): {
    upsert(
      row: Record<string, unknown>,
    ): Promise<{ error: { message: string } | null }>;
  };
}

interface Deps {
  createCallerClient: (authHeader: string) => SupabaseLike;
  publishSamplingInterval: (
    mac: string,
    samplingIntervalMs: number,
  ) => Promise<void>;
}

// The JWT is the access boundary; the browser only needs the preflight to pass.
const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

function jsonResponse(body: unknown, status: number): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}

/** Builds the request handler with its collaborators injected, so tests never touch the network. */
export function createHandler(deps: Deps) {
  return async function handleRequest(req: Request): Promise<Response> {
    if (req.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders });
    }
    if (req.method !== "POST") {
      return jsonResponse({ error: "method not allowed" }, 400);
    }

    const authHeader = req.headers.get("Authorization");
    if (!authHeader) {
      return jsonResponse({ error: "authentication required" }, 401);
    }

    let rawBody: unknown;
    try {
      rawBody = await req.json();
    } catch {
      return jsonResponse({ error: "invalid request body" }, 400);
    }

    const parsed = parseRequestBody(rawBody);
    if (!parsed) {
      return jsonResponse({ error: "invalid request body" }, 400);
    }

    const client = deps.createCallerClient(authHeader);

    const { data: device, error: readError } = await client
      .from("devices")
      .select("mac_address")
      .eq("id", parsed.deviceId)
      .maybeSingle();

    if (readError) {
      return jsonResponse({ error: "failed to read device" }, 500);
    }
    if (!device) {
      return jsonResponse({ error: "device not found" }, 404);
    }

    const mac = parseMac(device.mac_address);
    if (!mac) {
      return jsonResponse({ error: "device record is invalid" }, 500);
    }

    const { error: writeError } = await client.from("device_configs").upsert({
      device_id: parsed.deviceId,
      sampling_interval_ms: parsed.samplingIntervalMs,
      requested_at: new Date().toISOString(),
    });
    if (writeError) {
      return jsonResponse({ error: "failed to write configuration" }, 500);
    }

    try {
      await deps.publishSamplingInterval(mac, parsed.samplingIntervalMs);
    } catch {
      // device_configs already reflects the request; applied_at stays NULL, which is honest.
      return jsonResponse({ error: "failed to publish configuration" }, 502);
    }

    return jsonResponse(
      {
        ok: true,
        deviceId: parsed.deviceId,
        samplingIntervalMs: parsed.samplingIntervalMs,
      },
      200,
    );
  };
}

function createCallerClient(authHeader: string): SupabaseLike {
  const url = Deno.env.get("SUPABASE_URL")!;
  const anonKey = Deno.env.get("SUPABASE_ANON_KEY")!;
  return createClient(url, anonKey, {
    global: { headers: { Authorization: authHeader } },
  }) as unknown as SupabaseLike;
}

// Guarded so importing this module for tests never binds a real port.
if (import.meta.main) {
  Deno.serve(createHandler({
    createCallerClient,
    publishSamplingInterval: defaultPublishSamplingInterval,
  }));
}
