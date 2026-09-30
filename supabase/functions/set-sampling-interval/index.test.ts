import { assertEquals } from "jsr:@std/assert@1";
import { createHandler } from "./index.ts";

const DEVICE_ID = "4a1e6c1e-2f0a-4b0a-9c3a-5e6f7a8b9c0d";
const VALID_MAC = "4022D83D6618";

interface DeviceRow {
  mac_address: string;
}

// deno-lint-ignore no-explicit-any
type FakeSupabaseClient = any;

function buildFakeClient(options: {
  device?: DeviceRow | null;
  readError?: boolean;
  writeError?: boolean;
}) {
  const upsertCalls: Record<string, unknown>[] = [];
  const client: FakeSupabaseClient = {
    from(table: string) {
      if (table === "devices") {
        return {
          select(_columns: string) {
            return {
              eq(_column: string, _value: string) {
                return {
                  maybeSingle() {
                    if (options.readError) {
                      return Promise.resolve({
                        data: null,
                        error: { message: "boom" },
                      });
                    }
                    return Promise.resolve({
                      data: options.device ?? null,
                      error: null,
                    });
                  },
                };
              },
            };
          },
        };
      }
      if (table === "device_configs") {
        return {
          upsert(row: Record<string, unknown>) {
            upsertCalls.push(row);
            if (options.writeError) {
              return Promise.resolve({ error: { message: "boom" } });
            }
            return Promise.resolve({ error: null });
          },
        };
      }
      throw new Error(`unexpected table ${table}`);
    },
  };
  return { client, upsertCalls };
}

function buildRequest(body: unknown, headers: Record<string, string> = {}) {
  return new Request("http://localhost/set-sampling-interval", {
    method: "POST",
    headers: { "Content-Type": "application/json", ...headers },
    body: JSON.stringify(body),
  });
}

Deno.test("rejects a request without an Authorization header", async () => {
  const { client } = buildFakeClient({ device: { mac_address: VALID_MAC } });
  let published = false;
  const handle = createHandler({
    createCallerClient: () => client,
    createServiceClient: () => client,
    publishSamplingInterval: () => {
      published = true;
      return Promise.resolve();
    },
  });

  const res = await handle(
    buildRequest({ deviceId: DEVICE_ID, samplingIntervalMs: 5000 }),
  );

  assertEquals(res.status, 401);
  assertEquals(published, false);
});

Deno.test("rejects an invalid JSON body", async () => {
  const { client } = buildFakeClient({ device: { mac_address: VALID_MAC } });
  const handle = createHandler({
    createCallerClient: () => client,
    createServiceClient: () => client,
    publishSamplingInterval: () => Promise.resolve(),
  });

  const req = new Request("http://localhost/set-sampling-interval", {
    method: "POST",
    headers: {
      Authorization: "Bearer token",
      "Content-Type": "application/json",
    },
    body: "{not json",
  });

  const res = await handle(req);
  assertEquals(res.status, 400);
});

Deno.test("rejects an out-of-range samplingIntervalMs", async () => {
  const { client } = buildFakeClient({ device: { mac_address: VALID_MAC } });
  const handle = createHandler({
    createCallerClient: () => client,
    createServiceClient: () => client,
    publishSamplingInterval: () => Promise.resolve(),
  });

  const res = await handle(
    buildRequest({ deviceId: DEVICE_ID, samplingIntervalMs: 500 }, {
      Authorization: "Bearer token",
    }),
  );

  assertEquals(res.status, 400);
});

Deno.test("returns 404 for an unknown device", async () => {
  const { client } = buildFakeClient({ device: null });
  let published = false;
  const handle = createHandler({
    createCallerClient: () => client,
    createServiceClient: () => client,
    publishSamplingInterval: () => {
      published = true;
      return Promise.resolve();
    },
  });

  const res = await handle(
    buildRequest({ deviceId: DEVICE_ID, samplingIntervalMs: 5000 }, {
      Authorization: "Bearer token",
    }),
  );

  assertEquals(res.status, 404);
  assertEquals(published, false);
});

Deno.test("returns 500 for a malformed MAC on the device row", async () => {
  const { client } = buildFakeClient({ device: { mac_address: "not-a-mac" } });
  const handle = createHandler({
    createCallerClient: () => client,
    createServiceClient: () => client,
    publishSamplingInterval: () => Promise.resolve(),
  });

  const res = await handle(
    buildRequest({ deviceId: DEVICE_ID, samplingIntervalMs: 5000 }, {
      Authorization: "Bearer token",
    }),
  );

  assertEquals(res.status, 500);
});

Deno.test("returns 500 when the device_configs write fails", async () => {
  const { client } = buildFakeClient({
    device: { mac_address: VALID_MAC },
    writeError: true,
  });
  let published = false;
  const handle = createHandler({
    createCallerClient: () => client,
    createServiceClient: () => client,
    publishSamplingInterval: () => {
      published = true;
      return Promise.resolve();
    },
  });

  const res = await handle(
    buildRequest({ deviceId: DEVICE_ID, samplingIntervalMs: 5000 }, {
      Authorization: "Bearer token",
    }),
  );

  assertEquals(res.status, 500);
  assertEquals(published, false);
});

Deno.test("returns 502 when the publish fails but keeps the written config row", async () => {
  const { client, upsertCalls } = buildFakeClient({
    device: { mac_address: VALID_MAC },
  });
  const handle = createHandler({
    createCallerClient: () => client,
    createServiceClient: () => client,
    publishSamplingInterval: () =>
      Promise.reject(new Error("broker unavailable")),
  });

  const res = await handle(
    buildRequest({ deviceId: DEVICE_ID, samplingIntervalMs: 5000 }, {
      Authorization: "Bearer token",
    }),
  );

  assertEquals(res.status, 502);
  assertEquals(upsertCalls.length, 1);
  assertEquals(upsertCalls[0].sampling_interval_ms, 5000);
});

Deno.test("returns 200 with the confirmed configuration on success", async () => {
  const { client, upsertCalls } = buildFakeClient({
    device: { mac_address: VALID_MAC },
  });
  let publishedMac: string | undefined;
  let publishedMs: number | undefined;
  const handle = createHandler({
    createCallerClient: () => client,
    createServiceClient: () => client,
    publishSamplingInterval: (mac: string, ms: number) => {
      publishedMac = mac;
      publishedMs = ms;
      return Promise.resolve();
    },
  });

  const res = await handle(
    buildRequest({ deviceId: DEVICE_ID, samplingIntervalMs: 5000 }, {
      Authorization: "Bearer token",
    }),
  );

  assertEquals(res.status, 200);
  assertEquals(await res.json(), {
    ok: true,
    deviceId: DEVICE_ID,
    samplingIntervalMs: 5000,
  });
  assertEquals(publishedMac, VALID_MAC);
  assertEquals(publishedMs, 5000);
  assertEquals(upsertCalls[0].device_id, DEVICE_ID);
});

Deno.test("answers a CORS preflight without touching the database", async () => {
  let clientBuilt = false;
  const handle = createHandler({
    createCallerClient: () => {
      clientBuilt = true;
      throw new Error("must not be called");
    },
    createServiceClient: () => {
      clientBuilt = true;
      throw new Error("must not be called");
    },
    publishSamplingInterval: () => Promise.resolve(),
  });

  const res = await handle(
    new Request("http://localhost/set-sampling-interval", {
      method: "OPTIONS",
    }),
  );

  assertEquals(res.status, 204);
  assertEquals(res.headers.get("Access-Control-Allow-Origin"), "*");
  assertEquals(
    res.headers.get("Access-Control-Allow-Headers")?.includes("authorization"),
    true,
  );
  assertEquals(clientBuilt, false);
});

Deno.test("every JSON response carries the CORS origin header", async () => {
  const { client } = buildFakeClient({ device: { mac_address: VALID_MAC } });
  const handle = createHandler({
    createCallerClient: () => client,
    createServiceClient: () => client,
    publishSamplingInterval: () => Promise.resolve(),
  });

  const res = await handle(
    buildRequest({ deviceId: DEVICE_ID, samplingIntervalMs: 5000 }),
  );

  assertEquals(res.headers.get("Access-Control-Allow-Origin"), "*");
});

Deno.test("writes device_configs with the service client, never the caller's", async () => {
  const caller = buildFakeClient({ device: { mac_address: VALID_MAC } });
  const service = buildFakeClient({});
  const handle = createHandler({
    createCallerClient: () => caller.client,
    createServiceClient: () => service.client,
    publishSamplingInterval: () => Promise.resolve(),
  });

  const res = await handle(
    buildRequest({ deviceId: DEVICE_ID, samplingIntervalMs: 5000 }, {
      Authorization: "Bearer token",
    }),
  );

  assertEquals(res.status, 200);
  assertEquals(caller.upsertCalls.length, 0);
  assertEquals(service.upsertCalls.length, 1);
});

Deno.test("never builds the service client when the caller cannot see the device", async () => {
  const { client } = buildFakeClient({ device: null });
  let serviceBuilt = false;
  const handle = createHandler({
    createCallerClient: () => client,
    createServiceClient: () => {
      serviceBuilt = true;
      return client;
    },
    publishSamplingInterval: () => Promise.resolve(),
  });

  const res = await handle(
    buildRequest({ deviceId: DEVICE_ID, samplingIntervalMs: 5000 }, {
      Authorization: "Bearer token",
    }),
  );

  assertEquals(res.status, 404);
  assertEquals(serviceBuilt, false);
});
