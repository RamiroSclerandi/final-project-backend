// Manual integration check against a real local Supabase stack (`supabase start`).
// Not picked up by `deno test` (no .test. suffix) — run explicitly:
//   deno run --allow-env --allow-net integration.local.ts
//
// Exercises createHandler with a REAL local Postgres/Auth/PostgREST client
// (real RLS, real JWT) and an injected fake for the MQTT boundary: this
// session holds no real HiveMQ credentials (they are Cloud-only secrets),
// so the broker step is covered instead by mqtt.test.ts (topic/payload
// shape) and by the S3 spike's observer-confirmed live publish.
import { createClient } from "npm:@supabase/supabase-js@2";
import { createHandler } from "./index.ts";

const API_URL = Deno.env.get("SUPABASE_URL") ?? "http://127.0.0.1:54321";
const ANON_KEY = Deno.env.get("SUPABASE_ANON_KEY");
const SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");

if (!ANON_KEY || !SERVICE_ROLE_KEY) {
  console.error("SUPABASE_ANON_KEY and SUPABASE_SERVICE_ROLE_KEY are required");
  Deno.exit(1);
}

const admin = createClient(API_URL, SERVICE_ROLE_KEY);
const email = `integration-${crypto.randomUUID()}@example.test`;
const password = crypto.randomUUID();
const mac = "AABBCCDDEEFF";

const { data: userData, error: userError } = await admin.auth.admin.createUser({
  email,
  password,
  email_confirm: true,
});
if (userError || !userData.user) {
  throw new Error(`failed to create test user: ${userError?.message}`);
}

const { data: device, error: deviceError } = await admin
  .from("devices")
  .insert({ mac_address: mac, name: "integration test device" })
  .select("id")
  .single();
if (deviceError || !device) {
  throw new Error(`failed to seed device: ${deviceError?.message}`);
}

const { data: signIn, error: signInError } = await createClient(
  API_URL,
  ANON_KEY,
).auth
  .signInWithPassword({ email, password });
if (signInError || !signIn.session) {
  throw new Error(`failed to sign in: ${signInError?.message}`);
}

function createCallerClient(authHeader: string) {
  return createClient(API_URL, ANON_KEY, {
    global: { headers: { Authorization: authHeader } },
  });
}

let publishedMac: string | undefined;
let publishedMs: number | undefined;
const handle = createHandler({
  // deno-lint-ignore no-explicit-any
  createCallerClient: createCallerClient as any,
  publishSamplingInterval: (m: string, ms: number) => {
    publishedMac = m;
    publishedMs = ms;
    return Promise.resolve();
  },
});

const res = await handle(
  new Request("http://localhost/set-sampling-interval", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${signIn.session.access_token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ deviceId: device.id, samplingIntervalMs: 45000 }),
  }),
);

const body = await res.json();
console.log("status:", res.status, "body:", JSON.stringify(body));

if (res.status !== 200) throw new Error(`expected 200, got ${res.status}`);
if (publishedMac !== mac) {
  throw new Error(`expected publish to ${mac}, got ${publishedMac}`);
}
if (publishedMs !== 45000) {
  throw new Error(`expected 45000ms, got ${publishedMs}`);
}

const { data: configRow, error: configError } = await admin
  .from("device_configs")
  .select("device_id, sampling_interval_ms")
  .eq("device_id", device.id)
  .single();
if (configError || !configRow) {
  throw new Error(`config row missing: ${configError?.message}`);
}
if (configRow.sampling_interval_ms !== 45000) {
  throw new Error(
    `expected persisted 45000ms, got ${configRow.sampling_interval_ms}`,
  );
}

console.log(
  "PASS: device_configs row persisted with sampling_interval_ms =",
  configRow.sampling_interval_ms,
);
