// Manual end-to-end check: the REAL Edge Function publishes to a REAL broker.
// Not picked up by `deno test` (no .test. suffix) — run explicitly, after the
// setup in supabase/README.md ("Broker round trip"):
//   deno run --allow-env --allow-net broker-roundtrip.local.ts
//
// Unlike integration.local.ts, nothing here is injected: the function runs in
// the local edge runtime (`supabase start`), reads MQTT_WS_URL from
// supabase/functions/.env, and publishes to an anonymous throwaway
// eclipse-mosquitto. The script subscribes to the device's config topic,
// calls the function with a real user JWT, then asserts both the delivered
// payload and the persisted device_configs row. No Cloud credential is used.
import { createClient } from "npm:@supabase/supabase-js@2";
import mqtt from "npm:mqtt@5";

const API_URL = Deno.env.get("SUPABASE_URL") ?? "http://127.0.0.1:54321";
const ANON_KEY = Deno.env.get("SUPABASE_ANON_KEY");
const SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
// The host-side address of the broker; the function reaches it by container name.
const BROKER_WS_URL = Deno.env.get("MQTT_TEST_BROKER_URL") ??
  "ws://127.0.0.1:9001";
const DELIVERY_TIMEOUT_MS = 15_000;
const SAMPLING_INTERVAL_MS = 15_000;

if (!ANON_KEY || !SERVICE_ROLE_KEY) {
  console.error("SUPABASE_ANON_KEY and SUPABASE_SERVICE_ROLE_KEY are required");
  Deno.exit(1);
}

const admin = createClient(API_URL, SERVICE_ROLE_KEY);
const email = `broker-roundtrip-${crypto.randomUUID()}@example.test`;
const password = crypto.randomUUID();
// Unique per run so a leftover device from a crashed run never collides.
const mac = crypto.randomUUID().replaceAll("-", "").slice(0, 12).toUpperCase();

let userId: string | undefined;
let deviceId: string | undefined;
let subscriber: ReturnType<typeof mqtt.connect> | undefined;

function waitForMessage(
  client: ReturnType<typeof mqtt.connect>,
): Promise<{ topic: string; payload: unknown }> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () =>
        reject(new Error("timed out waiting for the broker to deliver config")),
      DELIVERY_TIMEOUT_MS,
    );
    client.once("message", (topic: string, payload: Uint8Array) => {
      clearTimeout(timer);
      resolve({
        topic,
        payload: JSON.parse(new TextDecoder().decode(payload)),
      });
    });
  });
}

function subscribe(
  client: ReturnType<typeof mqtt.connect>,
  topic: string,
): Promise<void> {
  return new Promise((resolve, reject) => {
    client.once("error", reject);
    client.subscribe(
      topic,
      { qos: 1 },
      (err?: Error | null) => err ? reject(err) : resolve(),
    );
  });
}

try {
  const { data: userData, error: userError } = await admin.auth.admin
    .createUser({ email, password, email_confirm: true });
  if (userError || !userData.user) {
    throw new Error(`failed to create test user: ${userError?.message}`);
  }
  userId = userData.user.id;

  const { data: device, error: deviceError } = await admin
    .from("devices")
    .insert({ mac_address: mac, name: "broker round trip device" })
    .select("id")
    .single();
  if (deviceError || !device) {
    throw new Error(`failed to seed device: ${deviceError?.message}`);
  }
  deviceId = device.id;

  const { data: signIn, error: signInError } = await createClient(
    API_URL,
    ANON_KEY,
  ).auth.signInWithPassword({ email, password });
  if (signInError || !signIn.session) {
    throw new Error(`failed to sign in: ${signInError?.message}`);
  }

  const topic = `dl/v1/${mac}/config`;
  subscriber = mqtt.connect(BROKER_WS_URL, { connectTimeout: 10_000 });
  await new Promise<void>((resolve, reject) => {
    subscriber!.once("connect", () => resolve());
    subscriber!.once("error", reject);
  });
  await subscribe(subscriber, topic);
  const delivered = waitForMessage(subscriber);

  const res = await fetch(`${API_URL}/functions/v1/set-sampling-interval`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${signIn.session.access_token}`,
      apikey: ANON_KEY,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      deviceId,
      samplingIntervalMs: SAMPLING_INTERVAL_MS,
    }),
  });
  console.log("status:", res.status, "body:", await res.text());
  if (res.status !== 200) throw new Error(`expected 200, got ${res.status}`);

  const message = await delivered;
  if (message.topic !== topic) {
    throw new Error(`expected topic ${topic}, got ${message.topic}`);
  }
  const received = JSON.stringify(message.payload);
  const expected = JSON.stringify({ samplingInterval: SAMPLING_INTERVAL_MS });
  if (received !== expected) {
    throw new Error(`expected payload ${expected}, got ${received}`);
  }

  const { data: configRow, error: configError } = await admin
    .from("device_configs")
    .select("sampling_interval_ms")
    .eq("device_id", deviceId)
    .single();
  if (configError || !configRow) {
    throw new Error(`config row missing: ${configError?.message}`);
  }
  if (configRow.sampling_interval_ms !== SAMPLING_INTERVAL_MS) {
    throw new Error(
      `expected persisted ${SAMPLING_INTERVAL_MS}ms, got ${configRow.sampling_interval_ms}`,
    );
  }

  console.log(
    `PASS: broker delivered ${received} on ${topic}; device_configs row persisted`,
  );
} finally {
  subscriber?.end(true);
  if (deviceId) await admin.from("devices").delete().eq("id", deviceId);
  if (userId) await admin.auth.admin.deleteUser(userId);
}
