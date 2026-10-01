// Manual check against a real local Supabase stack (`supabase start`): an
// authenticated client receives postgres_changes events for rows written by
// the service role. Not picked up by `deno test` or CI — run explicitly:
//   deno run --allow-env --allow-net supabase/manual/realtime-latency.local.ts
//
// The INSERT on `measurements` must arrive in under 2 s (the dashboard's
// live-update budget). The UPDATE on `devices` only has to arrive; its
// delivery is asserted against a looser 5 s ceiling. Pair this with
// test_realtime_publication.py, which asserts the CI-checkable precondition
// (both tables belong to the supabase_realtime publication).
import { createClient } from "npm:@supabase/supabase-js@2";

const API_URL = Deno.env.get("SUPABASE_URL") ?? "http://127.0.0.1:54321";
const ANON_KEY = Deno.env.get("SUPABASE_ANON_KEY");
const SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
const INSERT_BUDGET_MS = 2_000;
const UPDATE_CEILING_MS = 5_000;
const SUBSCRIBE_TIMEOUT_MS = 20_000;

if (!ANON_KEY || !SERVICE_ROLE_KEY) {
  console.error("SUPABASE_ANON_KEY and SUPABASE_SERVICE_ROLE_KEY are required");
  Deno.exit(1);
}

const admin = createClient(API_URL, SERVICE_ROLE_KEY);
const authed = createClient(API_URL, ANON_KEY);
const email = `realtime-latency-${crypto.randomUUID()}@example.test`;
const password = crypto.randomUUID();
const mac = crypto.randomUUID().replaceAll("-", "").slice(0, 12).toUpperCase();
const insertedValue = 42.5;

let userId: string | undefined;
let deviceId: string | undefined;
let sensorTypeId: string | undefined;

type RealtimeChannel = ReturnType<typeof authed.channel>;

/** Resolves once the server confirms postgres_changes is streaming for the channel. */
function subscribed(channel: RealtimeChannel): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error("channel never reached SUBSCRIBED")),
      SUBSCRIBE_TIMEOUT_MS,
    );
    channel.subscribe((status, err) => {
      if (status === "SUBSCRIBED") {
        clearTimeout(timer);
        resolve();
      } else if (status === "CHANNEL_ERROR" || status === "TIMED_OUT") {
        clearTimeout(timer);
        reject(new Error(`subscribe failed (${status}): ${err?.message}`));
      }
    });
  });
}

/**
 * Registers the handler now (it must exist before `subscribe()`) and resolves
 * on the first event whose new row satisfies `matches`. The delivery clock is
 * armed separately, by `within`, once the channel is subscribed.
 */
function firstMatching(
  channel: RealtimeChannel,
  event: "INSERT" | "UPDATE",
  table: string,
  matches: (row: Record<string, unknown>) => boolean,
): Promise<void> {
  return new Promise((resolve) => {
    channel.on(
      "postgres_changes",
      { event, schema: "public", table },
      (payload) => {
        if (matches(payload.new as Record<string, unknown>)) resolve();
      },
    );
  });
}

/** Rejects with `label` if `promise` has not settled within `ms`. */
async function within(
  promise: Promise<void>,
  ms: number,
  label: string,
): Promise<void> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error(`timed out waiting for ${label}`)),
          ms,
        );
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
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
    .insert({ mac_address: mac, name: "realtime latency device" })
    .select("id")
    .single();
  if (deviceError || !device) {
    throw new Error(`failed to seed device: ${deviceError?.message}`);
  }
  deviceId = device.id;

  const { data: sensorType, error: typeError } = await admin
    .from("sensor_types")
    .insert({ name: `latency-${mac}`, unit: "u" })
    .select("id")
    .single();
  if (typeError || !sensorType) {
    throw new Error(`failed to seed sensor type: ${typeError?.message}`);
  }
  sensorTypeId = sensorType.id;

  const { data: sensor, error: sensorError } = await admin
    .from("sensors")
    .insert({ device_id: deviceId, type_id: sensorTypeId, source: "manual" })
    .select("id")
    .single();
  if (sensorError || !sensor) {
    throw new Error(`failed to seed sensor: ${sensorError?.message}`);
  }

  const { error: signInError } = await authed.auth.signInWithPassword({
    email,
    password,
  });
  if (signInError) throw new Error(`failed to sign in: ${signInError.message}`);

  // `wait: true` holds SUBSCRIBED until replication is actually streaming; the
  // first join after `supabase start` can take several seconds, which must not
  // count against the delivery budget.
  const config = { config: { postgres_changes_options: { wait: true } } };
  const measurementsChannel = authed.channel("measurements-latency", config);
  const devicesChannel = authed.channel("devices-latency", config);

  const insertSeen = firstMatching(
    measurementsChannel,
    "INSERT",
    "measurements",
    (row) => row.sensor_id === sensor.id && row.value === insertedValue,
  );
  const updateSeen = firstMatching(
    devicesChannel,
    "UPDATE",
    "devices",
    (row) => row.id === deviceId && row.status === true,
  );

  await Promise.all([
    subscribed(measurementsChannel),
    subscribed(devicesChannel),
  ]);

  const insertStart = performance.now();
  const { error: insertError } = await admin.from("measurements").insert({
    sensor_id: sensor.id,
    value: insertedValue,
    timestamp: new Date().toISOString(),
  });
  if (insertError) throw new Error(`insert failed: ${insertError.message}`);
  // A generous outer timeout only keeps a lost event from hanging the run; the
  // 2 s budget itself is asserted on the measured delay below.
  await within(insertSeen, INSERT_BUDGET_MS * 5, "the measurements INSERT");
  const insertMs = performance.now() - insertStart;
  console.log(`measurements INSERT delivered in ${insertMs.toFixed(0)} ms`);
  if (insertMs >= INSERT_BUDGET_MS) {
    throw new Error(
      `INSERT took ${insertMs.toFixed(0)} ms, budget is ${INSERT_BUDGET_MS} ms`,
    );
  }

  const updateStart = performance.now();
  const { error: updateError } = await admin
    .from("devices")
    .update({ status: true })
    .eq("id", deviceId);
  if (updateError) throw new Error(`update failed: ${updateError.message}`);
  await within(updateSeen, UPDATE_CEILING_MS, "the devices UPDATE");
  const updateMs = performance.now() - updateStart;
  console.log(`devices UPDATE delivered in ${updateMs.toFixed(0)} ms`);

  console.log("PASS: both realtime events delivered within budget");
} finally {
  await authed.removeAllChannels();
  // Deleting the device cascades to sensors and measurements.
  if (deviceId) await admin.from("devices").delete().eq("id", deviceId);
  if (sensorTypeId) {
    await admin.from("sensor_types").delete().eq("id", sensorTypeId);
  }
  if (userId) await admin.auth.admin.deleteUser(userId);
}
