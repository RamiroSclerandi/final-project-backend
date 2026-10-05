// Runs against a real local Supabase stack (`supabase start`; CI starts one):
// an authenticated client must receive postgres_changes events for rows the
// service role writes. Skipped unless SUPABASE_ANON_KEY and
// SUPABASE_SERVICE_ROLE_KEY are set.
//
// The INSERT on `measurements` must arrive within the dashboard's 2 s
// live-update budget and the UPDATE on `devices` within 5 s. Both are
// timeouts, not benchmarks: a lost event fails the test, a fast one passes.
// test_realtime_publication.py asserts the precondition this relies on (both
// tables belong to the supabase_realtime publication).
import {
  createClient,
  type RealtimeChannel,
} from "npm:@supabase/supabase-js@2";

const API_URL = Deno.env.get("SUPABASE_URL") ?? "http://127.0.0.1:54321";
const ANON_KEY = Deno.env.get("SUPABASE_ANON_KEY");
const SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
const INSERT_TIMEOUT_MS = 2_000;
const UPDATE_TIMEOUT_MS = 5_000;
const SUBSCRIBE_TIMEOUT_MS = 20_000;
const INSERTED_VALUE = 42.5;

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
          () =>
            reject(new Error(`timed out after ${ms} ms waiting for ${label}`)),
          ms,
        );
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

// supabase-js returns errors instead of throwing, so a failed step must be
// turned into an exception explicitly.
function check(what: string, error: { message: string } | null): void {
  if (error) throw new Error(`${what} failed: ${error.message}`);
}

// Cleanup must not mask the test's own failure, so it logs instead of throwing.
function reportCleanupError(what: string, error: { message: string } | null) {
  if (error) console.error(`cleanup failed for ${what}: ${error.message}`);
}

Deno.test({
  name: "authenticated clients receive measurements INSERT and devices UPDATE",
  ignore: !ANON_KEY || !SERVICE_ROLE_KEY,
  // The realtime socket closes asynchronously after removeAllChannels().
  sanitizeOps: false,
  sanitizeResources: false,
  async fn() {
    const admin = createClient(API_URL, SERVICE_ROLE_KEY!);
    const authed = createClient(API_URL, ANON_KEY!);
    const email = `realtime-delivery-${crypto.randomUUID()}@example.test`;
    const password = crypto.randomUUID();
    // Unique per run so leftovers from a crashed run never collide.
    const mac = crypto.randomUUID().replaceAll("-", "").slice(0, 12)
      .toUpperCase();
    let userId: string | undefined;
    let deviceId: string | undefined;
    let sensorTypeId: string | undefined;

    try {
      const { data: userData, error: userError } = await admin.auth.admin
        .createUser({ email, password, email_confirm: true });
      check("creating the test user", userError);
      userId = userData.user!.id;

      const { data: device, error: deviceError } = await admin
        .from("devices")
        .insert({ mac_address: mac, name: "realtime delivery device" })
        .select("id")
        .single();
      check("seeding the device", deviceError);
      deviceId = device!.id;

      const { data: sensorType, error: typeError } = await admin
        .from("sensor_types")
        .insert({ name: `realtime-${mac}`, unit: "u" })
        .select("id")
        .single();
      check("seeding the sensor type", typeError);
      sensorTypeId = sensorType!.id;

      const { data: sensor, error: sensorError } = await admin
        .from("sensors")
        .insert({
          device_id: deviceId,
          type_id: sensorTypeId,
          source: "manual",
        })
        .select("id")
        .single();
      check("seeding the sensor", sensorError);

      const { error: signInError } = await authed.auth.signInWithPassword({
        email,
        password,
      });
      check("signing in", signInError);

      // `wait: true` holds SUBSCRIBED until replication is actually streaming;
      // the first join after `supabase start` can take several seconds, which
      // must not count against the delivery timeout.
      const config = { config: { postgres_changes_options: { wait: true } } };
      const measurementsChannel = authed.channel(
        "measurements-delivery",
        config,
      );
      const devicesChannel = authed.channel("devices-delivery", config);

      const insertSeen = firstMatching(
        measurementsChannel,
        "INSERT",
        "measurements",
        (row) => row.sensor_id === sensor!.id && row.value === INSERTED_VALUE,
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

      const { error: insertError } = await admin.from("measurements").insert({
        sensor_id: sensor!.id,
        value: INSERTED_VALUE,
        timestamp: new Date().toISOString(),
      });
      check("inserting the measurement", insertError);
      await within(insertSeen, INSERT_TIMEOUT_MS, "the measurements INSERT");

      const { error: updateError } = await admin
        .from("devices")
        .update({ status: true })
        .eq("id", deviceId);
      check("updating the device", updateError);
      await within(updateSeen, UPDATE_TIMEOUT_MS, "the devices UPDATE");
    } finally {
      await authed.removeAllChannels();
      // Deleting the device cascades to sensors and measurements.
      if (deviceId) {
        const { error } = await admin.from("devices").delete().eq(
          "id",
          deviceId,
        );
        reportCleanupError(`device ${deviceId}`, error);
      }
      if (sensorTypeId) {
        const { error } = await admin.from("sensor_types").delete().eq(
          "id",
          sensorTypeId,
        );
        reportCleanupError(`sensor type ${sensorTypeId}`, error);
      }
      if (userId) {
        const { error } = await admin.auth.admin.deleteUser(userId);
        reportCleanupError(`user ${userId}`, error);
      }
    }
  },
});
