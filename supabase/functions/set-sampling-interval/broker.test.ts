// Round trip against a real, throwaway MQTT broker: the handler runs with the
// real mqtt module, and a separate subscriber must receive the config message.
// Runs only when MQTT_TEST_BROKER_URL points at an anonymous websockets broker
// (CI starts one); the Supabase side stays faked, as in index.test.ts.
import { assertEquals } from "jsr:@std/assert@1";
import mqtt from "npm:mqtt@5";
import { createHandler } from "./index.ts";
import { publishSamplingInterval } from "./mqtt.ts";

const BROKER_URL = Deno.env.get("MQTT_TEST_BROKER_URL");
const DEVICE_ID = "4a1e6c1e-2f0a-4b0a-9c3a-5e6f7a8b9c0d";
const DELIVERY_TIMEOUT_MS = 15_000;
const SAMPLING_INTERVAL_MS = 15_000;

// deno-lint-ignore no-explicit-any
type FakeSupabaseClient = any;

function buildFakeClient(mac: string): FakeSupabaseClient {
  return {
    from(table: string) {
      if (table === "devices") {
        return {
          select: () => ({
            eq: () => ({
              maybeSingle: () =>
                Promise.resolve({ data: { mac_address: mac }, error: null }),
            }),
          }),
        };
      }
      return { upsert: () => Promise.resolve({ error: null }) };
    },
  };
}

function connectSubscriber(url: string): Promise<mqtt.MqttClient> {
  return new Promise((resolve, reject) => {
    // No reconnects: an unreachable broker must fail the test, not hang it.
    const client = mqtt.connect(url, {
      connectTimeout: 10_000,
      reconnectPeriod: 0,
    });
    client.once("connect", () => resolve(client));
    client.once("error", (err) => {
      client.end(true);
      reject(err);
    });
  });
}

function waitForMessage(
  client: mqtt.MqttClient,
): Promise<{ topic: string; payload: string }> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error("broker did not deliver the config message")),
      DELIVERY_TIMEOUT_MS,
    );
    client.once("message", (topic: string, payload: Uint8Array) => {
      clearTimeout(timer);
      resolve({ topic, payload: new TextDecoder().decode(payload) });
    });
  });
}

Deno.test({
  name: "a POST publishes the sampling interval to a real broker",
  ignore: !BROKER_URL,
  // The mqtt client closes its socket asynchronously after end().
  sanitizeOps: false,
  sanitizeResources: false,
  async fn() {
    Deno.env.set("MQTT_WS_URL", BROKER_URL!);
    Deno.env.set("MQTT_USER", "test");
    Deno.env.set("MQTT_PASSWORD", "test");
    // Unique per run so a retained or stray message from another run never matches.
    const mac = crypto.randomUUID().replaceAll("-", "").slice(0, 12)
      .toUpperCase();
    const topic = `dl/v1/${mac}/config`;

    const subscriber = await connectSubscriber(BROKER_URL!);
    try {
      await subscriber.subscribeAsync(topic, { qos: 1 });
      const delivered = waitForMessage(subscriber);

      const handle = createHandler({
        createCallerClient: () => buildFakeClient(mac),
        createServiceClient: () => buildFakeClient(mac),
        publishSamplingInterval,
      });
      const res = await handle(
        new Request("http://localhost/set-sampling-interval", {
          method: "POST",
          headers: {
            Authorization: "Bearer token",
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            deviceId: DEVICE_ID,
            samplingIntervalMs: SAMPLING_INTERVAL_MS,
          }),
        }),
      );
      assertEquals(res.status, 200);

      const message = await delivered;
      assertEquals(message.topic, topic);
      assertEquals(
        JSON.parse(message.payload),
        { samplingInterval: SAMPLING_INTERVAL_MS },
      );
    } finally {
      await subscriber.endAsync(true);
      for (const key of ["MQTT_WS_URL", "MQTT_USER", "MQTT_PASSWORD"]) {
        Deno.env.delete(key);
      }
    }
  },
});
