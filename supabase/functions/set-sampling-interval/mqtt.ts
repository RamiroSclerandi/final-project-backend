// The only module that talks to the broker. It exports exactly one function
// and builds the topic and payload itself — there is no code path that can
// publish a different topic or a different payload shape.
import mqtt from "npm:mqtt@5";

const CONNECT_TIMEOUT_MS = 10_000;
const PUBLISH_TIMEOUT_MS = 15_000;

const topicFor = (mac: string) => `dl/v1/${mac}/config`;

// A full MQTT_WS_URL lets a test stack point at a plain ws:// broker on the
// container network; production keeps composing the TLS HiveMQ endpoint.
function brokerUrl(): string | undefined {
  const explicit = Deno.env.get("MQTT_WS_URL");
  if (explicit) return explicit;
  const host = Deno.env.get("MQTT_HOST");
  const port = Deno.env.get("MQTT_WS_PORT");
  return host && port ? `wss://${host}:${port}/mqtt` : undefined;
}

/**
 * Publishes the validated sampling interval to the device's fixed config topic.
 * `connect` defaults to the real broker client; tests supply a fake one here —
 * this is the only injection point, and it never carries a topic or a payload.
 */
export async function publishSamplingInterval(
  mac: string,
  samplingIntervalMs: number,
  connect: typeof mqtt.connect = mqtt.connect,
): Promise<void> {
  const username = Deno.env.get("MQTT_USER");
  const password = Deno.env.get("MQTT_PASSWORD");
  const url = brokerUrl();

  if (!url || !username || !password) {
    throw new Error("missing MQTT broker configuration");
  }

  const payload = JSON.stringify({ samplingInterval: samplingIntervalMs });

  await new Promise<void>((resolve, reject) => {
    const client = connect(url, {
      username,
      password,
      connectTimeout: CONNECT_TIMEOUT_MS,
    });
    let settled = false;

    const finish = (err?: Error) => {
      if (settled) return;
      settled = true;
      clearTimeout(watchdog);
      client.end(true);
      if (err) reject(err);
      else resolve();
    };

    const watchdog = setTimeout(
      () => finish(new Error("mqtt publish timeout")),
      PUBLISH_TIMEOUT_MS,
    );

    client.on("connect", () => {
      client.publish(
        topicFor(mac),
        payload,
        { qos: 1 },
        (err?: Error) => finish(err),
      );
    });

    client.on("error", (err: Error) => finish(err));
  });
}
