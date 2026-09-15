// Spike S3 — does MQTT-over-WebSocket work from the Deno Edge Function runtime?
// Publishes one fixed message to the real node's config topic and reports timings.
// This is a throwaway measurement function, not the real set-device-config Edge Function.
import mqtt from "npm:mqtt@5";

const MAC = "4022D83D6618";
const TOPIC = `dl/v1/${MAC}/config`;
const PAYLOAD = JSON.stringify({ samplingInterval: 15000 });

Deno.serve(async () => {
  const host = Deno.env.get("MQTT_HOST");
  const port = Deno.env.get("MQTT_WS_PORT");
  const username = Deno.env.get("MQTT_USER");
  const password = Deno.env.get("MQTT_PASSWORD");

  if (!host || !port || !username || !password) {
    return Response.json({ ok: false, error: "missing MQTT env vars" }, { status: 500 });
  }

  const url = `wss://${host}:${port}/mqtt`;
  const connectStart = performance.now();

  return await new Promise<Response>((resolve) => {
    const client = mqtt.connect(url, { username, password, connectTimeout: 10_000 });
    let connectMs: number | undefined;
    let settled = false;

    const finish = (body: Record<string, unknown>, status: number) => {
      if (settled) return;
      settled = true;
      clearTimeout(watchdog);
      client.end(true);
      resolve(Response.json(body, { status }));
    };

    const watchdog = setTimeout(() => finish({ ok: false, connectMs, error: "timeout" }, 504), 15_000);

    client.on("connect", () => {
      connectMs = Math.round(performance.now() - connectStart);
      const publishStart = performance.now();
      client.publish(TOPIC, PAYLOAD, { qos: 1, retain: false }, (err) => {
        const publishMs = Math.round(performance.now() - publishStart);
        if (err) {
          finish({ ok: false, connectMs, publishMs, error: String(err) }, 502);
          return;
        }
        finish({ ok: true, connectMs, publishMs }, 200);
      });
    });

    client.on("error", (err) => finish({ ok: false, connectMs, error: String(err) }, 502));
  });
});
