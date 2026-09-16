import { assertEquals, assertRejects } from "jsr:@std/assert@1";
import * as mqttModule from "./mqtt.ts";
import { publishSamplingInterval } from "./mqtt.ts";

function withEnv(vars: Record<string, string>, fn: () => Promise<void>) {
  const previous: Record<string, string | undefined> = {};
  for (const key of Object.keys(vars)) {
    previous[key] = Deno.env.get(key);
    Deno.env.set(key, vars[key]);
  }
  return fn().finally(() => {
    for (const key of Object.keys(vars)) {
      if (previous[key] === undefined) Deno.env.delete(key);
      else Deno.env.set(key, previous[key]!);
    }
  });
}

// deno-lint-ignore no-explicit-any
type FakeClient = any;

function fakeConnect(
  onPublish: (topic: string, payload: string) => Error | undefined,
  // deno-lint-ignore no-explicit-any
): (...args: any[]) => FakeClient {
  const handlers: Record<string, (...args: unknown[]) => void> = {};
  const client: FakeClient = {
    on(event: string, cb: (...args: unknown[]) => void) {
      handlers[event] = cb;
      return client;
    },
    publish(
      topic: string,
      payload: string,
      _opts: unknown,
      cb: (err?: Error) => void,
    ) {
      cb(onPublish(topic, payload));
    },
    end(_force?: boolean) {},
  };
  // deno-lint-ignore no-explicit-any
  return (..._args: any[]) => {
    queueMicrotask(() => handlers["connect"]?.());
    return client;
  };
}

const BROKER_ENV = {
  MQTT_HOST: "broker.example.com",
  MQTT_WS_PORT: "8884",
  MQTT_USER: "edge-function",
  MQTT_PASSWORD: "secret",
};

Deno.test("mqtt module exports only publishSamplingInterval", () => {
  assertEquals(Object.keys(mqttModule), ["publishSamplingInterval"]);
});

Deno.test("publishSamplingInterval builds the exact topic and payload", async () => {
  await withEnv(BROKER_ENV, async () => {
    let capturedTopic: string | undefined;
    let capturedPayload: string | undefined;
    const connect = fakeConnect((topic, payload) => {
      capturedTopic = topic;
      capturedPayload = payload;
      return undefined;
    });

    // deno-lint-ignore no-explicit-any
    await publishSamplingInterval("4022D83D6618", 15000, connect as any);

    assertEquals(capturedTopic, "dl/v1/4022D83D6618/config");
    assertEquals(capturedPayload, JSON.stringify({ samplingInterval: 15000 }));
  });
});

Deno.test("publishSamplingInterval payload carries only the validated interval", async () => {
  await withEnv(BROKER_ENV, async () => {
    let capturedPayload: string | undefined;
    const connect = fakeConnect((_topic, payload) => {
      capturedPayload = payload;
      return undefined;
    });

    // deno-lint-ignore no-explicit-any
    await publishSamplingInterval("4022D83D6618", 60000, connect as any);

    assertEquals(JSON.parse(capturedPayload!), { samplingInterval: 60000 });
  });
});

Deno.test("publishSamplingInterval rejects when the broker rejects the publish", async () => {
  await withEnv(BROKER_ENV, async () => {
    const connect = fakeConnect(() => new Error("not authorized"));

    await assertRejects(() =>
      // deno-lint-ignore no-explicit-any
      publishSamplingInterval("4022D83D6618", 15000, connect as any)
    );
  });
});

Deno.test("publishSamplingInterval rejects when broker env vars are missing", async () => {
  await withEnv(
    { MQTT_HOST: "", MQTT_WS_PORT: "", MQTT_USER: "", MQTT_PASSWORD: "" },
    async () => {
      await assertRejects(() => publishSamplingInterval("4022D83D6618", 15000));
    },
  );
});

function connectCapturingUrl(seen: string[]) {
  const inner = fakeConnect(() => undefined);
  // deno-lint-ignore no-explicit-any
  return (url: string, ...rest: any[]) => {
    seen.push(url);
    return inner(url, ...rest);
  };
}

Deno.test("publishSamplingInterval uses MQTT_WS_URL verbatim when set", async () => {
  await withEnv(
    { ...BROKER_ENV, MQTT_WS_URL: "ws://mosquitto:9001" },
    async () => {
      const seen: string[] = [];
      await publishSamplingInterval(
        "4022D83D6618",
        15000,
        // deno-lint-ignore no-explicit-any
        connectCapturingUrl(seen) as any,
      );
      assertEquals(seen, ["ws://mosquitto:9001"]);
    },
  );
});

Deno.test("publishSamplingInterval composes wss from host and port without MQTT_WS_URL", async () => {
  await withEnv(BROKER_ENV, async () => {
    const seen: string[] = [];
    await publishSamplingInterval(
      "4022D83D6618",
      15000,
      // deno-lint-ignore no-explicit-any
      connectCapturingUrl(seen) as any,
    );
    assertEquals(seen, ["wss://broker.example.com:8884/mqtt"]);
  });
});
