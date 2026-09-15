import { assertEquals } from "jsr:@std/assert@1";
import { parseMac, parseRequestBody } from "./parsers.ts";

const VALID_DEVICE_ID = "4a1e6c1e-2f0a-4b0a-9c3a-5e6f7a8b9c0d";

Deno.test("parseRequestBody rejects a missing deviceId", () => {
  assertEquals(parseRequestBody({ samplingIntervalMs: 5000 }), null);
});

Deno.test("parseRequestBody rejects a missing samplingIntervalMs", () => {
  assertEquals(parseRequestBody({ deviceId: VALID_DEVICE_ID }), null);
});

Deno.test("parseRequestBody rejects a non-string deviceId", () => {
  assertEquals(
    parseRequestBody({ deviceId: 123, samplingIntervalMs: 5000 }),
    null,
  );
});

Deno.test("parseRequestBody rejects a non-uuid deviceId", () => {
  assertEquals(
    parseRequestBody({ deviceId: "not-a-uuid", samplingIntervalMs: 5000 }),
    null,
  );
});

Deno.test("parseRequestBody rejects a non-number samplingIntervalMs", () => {
  assertEquals(
    parseRequestBody({ deviceId: VALID_DEVICE_ID, samplingIntervalMs: "5000" }),
    null,
  );
});

Deno.test("parseRequestBody rejects a non-integer samplingIntervalMs", () => {
  assertEquals(
    parseRequestBody({ deviceId: VALID_DEVICE_ID, samplingIntervalMs: 5000.5 }),
    null,
  );
});

Deno.test("parseRequestBody rejects samplingIntervalMs below the range", () => {
  assertEquals(
    parseRequestBody({ deviceId: VALID_DEVICE_ID, samplingIntervalMs: 999 }),
    null,
  );
});

Deno.test("parseRequestBody rejects samplingIntervalMs above the range", () => {
  assertEquals(
    parseRequestBody({ deviceId: VALID_DEVICE_ID, samplingIntervalMs: 300001 }),
    null,
  );
});

Deno.test("parseRequestBody accepts the lower boundary", () => {
  assertEquals(
    parseRequestBody({ deviceId: VALID_DEVICE_ID, samplingIntervalMs: 1000 }),
    {
      deviceId: VALID_DEVICE_ID,
      samplingIntervalMs: 1000,
    },
  );
});

Deno.test("parseRequestBody accepts the upper boundary", () => {
  assertEquals(
    parseRequestBody({ deviceId: VALID_DEVICE_ID, samplingIntervalMs: 300000 }),
    {
      deviceId: VALID_DEVICE_ID,
      samplingIntervalMs: 300000,
    },
  );
});

Deno.test("parseRequestBody rejects extra fields being required to pass through", () => {
  const result = parseRequestBody({
    deviceId: VALID_DEVICE_ID,
    samplingIntervalMs: 5000,
    topic: "dl/v1/other/data",
  });
  assertEquals(result, { deviceId: VALID_DEVICE_ID, samplingIntervalMs: 5000 });
});

Deno.test("parseMac accepts a well-formed MAC", () => {
  assertEquals(parseMac("4022D83D6618"), "4022D83D6618");
});

Deno.test("parseMac rejects a lowercase MAC", () => {
  assertEquals(parseMac("4022d83d6618"), null);
});

Deno.test("parseMac rejects a MAC with the wrong length", () => {
  assertEquals(parseMac("4022D83D66"), null);
});
