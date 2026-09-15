const MIN_INTERVAL_MS = 1000;
const MAX_INTERVAL_MS = 300000;
const MAC_PATTERN = /^[0-9A-F]{12}$/;
const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export interface SetSamplingIntervalBody {
  deviceId: string;
  samplingIntervalMs: number;
}

/** Validates and narrows an untrusted request body to only the two fields the function uses. */
export function parseRequestBody(
  body: unknown,
): SetSamplingIntervalBody | null {
  if (typeof body !== "object" || body === null) return null;
  const { deviceId, samplingIntervalMs } = body as Record<string, unknown>;

  if (typeof deviceId !== "string" || !UUID_PATTERN.test(deviceId)) return null;
  if (!isValidInterval(samplingIntervalMs)) return null;

  return { deviceId, samplingIntervalMs };
}

function isValidInterval(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value) &&
    value >= MIN_INTERVAL_MS && value <= MAX_INTERVAL_MS;
}

/** Defence in depth: re-validates a MAC read from the database against the firmware's format. */
export function parseMac(value: string): string | null {
  return MAC_PATTERN.test(value) ? value : null;
}
