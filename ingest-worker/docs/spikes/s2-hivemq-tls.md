# Spike S2 — does HiveMQ Cloud on port 8883 need a pinned CA?

**Answer: no.** `MQTT_CA_CERT_PATH` stays empty and the default bundle is used.

## The question

The research phase could not establish from primary sources whether HiveMQ
Cloud's certificate chains to a publicly trusted root already present in the
default certificate store, or whether the worker has to ship and pin a custom
CA bundle. The design therefore made CA pinning a configuration value rather
than a code path, so that either answer would be a one-line change.

## The trap, which cost a wrong conclusion first

Run from Windows, `openssl s_client` reports:

```
depth=2 C=US, O=ISRG, CN=Root YR
verify error:num=20:unable to get local issuer certificate
CONNECTION ESTABLISHED
Peer certificate: CN=*.s1.eu.hivemq.cloud
Verification error: unable to get local issuer certificate
```

That reads like a server problem. It is not one. Two facts explain it:

1. Windows builds of `openssl` ship without a default `CApath`, so they cannot
   verify *any* public chain. The same command fails against any HTTPS host.
2. `paho-mqtt` does not use the Windows certificate store either. It uses
   `certifi`, the bundle shipped with the Python environment.

So an `openssl` result on this platform says nothing about what the worker will
do. The only meaningful check goes through the same path production takes.

## Method

```python
import socket, ssl, certifi

HOST, PORT = "<cluster>.s1.eu.hivemq.cloud", 8883
ctx = ssl.create_default_context(cafile=certifi.where())
with socket.create_connection((HOST, PORT), timeout=15) as raw:
    with ctx.wrap_socket(raw, server_hostname=HOST) as tls:
        print(tls.version(), tls.getpeercert())
```

## Result

```
HANDSHAKE OK con el bundle de certifi
protocolo : TLSv1.2
emisor    : Let's Encrypt
sujeto    : *.s1.eu.hivemq.cloud
```

The chain validates against the default bundle. Let's Encrypt's roots are
present in `certifi`, so `tls_set(ca_certs=None)` is correct and no certificate
material needs to be distributed with the worker.

## Consequences

- `MQTT_CA_CERT_PATH` remains empty in `.env.example` and in deployment. It stays
  in the configuration surface so that a future broker with a private CA is a
  configuration change, not a code change.
- `tls_insecure_set` is never called. Verification stays on.
- Client-certificate authentication is a Starter-plan feature on HiveMQ Cloud and
  is unavailable on the Free plan, so mutual TLS is not an option here regardless.

## How to re-check

If the broker moves or its certificate authority changes, re-run the Python
snippet above. Do not re-run `openssl` on Windows and read its verification
result — it will fail for reasons unrelated to the broker.
