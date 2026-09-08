# SDD — Worker de Ingesta MQTT → Supabase

## Proyecto Final — Ingeniería en Computación · UNRaf

**Estado:** listo para implementar. El contrato de payload está **congelado y verificado sobre hardware**, y el esquema de base de datos está definido.

**Documentos relacionados:**
- `SDD_Plataforma_Web.md` — base de datos, tiempo real y frontend
- `Estado_y_Pendientes_ESP32.md` — qué se verificó en el nodo y con qué evidencia
- `supabase.sql` — esquema canónico (en el repositorio del firmware)

---

## 1. Alcance

Un proceso permanente que consume mensajes del broker MQTT, los normaliza, valida e inserta en Supabase de forma idempotente.

**Fuera de alcance:** API HTTP (Supabase provee PostgREST), lógica de presentación, y cualquier cosa que el frontend pueda resolver contra la base directamente.

### Por qué un proceso separado y no una función serverless

No es preferencia: mantener una suscripción MQTT exige una conexión TCP persistente y un proceso de larga vida. Las Edge Functions de Supabase y las funciones de Vercel son *request-scoped* y tienen límite de ejecución. **No pueden sostener una suscripción MQTT.**

---

## 2. Contrato de entrada — `datalogger.v1`

Congelado. Ejemplos reales capturados del broker durante la validación.

### 2.1 Mensaje sin agregación (WiFi, `transmitInterval == samplingInterval`)

```json
{"v":1,"dev":"4022D83D6618","ts":1788804294,"seq":3,
 "meta":{"rssi":-69,"fw":"1.1.0","boot":17,"ts_src":"device",
         "store":{"k":"littlefs","pct":0,"pend":0,"drop":0}},
 "ch":[{"c":"temperature","u":"degC","ok":true,"src":"BMP280","val":21.12},
       {"c":"pressure","u":"hPa","ok":true,"src":"BMP280","val":1011.119}]}
```

### 2.2 Mensaje con agregación (muestreo 10 s, envío 60 s)

```json
{"ch":[{"c":"temperature","u":"degC","ok":true,"src":"BMP280",
        "val":21.05333,"min":21.03,"max":21.1,"n":6}]}
```

### 2.3 Canal fallido

```json
{"ch":[{"c":"temperature","u":"degC","ok":false,"src":"BMP280"}]}
```

**Sin campo `val`.** Verificado desconectando el bus I2C en caliente.

### 2.4 Campos

| Campo | Tipo | Obligatorio | Notas |
|---|---|---|---|
| `v` | int | sí | Versión del contrato. Rechazar lo que no sea 1 |
| `dev` | string | sí | 12 hex mayúsculas. Clave contra `devices.mac_address` |
| `ts` | int | sí | Epoch UTC segundos. **`0` significa reloj no sincronizado** |
| `seq` | int | sí | Monótono dentro de una sesión |
| `meta.rssi` | int | sí | dBm |
| `meta.fw` | string | sí | Versión de firmware |
| `meta.boot` | int | sí | Contador de arranques |
| `meta.ts_src` | string | sí | `"device"` o `"server"` |
| `meta.rst` | string | no | Solo en el primer mensaje tras reiniciar |
| `meta.store.k` | string | sí | `"sd"`, `"littlefs"` o `"none"` |
| `meta.store.pct` | int | sí | Uso del buffer, 0–100 |
| `meta.store.pend` | int | sí | Registros pendientes de envío |
| `meta.store.drop` | int | sí | Registros descartados por buffer lleno |
| `ch[].c` | string | sí | Canal → `sensor_types.name` |
| `ch[].u` | string | sí | Unidad → `sensor_types.unit` |
| `ch[].t` | string | no | Discriminador. **Ausente significa cadena vacía** |
| `ch[].src` | string | sí | Sensor físico → `sensors.source` |
| `ch[].ok` | bool | sí | Validez del canal |
| `ch[].val` | float | si `ok` | Valor, o media si hay agregación |
| `ch[].min` / `max` / `n` | float/int | no | Presentes solo si `n > 1` |

### 2.5 Tópicos

```
dl/v1/{MAC}/data      # uplink de mediciones     -> suscribirse a dl/v1/+/data
dl/v1/{MAC}/status    # "online" | "offline", retenido -> dl/v1/+/status
dl/v1/{MAC}/config    # downlink de configuración (el worker publica acá)
```

---

## 3. Casos borde descubiertos en la validación

Estos no son hipótesis: surgieron probando el nodo y **deben resolverse en el worker**.

### 3.1 `ts: 0` — reloj sin sincronizar

El primer mensaje tras arrancar puede salir antes de que SNTP responda:

```json
{"ts":0,"meta":{"ts_src":"server","rst":"poweron"}, ...}
```

**Tratamiento:** usar la hora de llegada y registrar `ts_source = 'server'`.

**Consecuencia que hay que aceptar explícitamente:** estos registros **no se pueden deduplicar**. La restricción `UNIQUE (sensor_id, timestamp)` se apoya en un timestamp determinista del dispositivo; si el servidor pone la hora de llegada, un reenvío desde el buffer local produce una hora distinta y entra como fila nueva. Son pocos —uno o dos por arranque— y quedan marcados como de calidad inferior, que es lo importante.

### 3.2 QoS 0: los mensajes se pierden, no se duplican

PubSubClient **no soporta publicar con QoS 1**. El SDD original asumía entrega "al menos una vez"; la realidad es "a lo sumo una vez".

**Consecuencias para el worker:**

- La deduplicación sigue siendo necesaria, porque el **reenvío desde el buffer local** sí duplica.
- La pérdida real existe y es invisible en el payload. **La única forma de medirla es el par `(boot, seq)`**: si llegan `seq` 5, 6 y 8, el mensaje 7 se perdió.
- Conviene registrar esa métrica: es un dato de calidad del enlace, y para la tesis es evidencia cuantitativa del comportamiento del sistema.

### 3.3 Un dispositivo, varios sensores con el mismo canal

El BMP280 y el DHT22 reportan ambos `temperature` en `degC`. El medidor trifásico reporta `voltage` en `V` tres veces.

Por eso la clave de resolución de un sensor es **`(device_id, type_id, source, tag)`**, no `(device_id, type_id)`. Resolver con menos campos mezcla series distintas en la misma fila.

### 3.4 `tag` ausente

El firmware omite `t` cuando está vacío, para ahorrar bytes. **El worker debe normalizar ausente → `''`**, que es el valor por defecto de la columna. Si no, se crean dos filas de sensor para el mismo canal.

---

## 4. Esquema de destino

Ver `supabase.sql`. Lo relevante para el worker:

```sql
sensor_types (id, name, unit, expected_min, expected_max)
  UNIQUE (name, unit)

devices (id, mac_address, name, transport, firmware_version,
         status, last_seen, provisioned, owner_id)
  UNIQUE (mac_address)

sensors (id, device_id, type_id, source, tag, label)
  UNIQUE (device_id, type_id, source, tag)

measurements (id, sensor_id, value, timestamp, ts_source, quality,
              battery_level, rssi, seq, boot,
              value_min, value_max, sample_count, metadata)
  UNIQUE (sensor_id, timestamp)
  INDEX (sensor_id, timestamp DESC)

raw_messages (id, topic, payload, source, received_at, processed, error)
```

Las tres restricciones `UNIQUE` son las que habilitan el alta automática con `ON CONFLICT`.

---

## 5. Diseño del worker

### 5.1 Estructura

```
ingest-worker/
├── pyproject.toml
├── Dockerfile
├── .env.example
└── src/ingest/
    ├── main.py              # Composición y arranque
    ├── config.py            # Variables de entorno, validadas al iniciar
    ├── sources/
    │   ├── base.py          # Protocolo MessageSource
    │   ├── hivemq.py        # Payload directo del ESP32
    │   └── ttn.py           # Desenvuelve los metadatos del network server
    ├── domain/
    │   ├── payload.py       # Modelos Pydantic de datalogger.v1
    │   └── normalize.py     # -> lista canónica de lecturas
    ├── registry.py          # Resolución y alta de device/sensor, con caché
    ├── sink/
    │   └── supabase_sink.py # Inserción por lotes, idempotente
    └── observability.py     # Log estructurado y métricas
```

La simetría con el firmware es deliberada: el mismo criterio que allá separa *sensor*, *canal* y *transporte*, acá separa *fuente*, *dominio* y *destino*.

### 5.2 Modelo de dominio

```python
@dataclass(frozen=True)
class Reading:
    """One channel of one message: exactly one row of `measurements`."""
    device_mac: str
    channel: str          # ch[].c
    unit: str             # ch[].u
    tag: str              # ch[].t, normalized to "" when absent
    source: str           # ch[].src
    value: float | None   # None when the channel failed
    value_min: float | None
    value_max: float | None
    sample_count: int | None
    valid: bool
    recorded_at: datetime
    ts_source: str        # "device" | "server"
    rssi: int
    seq: int
    boot: int
```

### 5.3 Resolución con alta automática

Es lo que permite dar de alta un nodo o un canal nuevo **sin desplegar código**, que es el equivalente en el backend del objetivo de modularidad del firmware.

```
Para cada Reading:

1. ¿(mac, channel, unit, tag, source) está en el caché?  -> sí: devolver sensor_id

2. devices: SELECT por mac_address
     no existe -> INSERT (mac_address, name='Nodo <mac>', provisioned=false)

3. sensor_types: SELECT por (name, unit)
     no existe -> INSERT ... ON CONFLICT (name, unit) DO NOTHING

4. sensors: SELECT por (device_id, type_id, source, tag)
     no existe -> INSERT ... ON CONFLICT DO NOTHING

5. Cachear y devolver
```

**El caché es imprescindible.** Sin él, cada canal de cada mensaje son cuatro consultas: con un nodo publicando 2 canales cada 15 s son ~46.000 consultas diarias para resolver algo que no cambia nunca.

### 5.4 Validación de calidad

Antes de insertar, comparar contra `sensor_types.expected_min/max`:

| Condición | `quality` |
|---|---|
| Dentro de rango | `ok` |
| Fuera de rango | `out_of_range` |
| `ok:false` en el payload | no se inserta fila |

Esto materializa el preprocesamiento que el marco teórico del Plan exige: **el dato entra marcado, no filtrado**. Descartar silenciosamente lo anómalo sería el mismo error que el bug del BMP280 — un valor fuera de rango puede ser un sensor roto o un evento real, y esa decisión no le corresponde a la capa de ingesta.

### 5.5 Inserción idempotente

```sql
INSERT INTO measurements
  (sensor_id, value, timestamp, ts_source, quality,
   rssi, seq, boot, value_min, value_max, sample_count)
VALUES (...)
ON CONFLICT (sensor_id, timestamp) DO NOTHING;
```

Insertar **por lotes**, un lote por mensaje: los canales de un mensaje comparten timestamp y metadatos, y una transacción por canal multiplica las idas y vueltas sin ganar nada.

### 5.6 Estado del dispositivo

Del tópico `status`:

```
"online"  -> UPDATE devices SET status = true
"offline" -> UPDATE devices SET status = false
```

De cada mensaje de datos: actualizar `last_seen` y `firmware_version`. **No en cada mensaje** — una escritura por dispositivo cada pocos minutos alcanza y evita castigar la tabla.

### 5.7 Archivo de crudos

`raw_messages` antes de procesar. Cuesta poco y habilita reprocesar el histórico si el parser tiene un bug. Sin esto, un mensaje mal interpretado se perdió. Retención sugerida: 90 días.

### 5.8 Manejo de fallos

| Fallo | Respuesta |
|---|---|
| Broker inalcanzable | Reconexión con backoff exponencial; sesión persistente |
| Supabase inalcanzable | Encolar en memoria con tope; **no** hacer ACK del mensaje |
| Payload malformado | Registrar en `raw_messages` con `error`, ACK, continuar |
| Canal desconocido | Alta automática, no descarte |
| `v` distinto de 1 | Registrar y descartar; log a nivel warning |
| Conflicto de unicidad | `DO NOTHING`. Es el caso esperado, no un error |

Principio general: **no hacer ACK de lo que no se persistió.**

---

## 6. Configuración

```env
MQTT_HOST=xxx.s1.eu.hivemq.cloud
MQTT_PORT=8883
MQTT_USER=worker
MQTT_PASSWORD=
MQTT_TOPIC_DATA=dl/v1/+/data
MQTT_TOPIC_STATUS=dl/v1/+/status
MQTT_CLIENT_ID=ingest-worker-1

SUPABASE_URL=https://xxx.supabase.co
SUPABASE_SERVICE_ROLE_KEY=

LOG_LEVEL=INFO
BATCH_MAX_SIZE=100
```

Validadas al arrancar, fallando ruidosamente si falta alguna. Sin valores por defecto para secretos.

**SECURITY:**

- La `service_role` key omite RLS por diseño. Vive **solo** en el entorno del worker. Nunca en el frontend ni versionada.
- El worker debe usar **credenciales MQTT propias, distintas de las del dispositivo**, con permiso de suscripción únicamente. Con un usuario compartido, cualquiera que extraiga las credenciales de un nodo —están en texto plano en NVS— puede leer toda la flota y publicar haciéndose pasar por cualquier dispositivo.
- El `MQTT_CLIENT_ID` debe ser único: dos clientes con el mismo id se desconectan mutuamente en un bucle.

---

## 7. Criterios de aceptación

| # | Criterio | Verificación |
|---|---|---|
| CA-1 | Toda medición publicada aparece en `measurements` | Contador publicado vs. filas, ventana de 24 h |
| CA-2 | El reenvío no genera duplicados | Republicar un lote conocido; el conteo no cambia |
| CA-3 | Un dispositivo nuevo se da de alta sin desplegar | Encender un nodo no registrado |
| CA-4 | Un canal nuevo no rompe la ingesta | Habilitar el DHT22 en el setup del nodo |
| CA-5 | Los mensajes agregados guardan `min`/`max`/`n` | Configurar envío 60 s, muestreo 10 s |
| CA-6 | `ts:0` se acepta y se marca | Reiniciar el nodo y observar el primer mensaje |
| CA-7 | Un payload corrupto no frena la cola | Publicar JSON inválido a mano |
| CA-8 | El worker se recupera solo tras caerse 10 min | Detenerlo, publicar, reiniciarlo |
| CA-9 | El estado online/offline se refleja | Desenchufar el nodo |

Los datos para CA-4, CA-5 y CA-6 se pueden generar hoy con el nodo existente y un BMP280.

---

## 8. Plan de trabajo

| # | Entregable | Demostrable |
|---|---|---|
| 1 | Suscripción + `raw_messages` | Los mensajes llegan y se archivan |
| 2 | Modelos Pydantic + normalización | Un payload real produce una lista de `Reading` |
| 3 | Registry con alta automática y caché | Un nodo nuevo crea sus filas solo |
| 4 | Sink idempotente | CA-1, CA-2 |
| 5 | Calidad y estado del dispositivo | CA-5, CA-6, CA-9 |
| 6 | Contenedor y despliegue | CA-8 |
| 7 | Adaptador TTN | Habilita la ruta LoRaWAN |

**Recomendación:** empezar por 1 y 2 con los mensajes reales ya capturados como fixtures de prueba. Son el mejor caso de test que hay: salieron del hardware, no de una suposición.

---

## Apéndice: para qué sirve `(boot, seq)`

Respuesta a una duda planteada durante la validación: `boot` incrementa siempre y `seq` vuelve a cero en cada arranque. Sirve para dos cosas distintas.

**Reconstruir el orden de emisión.** Ordenar por `(boot, seq)` da el orden exacto en que el nodo produjo los mensajes, independientemente del orden de llegada o de problemas con el reloj. Es especialmente útil para los registros reenviados desde el buffer local, que llegan mucho después de haber sido generados.

**Medir la pérdida real.** `seq` es contiguo por construcción. Si el servidor recibe 5, 6 y 8, el mensaje 7 se perdió — y con QoS 0 eso pasa de verdad. Es la **única** forma de cuantificarlo, porque nada más en el sistema lo delata.

Por qué hacen falta los dos: sin `boot`, tras un reinicio `seq` salta de 100 a 0 y el servidor no puede distinguir una sesión nueva de un contador corrupto o de un hueco enorme. Y `boot` por sí solo es además telemetría útil: un salto inesperado significa que el nodo se reinició, y combinado con `meta.rst` dice por qué.
