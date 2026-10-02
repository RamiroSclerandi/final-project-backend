-- Canonical schema for the datalogger platform.
-- Brought into version control from the firmware repository, which is not
-- a git repository, so until now the contract between worker, database and
-- frontend had no history at all.

-- =============================================================================
-- Datalogger — Esquema PostgreSQL / Supabase
--
-- Contrato de ingesta: datalogger.v1
-- Modelo de acceso: instalación única, preparada para multiusuario (owner_id)
--
-- ADVERTENCIA: este archivo RECREA el esquema. La clave primaria de
-- `measurements` cambió de tipo (UUID -> BIGINT IDENTITY), lo cual no es un
-- ALTER trivial. Si ya hay datos que interese conservar, exportarlos antes.
-- =============================================================================

-- gen_random_uuid() es nativo desde PostgreSQL 13: ya no hace falta uuid-ossp.

DROP MATERIALIZED VIEW IF EXISTS mv_measurements_daily;
DROP MATERIALIZED VIEW IF EXISTS mv_measurements_hourly;
DROP VIEW IF EXISTS v_frontend_monitoring;
DROP VIEW IF EXISTS v_latest_readings;
DROP TABLE IF EXISTS measurements;
DROP TABLE IF EXISTS device_configs;
DROP TABLE IF EXISTS sensors;
DROP TABLE IF EXISTS sensor_types;
DROP TABLE IF EXISTS devices;
DROP TABLE IF EXISTS raw_messages;


-- =============================================================================
-- 1. CATÁLOGO DE TIPOS DE SENSOR
--
-- Una fila por par (magnitud, unidad). Los valores de `name` y `unit` son
-- exactamente los que emite el firmware en los campos `c` y `u` del payload:
-- `Channel::*` de core/Measurement.h y unitToString() respectivamente.
--
-- La restricción UNIQUE habilita el alta automática con ON CONFLICT desde el
-- worker: un canal desconocido se registra solo, sin desplegar código.
-- =============================================================================

CREATE TABLE sensor_types (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name         TEXT NOT NULL,
    unit         TEXT NOT NULL,
    expected_min DOUBLE PRECISION,
    expected_max DOUBLE PRECISION,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT sensor_types_name_unit_unique UNIQUE (name, unit)
);

COMMENT ON COLUMN sensor_types.name IS
    'Canal tal como lo emite el firmware (campo "c"): temperature, pressure, ...';
COMMENT ON COLUMN sensor_types.expected_min IS
    'Límite inferior plausible. La ingesta marca measurements.quality fuera de rango.';


-- =============================================================================
-- 2. DISPOSITIVOS
--
-- `mac_address` es la identidad canónica y coincide con DeviceInfo::deviceId()
-- del firmware: 12 caracteres hexadecimales en mayúscula, sin separadores.
-- Se eligió por sobre un identificador de compilación porque es intrínseca al
-- hardware: flashear el mismo binario en otra placa produce otro dispositivo.
-- =============================================================================

CREATE TABLE devices (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    mac_address      TEXT NOT NULL UNIQUE,
    name             TEXT NOT NULL,
    location_ref     TEXT,
    transport        TEXT NOT NULL DEFAULT 'wifi-mqtt',
    firmware_version TEXT,
    status           BOOLEAN NOT NULL DEFAULT false,
    last_seen        TIMESTAMPTZ,
    provisioned      BOOLEAN NOT NULL DEFAULT false,

    -- Preparación para multiusuario. Hoy no se usa y las políticas RLS lo
    -- ignoran; cuando haga falta, se rellena y se cambia una sola política,
    -- sin ALTER TABLE sobre datos existentes.
    owner_id         UUID REFERENCES auth.users(id) ON DELETE SET NULL,

    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT devices_mac_format CHECK (mac_address ~ '^[0-9A-F]{12}$'),
    CONSTRAINT devices_transport_valid CHECK (transport IN ('wifi-mqtt', 'lorawan', 'cellular'))
);

COMMENT ON COLUMN devices.transport IS
    'Protocolo activo. Permite comparar el desempeño de WiFi y LoRaWAN en la validación.';
COMMENT ON COLUMN devices.status IS
    'Online/offline, alimentado por el Last Will and Testament de MQTT.';
COMMENT ON COLUMN devices.provisioned IS
    'false = dado de alta automáticamente por la ingesta, pendiente de que el usuario lo nombre.';

CREATE INDEX idx_devices_owner ON devices (owner_id) WHERE owner_id IS NOT NULL;


-- =============================================================================
-- 3. INSTANCIAS DE SENSOR POR DISPOSITIVO
--
-- `source` es el ISensor::getName() del firmware, y es imprescindible en la
-- clave única: el BMP280 y el DHT22 reportan AMBOS temperature/degC. Sin
-- `source`, los dos canales colapsarían en una sola fila y las series se
-- mezclarían.
-- =============================================================================

CREATE TABLE sensors (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    device_id      UUID NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    type_id        UUID NOT NULL REFERENCES sensor_types(id),
    source         TEXT NOT NULL,
    tag            TEXT NOT NULL DEFAULT '',
    label          TEXT,
    pin_connection TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT sensors_device_type_source_unique UNIQUE (device_id, type_id, source, tag)
);

COMMENT ON COLUMN sensors.source IS
    'Sensor físico que produce el canal (campo "src" del payload): BMP280, DHT22, CIRWATT-B.';
COMMENT ON COLUMN sensors.tag IS
    'Discrimina canales de igual magnitud del mismo sensor (campo "t"): l1, l2, l3, total.
     Cadena vacía cuando el sensor produce un único canal de esa magnitud.';

CREATE INDEX idx_sensors_device ON sensors (device_id);


-- =============================================================================
-- 4. CONFIGURACIÓN POR DISPOSITIVO
--
-- Permite que la UI muestre la configuración vigente de cada nodo. `applied_at`
-- distingue lo solicitado de lo confirmado: bajo LoRaWAN Clase A el downlink se
-- entrega recién en la próxima ventana de recepción, que puede tardar horas.
-- =============================================================================

CREATE TABLE device_configs (
    device_id            UUID PRIMARY KEY REFERENCES devices(id) ON DELETE CASCADE,
    sampling_interval_ms INTEGER NOT NULL DEFAULT 5000,
    transmit_interval_ms INTEGER,
    requested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    applied_at           TIMESTAMPTZ,

    CONSTRAINT device_configs_sampling_range
        CHECK (sampling_interval_ms BETWEEN 1000 AND 300000),
    CONSTRAINT device_configs_transmit_range
        CHECK (transmit_interval_ms IS NULL OR transmit_interval_ms >= sampling_interval_ms)
);


-- =============================================================================
-- 5. MEDICIONES (serie de tiempo)
--
-- Cada fila corresponde exactamente a un elemento del array `ch` del payload.
--
-- Entero en lugar de UUID: un UUID v4 es aleatorio, de modo que cada inserción
-- cae en una página arbitraria del índice y fragmenta el B-tree. En la tabla
-- más grande del sistema eso degrada la escritura sostenida, además de costar
-- 16 bytes por fila contra 8. UUIDv7 resolvería la localidad pero mantiene el
-- costo de tamaño sin aportar nada: ninguna fila de measurements se referencia
-- por su id desde la UI, que siempre consulta por (sensor_id, rango de tiempo).
--
-- IDENTITY en lugar de SERIAL: SERIAL no es SQL estándar y permite insertar un
-- valor explícito sin avanzar la secuencia, que luego colisiona. GENERATED
-- ALWAYS lo impide salvo OVERRIDING SYSTEM VALUE explícito.
-- =============================================================================

CREATE TABLE measurements (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sensor_id     UUID NOT NULL REFERENCES sensors(id) ON DELETE CASCADE,
    value         DOUBLE PRECISION NOT NULL,
    timestamp     TIMESTAMPTZ NOT NULL,
    ts_source     TEXT NOT NULL DEFAULT 'device',
    quality       TEXT NOT NULL DEFAULT 'ok',
    battery_level DOUBLE PRECISION,
    rssi          INTEGER,
    seq           BIGINT,
    boot          INTEGER,

    -- Agregacion: NULL significa muestra cruda. Cuando el transporte no permite
    -- enviar cada muestra, el dispositivo manda la media en `value` y estas tres
    -- describen la ventana, conservando picos que el promedio escondería.
    value_min     DOUBLE PRECISION,
    value_max     DOUBLE PRECISION,
    sample_count  INTEGER,

    metadata      JSONB,

    -- Idempotency: the firmware publishes at MQTT QoS 0 (no delivery
    -- guarantee); duplicates come from the local-buffer replay intentionally
    -- resending. Combined with ON CONFLICT DO NOTHING in the worker, replay
    -- duplicates are dropped here. This only works because the device sets
    -- the timestamp -- DEFAULT NOW() would give each retry a different value.
    CONSTRAINT measurements_unique_reading UNIQUE (sensor_id, timestamp),

    CONSTRAINT measurements_ts_source_valid CHECK (ts_source IN ('device', 'server')),
    CONSTRAINT measurements_quality_valid   CHECK (quality IN ('ok', 'out_of_range', 'suspect')),
    CONSTRAINT measurements_battery_range   CHECK (battery_level IS NULL
                                                  OR battery_level BETWEEN 0 AND 100)
);

COMMENT ON COLUMN measurements.ts_source IS
    '"server" indica que el nodo no tenía reloj sincronizado: dato de calidad inferior.';
COMMENT ON COLUMN measurements.quality IS
    'Resultado de validar contra sensor_types.expected_min/max durante la ingesta.';
COMMENT ON COLUMN measurements.seq IS
    'Contador monótono dentro de una sesión. Junto con `boot` da el orden total de emisión.';
COMMENT ON COLUMN measurements.boot IS
    'Contador de arranques. `seq` reinicia en cada boot; ordenar por (boot, seq).';
COMMENT ON COLUMN measurements.sample_count IS
    'Muestras válidas en la ventana. NULL = muestra cruda. Un valor bajo indica un sensor intermitente.';

-- Índice principal. Toda consulta del dashboard filtra por sensor e intervalo:
--   WHERE sensor_id = $1 AND timestamp BETWEEN $2 AND $3 ORDER BY timestamp
-- sensor_id va primero porque siempre se compara por igualdad.
CREATE INDEX idx_measurements_sensor_time
    ON measurements (sensor_id, timestamp DESC);

-- BRIN sobre el tiempo: las inserciones llegan aproximadamente ordenadas, así
-- que este índice ocupa unos pocos KB y acelera los barridos por rango amplio
-- que hacen las vistas materializadas.
CREATE INDEX idx_measurements_time_brin
    ON measurements USING BRIN (timestamp);

-- Soporta la búsqueda de datos sospechosos sin recorrer la tabla completa.
CREATE INDEX idx_measurements_quality
    ON measurements (timestamp DESC) WHERE quality <> 'ok';


-- =============================================================================
-- 6. ARCHIVO DE MENSAJES CRUDOS
--
-- Guardar el mensaje tal como llegó cuesta poco y habilita reprocesar el
-- histórico si se corrige un bug del parser. Sin esto, un dato mal interpretado
-- se perdió. Es además el nivel más fuerte de trazabilidad disponible.
-- =============================================================================

CREATE TABLE raw_messages (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic       TEXT NOT NULL,
    payload     JSONB NOT NULL,
    source      TEXT NOT NULL,
    device_hint TEXT,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed   BOOLEAN NOT NULL DEFAULT false,
    error       TEXT,

    CONSTRAINT raw_messages_source_valid CHECK (source IN ('hivemq', 'ttn', 'chirpstack', 'http'))
);

CREATE INDEX idx_raw_messages_unprocessed
    ON raw_messages (received_at) WHERE NOT processed;

CREATE INDEX idx_raw_messages_failed
    ON raw_messages (received_at DESC) WHERE error IS NOT NULL;


-- =============================================================================
-- 7. VISTAS
-- =============================================================================

-- Último valor por sensor. Es la consulta más frecuente del dashboard.
-- DISTINCT ON resuelve el "último por grupo" apoyándose en idx_measurements_sensor_time.
CREATE OR REPLACE VIEW v_latest_readings
WITH (security_invoker = on)
AS
SELECT DISTINCT ON (m.sensor_id)
    m.sensor_id,
    m.value,
    m.timestamp,
    m.quality,
    m.battery_level,
    m.rssi,
    s.source        AS sensor_source,
    s.tag           AS sensor_tag,
    s.label         AS sensor_label,
    st.name         AS channel,
    st.unit,
    d.id            AS device_id,
    d.name          AS device_name,
    d.mac_address,
    d.location_ref,
    d.transport,
    d.status        AS device_online
FROM measurements m
JOIN sensors      s  ON m.sensor_id = s.id
JOIN sensor_types st ON s.type_id   = st.id
JOIN devices      d  ON s.device_id = d.id
ORDER BY m.sensor_id, m.timestamp DESC;


-- Agregados horarios. Un año de datos a 5 s son ~6,3 millones de filas por
-- canal; un gráfico de 800 px no puede representar más puntos que píxeles.
-- El frontend elige la granularidad según el rango solicitado.
CREATE MATERIALIZED VIEW mv_measurements_hourly AS
SELECT
    m.sensor_id,
    date_trunc('hour', m.timestamp) AS bucket,
    avg(m.value)   AS avg_value,
    min(m.value)   AS min_value,
    max(m.value)   AS max_value,
    count(*)       AS sample_count
FROM measurements m
WHERE m.quality = 'ok'
GROUP BY m.sensor_id, date_trunc('hour', m.timestamp);

CREATE UNIQUE INDEX idx_mv_hourly ON mv_measurements_hourly (sensor_id, bucket);


CREATE MATERIALIZED VIEW mv_measurements_daily AS
SELECT
    m.sensor_id,
    date_trunc('day', m.timestamp) AS bucket,
    avg(m.value)   AS avg_value,
    min(m.value)   AS min_value,
    max(m.value)   AS max_value,
    count(*)       AS sample_count
FROM measurements m
WHERE m.quality = 'ok'
GROUP BY m.sensor_id, date_trunc('day', m.timestamp);

CREATE UNIQUE INDEX idx_mv_daily ON mv_measurements_daily (sensor_id, bucket);

-- Refrescar periódicamente con pg_cron. CONCURRENTLY no bloquea las lecturas
-- y requiere el índice único definido arriba.
--   SELECT cron.schedule('refresh-hourly', '5 * * * *',
--     $$REFRESH MATERIALIZED VIEW CONCURRENTLY mv_measurements_hourly$$);


-- =============================================================================
-- 8. ROW LEVEL SECURITY
--
-- Modelo: lectura para usuarios autenticados; NINGUNA escritura desde clientes.
-- El worker de ingesta usa la clave service_role, que omite RLS por diseño, de
-- modo que no hace falta —ni conviene— una política de INSERT.
--
-- El esquema anterior tenía "ON measurements FOR INSERT TO authenticated
-- WITH CHECK (true)": cualquier usuario registrado podía inyectar mediciones
-- falsas contra cualquier sensor. Se eliminó deliberadamente.
-- =============================================================================

ALTER TABLE sensor_types    ENABLE ROW LEVEL SECURITY;
ALTER TABLE devices         ENABLE ROW LEVEL SECURITY;
ALTER TABLE sensors         ENABLE ROW LEVEL SECURITY;
ALTER TABLE measurements    ENABLE ROW LEVEL SECURITY;
ALTER TABLE device_configs  ENABLE ROW LEVEL SECURITY;
ALTER TABLE raw_messages    ENABLE ROW LEVEL SECURITY;

CREATE POLICY "read_sensor_types" ON sensor_types
    FOR SELECT TO authenticated USING (true);

CREATE POLICY "read_devices" ON devices
    FOR SELECT TO authenticated USING (true);

CREATE POLICY "read_sensors" ON sensors
    FOR SELECT TO authenticated USING (true);

CREATE POLICY "read_measurements" ON measurements
    FOR SELECT TO authenticated USING (true);

CREATE POLICY "read_device_configs" ON device_configs
    FOR SELECT TO authenticated USING (true);

-- El usuario puede renombrar y ubicar sus dispositivos desde la UI. RLS decide
-- QUÉ FILAS son alcanzables, no qué columnas: la restricción por columna es un
-- GRANT, y sin él esta política permitiría reescribir mac_address u owner_id.
CREATE POLICY "update_device_metadata" ON devices
    FOR UPDATE TO authenticated USING (true) WITH CHECK (true);

REVOKE UPDATE ON devices FROM authenticated;
GRANT UPDATE (name, location_ref, transport, provisioned) ON devices TO authenticated;

-- Lo mismo para las etiquetas de sensor: el usuario rotula, la ingesta manda
-- sobre device_id, type_id y source.
CREATE POLICY "update_sensor_label" ON sensors
    FOR UPDATE TO authenticated USING (true) WITH CHECK (true);

REVOKE UPDATE ON sensors FROM authenticated;
GRANT UPDATE (label, pin_connection) ON sensors TO authenticated;

CREATE POLICY "upsert_device_configs" ON device_configs
    FOR ALL TO authenticated USING (true) WITH CHECK (true);

-- raw_messages no lleva política de lectura: es material de diagnóstico y solo
-- se accede con service_role.

-- Cuando se active multiusuario, alcanza con reemplazar las políticas de
-- lectura por la forma:
--   USING (EXISTS (SELECT 1 FROM devices d WHERE d.id = <device_id>
--                    AND d.owner_id = auth.uid()))


-- =============================================================================
-- 9. REALTIME
--
-- Habilita que el cliente JS se suscriba por WebSocket a los INSERT. El
-- frontend debe filtrar del lado del servidor por sensor_id: suscribirse a
-- todas las inserciones satura al navegador con varios nodos activos.
-- =============================================================================

-- Guardado en un bloque para que el script no falle si la publicación no existe
-- todavía (proyecto recién creado) o si la tabla ya estaba incluida.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'supabase_realtime') THEN
        ALTER PUBLICATION supabase_realtime ADD TABLE measurements;
        ALTER PUBLICATION supabase_realtime ADD TABLE devices;
    ELSE
        CREATE PUBLICATION supabase_realtime FOR TABLE measurements, devices;
    END IF;
END $$;

