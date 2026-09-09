-- Sensor types the current firmware emits.
-- Not mandatory: the worker registers whatever is missing. It preloads the
-- expected ranges, which is what enables quality banding during ingestion.

-- =============================================================================
-- 10. SEMILLA: tipos de sensor que emite el firmware actual
--
-- No es obligatoria — el worker da de alta lo que falte — pero deja los rangos
-- esperados cargados desde el arranque, que es lo que habilita la validación
-- de calidad en la ingesta.
-- =============================================================================

INSERT INTO sensor_types (name, unit, expected_min, expected_max) VALUES
    ('temperature',   'degC',  -40,    85),
    ('pressure',      'hPa',   300,    1100),
    ('humidity',      'pct',   0,      100),
    ('voltage',       'V',     0,      60),
    ('current',       'A',     -10,    10),
    ('power',         'W',     -600,   600),
    ('illuminance',   'lx',    0,      100000),
    ('co2',           'ppm',   300,    10000),
    ('soil_moisture', 'pct',   0,      100),
    -- Canales electricos del medidor Modbus trifasico. Los rangos suponen una
    -- red de 400 V; ajustar segun la instalacion real antes de confiar en
    -- measurements.quality.
    ('frequency',      'Hz',   45,     65),
    ('power',          'kW',   -600,   600),
    ('reactive_power', 'kvar', -600,   600),
    ('apparent_power', 'kVA',  -600,   600),
    ('power_factor',   'none', -1,     1),
    ('active_energy',  'kWh',  0,      99999999)
ON CONFLICT (name, unit) DO NOTHING;
