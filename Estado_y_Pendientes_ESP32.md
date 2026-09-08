# Estado del Nodo ESP32 y Caminos Pendientes

## Proyecto Final — Ingeniería en Computación · UNRaf

**Propósito:** consolidar qué se construyó, qué se verificó, con qué fundamento se decidió cada cosa, y qué queda por delante con el detalle necesario para retomarlo.

**Documento técnico de referencia:** `firmware_arquitectura_tecnica.md` (en el repositorio del firmware).

---

## PARTE I — LO CONSTRUIDO

## 1. Estado alcanzado

El nodo captura, estructura y publica mediciones de forma verificada sobre hardware real, con persistencia local ante corte de enlace y sin pérdida de datos.

**Evidencia de campo:**

```json
{"v":1,"dev":"4022D83D6618","ts":1788803329,"seq":7,
 "meta":{"rssi":-58,"fw":"1.1.0","boot":15,"ts_src":"device",
         "store":{"k":"littlefs","pct":0,"pend":0,"drop":0}},
 "ch":[{"c":"temperature","u":"degC","ok":true,"src":"BMP280","val":20.9},
       {"c":"pressure","u":"hPa","ok":true,"src":"BMP280","val":1011.434}]}
```

**Métricas:** RAM 15,2 %, Flash 77,2 %, compilación sin warnings.

---

## 2. Punto de partida y hallazgos

El firmware inicial tenía una base correcta —separación `include`/`src`, interfaz abstracta de sensor, productor/consumidor sobre colas FreeRTOS, persistencia NVS— pero con una brecha entre la modularidad declarada y la real.

| Hallazgo | Severidad | Estado |
|---|---|---|
| Detección de fallo del BMP280 inoperante | Crítico | Corregido |
| Un solo sensor activo; agregar uno exigía editar `main.cpp` | Alto | Corregido |
| Payload sin contrato; no mapeaba al modelo relacional | Alto | Corregido |
| Pérdida silenciosa de datos con la red caída | Alto | Corregido |
| Mediciones sin marca temporal en origen | Alto | Corregido |
| Identidad del dispositivo inconsistente | Medio | Corregido |
| Configuración en tiempo de compilación | Medio | Corregido |
| Sin gestión de energía | Medio | Pendiente |
| Sin pruebas automatizadas | Bajo | Pendiente |

### 2.1 El bug crítico

`BMP280Sensor::read()` evaluaba el fallo comparando ambos valores contra `0.0f`, siguiendo un comentario que afirmaba que la biblioteca devuelve cero al fallar. **La biblioteca devuelve `NAN`** (`Adafruit_BMP280.cpp:251`).

Por IEEE 754 toda comparación con NaN es falsa, incluso `NAN == NAN`. La expresión evaluaba a falso, se negaba, y el resultado quedaba en `true`: **un fallo total de hardware se publicaba etiquetado como lectura correcta**, con los valores serializados como `null`.

En un proyecto cuya tesis es la calidad del dato como prerrequisito del DDDM, era el hallazgo de mayor impacto: contaminaba el dataset de forma indetectable aguas abajo.

**Corrección:** validación por `isnan()` más los rangos operativos del datasheet, que además detectan corrupción de datos en el bus.

---

## 3. Decisiones de diseño y su fundamento

### 3.1 El canal de medición como entidad de primera clase

Tres problemas abiertos —multi-sensor, mapeo al modelo relacional y canales de tensión/corriente— compartían causa raíz: la abstracción cubría *el* sensor, no *la colección*, y cada sensor inventaba el esquema del mensaje.

```cpp
struct Measurement {
    const char *channel;   // "temperature", "voltage"
    const char *tag;       // "l1", "l2", "total", ""
    Unit unit;             // CELSIUS, VOLT, AMPERE...
    float value;
    bool valid;            // por canal, no por sensor
};
```

Un `Measurement` corresponde exactamente a una fila de la tabla `measurements`. Un único cambio conceptual resolvió los cuatro problemas.

**El campo `tag` no estaba previsto.** Apareció al modelar el medidor trifásico: reporta `voltage` tres veces desde el mismo sensor, y sin discriminador las fases colapsaban en una sola serie.

### 3.2 Los tres ejes de variación

| Eje | Abstracción | Estado |
|---|---|---|
| Qué se mide | `ISensor` + `SensorRegistry` | Operativo |
| Por dónde se transmite | `ITransport` | Interfaz lista; WiFi operativo |
| Cómo se codifica | `IPayloadCodec` | Interfaz lista; JSON operativo |

Aplicar el mismo principio a cada eje —en lugar de resolver solo el evidente— es la decisión estructural del firmware.

### 3.3 Prueba falsable de la modularidad

Se implementó `DHT22Sensor` **no por la humedad**, sino para verificar el refactor: si hubiera requerido tocar algo más que sus dos archivos y una línea de registro, el diseño habría fallado.

Resultado: `registry.add(&dhtSensor);`. Cero cambios en el resto del sistema.

**Una arquitectura declarada modular que nunca se ejercitó con un segundo caso es una hipótesis, no un resultado.**

### 3.4 Agregación condicional

Planteo inicial: agregar en el dispositivo para viabilizar LoRaWAN. **Objeción correcta durante la revisión:** si el ancho de banda lo permite, agregar destruye información irreversiblemente y el servidor puede hacerlo al leer.

**Resolución:** la agregación no es una política global, es función del presupuesto de ancho de banda del transporte.

| Condición | Comportamiento |
|---|---|
| `transmitInterval == samplingInterval` (WiFi) | Cada muestra cruda |
| `transmitInterval > samplingInterval` (LoRaWAN) | Media, mínimo, máximo y n |

**Por qué la excepción importa:** con ~30 s de airtime diario, muestrear un compresor de aire acondicionado cada 10 s y enviar solo la última muestra produce *aliasing* — se le puede pegar siempre apagado y concluir que nunca arranca.

### 3.5 Otras decisiones con su fundamento

| Decisión | Fundamento |
|---|---|
| Identidad = MAC de efuse | Intrínseca al hardware: el mismo binario en otra placa ya es otro dispositivo |
| Tópicos con prefijo `dl/v1` | Un cambio de contrato puede convivir con nodos desplegados |
| Last Will and Testament | Un equipo que pierde alimentación no puede avisar que se cayó |
| Buffer en formato JSONL | Un corte de energía corrompe a lo sumo la última línea |
| Borrar del buffer solo tras envío confirmado | Duplicar es recuperable en la base; perder no |
| Descartar lo más viejo, y contarlo | El dato reciente vale más, pero la pérdida debe ser visible |
| Reenvío de a un registro por ciclo ocioso | El histórico no debe hambrear a la medición en vivo |
| Watchdog a 30 s | Debe superar el handshake TLS; uno que dispara sano genera reinicios cíclicos |
| Reinicio solo después del buffer | Antes, reiniciar habría vaciado la cola en RAM |
| Backoff con jitter | Sin él, varios nodos vuelven en bloque y castigan al broker al recuperarse |
| Escrituras NVS condicionales | Un borrado de sector bloquea decenas de ms y puede cortar el socket TLS |
| Contador de arranques | Persistir `seq` por mensaje agotaría la resistencia de escritura en días |
| Ningún sensor habilitado por defecto | Se sondea lo que alguien declaró instalado, no por descarte |
| Ventana de 3 s para reabrir el setup | Un nodo LoRaWAN mal configurado no se puede corregir por aire |

---

## 4. Validación sobre hardware

| Prueba | Método | Resultado |
|---|---|---|
| Provisioning y persistencia | Menú por consola, reinicios | Correcto |
| Registro condicional de sensores | Solo BMP280 habilitado | `1/1 sensors ready, 2 channels` |
| Lectura del sensor | Operación normal | 20,9 °C / 1011,434 hPa |
| Timestamp en origen | SNTP | `ts` válido, `ts_src:"device"` |
| Identidad y tópicos | Publicación | `dl/v1/4022D83D6618/data` |
| **Buffer ante corte de enlace** | Fallos de WiFi reales | Acumuló 114 registros |
| **Reenvío sin pérdida** | Restablecer el enlace | Drenó todo: `pend:0, drop:0` |
| **Fallo de sensor** | Desconexión de I2C en caliente | `ok:false` sin campo `val` |
| **Agregación** | Muestreo 10 s / envío 60 s | `val` media, `min`, `max`, `n:6` |
| **Last Will** | Corte abrupto de alimentación | `offline` retenido |
| **Configuración remota** | Publicación en `config` | Se aplica y persiste |
| **`(boot, seq)`** | Reinicios sucesivos | `boot` incrementa, `seq` reinicia |
| Backoff | Enlace caído | Intervalos crecientes |

### Hallazgos derivados

**Mensajes con `ts:0`.** El primero tras arrancar puede emitirse antes de que SNTP responda. Es correcto —el nodo declara que no tiene hora en lugar de inventarla— pero esos registros **no se pueden deduplicar**: el servidor les asigna la hora de llegada y un reenvío produciría otra.

**Transiciones `offline → online` al reconfigurar.** Cada mensaje de configuración escribía NVS aunque el valor no cambiara. Corregido con escrituras condicionales.

---

## 5. Incidente de diagnóstico WiFi

Se registra por su valor metodológico.

**Síntoma:** `NO_AP_FOUND` persistente contra una red existente con buena señal.

**Secuencia:**

1. Se agregó un escaneo de redes al fallar la conexión.
2. El escaneo informó cero redes. Se concluyó fallo de alimentación.
3. **La conclusión era inválida: el escaneo estaba roto.** Usaba `WiFi.disconnect(true)`, cuyo primer parámetro es `wifioff` y no `eraseap`: apagaba la radio, y el escaneo competía contra el módulo reencendiéndose.
4. Corregido. Se detectó además que el modo estación estaba implícito; se hizo explícito y se desactivó modem sleep.
5. Con la medición ya confiable, la red apareció a **-34 dBm** con **un espacio al final del SSID**: `"Flia Sclerandi "`. La configuración decía `"Flia Sclerandi"`.

**Lección, y es la misma que el bug del BMP280:** una medición defectuosa que no se reporta como defectuosa produce conclusiones firmes y equivocadas. En ambos casos el error no fue la falla en sí, sino que el sistema no distinguía **"no pude medir"** de **"medí y no hay nada"**.

Se incorporó detección de coincidencia aproximada de SSID: ante diferencias de espacios o mayúsculas informa ambas cadenas con su longitud.

---

## PARTE II — LO PENDIENTE

## 6. Panorama

| Camino | Bloqueado por | Esfuerzo | Prioridad |
|---|---|---|---|
| Worker de ingesta | Nada | 1–2 semanas | **Inmediata** |
| Plataforma web | Worker | 4–6 semanas | Alta |
| Medidor Circutor | Identificar el modelo | 1 semana | Alta |
| Módulo SD | Hardware | 1 día | Media |
| INA219 (tensión/corriente del nodo) | Hardware | 1 día | Media |
| Pruebas automatizadas | Nada | 2–3 días | Media |
| LoRaWAN | Radio + credenciales | 3–4 semanas | Media |
| Deep sleep | Nada | 3–4 días | Baja |

**El worker no está bloqueado por nada.** El contrato está congelado, el esquema definido y existen mensajes reales para usar como fixtures.

---

## 7. Medidor Circutor: cómo abordarlo

### 7.1 Ambigüedad a resolver primero

Se dispone de documentación de **dos productos distintos**:

- `VariablesModbusSerieCirwattTipoB.pdf` → mapa de registros del **Cirwatt serie B**
- Manual y ficha del **Cir-SET 130/320/340** → otro equipo (registrador para subestaciones)

**Las direcciones de registro son del Cirwatt B.** Si el equipo instalado es un Cir-SET, ese mapa puede no aplicar.

> **Acción bloqueante:** fotografiar la etiqueta del equipo antes de escribir código.

### 7.2 Mapa de registros del Cirwatt B

Solo funciones de lectura `0x03`/`0x04`. Todos los valores son enteros de 32 bits con decimales implícitos, repartidos en dos registros de 16 bits.

| Magnitud | Dirección | Escala |
|---|---|---|
| Tensión L1/L2/L3 | `0x0732`–`0x0736` | ÷10 → V |
| Corriente L1/L2/L3 | `0x0738`–`0x073C` | ÷100 → A |
| cos φ L1/L2/L3 | `0x073E`–`0x0742` | ÷100 |
| Frecuencia | `0x0744` | ÷10 → Hz |
| Potencia activa L1/L2/L3/total | `0x0746`–`0x074C` | ÷100 → kW |
| Potencia reactiva L1/L2/L3/total | `0x074E`–`0x0754` | ÷100 → kvar |
| Potencia aparente L1/L2/L3/total | `0x0756`–`0x075C` | ÷100 → kVA |
| Energía activa importada | `0x0708` | kWh, sin decimales |

**Ventaja clave:** el bloque `0x0732`→`0x075C` es **contiguo**. Son 44 registros, es decir **22 variables en una sola transacción Modbus** en lugar de 22 consultas.

### 7.3 Comunicaciones

Del manual del Cir-SET: RS-232 punto a punto, RS-485 en bus (hasta 32 equipos, 1200 m), 9600 a 38400 baudios.

**Usar RS-485.** Es diferencial e inmune al ruido; en un tablero eléctrico con contactores de aire acondicionado conmutando, RS-232 single-ended entregaría datos corruptos.

Hardware necesario: transceptor **MAX485**, y en tablero eléctrico conviene uno **aislado** (ADM2483, ~USD 5) para evitar lazos de masa y transitorios.

### 7.4 Por qué este camino es valioso

Resuelve cuatro objetivos del Plan simultáneamente:

1. **Es el escenario "Industrial"** de la validación, con un instrumento profesional clase 0,5S.
2. **Mide tensión y corriente reales** — responde el planteo sobre canales eléctricos con un equipo certificado, no con un demostrador.
3. **La terraza inaccesible sin WiFi justifica LoRaWAN honestamente**, pasando de "lo usé porque estaba en el plan" a "la ubicación lo exigía".
4. **El consumo de aires acondicionados es un caso DDDM real:** cuándo arrancan, cuánto consumen, qué se puede optimizar.

### 7.5 Encaje arquitectónico

Un medidor Modbus implementa `ISensor` sin fricción, y es la validación más fuerte del diseño porque demuestra que `ISensor` modela **"una fuente de N canales"** y no "un chip":

```cpp
class ModbusEnergyMeter : public ISensor {
    // channelCount() -> 22.  read() -> UNA transacción Modbus.
};
```

Existe ya como plantilla en el repositorio, con el framing Modbus RTU y el CRC-16 implementados a mano para evitar una dependencia por algo aún sin validar.

### 7.6 Complicaciones detectadas

| # | Complicación | Peso |
|---|---|---|
| 1 | **¿Cirwatt B o Cir-SET?** Mapa equivocado = datos basura | **Alto** — verificar primero |
| 2 | **22 variables no entran en LoRaWAN** (~110 B binarios vs. 51) | **Alto** — elegir subconjunto |
| 3 | **Word order de los 32 bits** — hay que verificarlo empíricamente contra el display | Medio |
| 4 | **Valores con signo** — medidor de 4 cuadrantes; leer como `uint32` da 4.294.967.xxx | Medio (resuelto en la plantilla) |
| 5 | **Modbus RTU es mono-maestro** — si la universidad ya interroga el medidor, hay colisión | Medio — **preguntar si el puerto está libre** |
| 6 | **Posible contraseña de comunicaciones** — el manual la menciona para la interfaz óptica | Medio |
| 7 | **Energía es contador, no medición instantánea** — kWh es acumulativo, kW instantáneo; el dashboard debe diferenciarlos | Medio |
| 8 | **Acceso físico difícil** — cada reflash implica subir a la terraza, y OTA sobre LoRaWAN no es viable | Medio — dejar una vía WiFi de mantenimiento |

> **SEGURIDAD:** es un tablero trifásico energizado. La intervención debe coordinarse con personal habilitado de la universidad. No debe realizarse en solitario.

### 7.7 Subconjunto sugerido para LoRaWAN

Muestrear los 22 canales cada 10 s por RS-485 —que es cable y no cuesta nada— y transmitir cada 15 minutos un subconjunto agregado:

`P_total`, tensión promedio, corriente por fase, cos φ y el contador de energía. Entra en ~30 bytes binarios.

**Aquí la separación muestreo/transmisión deja de ser una optimización y pasa a ser obligatoria.**

---

## 8. LoRaWAN: cómo abordarlo

### 8.1 Cómo funciona

La confusión habitual es pensar que el gateway es como un router WiFi al que uno se conecta. **No lo es.**

```
Nodo  ──radio LoRa──►  Gateway  ──Internet──►  Network Server  ──MQTT──►  Backend
DevEUI + AppKey       solo repite            desencripta, deduplica     (acá se
                      paquetes de radio      y rutea                     conecta el worker)
```

El **gateway es tonto**: escucha radio y reenvía. Toda la inteligencia está en el **Network Server**, que es donde se registran los dispositivos y donde se expone el broker MQTT del que consume el backend. **El Network Server es la pieza que importa**, no el gateway.

### 8.2 Equipamiento disponible en la universidad

| Equipo | Qué es |
|---|---|
| **Milesight mini gateway** (`swe-wwaldbronn-rafaela-001`) | La antena. Serie UG6x. Puede correr un Network Server embebido o reenviar a uno externo — hay que determinar cuál |
| **Adeunis ARF8124A** | Adeunis fabrica dos familias relevantes: *Field Test Device* (medidor de cobertura) o *módem LoRaWAN* (radio por comandos AT). **Modelo a confirmar con la etiqueta** |
| **Elsys ELT-2** (`swe-wwaldbronn-rafaela-elsys-001`) | Nodo LoRaWAN comercial multisensor, de gama profesional |

**El EUI** es un identificador de 64 bits, el equivalente al MAC del gateway ante el Network Server. La "dirección" provista es, casi con seguridad, la URL del Network Server — el dato más valioso de los tres.

### 8.3 Dos oportunidades del equipamiento existente

**Si el Adeunis es un módem AT, cambia todo:** el ESP32 le hablaría por UART con comandos de texto y **no harían falta ni el SX1276 ni el stack LMIC**. Sería el camino más simple con diferencia.

**El Elsys ELT-2 es un regalo para el cronograma.** Si ya está dado de alta y transmitiendo:

1. Provee **datos LoRaWAN reales para desarrollar el backend hoy**, sin esperar a que el ESP32 tenga radio. Desacopla completamente el desarrollo de la plataforma del hardware de radio.
2. Es un **patrón de comparación profesional**. El Plan tiene una sección de benchmarking y cita a Cannon et al. sobre "precisión comparable a las opciones comerciales": poner el datalogger propio junto al ELT-2 midiendo lo mismo **es** esa validación.

### 8.4 Acción bloqueante

Una sola pregunta a quien facilitó el equipo:

> ¿Qué Network Server usa el gateway, y se pueden obtener credenciales MQTT para consumir los datos de la aplicación?

Sin eso no se puede avanzar. Con eso, se puede hacer todo.

### 8.5 Restricciones de fondo

LoRaWAN **no es "WiFi más lento"**; sus límites son cualitativamente distintos.

**El payload JSON no entra.** El límite es de ~51 bytes en los data rates lentos, hasta ~222 en los rápidos. El payload actual ronda los 200 bytes. Requiere `CompactBinaryCodec`: enteros escalados, identificadores de canal de un byte, sin delimitadores. Un registro con timestamp entra en 15–20 bytes.

**Frecuencia de muestreo ≠ frecuencia de transmisión.** La Fair Use Policy de TTN limita a **30 segundos de airtime por dispositivo y día**, del orden de decenas de mensajes diarios. Muestrear cada 15 s implicaría 5.760 mensajes: dos o tres órdenes de magnitud de diferencia. **Ya resuelto en el firmware** mediante intervalos separados y agregación.

**La configuración remota pasa a ser asíncrona.** En Clase A el nodo solo escucha en dos ventanas breves inmediatamente después de transmitir. La latencia de un cambio de configuración pasa de milisegundos al orden del intervalo de transmisión. La Clase C escucha continuamente pero consume como WiFi y anula el motivo de usar LoRaWAN.

**Seguridad distinta.** No usa TLS ni certificados: emplea AES-128 con `DevEUI`, `JoinEUI` y `AppKey` para activación OTAA.

**El broker cambia de lugar.** El worker se suscribe al Network Server (TTN/ChirpStack) en lugar de HiveMQ, con estructura de tópicos distinta y el payload envuelto en metadatos. Esto es lo que justifica el adaptador de normalización por fuente en el worker.

**Hardware.** El ESP32-WROOM-32D **no tiene radio LoRa**: requiere SX1276/SX1262 por SPI, o una placa integrada (Heltec WiFi LoRa 32, TTGO LoRa32). En Argentina la banda ISM aplicable es 902–928 MHz, plan **AU915**; conviene confirmarlo con el operador del gateway.

**Flash.** El firmware usa 77,2 % de la partición. Compilar ambos stacks —única forma de permitir selección en tiempo de ejecución— agrega 50–80 KB. Entra, pero el margen se estrecha considerando OTA. Plan de contingencia: conservar `ITransport` y decidir con dos entornos de build, sacrificando la selección en runtime pero preservando la arquitectura.

### 8.6 Secuencia recomendada

1. Confirmar Network Server y obtener credenciales MQTT.
2. Identificar el modelo del Adeunis.
3. Consumir el ELT-2 desde el worker — valida la ruta LoRaWAN completa sin tocar el ESP32.
4. Decidir el hardware de radio.
5. Implementar `CompactBinaryCodec`.
6. Implementar `LoRaWANTransport`.

Con este orden, si LoRaWAN cae del alcance por hardware o tiempo, **la arquitectura sigue siendo defendible**: la interfaz existe, está justificada, y la extensión queda documentada con su punto de inserción exacto. Es mucho más sólido que prometer dos protocolos y llegar con uno.

---

## 9. Otros pendientes

### 9.1 Módulo SD

`ILocalBuffer` ya existe y `LittleFsBuffer` lo implementa. Agregar `SdCardBuffer` es escribir una clase; la selección al arrancar —intentar SD, caer a LittleFS— ya está prevista. `SD.h` forma parte del core de ESP32, sin dependencia nueva.

**Motivo para hacerlo:** la flash interna es contingencia con capacidad limitada y desgaste acotado (~100.000 ciclos por sector). Una SD ofrece capacidad para meses de operación autónoma.

### 9.2 INA219 — tensión y corriente del propio nodo

Existe una tensión y una corriente que **siempre** se pueden medir, independientemente de los sensores conectados: las del propio dispositivo. Un INA219 o INA226 (I2C, ~USD 3) entrega tensión de batería, corriente consumida y potencia.

Vale la pena porque:

- La columna `battery_level` existe en el esquema y hoy nadie la alimenta.
- Es **imprescindible** para el despliegue solar: sin tensión de batería no hay forma de diagnosticar un nodo caído ni de dimensionar el panel.
- Convierte el consumo energético en dato medido en lugar de estimado.
- Es una respuesta directa y demostrable al planteo sobre canales eléctricos.

### 9.3 Vía analógica

Para sensores que sí producen tensión nativamente (4–20 mA, 0–10 V, termopares, termistores), corresponde una base `AnalogSensor` con la función de transferencia inyectada. Con ella, un transmisor industrial pasa a ser un archivo de veinte líneas.

**Nota sobre precisión:** el ADC interno del ESP32 es de 12 bits, no lineal en los extremos y ruidoso. Para un proyecto cuyo argumento central es la calidad del dato, un **ADS1115** (16 bits, I2C, ~USD 3) es una decisión barata que sostiene la afirmación de precisión. Para leer un lazo de 4–20 mA hace falta además una resistencia shunt de precisión: con 150 Ω el rango se traduce en 0,6–3,0 V.

**Reportar la señal cruda junto a la convertida** permite recalcular el histórico si se detecta un error de calibración. Materializa la trazabilidad que el Plan exige.

### 9.4 Pruebas automatizadas

PlatformIO integra Unity y admite un entorno `native` que compila en el host. La abstracción `ISensor` es precisamente lo que permite inyectar un `FakeSensor` y probar sin hardware.

Candidatos naturales: validación de rango, detección de fallo, serialización del payload, lógica del acumulador, rotación del buffer.

Respalda además la metodología iterativa que declara el Plan.

### 9.5 Deep sleep

El Plan especifica alimentación solar para los escenarios agrícola y urbano. El firmware mantiene dos tareas permanentemente activas con WiFi continuo: del orden de 80–160 mA sostenidos. Un despliegue solar autónomo requiere decenas de microamperios en reposo.

La separación muestreo/transmisión, ya implementada, es la precondición. Nótese que deep sleep y MQTT sobre TLS conviven mal: cada despertar exige rehacer el handshake (3–6 s y varios cientos de milijulios), lo que es un argumento técnico adicional a favor de LoRaWAN para el escenario agrícola.

---

## 10. Limitaciones conocidas

| Limitación | Impacto | Resolución |
|---|---|---|
| **PubSubClient no publica con QoS 1** | Entrega "a lo sumo una vez": un mensaje puede perderse sin que el nodo lo advierta. `publish()` en `true` significa "escrito al socket", no "recibido por el broker" | Migrar a `espMqttClient`, o aceptarlo midiendo la pérdida con `(boot, seq)` |
| Credenciales en NVS sin cifrar | Legibles con acceso físico y un lector de flash | Credenciales por dispositivo; cifrado de NVS; flash encryption |
| Usuario MQTT compartido | Un nodo comprometido da lectura de toda la flota y permite suplantar dispositivos | Credenciales separadas por rol, con publicación acotada al propio tópico |
| Registros con `ts:0` no deduplicables | Uno o dos por arranque, marcados como de calidad inferior | Un RTC externo (DS3231, ~USD 3) elimina el caso |
| Sin deep sleep | Incompatible con alimentación solar autónoma | §9.5 |
| Sin pruebas automatizadas | Sin red de seguridad ante regresiones | §9.4 |

---

## 11. Próximo paso recomendado

**El worker de ingesta**, porque no está bloqueado por nada: el contrato está congelado, el esquema definido y existen mensajes reales capturados del hardware para usar como fixtures de prueba — incluyendo el caso de fallo con `ok:false` y el agregado con `n:6`. Son mejores casos de test que cualquiera que se pudiera inventar.

En paralelo, dos consultas que desbloquean caminos y no dependen de escribir código:

1. La etiqueta del medidor Circutor.
2. El Network Server del gateway LoRaWAN y sus credenciales MQTT.
