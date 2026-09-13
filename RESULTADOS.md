# Resultados de evaluación — BertsonalTrainer backend (TFM)

Resumen en español de las métricas de evaluación del backend: lo esperado (objetivos SMART y SLO) frente a lo logrado (evidencia registrada). Fuentes: `docs/acceptance-criteria.md` (objetivos y SLO), `docs/evidence/` (evidencia de ejecuciones) y `scripts/acceptance-measure.sh` (medición de SLIs).

## SLO y SLI iniciales

- **SLI** (*Service Level Indicator*, indicador de nivel de servicio): la medición concreta que se observa.
- **SLO** (*Service Level Objective*, objetivo de nivel de servicio): el umbral que se compromete a cumplir sobre un SLI.

| Aspecto | SLI / medición | SLO inicial | Aplica a |
| --- | --- | --- | --- |
| Latencia de puntuación provisional | `serving.provisional_scores.calculated_at − kafka_timestamp` | p95 ≤ 5 s; p99 ≤ 10 s | Perfil nominal |
| Completitud de ingesta en Bronze | Entregas de Kafka procesadas con éxito vs. registros en Bronze | 100 % | Tests funcionales/de integración |
| Idempotencia provisional | Filas provisionales por evento lógico | Exactamente 1 | Tests de entrega duplicada |
| Rendimiento (throughput) en carga acelerada | Eventos de sesión totalmente procesados / segundos transcurridos | ≥ 100 eventos/s de media | Perfil de 60.000 usuarios |
| Completitud de la carga acelerada | Tiempo de procesado extremo a extremo de 60.000 sesiones | ≤ 10 min | Perfil de 60.000 usuarios |
| Tiempo de ejecución del batch diario | Inicio del batch hasta el reemplazo correcto de Gold | ≤ 10 min | Perfil de 60.000 usuarios |
| Idempotencia de reproceso de Gold | Diff funcional tras re-ejecutar la misma fecha de negocio | Sin cambios; 0 filas lógicas duplicadas | Tests de integración del batch |
| Flujo de demo grabado | Simulación de 20 usuarios hasta resultado Gold inspeccionable con batch forzado (infra ya en marcha) | ≤ 3 min | Perfil demo |

Nota: no se exige la SLO de latencia del perfil nominal durante la prueba de carga acelerada de 60.000 sesiones (intencionadamente rápida); ese perfil valida throughput, integridad y tiempo de completitud.

## Métricas de evaluación — esperado vs. logrado

| Métrica (SLI) | Esperado (SLO) | Logrado | Perfil |
| --- | --- | --- | --- |
| Latencia de puntuación provisional (`calculated_at − kafka_timestamp`) | p95 ≤ 5 s; p99 ≤ 10 s | **FAIL**: nominal 2026-09-16 → p95 21,2 s / p99 21,2 s; nominal 2026-09-17 → p95 9,3 s / p99 10,0 s. Re-medida 2026-09-13 desde la BD (ver `docs/evidence/20260913T234600Z_latency_remeasurement.md`) | Nominal |
| Completitud de ingesta en Bronze (entregas de Kafka procesadas vs. registros en Bronze) | 100 % | Cubierta por la suite de tests (sin evidencia numérica registrada) | Tests funcionales/integración |
| Idempotencia provisional (filas por evento lógico) | Exactamente 1 | Cubierta por `tests/streaming/test_provisional_insert_idempotency.py` | Tests de entrega duplicada |
| Rendimiento (throughput) en carga acelerada | ≥ 100 eventos/s de media | **731,71 ev/s** → CUMPLE (~7× margen) | 60.000 usuarios |
| Tiempo total de procesado de 60.000 sesiones | ≤ 10 min | **82 s** (~1,4 min) → CUMPLE | 60.000 usuarios |
| Tiempo del batch diario (inicio → reemplazo de Gold) | ≤ 10 min | **139 s** (~2,3 min) → CUMPLE | 60.000 usuarios |
| Idempotencia de reproceso de Gold (re-ejecutar la misma fecha de negocio) | Sin cambios; 0 filas lógicas duplicadas | Cubierta por `tests/batch/test_gold_idempotency.py` | Tests de integración del batch |
| Flujo de demo grabado (20 usuarios → Gold inspeccionable, infra ya en marcha) | ≤ 3 min | **20 s de ingesta + 68 s de batch = 88 s** (~1,5 min) → CUMPLE | Demo |

## Ejecuciones registradas en `docs/evidence/`

| Perfil | Usuarios | Ingesta (s) | Throughput (ev/s) | Batch (s) | Run ID | Commit |
| --- | --- | --- | --- | --- | --- | --- |
| demo | 20 | 20 | 1,00 | 68 | 20260908T173734Z | 07e7d7b |
| dev | 500 | 47 | 10,64 | 77 | 20260908T185344Z | 0895177 |
| dev (escala intermedia) | 10.000 | 54 | 185,19 | 88 | 20260908T190635Z | 5d1c55e |
| load | 60.000 | 82 | 731,71 | 139 | 20260908T191005Z | 32e0adf |
| nominal | 1.000 | 37 | 27,03 | 92 | 20260908T203740Z | b59c328 |
| nominal (retardo 500 ms) | 1.000 | 520 | 1,92 | 128 | 20260908T204250Z | 5aafd41 |

Los logs brutos de cada ejecución (`measure.out`) se guardan en `evidence/runs/<run_id>/` y están excluidos de Git por diseño.

## Objetivos SMART — traducción y estado

1. **SMART-1 — Rebanada vertical de streaming (P0):** ruta reproducible simulador → Kafka → Spark Structured Streaming → Bronze + Silver + PostgreSQL provisional, procesando al menos 1.000 sesiones completadas simuladas. *Logrado:* hasta 60.000 sesiones procesadas.
2. **SMART-2 — Calidad e idempotencia (P0):** pruebas automatizadas que demuestran el comportamiento acordado ante entradas malformadas/inválidas, palabras inválidas, sesiones vacías y reintentos técnicos; un evento lógico nunca produce más de una puntuación provisional. *Logrado:* suite de tests.
3. **SMART-3 — Ruta analítica diaria (P1):** Silver → batch Spark → PostgreSQL Gold, incluyendo métricas diarias de rima, métricas de originalidad, ponderación de dureza (dificultad) y rankings diario/semanal/mensual con reproceso reproducible e idempotente. *Logrado:* implementado y medido por `scripts/acceptance-measure.sh`.
4. **SMART-4 — Validación de escala local (P1):** validar y documentar cuatro perfiles — 20 usuarios/día (demo), 500 (desarrollo), 1.000 (nominal), 60.000 (prueba de carga) — registrando latencia, throughput, tiempo total de procesado y observaciones de recursos. *Logrado:* los 4 perfiles tienen evidencia registrada.
5. **SMART-5 — Demo manual reproducible (pre-entrega):** el flujo guionizado `simular mensajes → observar resultados de streaming → ejecutar batch forzado → inspeccionar Gold` se completa sin esperar tiempo de calendario real. *Logrado:* 88 s ≤ 3 min.
6. **SMART-6 — Calidad de ingeniería (pre-entrega):** instrucciones reproducibles de montaje local, tests unitarios/integración significativos y CI que ejecute las comprobaciones automáticas. *Logrado:* setup reproducible, suite de tests y CI en GitHub Actions (`.github/workflows/ci.yml`: Python 3.11 + Java 17 + servicio de PostgreSQL 15; ejecuta `./scripts/test.sh`).

## Fórmulas del modelo de puntuación

Fuente: `docs/batch-and-scoring.md` y su implementación en `services/spark/bertsonal_spark/batch/main.py` (coinciden).

### Variables

| Símbolo | Nombre en el sistema | Significado |
| --- | --- | --- |
| `R_w` | `daily_repetitions` | Nº de *otros* usuarios que enviaron la palabra normalizada `w` ese día (usuarios distintos − 1) |
| `R_máx` | `rmax` | Máximo `R_w` observado en el día |
| `W_s` | — | Conjunto de palabras válidas y distintas de la sesión `s` |
| `s_w` | `daily_word_score` | Puntuación de originalidad de la palabra `w` |
| `daily_score` | `daily_score` | Suma de `s_w` sobre `W_s` |
| `E_r` | `dictionary_word_count` | Nº de entradas del diccionario con la terminación de rima `r` |
| `m_r` | multiplicador de dureza | Ponderación por dificultad de la terminación `r` |
| `final_score` | `final_score` | Puntuación definitiva escrita en `gold.daily_scores` |

### Fórmulas (renderizadas)

**1. Originalidad por palabra** (rango de `s_w`: [0,5, 1]; si `R_máx = 0`, toda palabra puntúa 1):

$$s_w = 1 - 0.5 \cdot \frac{R_w}{R_{\max}}$$

**2. Puntuación diaria de la sesión** (suma sobre las palabras válidas y distintas; los duplicados dentro de la sesión no llegan a Gold):

$$\text{daily\_score} = \sum_{w \in W_s} s_w$$

**3. Multiplicador de dureza** (rango de `m_r`: [0,4, 1]; las terminaciones con menos palabras en el diccionario son más difíciles y penalizan menos, y la más común — 74 entradas — reduce hasta un 60 %):

$$m_r = \max\left(0,\ 1 - 0.6 \cdot \frac{E_r}{74}\right)$$

**4. Puntuación final** (si la terminación de la sesión no tiene multiplicador, el código aplica `final_score = daily_score`; ver `main.py:178-183`):

$$\text{final\_score} = \text{daily\_score} \cdot m_r$$

### Listo para copiar a PowerPoint

LaTeX (para el editor de ecuaciones):

```latex
s_w = 1 - 0.5 \cdot \frac{R_w}{R_{\max}}
\text{daily\_score} = \sum_{w \in W_s} s_w
m_r = \max\left(0,\ 1 - 0.6 \cdot \frac{E_r}{74}\right)
\text{final\_score} = \text{daily\_score} \cdot m_r
```

Texto plano Unicode (para un cuadro de texto normal):

```text
s_w = 1 − 0,5 × (R_w / R_máx)                →  si R_máx = 0: s_w = 1
daily_score = Σ s_w                          →  suma sobre las palabras válidas y distintas
m_r = máx(0, 1 − 0,6 × (E_r / 74))
final_score = daily_score × m_r
```

### Cómo cargarlas en PowerPoint

- **Como ecuación profesional** (Office 2016/365): Insertar → Ecuación → en la pestaña Ecuación, grupo Conversiones, activar `{}LaTeX` → pegar una línea del bloque `latex` → Convertir.
- **Como texto directo**: copiar una línea del bloque `text` en un cuadro de texto de la diapositiva.

## Limitaciones de la evidencia

- Los percentiles p95/p99 de latencia no constaban en los resúmenes de `docs/evidence/` (los `measure.out` brutos se excluyen de Git); se re-midieron el 2026-09-13 directamente sobre `serving.provisional_scores` con el mismo SQL de `scripts/acceptance-measure.sh`. Resultado: **FAIL** en ambas ejecuciones nominales (evidencia en `docs/evidence/20260913T234600Z_latency_remeasurement.md`). El SLI mide de producción en Kafka a escritura en PostgreSQL e incluye la espera en cola del microbatch; en ejecuciones sin backlog el flujo tarda 30–50 ms, por lo que la brecha la domina el coste fijo por microbatch, no el scoring en sí. El SLO queda sin cambiar; su recalibración es una decisión abierta.
- Las SLO basadas en tests (completitud de Bronze, idempotencia provisional, idempotencia de Gold) se acreditan mediante la suite automatizada, no mediante valores numéricos registrados.
- La SLO de latencia nominal no se exige durante la prueba de carga acelerada de 60.000 sesiones; ese perfil valida throughput, integridad y tiempo de completitud.
