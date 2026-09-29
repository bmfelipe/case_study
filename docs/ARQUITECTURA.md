# Guía técnica y de negocio: cómo y por qué funciona este pipeline

Este documento explica los criterios adoptados para el caso. Es importante separar **lo que está demostrado por el CSV y su esquema**, **lo que asumimos** y **lo que requiere una validación real en Docker**.

## 1. Objetivo, límites y datos observados

El reto pide PostgreSQL para almacenamiento, Airflow para orquestación, dbt para transformaciones, hechos, dimensiones, agregados y controles de calidad, todo reproducible con Compose. El contrato actual solicitado publica `public.fact_transactions`, `public.dim_product` y `public.dim_customers`; los antiguos `dim_table` y `fact_table` se conservan como vistas para no romper consultas. En un enfoque de plataforma no basta con llegar a una tabla final: debemos saber **qué archivo llegó, qué se perdió, qué se corrigió y qué significan las métricas**.

El CSV suministrado contiene 100 registros, con las columnas `transaction_id, customer_id, transaction_date, product_id, product_name, quantity, price, tax`. Un perfilado local detectó 14 `customer_id` vacíos, 16 `quantity` vacías, 11 precios textuales (`Two Hundred`), 9 impuestos textuales (`Fifteen`), 12 fechas DMY, 10 IDs de transacción con `T` y 6 IDs de producto con `P`. La suma de incidencias excede el número de rechazados porque **una fila puede tener más de un problema**. Bajo las reglas del modelo, PostgreSQL confirmó **71 aceptadas y 29 rechazadas**.

### Supuestos explícitos

1. `price` es el precio **por unidad** y `tax` es el importe monetario **del conjunto de la transacción**. El enunciado no proporciona la semántica exacta, y esta elección se acordó antes de implementar. Si negocio confirma que `tax` es una tasa o es por unidad, hay que modificar `public.fact_transactions`, los agregados y sus pruebas. Ejemplo del registro 1001: `quantity=1`, `price=76.27`, `tax=8.23` ⇒ `subtotal=76.27`, `total=84.50`.
2. No se conoce la **moneda**. Los importes son números decimales sin moneda; no se deben comparar transacciones de monedas diferentes ni llamar al total «EUR» sin un atributo de moneda.
3. `transaction_id` es un **identificador**, no un valor para operar aritméticamente. `T1010` se mantiene como texto; `T1010`, `1010` y `01010` son distintos hasta que el dueño del dominio declare lo contrario.
4. `product_id=P100` se acepta como `100` porque es un patrón concreto observado en la fuente; cualquier prefijo no contemplado se rechaza. `customer_id=501.0` se convierte en `501` si y solo si la parte decimal es cero. `quantity=2.0` se convierte en 2 por la misma razón.
5. `customer_id` vacío significa «cliente desconocido»; **no** se atribuye a un cliente ficticio. No se interpreta una cantidad vacía como 1, un precio de texto como 200 ni un impuesto de texto como 15: hacerlo inventaría ingresos.

## 2. Infraestructura y ciclo de arranque

`docker-compose.yml` crea PostgreSQL 16, un init one-shot de Airflow, webserver y scheduler. Hay una base `airflow` para la metadata de Airflow y otra `warehouse` para los esquemas `raw`, `public` y los generados por dbt. Comparten el mismo servidor para ahorrar recursos **solo en este entorno demostrativo**. `postgres-data` conserva ambas bases; `snapshots` guarda archivos adquiridos y `airflow-logs` persiste logs. Los SQL iniciales solo se aplican al crear el volumen vacío. `raw` ya existía: su tabla `customer_transactions` mantiene los campos originales como `TEXT` y los metadatos de cada lote se conservan en `raw.ingestion_batches`.

Por defecto, dbt antepone `analytics_` al schema indicado en `+schema`. Para que las tres **tablas físicas** de consumo existan exactamente en `public` (y no en `analytics_public`), `dbt/macros/generate_schema_name.sql` trata `public` como un destino explícito. Los modelos `staging`, `marts` y `quality` conservan respectivamente `analytics_staging`, `analytics_marts` y `analytics_quality`. PostgreSQL permite al usuario `pipeline`, propietario de `warehouse`, crear tablas en `public`.

El healthcheck PostgreSQL consulta por **TCP** la existencia de las tablas `raw.ingestion_batches` y `raw.customer_transactions`. Esta distinción importa: el servidor oficial arranca PostgreSQL temporalmente por socket mientras ejecuta sus scripts, y `pg_isready` por socket podría anunciar «healthy» *antes* de que exista `warehouse`.

La imagen de Airflow instala `psycopg2-binary` para `COPY` y dbt-postgres en `/opt/dbt-venv`: **aislar dbt** impide que la resolución de sus paquetes modifique las dependencias de Airflow. El DAG invoca ese ejecutable por ruta absoluta; el servicio `dbt` de Compose solo ofrece una interfaz interactiva para desarrolladores. El DAG y dbt ven el proyecto mediante montajes de solo lectura; los artefactos/logs de dbt se guardan en `/tmp` dentro de su contenedor. No se concede el socket Docker al orquestador.

`@once` + `DAGS_ARE_PAUSED_AT_CREATION=false` implica ejecutar la primera carga automáticamente cuando el scheduler vea el DAG, no una ingestión periódica indefinida. Se puede lanzar manualmente otra ejecución. `LocalExecutor` usa el mismo contenedor scheduler para ejecutar tareas, de modo que la ruta del snapshot compartida por XCom existe para las tareas; migrar a Celery/Kubernetes requeriría sustituirla por almacenamiento objeto y referencias a objetos.

## 3. ¿Qué hace cada tarea?

```text
origen local/HTTPS
  └─ acquire: descarga/copia y valida forma → snapshot SHA-256 en volumen
       └─ ingest: lock + staging temporal + COPY + transacción → raw
            └─ build_models: comprueba batch_id, dbt build → public.{fact_transactions,dim_customers,dim_product} + tests
                 └─ report_quality: publica métricas agregadas en logs
```

### `acquire` — contrato de archivo

El origen local por defecto es `customer_transactions.csv`; una URL HTTPS opcional debe aparecer en `SOURCE_URL` y su hostname en `SOURCE_ALLOWED_HOSTS`. También se validan los hostnames de las redirecciones. La lectura en trozos de 1 MiB limita memoria y el origen no puede superar 100 MiB; se verifican codificación UTF-8 (BOM permitido), cabecera exacta, ocho campos por registro, al menos un registro y que ningún campo tenga NUL (PostgreSQL no permite NUL en `text`). Un CSV con estructura mal formada **falla pronto**: no produce un lote parcial.

En cambio, la adquisición **no limpia datos de negocio**. La fila con `quantity` vacía debe llegar al origen intacta para poder ser explicada en el modelo de calidad. El contenido se guarda con nombre derivado de su SHA-256 y se publica tras completar la descarga (`os.replace`). La tarea devuelve solo ruta, hash, nombre de origen saneado y recuento, no copia las 100 filas a los metadatos de Airflow.

### `ingest` — idempotencia, eficiencia y atomicidad

PostgreSQL recibe el CSV mediante `COPY FROM STDIN` en una tabla **temporal**, no con 100 `INSERT` individuales ni con un dataframe completo en RAM. Las ocho columnas se almacenan como `TEXT`; la tipificación **ocurre después** en dbt. Se comprueba que el hash del archivo coincida con el adquirido y se recalcula sobre los **bytes leídos efectivamente por COPY**: una sustitución/cambio del snapshot en mitad de la carga da error y hace rollback.

La tabla `raw.ingestion_batches` tiene un índice `UNIQUE(file_sha256)` y registra origen, tamaño en registros y timestamp. `pg_advisory_xact_lock` serializa ingestas competidoras; una carga del mismo contenido devuelve el mismo `batch_id`. Solo tras verificar que `COPY` ha recibido el recuento previsto se insertan batch y filas en la **misma transacción**. Un fallo antes del commit descarta tabla temporal e inserciones. `raw.customer_transactions` usa `(batch_id, source_row_number)` como PK; `source_row_number` es número de **registro lógico** empezando en 2 (no necesariamente línea física cuando un CSV tiene campos multilínea). Los batches son append-only para auditoría, pero los marts modelan únicamente uno.

El hash es de **bytes**. Así, LF frente a CRLF es un lote nuevo aunque la información de negocio sea idéntica; esto se eligió conscientemente para rastrear el artefacto exacto recibido. Ni la deduplicación por hash ni el lock justifican usar `COPY` de un archivo que puede cambiar: por eso existe el hash en streaming durante el propio `COPY`.

### `build_models` / `report_quality` — orquestación y visibilidad

El DAG fija el `batch_id` en `--vars` de dbt. La vista staging selecciona ese ID, o el más reciente si se ejecuta dbt manualmente sin `--vars`. Antes de publicar, el DAG consulta `max(batch_id)`: una ejecución antigua posterior a otra nueva se marca `skipped` en vez de reconstruir métricas antiguas. `max_active_runs=1` serializa los runs del propio DAG, **no** los lanzamientos dbt fuera del DAG ni actores externos. dbt ejecuta modelos y tests con un único `dbt build`; cualquier error impide pasar a `report_quality`. La última tarea resume hechos, rechazados y códigos de calidad en logs sin mostrar datos personales fila a fila. Hay 2 reintentos con 1 minuto de espera; no hay webhook/email porque se deja su integración dependiente de la plataforma de alertas real.

## 4. Del CSV a hechos, dimensión y agregados

### Capa raw vs. staging

`raw` responde «¿qué recibimos y cuándo?». `analytics_staging.stg_transactions` responde «¿cómo se interpreta cada campo?». Contiene **una fila por registro**, valores originales, versiones tipadas, `validation_errors` y `quality_warnings`. Las conversiones usan regex **antes** de un cast con límites de longitud; por ejemplo, `Two Hundred` produce `unit_price = NULL` y `invalid_unit_price` en vez de hacer fallar toda la consulta. Los importes usan `NUMERIC(18,2)` y no `FLOAT` para evitar que los céntimos dependan de redondeos binarios; una fuente con más de dos decimales se rechaza (no se redondea silenciosamente).

Las fechas ISO y DMY se convierten a una representación ISO canónica, se verifica mes y día permitidos y se comprueba el **último día real del mes** a partir de `make_date(año, mes, 1)`. Solo entonces se invoca `make_date` con el día original: `2024-02-29` es válido y `2023-02-29` no bloquea todo el build, sino que queda como `invalid_transaction_date`. La fecha es `DATE`, sin zona horaria: no se debe fingir que el CSV incluye hora o huso.

### Clasificación y grano

`analytics_staging.int_classified_transactions` detecta IDs de transacción duplicados y productos que tengan varios nombres normalizados. Rechaza **todas** las filas con ese ID/conflicto, en lugar de usar `row_number()` para elegir arbitrariamente el primero. `public.fact_transactions` tiene grano **1 transacción válida** y `public.dim_product` tiene grano **1 producto**, identificado por `product_id` y nombre canónico normalizado.

`public.dim_customers` tiene grano **1 cliente conocido** (`customer_id` no nulo) con `first_transaction_date` y `last_transaction_date`, calculadas como el mínimo/máximo de las fechas de sus **transacciones aceptadas en el lote vigente**. El CSV no tiene nombre, email, dirección o segmentos; la dimensión no los inventa. Tampoco contiene importes o recuentos que cambien al agregar: esos indicadores viven en `analytics_marts.agg_customer_monthly`. Es una dimensión descriptiva mínima basada en actividad, **no un maestro de clientes independiente**: un cliente sin transacciones aceptadas no puede aparecer y el rango de fechas cambia cuando cambia el lote.

`analytics_marts.fact_table` y `analytics_marts.dim_table` son **vistas de compatibilidad** sobre las nuevas tablas, no copias físicas: conservan los nombres solicitados originalmente sin duplicar el hecho ni desincronizarlo. Los agregados y tests usan directamente las tablas de `public`.

En `public.fact_transactions`, `subtotal_amount = quantity * unit_price` y `total_amount = subtotal_amount + tax_amount`. El total incluye el impuesto **una vez** por transacción. Si el cliente no se conoce pero cantidad/precio/impuesto son válidos, el hecho permanece con `customer_id=NULL` y `missing_customer_id` como advertencia: excluirlo del total mensual falsearía la facturación. Por el contrario, una cantidad o importe no confiable rechaza toda la fila para **todas** las métricas financieras. Los `customer_id` conocidos deben encontrarse en `dim_customers` y todos los `product_id` de los hechos deben encontrarse en `dim_product`; ambas relaciones se comprueban con tests dbt (los nulos de cliente se permiten).

`analytics_marts.agg_monthly` agrupa por el día 1 de cada mes. Expone cantidad de transacciones, clientes conocidos distintos, ventas sin cliente identificado, unidades, subtotal, impuesto y total; incluye clientes ausentes con medidas válidas. `analytics_marts.agg_customer_monthly` agrupa por **cliente conocido × mes**: estos totales no tienen por qué sumar el total general si hay clientes NULL. El dataset incluido solo tiene fechas de julio de 2023; para probar varios meses se necesita otra fuente CSV.

### Rechazos y eventos

`analytics_quality.rejected_transactions` guarda los ocho campos **originales** y la lista `rejection_reasons`, incluso cuando hay múltiples motivos. `analytics_quality.quality_events` descompone listas en filas `error` o `warning`, que se pueden contar por motivo y conectar a alertas. Una advertencia también puede aparecer en una fila posteriormente rechazada por otra causa; contar eventos **no** equivale a contar filas rechazadas. Cliente ausente, fecha DMY convertida y producto `P100` son advertencias; identificadores inválidos, importes/cantidades imposibles, fechas inválidas, ID duplicado y catálogo contradictorio son errores.

## 5. Contratos verificables y pruebas

| Mecanismo | Propiedad que comprueba |
| --- | --- |
| `tests/test_transactions_pipeline.py` (`unittest`) | Cabecera, campos, BOM, NUL, CSV vacío, tamaño máximo, SHA estable, hash de bytes de COPY, snapshot corrupto, HTTP/redirects no autorizados y reintento idempotente simulado. No sustituye a un PostgreSQL real. |
| `dbt/models/_models.yml`, `_sources.yml` | `not_null`, `unique`, `relationships` hecho→dimensión y severidades aceptadas. |
| `dbt/tests/assert_batch_reconciled.sql` | Recuento del lote original = hechos + registros rechazados; prueba también que el lote exista. |
| `dbt/tests/assert_fact_amounts.sql` | Valores monetarios coherentes y no negativos; cantidades positivas. |
| `dbt/tests/assert_fact_matches_raw.sql` | ID, cantidad, precio, impuesto y fecha del hecho cotejados con la fila **original** del mismo batch/registro. |
| `dbt/tests/assert_customer_dimension_dates.sql` | Cada cliente y sus fechas primera/última coinciden exactamente con los hechos aceptados; no se inventan filas ni fechas. |
| `dbt/tests/assert_monthly_totals.sql`, `assert_customer_monthly_totals.sql` | Agregados recalculados desde hechos, cobertura de meses y unicidad cliente×mes. |

Los errores de calidad **esperados** en la fuente no hacen fallar el DAG si quedan registrados y las invariantes se cumplen. Esto evita el patrón de «rellenar con cero para que un test pase». Si cambia el contrato (por ejemplo, tasa de rechazo máxima), debe añadirse una prueba/alerta explícita con umbral aprobado por negocio y métricas temporales.

## 6. Gobernanza, escalabilidad y límites honestos

- **Seguridad:** `admin/admin`, claves de ejemplo, usuario `pipeline` compartido entre ingesta/dbt y PostgreSQL sin TLS son para localhost; en producción hacen falta secret manager, roles mínimos diferenciados, TLS, control de salida de red, imágenes fijadas por digest, análisis de dependencias y backup/PITR. La allowlist HTTPS no sustituye el aislamiento de red ni resuelve todos los riesgos de DNS rebinding. No imprimir URLs firmadas ni filas con identificadores de cliente en logs.
- **Versionado/retención:** `raw` y el volumen de snapshots crecen con cada archivo; definir retención, cifrado, clasificación PII y políticas de borrado para una plataforma real. El `init.sql` no es un sistema de migraciones de esquema. Mantener owners, linaje, contratos de versión y documentación de campos.
- **Atomicidad de publicación:** la ingesta sí es atómica **por lote**. Las tablas de dbt se reemplazan una a una; un test tardío fallido podría dejar parte de los marts actualizada, aunque el DAG informe fallo. Para una plataforma productiva: generar en un esquema/versión nueva, probar, comparar métricas y promocionar de forma controlada. Tampoco se garantiza exclusión frente a ejecuciones dbt manuales simultáneas; requiere lock de publicación o permisos operativos.
- **Escalado:** CSV de 100 MiB máximo y `LocalExecutor` son límites intencionales del ejercicio. Para millones de filas usar objetos inmutables en storage, manifiestos de lotes, particiones, carga paralela controlada, incrementalidad dbt y tests por partición; decidir índices/CLUSTER en función de consultas reales, no solo por intuición. Medir tiempos de `COPY`, consultas, lag, porcentaje rechazado y volumen. El archivo actual de 100 filas no justificaría complejidad adicional.
- **Semántica pendiente:** confirmar divisa, naturaleza exacta del impuesto, política de devoluciones/cantidades negativas, catálogo oficial de productos, equivalencia o no de IDs con prefijo, exactitud histórica de nombres y tratamiento de clientes desconocidos. Sin esas definiciones, la solución es una **hipótesis reproducible y auditada**, no un cálculo regulatorio certificado.
- **Verificación integrada ejecutada:** en Windows con Docker Desktop se construyeron y arrancaron los servicios; se ejecutó `dbt parse` con core 1.9.11/adapter postgres 1.9.0 y se relanzó el DAG, que pasó **51/51 pasos** (7 tablas, 4 vistas, 40 tests) sin avisos ni fallos. PostgreSQL confirmó **1 batch, 100 filas raw, 71 hechos en `public.fact_transactions`, 29 rechazadas, 10 clientes en `public.dim_customers` y 5 productos en `public.dim_product`**. Los nombres históricos siguen disponibles como vistas con 71 y 5 filas respectivamente. El `PATH` de la sesión antigua de VS Code no incluía la instalación de Docker por usuario; el [README](../README.md) explica cómo renovarlo. Queda pendiente observar el workflow de GitHub Actions cuando se publique el repositorio y validar un despliegue en un entorno diferente al local.