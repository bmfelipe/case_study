# Customer transactions | Case Study Data Engineering

Implementación del [enunciado](Case%20Study%20-%20Data%20Engineering.md): CSV → PostgreSQL → dbt → tablas dimensionales y agregados, con Airflow y Docker Compose. Para entender **todas las decisiones y la semántica de los datos**, leer también la [explicación de arquitectura y calidad](docs/ARQUITECTURA.md).

> **Verificado en Windows con Docker Desktop:** `docker compose config`, `docker compose up --build -d`, ejecuciones exitosas del DAG, `dbt parse`, `dbt build` (**51/51 pasos: 7 tablas, 4 vistas y 40 tests**), 10 pruebas Python y reconciliación `1 lote | 100 filas raw | 71 hechos | 29 rechazadas | 10 clientes | 5 productos`. El workflow de GitHub Actions aún no se ha ejecutado hasta publicar el repositorio.

## Requisitos y puesta en marcha

- Docker Desktop/Engine con Compose v2 en funcionamiento, con al menos 4 GB de RAM recomendados para Airflow y PostgreSQL.
- No requiere instalar Airflow, dbt ni PostgreSQL en el host.

### Windows: Docker Desktop instalado pero `docker` no se reconoce

Si Docker Desktop **ya está funcionando** y aun así la terminal de VS Code muestra «`docker` no se reconoce», lo normal es que VS Code siga usando el `PATH` anterior a la instalación. En este equipo el ejecutable está en `%LOCALAPPDATA%\Programs\DockerDesktop\resources\bin`, **ya incluido en el PATH del usuario**, pero no en el PATH del proceso antiguo de VS Code. Cierra **todas** las ventanas de VS Code, ábrelo de nuevo desde Inicio (no desde otra terminal antigua), abre una terminal nueva y comprueba `where docker`, `docker version` y `docker compose version`.

Sin reiniciar VS Code, en **PowerShell** puedes refrescar el PATH únicamente de esa sesión (también permite encontrar `docker-credential-desktop`, imprescindible para descargar imágenes):

```powershell
$env:Path = [Environment]::GetEnvironmentVariable('Path', 'Machine') + ';' + [Environment]::GetEnvironmentVariable('Path', 'User')
docker compose up --build -d
```

No hace falta reinstalar Docker ni modificar permanentemente las variables de Windows.

En la raíz del repositorio:

```sh
docker compose config
docker compose up --build -d
docker compose ps
```

Se puede usar `docker compose up --build` en primer plano. El primer build descargará imágenes e instalará dependencias. Abrir **http://localhost:8080** (usuario **admin**, contraseña **admin**; solo desarrollo local). El DAG `customer_transactions` usa `@once` y arranca automáticamente cuando el scheduler está listo. Esperar a que `acquire`, `ingest`, `build_models` y `report_quality` terminen en verde. Para investigar:

```sh
docker compose logs --tail=100 airflow-init airflow-scheduler
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec postgres psql -U pipeline -d warehouse -c "SELECT batch_id, row_count FROM raw.ingestion_batches;"
```

Reejecutar manualmente el pipeline (también después de cambiar el archivo local, levantando de nuevo los contenedores):

```sh
docker compose exec airflow-scheduler airflow dags trigger customer_transactions
```

Una fuente con el mismo hash **no se inserta dos veces**, pero los marts se pueden recalcular para verificar cambios en dbt. Si el lote solicitado ya no es el último, `build_models` se marca `skipped` para evitar que un replay haga retroceder las métricas publicadas. Para ejecutar dbt manualmente cuando el DAG no esté en ejecución:

```sh
docker compose run --rm dbt build --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
```

Por defecto, esa ejecución usa el lote más reciente. `dbt` es un servicio Compose de uso interactivo (`profiles: [tools]`); el DAG ejecuta la misma instalación dbt en un virtualenv aislado dentro de la imagen de Airflow, sin montar el socket Docker.

Para apagar conservando datos: `docker compose down`. **Solo para reiniciar perdiendo todos los datos:** `docker compose down -v`. El script `docker/postgres/init.sql` se ejecuta únicamente con un volumen PostgreSQL nuevo: para un volumen ya existente se necesitan migraciones.

### Configuración opcional

Los valores por defecto permiten arrancar sin `.env`. Para personalizar, copiar `.env.example` a `.env` (en Windows CMD: `copy .env.example .env`) y editar antes del primer arranque. `.env` está ignorado por Git. Usar una contraseña PostgreSQL alfanumérica o con `_` en este Compose: la URI de Airflow incorpora `POSTGRES_PASSWORD` y los caracteres reservados (`@`, `/`, `:`...) requieren URL encoding. Cambiar la contraseña en `.env` **no** cambia el usuario de un volumen ya inicializado.

Por defecto, el CSV del repositorio está montado en solo lectura. Para sustituirlo por una descarga HTTPS, poner en `.env` `SOURCE_URL=https://dominio.example/ruta.csv` y `SOURCE_ALLOWED_HOSTS=dominio.example` (añadir hosts de redirección permitidos separados por comas) y reiniciar los servicios con `docker compose up -d`. No aceptar URLs de usuarios no confiables. No se registra la query string, que puede contener tokens.

PostgreSQL solo escucha en el host en `127.0.0.1:5433` (usuario `pipeline`, base `warehouse`); Airflow, en `127.0.0.1:8080`. Cambiar las credenciales predeterminadas si el entorno no es puramente local.

## Estructura

```text
docker-compose.yml, Dockerfile.airflow, docker/postgres/init.sql  # servicios y DDL raw
customer_transactions.csv                                        # fuente por defecto
airflow/dags/customer_transactions.py                             # DAG con dependencias
airflow/include/transactions_pipeline.py                          # snapshot + ingesta COPY
dbt/dbt_project.yml, dbt/profiles.yml                             # proyecto dbt
dbt/models/staging/, dbt/models/intermediate/                     # limpieza/clasificación
dbt/models/public/, dbt/macros/                                   # hechos/dimensiones en public
dbt/models/marts/, dbt/models/quality/, dbt/tests/                # agregados, vistas y tests
tests/test_transactions_pipeline.py                               # pruebas locales sin Docker
```

La base `warehouse` contiene `raw.ingestion_batches` y `raw.customer_transactions` (**originales, columnas de texto**). El contrato de consumo principal son **tablas reales** en `public`: `fact_transactions` (una transacción válida), `dim_product` (un producto) y `dim_customers` (un cliente conocido con `first_transaction_date` y `last_transaction_date`). El CSV no incluye nombre, email ni dirección de cliente; no se inventan. Los agregados siguen en `analytics_marts`. Para consultas antiguas, `analytics_marts.fact_table` y `analytics_marts.dim_table` continúan disponibles como **vistas de compatibilidad**; nuevas consultas deben usar `public`.

## Consultas de verificación

Abrir `psql` dentro del contenedor:

```sh
docker compose exec postgres psql -U pipeline -d warehouse
```

Si ya estás en **Exec** del contenedor `customer-transactions-postgres-1` en Docker Desktop, escribe `/usr/bin/psql -U pipeline -d warehouse` (sin `sudo`). Desde DBeaver/pgAdmin usa host `localhost`, puerto `5433`, base `warehouse`, usuario `pipeline` y la contraseña configurada en `.env` o la predeterminada `local_only_change_me`.

Ejecutar estas consultas (salir de `psql` con `\q`):

```sql
-- Para el CSV incluido, contrastar 100 = 71 + 29 tras ejecutar dbt.
SELECT (SELECT count(*) FROM raw.customer_transactions
        WHERE batch_id = (SELECT max(batch_id) FROM raw.ingestion_batches)) AS origen,
       (SELECT count(*) FROM public.fact_transactions) AS aceptadas,
       (SELECT count(*) FROM analytics_quality.rejected_transactions) AS rechazadas;

SELECT product_id, product_name FROM public.dim_product ORDER BY product_id;
SELECT customer_id, first_transaction_date, last_transaction_date
FROM public.dim_customers ORDER BY customer_id;
SELECT transaction_id, customer_id, quantity, unit_price, tax_amount,
       subtotal_amount, total_amount
FROM public.fact_transactions WHERE transaction_id = '1001';
SELECT f.transaction_id, p.product_name, c.first_transaction_date, f.total_amount
FROM public.fact_transactions AS f
JOIN public.dim_product AS p USING (product_id)
LEFT JOIN public.dim_customers AS c USING (customer_id)
ORDER BY f.transaction_id LIMIT 5;
SELECT * FROM raw.customer_transactions ORDER BY source_row_number LIMIT 5;
SELECT * FROM analytics_marts.agg_monthly ORDER BY month_start;
SELECT * FROM analytics_marts.agg_customer_monthly ORDER BY month_start, customer_id;

SELECT source_row_number, raw_quantity, raw_price, raw_tax, rejection_reasons
FROM analytics_quality.rejected_transactions ORDER BY source_row_number LIMIT 20;
SELECT severity, issue_code, count(*) AS incidencias
FROM analytics_quality.quality_events GROUP BY 1, 2 ORDER BY 1, 2;
```

Disparar de nuevo el DAG sin cambiar el archivo: `SELECT count(*) FROM raw.ingestion_batches;` debe seguir dando **1**. Tras modificar el CSV y relanzar, habrá otro lote y los marts mostrarán solo el lote nuevo. Si el DAG falla, examinar logs de la tarea y ejecutar las consultas de calidad después de resolver el problema.

Para probar Python localmente (3.11+):

```sh
python -m unittest discover -s tests -v
python -m compileall -q airflow tests
```

El workflow `.github/workflows/ci.yml` automatiza ambos niveles al hacer push/PR: pruebas unitarias y un test de integración en el runner Linux (`docker compose`, `airflow dags test`, dbt y aserción `100|71|29|10|5`). **El workflow aún no se ha ejecutado en GitHub Actions**; la integración local en Docker sí se verificó.

> Los ficheros SQL incluyen Jinja (`{{ ref(...) }}`) y sintaxis PostgreSQL (`::numeric`, `FILTER`): un linter SQL genérico de VS Code puede marcarlos incorrectamente. Su prueba real es `dbt build` en los contenedores.

**Antes de enviar el enlace del repositorio:** validar el despliegue en una máquina con Docker, confirmar todas las consultas/tests, versionar los archivos nuevos y hacer push. No se han publicado cambios en Git automáticamente.