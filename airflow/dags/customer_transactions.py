"""Pipeline de snapshot CSV a marts PostgreSQL, con tests dbt como gate."""

from __future__ import annotations

import json
import logging
import os
import subprocess
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from transactions_pipeline import acquire_snapshot, ingest_snapshot

logger = logging.getLogger(__name__)


@dag(
    dag_id="customer_transactions",
    start_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
    schedule="@once",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["data-engineering", "dbt", "quality"],
)
def customer_transactions():
    @task(execution_timeout=timedelta(minutes=5))
    def acquire() -> dict:
        snapshot = acquire_snapshot(
            source_path=Path("/opt/airflow/data/customer_transactions.csv"),
            snapshot_dir=Path("/opt/airflow/shared"),
            source_url=os.getenv("SOURCE_URL", ""),
        )
        logger.info("Snapshot SHA-256=%s registros=%s", snapshot["sha256"], snapshot["row_count"])
        return snapshot

    @task(execution_timeout=timedelta(minutes=15))
    def ingest(snapshot: dict) -> dict:
        result = ingest_snapshot(snapshot)
        logger.info("Ingesta: batch_id=%s filas=%s nuevo=%s", result["batch_id"], result["row_count"], result["new"])
        return result

    @task(execution_timeout=timedelta(minutes=20))
    def build_models(batch: dict) -> int:
        import psycopg2

        batch_id = batch["batch_id"]
        with closing(
            psycopg2.connect(
                host=os.environ["PGHOST"], port=os.environ["PGPORT"],
                user=os.environ["PGUSER"], password=os.environ["PGPASSWORD"],
                dbname=os.environ["PGDATABASE"], connect_timeout=10,
            )
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT max(batch_id) FROM raw.ingestion_batches")
                latest_batch_id = cursor.fetchone()[0]
        if latest_batch_id != batch_id:
            raise AirflowSkipException(
                f"Lote {batch_id} obsoleto; el vigente es {latest_batch_id}. No se sobrescriben los marts."
            )

        logger.info("dbt build + tests: batch_id=%s", batch_id)
        subprocess.run(
            [
                "/opt/dbt-venv/bin/dbt",
                "build",
                "--project-dir", "/opt/airflow/dbt",
                "--profiles-dir", "/opt/airflow/dbt",
                "--vars", json.dumps({"batch_id": batch_id}),
            ],
            check=True,
            timeout=1100,
        )
        return batch_id

    @task(execution_timeout=timedelta(minutes=2))
    def report_quality(batch_id: int) -> None:
        import psycopg2

        with closing(
            psycopg2.connect(
                host=os.environ["PGHOST"], port=os.environ["PGPORT"],
                user=os.environ["PGUSER"], password=os.environ["PGPASSWORD"],
                dbname=os.environ["PGDATABASE"], connect_timeout=10,
            )
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """SELECT 'accepted', count(*) FROM public.fact_transactions WHERE batch_id = %s
                       UNION ALL
                       SELECT 'rejected', count(*) FROM analytics_quality.rejected_transactions WHERE batch_id = %s
                       UNION ALL
                       SELECT 'customers', count(*) FROM public.dim_customers
                       UNION ALL
                       SELECT 'products', count(*) FROM public.dim_product""",
                    (batch_id, batch_id),
                )
                logger.info("Calidad lote %s: %s", batch_id, cursor.fetchall())
                cursor.execute(
                    """SELECT severity, issue_code, count(*)
                       FROM analytics_quality.quality_events WHERE batch_id = %s
                       GROUP BY severity, issue_code ORDER BY severity, issue_code""",
                    (batch_id,),
                )
                for severity, code, count in cursor.fetchall():
                    logger.info("Calidad lote %s: %s %s: %s", batch_id, severity, code, count)

    report_quality(build_models(ingest(acquire())))


customer_transactions()