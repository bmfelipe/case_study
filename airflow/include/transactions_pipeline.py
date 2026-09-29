"""Adquisición e ingesta sin dependencias de Airflow (testable por separado)."""

from __future__ import annotations

import csv
import hashlib
import os
import tempfile
from contextlib import closing
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, build_opener

CSV_COLUMNS = (
    "transaction_id",
    "customer_id",
    "transaction_date",
    "product_id",
    "product_name",
    "quantity",
    "price",
    "tax",
)
MAX_BYTES = 100 * 1024 * 1024  # límite operativo para el ejemplo; configurable al escalar
CHUNK_BYTES = 1024 * 1024


class HttpsOnlyRedirects(HTTPRedirectHandler):
    """No permitir que una URL HTTPS redirija hacia HTTP/file/etc."""

    def __init__(self, allowed_hosts: set[str]):
        self.allowed_hosts = allowed_hosts
        super().__init__()

    def redirect_request(self, request, fp, code, msg, headers, newurl):
        parsed = urlsplit(newurl)
        if parsed.scheme.lower() != "https" or parsed.hostname not in self.allowed_hosts:
            raise ValueError("La redirección debe permanecer en HTTPS y en un host autorizado")
        return super().redirect_request(request, fp, code, msg, headers, newurl)


class HashingReader:
    """Hash de los bytes exactos que COPY lee, no de una apertura anterior."""

    def __init__(self, source):
        self.source = source
        self.digest = hashlib.sha256()

    def read(self, size=-1):
        chunk = self.source.read(size)
        self.digest.update(chunk)
        return chunk


def validate_csv(path: Path) -> int:
    """Validación estructural en streaming; no descarta filas con errores de negocio."""
    csv.field_size_limit(1_000_000)
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.reader(file, strict=True)
        if next(reader, None) != list(CSV_COLUMNS):
            raise ValueError("Cabecera CSV incorrecta: se esperan las 8 columnas originales")
        count = 0
        for number, row in enumerate(reader, start=2):
            if len(row) != len(CSV_COLUMNS):
                raise ValueError(f"Registro CSV {number}: {len(row)} columnas, se esperan 8")
            if any("\x00" in value for value in row):
                raise ValueError(f"Registro CSV {number}: PostgreSQL no admite bytes NUL")
            count += 1
    if count == 0:
        raise ValueError("El CSV no contiene ningún registro")
    return count


def acquire_snapshot(
    source_path: Path, snapshot_dir: Path, source_url: str = ""
) -> dict[str, str | int]:
    """Copia/descarga de forma acotada y publica un archivo inmutable por SHA-256."""
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    url = source_url.strip()
    if url:
        parsed = urlsplit(url)
        allowed_hosts = {
            host.strip().lower() for host in os.getenv("SOURCE_ALLOWED_HOSTS", "").split(",")
            if host.strip()
        }
        if parsed.scheme.lower() != "https" or not parsed.hostname or parsed.username:
            raise ValueError("SOURCE_URL debe ser una URL HTTPS válida")
        if parsed.hostname not in allowed_hosts:
            raise ValueError("SOURCE_URL requiere SOURCE_ALLOWED_HOSTS con el host autorizado")
        # Nunca guardar query strings ni credenciales (pueden contener secretos).
        source_name = f"https://{parsed.hostname}{parsed.path}"
        stream = build_opener(HttpsOnlyRedirects(allowed_hosts)).open(url, timeout=30)
    else:
        source_name = source_path.name
        stream = source_path.open("rb")

    digest = hashlib.sha256()
    total_bytes = 0
    tmp_path = None
    try:
        with stream, tempfile.NamedTemporaryFile(
            mode="wb", dir=snapshot_dir, prefix=".downloading-", delete=False
        ) as tmp:
            tmp_path = Path(tmp.name)
            while chunk := stream.read(CHUNK_BYTES):
                total_bytes += len(chunk)
                if total_bytes > MAX_BYTES:
                    raise ValueError(f"Fuente demasiado grande (> {MAX_BYTES} bytes)")
                digest.update(chunk)
                tmp.write(chunk)

        row_count = validate_csv(tmp_path)
        snapshot = snapshot_dir / f"{digest.hexdigest()}.csv"
        if snapshot.exists():
            if file_sha256(snapshot) != digest.hexdigest():
                raise ValueError("El snapshot existente no coincide con su hash; requiere intervención")
            tmp_path.unlink()
        else:
            os.replace(tmp_path, snapshot)
        return {
            "path": str(snapshot),
            "sha256": digest.hexdigest(),
            "source_name": source_name,
            "row_count": row_count,
        }
    except Exception:
        if tmp_path is not None:
            tmp_path.unlink(missing_ok=True)
        raise


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        while chunk := file.read(CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


def ingest_snapshot(snapshot: dict[str, str | int]) -> dict[str, int | bool]:
    """COPY en una transacción: o se confirma el lote entero o no se publica nada.

    El candado de transacción serializa ingestas concurrentes. El índice UNIQUE
    sobre SHA-256 garantiza idempotencia también ante reintentos tras un fallo.
    """
    import psycopg2  # se instala solo en la imagen; validar/adquirir no lo requiere

    path = Path(str(snapshot["path"]))
    if file_sha256(path) != snapshot["sha256"]:
        raise ValueError("El contenido del snapshot ha cambiado desde su adquisición")

    with closing(
        psycopg2.connect(
            host=os.environ["PGHOST"],
            port=os.environ.get("PGPORT", "5432"),
            user=os.environ["PGUSER"],
            password=os.environ["PGPASSWORD"],
            dbname=os.environ["PGDATABASE"],
            connect_timeout=10,
        )
    ) as connection:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(930501101)")
                cursor.execute(
                    "SELECT batch_id, row_count FROM raw.ingestion_batches WHERE file_sha256 = %s",
                    (snapshot["sha256"],),
                )
                existing = cursor.fetchone()
                if existing:
                    if existing[1] != snapshot["row_count"]:
                        raise ValueError("Mismo hash con diferente cantidad de filas")
                    return {"batch_id": existing[0], "row_count": existing[1], "new": False}

                cursor.execute(
                    """CREATE TEMP TABLE incoming_transactions (
                        source_row_number bigint GENERATED BY DEFAULT AS IDENTITY,
                        transaction_id text, customer_id text, transaction_date text,
                        product_id text, product_name text, quantity text,
                        price text, tax text
                    ) ON COMMIT DROP"""
                )
                with path.open("rb") as csv_file:
                    copied_bytes = HashingReader(csv_file)
                    cursor.copy_expert(
                        """COPY incoming_transactions (
                            transaction_id, customer_id, transaction_date, product_id,
                            product_name, quantity, price, tax
                        ) FROM STDIN WITH (FORMAT csv, HEADER true)""",
                        copied_bytes,
                    )
                    if copied_bytes.digest.hexdigest() != snapshot["sha256"]:
                        raise ValueError("El snapshot cambió mientras PostgreSQL ejecutaba COPY")

                cursor.execute("SELECT count(*) FROM incoming_transactions")
                actual_count = cursor.fetchone()[0]
                if actual_count != snapshot["row_count"]:
                    raise ValueError("El número de filas COPIADAS no coincide con el CSV")

                cursor.execute(
                    """INSERT INTO raw.ingestion_batches
                           (file_sha256, source_name, row_count)
                       VALUES (%s, %s, %s) RETURNING batch_id""",
                    (snapshot["sha256"], snapshot["source_name"], actual_count),
                )
                batch_id = cursor.fetchone()[0]
                cursor.execute(
                    """INSERT INTO raw.customer_transactions (
                        batch_id, source_row_number, transaction_id, customer_id,
                        transaction_date, product_id, product_name, quantity, price, tax
                    ) SELECT %s, source_row_number + 1, transaction_id, customer_id,
                             transaction_date, product_id, product_name, quantity,
                             price, tax
                      FROM incoming_transactions""",
                    (batch_id,),
                )
                return {"batch_id": batch_id, "row_count": actual_count, "new": True}