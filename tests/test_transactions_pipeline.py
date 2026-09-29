"""Pruebas locales de adquisición (sin Docker ni base de datos)."""

import hashlib
import io
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "airflow" / "include"))

from transactions_pipeline import (
    HashingReader,
    HttpsOnlyRedirects,
    acquire_snapshot,
    file_sha256,
    ingest_snapshot,
    validate_csv,
)


HEADER = "transaction_id,customer_id,transaction_date,product_id,product_name,quantity,price,tax\n"


class AcquisitionTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.base = Path(self.directory.name)
        self.input = self.base / "input.csv"
        self.snapshots = self.base / "snapshots"

    def test_snapshot_is_content_addressed_and_reusable(self):
        data = HEADER + "T1010,501.0,18-07-2023,P100,Product E,2.0,25.50,5.00\n"
        self.input.write_bytes(data.encode("utf-8"))
        first = acquire_snapshot(self.input, self.snapshots)
        second = acquire_snapshot(self.input, self.snapshots)
        self.assertEqual(first, second)
        self.assertEqual(first["row_count"], 1)
        self.assertEqual(first["sha256"], hashlib.sha256(data.encode()).hexdigest())
        self.assertEqual(file_sha256(Path(first["path"])), first["sha256"])
        self.assertEqual(len(list(self.snapshots.glob("*.csv"))), 1)

    def test_bad_header_does_not_publish_snapshot(self):
        self.input.write_text("id,price\n1,2\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Cabecera"):
            acquire_snapshot(self.input, self.snapshots)
        self.assertEqual(list(self.snapshots.iterdir()), [])

    def test_bad_column_count_fails_with_record_number(self):
        self.input.write_text(HEADER + "1,2\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Registro CSV 2"):
            acquire_snapshot(self.input, self.snapshots)

    def test_utf8_bom_and_missing_business_fields_are_preserved(self):
        self.input.write_text(HEADER + "1001,,2023-07-10,100,Product E,,5.00,2.00\n", encoding="utf-8-sig")
        self.assertEqual(validate_csv(self.input), 1)

    def test_non_https_remote_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "HTTPS"):
            acquire_snapshot(self.input, self.snapshots, "http://example.com/data.csv")

    def test_https_remote_needs_an_explicit_host_allowlist(self):
        with patch.dict(os.environ, {"SOURCE_ALLOWED_HOSTS": ""}):
            with self.assertRaisesRegex(ValueError, "SOURCE_ALLOWED_HOSTS"):
                acquire_snapshot(self.input, self.snapshots, "https://example.com/data.csv")
        redirects = HttpsOnlyRedirects({"example.com"})
        with self.assertRaisesRegex(ValueError, "host autorizado"):
            redirects.redirect_request(None, None, 302, "", {}, "https://internal.local/data.csv")

    def test_empty_and_nul_data_are_not_published(self):
        for data in (HEADER, HEADER + "1,2,2023-07-10,100,Product E,2,5.00,\x00\n"):
            with self.subTest(data=data):
                self.input.write_bytes(data.encode())
                with self.assertRaises(ValueError):
                    acquire_snapshot(self.input, self.snapshots)
                self.assertEqual(list(self.snapshots.iterdir()), [])

    def test_max_bytes_and_corrupted_existing_snapshot(self):
        self.input.write_bytes((HEADER + "1,2,2023-07-10,100,Product E,2,5.00,1.00\n").encode())
        with patch("transactions_pipeline.MAX_BYTES", 10):
            with self.assertRaisesRegex(ValueError, "demasiado grande"):
                acquire_snapshot(self.input, self.snapshots)
        self.assertEqual(list(self.snapshots.iterdir()), [])
        first = acquire_snapshot(self.input, self.snapshots)
        Path(first["path"]).write_bytes(b"corrupt")
        with self.assertRaisesRegex(ValueError, "snapshot existente"):
            acquire_snapshot(self.input, self.snapshots)
        self.assertEqual(len(list(self.snapshots.iterdir())), 1)

    def test_copy_stream_hashes_exactly_what_is_read(self):
        reader = HashingReader(io.BytesIO(b"\xef\xbb\xbfhello\r\n"))
        self.assertEqual(reader.read(3), b"\xef\xbb\xbf")
        self.assertEqual(reader.read(), b"hello\r\n")
        self.assertEqual(reader.digest.hexdigest(), hashlib.sha256(b"\xef\xbb\xbfhello\r\n").hexdigest())

    def test_existing_batch_is_not_copied_again(self):
        self.input.write_bytes((HEADER + "1,2,2023-07-10,100,Product E,2,5.00,1.00\n").encode())
        snapshot = acquire_snapshot(self.input, self.snapshots)

        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            def execute(self, statement, _params=None):
                if "COPY" in statement or "CREATE TEMP" in statement:
                    raise AssertionError("Un lote existente no debe copiar datos")

            def fetchone(self):
                return (7, 1)

        class Connection:
            def __init__(self):
                self.closed = False

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            def cursor(self):
                return Cursor()

            def close(self):
                self.closed = True

        connection = Connection()
        fake_psycopg2 = SimpleNamespace(connect=lambda **_: connection)
        variables = {"PGHOST": "db", "PGUSER": "user", "PGPASSWORD": "password", "PGDATABASE": "warehouse"}
        with patch.dict(sys.modules, {"psycopg2": fake_psycopg2}), patch.dict(os.environ, variables):
            self.assertEqual(ingest_snapshot(snapshot), {"batch_id": 7, "row_count": 1, "new": False})
        self.assertTrue(connection.closed)


if __name__ == "__main__":
    unittest.main()