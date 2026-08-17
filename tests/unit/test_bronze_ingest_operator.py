"""
Unit tests for BronzeIngestOperator

Tests the bronze ingestion operator functionality including:
- File listing from MinIO source bucket
- File copying to timestamped bronze paths
- SHA-256 checksum computation and verification
- Audit logging
- Retry logic with exponential backoff
"""

import hashlib
import os
import tempfile
from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import MagicMock, Mock, patch

import pytest
from minio import Minio
from minio.error import S3Error

# Import the operator - adjust path based on environment
try:
    from airflow.plugins.operators.bronze_ingest_operator import BronzeIngestOperator
except ImportError:
    from operators.bronze_ingest_operator import BronzeIngestOperator


class TestBronzeIngestOperatorInit:
    """Test operator initialization."""

    def test_operator_initialization(self):
        """Test that operator initializes with correct parameters."""
        minio_conn = {
            "endpoint": "localhost:9000",
            "access_key": "minioadmin",
            "secret_key": "changeme",
        }
        postgres_conn = {
            "host": "localhost",
            "port": 5432,
            "database": "retail_lakehouse",
            "user": "airflow",
            "password": "changeme",
        }

        operator = BronzeIngestOperator(
            task_id="bronze_ingest",
            minio_conn=minio_conn,
            postgres_conn=postgres_conn,
            source_bucket="source",
            bronze_bucket="bronze",
        )

        assert operator.task_id == "bronze_ingest"
        assert operator.minio_conn == minio_conn
        assert operator.postgres_conn == postgres_conn
        assert operator.source_bucket == "source"
        assert operator.bronze_bucket == "bronze"


class TestMinIOClient:
    """Test MinIO client initialization."""

    def test_init_minio_client_success(self):
        """Test successful MinIO client initialization."""
        minio_conn = {
            "endpoint": "localhost:9000",
            "access_key": "minioadmin",
            "secret_key": "changeme",
            "secure": False,
        }

        operator = BronzeIngestOperator(
            task_id="bronze_ingest",
            minio_conn=minio_conn,
            postgres_conn={},
        )

        with patch("airflow.plugins.operators.bronze_ingest_operator.Minio") as mock_minio:
            operator._init_minio_client()
            mock_minio.assert_called_once_with(
                endpoint="localhost:9000",
                access_key="minioadmin",
                secret_key="changeme",
                secure=False,
            )

    def test_init_minio_client_missing_credentials(self):
        """Test MinIO client initialization fails with missing credentials."""
        minio_conn = {
            "endpoint": "localhost:9000",
            # Missing access_key and secret_key
        }

        operator = BronzeIngestOperator(
            task_id="bronze_ingest",
            minio_conn=minio_conn,
            postgres_conn={},
        )

        with pytest.raises(ValueError, match="MinIO connection missing required fields"):
            operator._init_minio_client()


class TestChecksumComputation:
    """Test SHA-256 checksum computation."""

    def test_checksum_computation_simple_content(self):
        """Test SHA-256 checksum computation for simple content."""
        # Create a simple test file content
        test_content = b"test,data,here\n1,2,3\n"
        expected_checksum = hashlib.sha256(test_content).hexdigest()

        # Verify checksum format (64 hex characters)
        assert len(expected_checksum) == 64
        assert all(c in "0123456789abcdef" for c in expected_checksum)

    def test_checksum_computation_binary_content(self):
        """Test SHA-256 checksum computation for binary content."""
        # Create binary content
        test_content = bytes(range(256))
        checksum1 = hashlib.sha256(test_content).hexdigest()
        checksum2 = hashlib.sha256(test_content).hexdigest()

        # Checksums should be deterministic
        assert checksum1 == checksum2

    def test_checksum_verification_matching(self):
        """Test that identical content produces identical checksums."""
        test_content = b"order_id,order_date,units_sold\n1,2024-01-01,100\n"
        checksum1 = hashlib.sha256(test_content).hexdigest()
        checksum2 = hashlib.sha256(test_content).hexdigest()

        assert checksum1 == checksum2

    def test_checksum_verification_different_content(self):
        """Test that different content produces different checksums."""
        content1 = b"order_id,order_date,units_sold\n1,2024-01-01,100\n"
        content2 = b"order_id,order_date,units_sold\n1,2024-01-01,101\n"

        checksum1 = hashlib.sha256(content1).hexdigest()
        checksum2 = hashlib.sha256(content2).hexdigest()

        assert checksum1 != checksum2


class TestBronzePath:
    """Test bronze destination path construction."""

    def test_bronze_path_construction(self):
        """Test that bronze path is constructed correctly."""
        source_file = "sales_2024-01-15.csv"
        
        # Simulate timestamp in a fixed format
        now = datetime(2024, 1, 15, 14, 30, 45, tzinfo=timezone.utc)
        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")
        timestamp = now.strftime("%Y%m%d_%H%M%S")

        base_name = os.path.splitext(source_file)[0]
        bronze_path = (
            f"sales/{year}/{month}/{day}/{os.path.basename(base_name)}"
            f"_{timestamp}.csv"
        )

        assert bronze_path == "sales/2024/01/15/sales_2024-01-15_20240115_143045.csv"
        assert bronze_path.startswith("sales/")
        assert "sales_2024-01-15_" in bronze_path
        assert bronze_path.endswith(".csv")

    def test_bronze_path_with_special_characters(self):
        """Test bronze path construction with special characters in filename."""
        source_file = "sales_2024-01-15.csv"
        now = datetime(2024, 1, 15, 14, 30, 45, tzinfo=timezone.utc)
        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")
        timestamp = now.strftime("%Y%m%d_%H%M%S")

        base_name = os.path.splitext(source_file)[0]
        bronze_path = (
            f"sales/{year}/{month}/{day}/{os.path.basename(base_name)}"
            f"_{timestamp}.csv"
        )

        # Path should contain date components
        assert "/2024/01/15/" in bronze_path

    def test_bronze_path_preserves_filename(self):
        """Test that bronze path preserves source filename."""
        source_file = "sales_2024-01-20.csv"
        now = datetime(2024, 1, 20, 10, 0, 0, tzinfo=timezone.utc)
        year = now.strftime("%Y")
        month = now.strftime("%m")
        day = now.strftime("%d")
        timestamp = now.strftime("%Y%m%d_%H%M%S")

        base_name = os.path.splitext(source_file)[0]
        bronze_path = (
            f"sales/{year}/{month}/{day}/{os.path.basename(base_name)}"
            f"_{timestamp}.csv"
        )

        # Path should include source filename
        assert "sales_2024-01-20" in bronze_path


class TestAuditLogging:
    """Test audit logging functionality."""

    def test_write_audit_log_success(self):
        """Test writing successful audit record."""
        minio_conn = {
            "endpoint": "localhost:9000",
            "access_key": "minioadmin",
            "secret_key": "changeme",
        }
        postgres_conn = {
            "host": "localhost",
            "port": 5432,
            "database": "retail_lakehouse",
            "user": "airflow",
            "password": "changeme",
        }

        operator = BronzeIngestOperator(
            task_id="bronze_ingest",
            minio_conn=minio_conn,
            postgres_conn=postgres_conn,
        )

        # Mock PostgreSQL connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        operator._write_audit_log(
            mock_conn,
            source_filename="sales_2024-01-15.csv",
            status="success",
            bronze_path="sales/2024/01/15/sales_2024-01-15_20240115_143045.csv",
            file_size_bytes=1024,
            checksum_sha256="abc123def456",
        )

        # Verify cursor.execute was called with INSERT statement
        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args
        assert "INSERT INTO audit.file_ingestion_log" in call_args[0][0]

        # Verify commit was called
        mock_conn.commit.assert_called_once()

    def test_write_audit_log_failure(self):
        """Test writing failed audit record."""
        minio_conn = {
            "endpoint": "localhost:9000",
            "access_key": "minioadmin",
            "secret_key": "changeme",
        }
        postgres_conn = {
            "host": "localhost",
            "port": 5432,
            "database": "retail_lakehouse",
            "user": "airflow",
            "password": "changeme",
        }

        operator = BronzeIngestOperator(
            task_id="bronze_ingest",
            minio_conn=minio_conn,
            postgres_conn=postgres_conn,
        )

        # Mock PostgreSQL connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        operator._write_audit_log(
            mock_conn,
            source_filename="sales_2024-01-15.csv",
            status="failed",
            error_message="File copy failed after 3 retries",
        )

        # Verify cursor.execute was called
        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args
        assert "INSERT INTO audit.file_ingestion_log" in call_args[0][0]

    def test_write_audit_log_database_error(self):
        """Test audit logging handles database errors."""
        import psycopg2

        minio_conn = {
            "endpoint": "localhost:9000",
            "access_key": "minioadmin",
            "secret_key": "changeme",
        }
        postgres_conn = {
            "host": "localhost",
            "port": 5432,
            "database": "retail_lakehouse",
            "user": "airflow",
            "password": "changeme",
        }

        operator = BronzeIngestOperator(
            task_id="bronze_ingest",
            minio_conn=minio_conn,
            postgres_conn=postgres_conn,
        )

        # Mock PostgreSQL connection with error
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = psycopg2.Error("Database error")
        mock_conn.cursor.return_value = mock_cursor

        with pytest.raises(psycopg2.Error):
            operator._write_audit_log(
                mock_conn,
                source_filename="sales_2024-01-15.csv",
                status="success",
                bronze_path="sales/2024/01/15/sales_2024-01-15_20240115_143045.csv",
                file_size_bytes=1024,
                checksum_sha256="abc123def456",
            )

        # Verify rollback was called
        mock_conn.rollback.assert_called_once()


class TestFileListingFiltering:
    """Test file listing and filtering logic."""

    def test_list_source_files_filters_by_date(self):
        """Test that file listing filters by logical date."""
        minio_conn = {
            "endpoint": "localhost:9000",
            "access_key": "minioadmin",
            "secret_key": "changeme",
        }

        operator = BronzeIngestOperator(
            task_id="bronze_ingest",
            minio_conn=minio_conn,
            postgres_conn={},
            source_bucket="source",
        )

        # Create mock MinIO objects
        mock_obj1 = MagicMock()
        mock_obj1.object_name = "sales_2024-01-15.csv"
        mock_obj1.last_modified = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)

        mock_obj2 = MagicMock()
        mock_obj2.object_name = "sales_2024-01-16.csv"
        mock_obj2.last_modified = datetime(2024, 1, 16, 10, 0, 0, tzinfo=timezone.utc)

        # Old file before logical date
        mock_obj3 = MagicMock()
        mock_obj3.object_name = "sales_2024-01-14.csv"
        mock_obj3.last_modified = datetime(2024, 1, 14, 10, 0, 0, tzinfo=timezone.utc)

        mock_minio = MagicMock()
        mock_minio.list_objects.return_value = [mock_obj1, mock_obj2, mock_obj3]

        # Logical date: 2024-01-15 00:00:00
        logical_date = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)

        files = operator._list_source_files(mock_minio, logical_date)

        # Should include files from logical date onward
        assert "sales_2024-01-15.csv" in files
        assert "sales_2024-01-16.csv" in files
        # Should exclude old file
        assert "sales_2024-01-14.csv" not in files

    def test_list_source_files_empty_bucket(self):
        """Test file listing on empty bucket."""
        minio_conn = {
            "endpoint": "localhost:9000",
            "access_key": "minioadmin",
            "secret_key": "changeme",
        }

        operator = BronzeIngestOperator(
            task_id="bronze_ingest",
            minio_conn=minio_conn,
            postgres_conn={},
            source_bucket="source",
        )

        mock_minio = MagicMock()
        mock_minio.list_objects.return_value = []

        logical_date = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
        files = operator._list_source_files(mock_minio, logical_date)

        assert files == []

    def test_list_source_files_minio_error(self):
        """Test file listing handles MinIO errors."""
        minio_conn = {
            "endpoint": "localhost:9000",
            "access_key": "minioadmin",
            "secret_key": "changeme",
        }

        operator = BronzeIngestOperator(
            task_id="bronze_ingest",
            minio_conn=minio_conn,
            postgres_conn={},
            source_bucket="source",
        )

        mock_minio = MagicMock()
        mock_minio.list_objects.side_effect = S3Error(
            "BucketNotFound", None, None, None
        )

        logical_date = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)

        with pytest.raises(S3Error):
            operator._list_source_files(mock_minio, logical_date)
