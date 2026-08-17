"""
Pytest configuration and fixtures for the Retail Sales Lakehouse Migration project.

This file provides:
- Shared test fixtures
- Environment setup for testing
- Mock configurations for external services (MinIO, PostgreSQL, Airflow)
"""

import sys
from pathlib import Path

import pytest

# Add project root to path so imports work correctly
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "airflow" / "plugins"))


@pytest.fixture
def minio_conn():
    """Fixture providing MinIO connection configuration."""
    return {
        "endpoint": "localhost:9000",
        "access_key": "minioadmin",
        "secret_key": "changeme",
        "secure": False,
    }


@pytest.fixture
def postgres_conn():
    """Fixture providing PostgreSQL connection configuration."""
    return {
        "host": "localhost",
        "port": 5432,
        "database": "retail_lakehouse",
        "user": "airflow",
        "password": "changeme",
    }


@pytest.fixture
def airflow_context():
    """Fixture providing mock Airflow context."""
    from datetime import datetime, timezone
    from unittest.mock import MagicMock

    context = {
        "logical_date": datetime(2024, 1, 15, 2, 0, 0, tzinfo=timezone.utc),
        "dag_run": MagicMock(dag_id="retail_sales_pipeline"),
        "task": MagicMock(task_id="bronze_ingest"),
    }
    return context
