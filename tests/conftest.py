"""Shared test fixtures for PySpark and sample data."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for testing.

    Uses a single session for the entire test suite to avoid
    the overhead of starting/stopping Spark per test.
    """
    session = (
        SparkSession.builder.master("local[2]")
        .appName("data_lake_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()


# Generic Brasileirão match shape used by the shared data-quality utilities
# tests. Domain-agnostic on purpose: it exercises null detection, row counts,
# and summaries — not any pipeline-specific logic.
MATCH_SCHEMA = StructType(
    [
        StructField("ge_match_id", StringType(), True),
        StructField("home_team", StringType(), True),
        StructField("away_team", StringType(), True),
        StructField("stadium", StringType(), True),
        StructField("stadium_state", StringType(), True),
        StructField("broadcast", StringType(), True),
        StructField("score_home", IntegerType(), True),
        StructField("score_away", IntegerType(), True),
    ]
)

SAMPLE_MATCHES = [
    ("ge-001", "Flamengo", "Vasco", "Maracana", "RJ", "Globo", 2, 1),
    ("ge-002", "Palmeiras", "Corinthians", "Allianz Parque", "SP", "SporTV", 1, 1),
    # Null stadium_state AND null broadcast (drives the null-detection asserts).
    ("ge-003", "Gremio", "Internacional", "Arena do Gremio", None, None, 0, 0),
    # Another null broadcast — keeps the broadcast null-count > 1.
    ("ge-004", "Cruzeiro", "Atletico-MG", "Mineirao", "MG", None, 3, 2),
    ("ge-005", "Bahia", "Vitoria", "Arena Fonte Nova", "BA", "Premiere", 2, 0),
]


@pytest.fixture
def sample_df(spark):
    """Create a sample DataFrame with realistic Brasileirão match data."""
    return spark.createDataFrame(SAMPLE_MATCHES, schema=MATCH_SCHEMA)


@pytest.fixture
def empty_df(spark):
    """Create an empty DataFrame with the match schema."""
    return spark.createDataFrame([], schema=MATCH_SCHEMA)
