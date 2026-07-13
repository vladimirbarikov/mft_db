# pylint: disable=too-many-lines
# pylint: disable=wrong-import-position
"""
Serialization utilities for Polars DataFrames in Airflow.

Provides functions to serialize Polars DataFrames to bytes and deserialize them back,
enabling safe and efficient data transfer between Airflow tasks through XCom.

Uses Apache Arrow format for optimal performance and compatibility.

For junction tables, use standard Python dict (JSON-serializable).
For core entity tables (large datasets), use Arrow bytes serialization.

Compatible with: Polars 1.36.1+, PyArrow 25.0.0+, Apache Airflow 3.0.6+
"""
# Standard library imports
from pathlib import Path
import sys

# Third-party imports
import polars as pl
import pyarrow as pa

# The relative path to the root project directory
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Add the project path to sys.path
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Local imports
from config import get_logger

# Logger setup
logger = get_logger(__name__)

def serialize_df(
        df: pl.DataFrame
    ) -> bytes:
    """
    Serialize Polars DataFrame to bytes using Arrow IPC format.
    
    Args:
        df: Polars DataFrame to serialize
    
    Returns:
        bytes: Serialized DataFrame in Arrow IPC format
    
    Raises:
        TypeError: If df is not a Polars DataFrame
        ValueError: If serialization fails
    
    Performance:
        - Arrow IPC format is optimized for columnar data
        - Typically 30-50% smaller than pickle
        - 2-5x faster serialization/deserialization
        - Memory efficient with zero-copy operations
    """
    if not isinstance(df, pl.DataFrame):
        raise TypeError(f"Expected pl.DataFrame, got {type(df)}")

    try:
        # Convert to Arrow Table
        arrow_table = df.to_arrow()

        # Write to Arrow IPC format (streaming format)
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, arrow_table.schema) as writer:
            writer.write_table(arrow_table)

        serialized = sink.getvalue().to_pybytes()
        logger.debug("Serialized DataFrame to %d bytes with Arrow IPC format", len(serialized))
        return serialized

    except Exception as e:
        logger.error("Arrow serialization failed: %s", e)
        raise ValueError(f"Failed to serialize DataFrame: {e}") from e

def deserialize_df(
        data: bytes
    ) -> pl.DataFrame:
    """
    Deserialize bytes back to Polars DataFrame from Arrow IPC format.
    
    Args:
        data: Serialized DataFrame bytes in Arrow IPC format
    
    Returns:
        pl.DataFrame: Deserialized DataFrame
    
    Raises:
        TypeError: If data is not bytes or result is not a DataFrame
        ValueError: If deserialization fails
    """
    if not isinstance(data, bytes):
        raise TypeError(f"Expected bytes, got {type(data)}")

    try:
        # Read from Arrow IPC format
        reader = pa.ipc.open_stream(data)
        arrow_table = reader.read_all()

        # Convert back to Polars DataFrame/Series
        result = pl.from_arrow(arrow_table)

        # Validate that result is a DataFrame
        if not isinstance(result, pl.DataFrame):
            raise TypeError(f"Expected DataFrame, got {type(result)}")

        logger.debug("Deserialized DataFrame with shape: %s", result.shape)
        return result

    except TypeError as e:
        logger.error("Type error during deserialization: %s", e)
        raise

    except Exception as e:
        logger.error("Arrow deserialization failed: %s", e)
        raise ValueError(f"Failed to deserialize data: {e}") from e
