# pylint: disable=too-many-lines
# pylint: disable=wrong-import-position

"""
Orchestrating ETL Pipeline DAG for Material Flow Table Database.

This module defines a comprehensive Airflow DAG that orchestrates a complete
ETL (Extract, Transform, Load) pipeline for technical changes of parts data (Breakpoint).
The pipeline processes Excel-based technical changes of parts data, transforms it, 
and loads it into a PostgreSQL database with proper referential integrity.

Version: 1.0.0
Compatibility: Python 3.14.4+, Apache Airflow 3.0.6+
Maintainer: PLD Engineering Center
Created: 2026-08-04
Last Modified: 2026-08-04
Status: Production Ready
License: MIT
"""
# Standard library imports
import base64
from pathlib import Path
import sys
from datetime import datetime, timedelta
from polars import DataFrame
import pytz

# Third-party imports
# import polars as pl
from airflow.exceptions import AirflowSkipException
from airflow.sdk import DAG, dag, task, get_current_context


# The relative path to the root project directory
try:
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
except NameError:
    # If __file__ is not defined (in exec() or interactive mode)
    PROJECT_ROOT = Path("/opt/airflow")

# Add the project path to sys.path
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Local imports
from config import get_logger

# Logger setup
logger = get_logger(__name__)

# Timezone setup
moscow_tz = pytz.timezone('Europe/Moscow')

# DAG configuration
@dag(
    dag_id="bp_etl_pipeline",
    schedule=None,
    start_date=datetime(2026, 2, 7, tzinfo=moscow_tz),
    end_date=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'airflow',
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'depends_on_past': False,
    },
    tags=['etl', 'manufacturing', 'postgres', 'triggered']
)

def bp_etl_pipeline():
    """
    Breakpoint ETL pipeline triggered by file upload API.
        
    Triggered automatically after successful file upload via upload-bp-excel endpoint.
    Processes Excel files containing Breakpoint data with comprehensive validation.
    """
    # ========== EXTRACT PHASE ==========
    # Extract data for main Dataframe
    @task(task_id="extract_main_data")
    def extract_main_data() -> str:
        """
        Task extracts raw main data from Excel content provided by upload API.

        Receives base64-encoded file content from upload_api via DAG run conf,
        decoes it to bytes, and creates the main Polars DataFrame.
        All processing is done in memory - no disk I/O.
        """
        # The import has been moved to a function in order to:
        #  - Speed up DAG loading (from 30+ seconds to < 2 seconds)
        #  - Reduce the load on the Scheduler
        #  - Reduce memory consumption
        from dags.tasks.serializer import serialize_df  # pylint: disable=import-outside-toplevel
        from dags.tasks.extractor import create_main_df  # pylint: disable=import-outside-toplevel

        context = get_current_context()

        dag_run = context.get('dag_run')
        if not dag_run or not dag_run.conf:
            error_msg = "No configuration provided. DAG must be triggered with file information."
            logger.error(error_msg)
            raise AirflowSkipException(error_msg)

        conf = dag_run.conf

        # Logging metadata from upload_api
        logger.info("=" * 60)
        logger.info("PROCESSING FILE FROM UPLOAD API (STREAMING MODE)")
        logger.info("=" * 60)
        logger.info("File ID: %s", conf.get('unique_id', 'N/A'))
        logger.info("Original filename: %s", conf.get('original_filename', 'N/A'))
        logger.info("Upload timestamp: %s", conf.get('upload_timestamp', 'N/A'))
        logger.info("File hash: %s", conf.get('file_hash', 'N/A'))
        logger.info("Total rows: %s", conf.get('total_rows', 'N/A'))
        logger.info("File format: %s", conf.get('file_format', 'N/A'))
        logger.info("File size: %s bytes", conf.get('file_size', 'N/A'))

        # Get the file content from the configuration (base64 encoded)
        file_content_b64 = conf.get('file_content')
        if not file_content_b64:
            error_msg = "No file_content provided in DAG run configuration"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Decode base64 to bytes
        try:
            file_content = base64.b64decode(file_content_b64)
            logger.info("Successfully decoded file content: %d bytes", len(file_content))
        except Exception as e:
            logger.error("Failed to decode base64 file content: %s", str(e))
            raise ValueError(f"Invalid base64 file content: {str(e)}") from e

        # Saving metadata in XCom for future tasks
        ti = context.get('ti')
        if ti:
            ti.xcom_push(key='file_metadata', value=conf)
        else:
            logger.warning("Could not get task instance from context")

        logger.info("Extracting main data from memory (streaming mode)")

        # Pass bytes directly to create_main_df (streaming mode)
        main_df = create_main_df(file_content)

        logger.info(
            "Successfully extracted main data.\n"
            "Shape: %d rows, %d columns.\n"
            "Columns: %s.",
            main_df.height,
            main_df.width,
            ', '.join(main_df.columns),
        )

        serialized_main_df = serialize_df(main_df)

        logger.debug(
            "Serialized main data to %d bytes.",
            len(serialized_main_df)
        )

        # Converting bytes → base64 string for XCom
        serialized_main_df_b64 = base64.b64encode(serialized_main_df).decode('utf-8')
        logger.debug(
            "Encoded serialized data to base64: %d chars.",
            len(serialized_main_df_b64)
        )

        return serialized_main_df_b64


    # Extract data for core entity tables.
    @task(task_id="extract_breakpoint_data")
    def extract_breakpoint_data(
        serialized_main_df_b64: str
    ) -> str:
        """Task extracts raw breakpoint-specific data"""
        # The import has been moved to a function in order to:
        #  - Speed up DAG loading (from 30+ seconds to < 2 seconds)
        #  - Reduce the load on the Scheduler
        #  - Reduce memory consumption
        from config.columns_config import BREAKPOINT_COLS  # pylint: disable=import-outside-toplevel
        from dags.tasks.serializer import serialize_df, deserialize_df  # pylint: disable=import-outside-toplevel
        from dags.tasks.extractor import create_specialized_df  # pylint: disable=import-outside-toplevel

        logger.info(
            "Extracting breakpoint data..."
        )
        # Decoding base64 → bytes
        serialized_main_df = base64.b64decode(serialized_main_df_b64)

        # Deserialize the DataFrame from bytes
        main_df = deserialize_df(serialized_main_df)

        raw_breakpoint_df: DataFrame = create_specialized_df(main_df, BREAKPOINT_COLS)

        logger.info(
            "Successfully extracted breakpoint data.\n"
            "Shape: %d rows, %d columns.\n"
            "Columns: %s.",
            raw_breakpoint_df.height,
            raw_breakpoint_df.width,
            ', '.join(raw_breakpoint_df.columns),
        )

        # Serializing the DataFrame to bytes
        serialized_raw_breakpoint_df = serialize_df(raw_breakpoint_df)
        logger.debug(
            "Serialized breakpoint data to %d bytes.",
            len(serialized_raw_breakpoint_df)
        )

        # Converting bytes → base64 string for XCom
        serialized_raw_breakpoint_df_b64 = base64.b64encode(serialized_raw_breakpoint_df).decode('utf-8')
        logger.debug("Encoded to base64: %d chars", len(serialized_raw_breakpoint_df_b64))

        return serialized_raw_breakpoint_df_b64


    @task(task_id="extract_parts_before_data")
    def extract_parts_before_data(
        serialized_main_df_b64: str
    ) -> str:
        """Task extracts raw parts-before-specific data"""
        # The import has been moved to a function in order to:
        #  - Speed up DAG loading (from 30+ seconds to < 2 seconds)
        #  - Reduce the load on the Scheduler
        #  - Reduce memory consumption
        from config.columns_config import PART_BEFORE_COLS  # pylint: disable=import-outside-toplevel
        from dags.tasks.serializer import serialize_df, deserialize_df  # pylint: disable=import-outside-toplevel
        from dags.tasks.extractor import create_specialized_df  # pylint: disable=import-outside-toplevel

        logger.info(
            "Extracting parts-before data..."
        )
        # Decoding base64 → bytes
        serialized_main_df = base64.b64decode(serialized_main_df_b64)

        # Deserialize the DataFrame from bytes
        main_df = deserialize_df(serialized_main_df)

        raw_parts_before_df: DataFrame = create_specialized_df(main_df, PART_BEFORE_COLS)

        logger.info(
            "Successfully extracted parts-before data.\n"
            "Shape: %d rows, %d columns.\n"
            "Columns: %s.",
            raw_parts_before_df.height,
            raw_parts_before_df.width,
            ', '.join(raw_parts_before_df.columns),
        )

        # Serializing the DataFrame to bytes
        serialized_raw_parts_before_df = serialize_df(raw_parts_before_df)
        logger.debug(
            "Serialized parts-before data to %d bytes.",
            len(serialized_raw_parts_before_df)
        )

        # Converting bytes → base64 string for XCom
        serialized_raw_parts_before_df_b64 = base64.b64encode(serialized_raw_parts_before_df).decode('utf-8')
        logger.debug("Encoded to base64: %d chars", len(serialized_raw_parts_before_df_b64))

        return serialized_raw_parts_before_df_b64


    @task(task_id="extract_parts_after_data")
    def extract_parts_after_data(
        serialized_main_df_b64: str
    ) -> str:
        """Task extracts raw parts-after-specific data"""
        # The import has been moved to a function in order to:
        #  - Speed up DAG loading (from 30+ seconds to < 2 seconds)
        #  - Reduce the load on the Scheduler
        #  - Reduce memory consumption
        from config.columns_config import PART_AFTER_COLS  # pylint: disable=import-outside-toplevel
        from dags.tasks.serializer import serialize_df, deserialize_df  # pylint: disable=import-outside-toplevel
        from dags.tasks.extractor import create_specialized_df  # pylint: disable=import-outside-toplevel

        logger.info(
            "Extracting parts-after data..."
        )
        # Decoding base64 → bytes
        serialized_main_df = base64.b64decode(serialized_main_df_b64)

        # Deserialize the DataFrame from bytes
        main_df = deserialize_df(serialized_main_df)

        raw_parts_after_df: DataFrame = create_specialized_df(main_df, PART_AFTER_COLS)

        logger.info(
            "Successfully extracted parts-after data.\n"
            "Shape: %d rows, %d columns.\n"
            "Columns: %s.",
            raw_parts_after_df.height,
            raw_parts_after_df.width,
            ', '.join(raw_parts_after_df.columns),
        )

        # Serializing the DataFrame to bytes
        serialized_raw_parts_after_df = serialize_df(raw_parts_after_df)
        logger.debug(
            "Serialized parts-before data to %d bytes.",
            len(serialized_raw_parts_after_df)
        )

        # Converting bytes → base64 string for XCom
        serialized_raw_parts_after_df_b64 = base64.b64encode(serialized_raw_parts_after_df).decode('utf-8')
        logger.debug("Encoded to base64: %d chars", len(serialized_raw_parts_after_df_b64))

        return serialized_raw_parts_after_df_b64


    # Extract data for junction tables (many-to-many relationships)
    @task(task_id="extract_parts_to_breakpoint_data")
    def extract_parts_to_breakpoint_data(
        serialized_main_df_b64: str
    ) -> str:
        """Task extracts raw parts-to-breakpoint-specific data"""
        # The import has been moved to a function in order to:
        #  - Speed up DAG loading (from 30+ seconds to < 2 seconds)
        #  - Reduce the load on the Scheduler
        #  - Reduce memory consumption
        from config.columns_config import PART_TO_BREAKPOINT_COLS  # pylint: disable=import-outside-toplevel
        from dags.tasks.serializer import serialize_df, deserialize_df  # pylint: disable=import-outside-toplevel
        from dags.tasks.extractor import create_specialized_df  # pylint: disable=import-outside-toplevel

        logger.info(
            "Extracting parts-to-breakpoint-specific data..."
        )
        # Decoding base64 → bytes
        serialized_main_df = base64.b64decode(serialized_main_df_b64)

        # Deserialize the DataFrame from bytes
        main_df = deserialize_df(serialized_main_df)

        raw_parts_to_breakpoint_df: DataFrame = create_specialized_df(main_df, PART_TO_BREAKPOINT_COLS)

        logger.info(
            "Successfully extracted parts-to-breakpoint data.\n"
            "Shape: %d rows, %d columns.\n"
            "Columns: %s.",
            raw_parts_to_breakpoint_df.height,
            raw_parts_to_breakpoint_df.width,
            ', '.join(raw_parts_to_breakpoint_df.columns),
        )

        # Serializing the DataFrame to bytes
        serialized_raw_parts_to_breakpoint_df = serialize_df(raw_parts_to_breakpoint_df)
        logger.debug(
            "Serialized parts-before data to %d bytes.",
            len(serialized_raw_parts_to_breakpoint_df)
        )

        # Converting bytes → base64 string for XCom
        serialized_raw_parts_to_breakpoint_df_b64 = base64.b64encode(serialized_raw_parts_to_breakpoint_df).decode('utf-8')
        logger.debug("Encoded to base64: %d chars", len(serialized_raw_parts_to_breakpoint_df_b64))

        return serialized_raw_parts_to_breakpoint_df_b64

    # ========== TRANSFORM PHASE ==========
    # Transform data for core entity tables.
    @task(task_id="transform_breakpoint_data")
    def transform_breakpoint_data(
        serialized_raw_breakpoint_df_b64: str
    ) -> str:
        """Transform supplier data with text cleaning, type conversion and removing duplicates."""
        # The import has been moved to a function in order to:
        #  - Speed up DAG loading (from 30+ seconds to < 2 seconds)
        #  - Reduce the load on the Scheduler
        #  - Reduce memory consumption
        import polars as pl  # pylint: disable=import-outside-toplevel
        from config.columns_config import SUPPLIER_COLS  # pylint: disable=import-outside-toplevel
        from dags.tasks.serializer import serialize_df, deserialize_df  # pylint: disable=import-outside-toplevel
        from dags.tasks.transformer import (  # pylint: disable=import-outside-toplevel
            columns_to_lowercase,
            convert_to_string,
            basic_clean_text,
            advanced_clean_text,
            pinyin_conversion
        )

        logger.info(
            "Transforming supplier data..."
        )

        # Decoding base64 → bytes
        serialized_raw_supplier_df = base64.b64decode(serialized_raw_breakpoint_df_b64)

        # Deserialize the DataFrame from bytes
        supplier_df = deserialize_df(serialized_raw_supplier_df)

        # Drop rows where all values are missing/NaN
        supplier_df = supplier_df.filter(pl.any_horizontal(pl.all().is_not_null()))

        str_cols = [
            'SUPPLIER_NAME',
            'LOCATION',
            'CITY',
            'STREET',
            'BUILDING'
        ]

        # Apply converting and cleaninig text
        for col in str_cols:
            supplier_df = convert_to_string(supplier_df, col)
            supplier_df = advanced_clean_text(supplier_df, col)
            supplier_df = pinyin_conversion(supplier_df, col)

        # Apply converting and basic cleaning text to LOCALIZATION as it is ENUM ('yes', 'no')
        supplier_df = convert_to_string(supplier_df, 'LOCALIZATION')
        supplier_df = basic_clean_text(supplier_df, 'LOCALIZATION')

        # Removing duplicates across all SUPPLIER_COLS
        transformed_supplier_df = supplier_df.unique(subset=SUPPLIER_COLS, keep='first')

        # Convert all column names to lowercase
        transformed_supplier_df = columns_to_lowercase(transformed_supplier_df)

        logger.info(
            "Successfully transformed supplier data.\n"
            "Shape: %d rows, %d columns.\n"
            "Columns: %s.",
            transformed_supplier_df.height,
            transformed_supplier_df.width,
            ', '.join(transformed_supplier_df.columns),
        )

        # Serializing the DataFrame to bytes
        serialized_transformed_supplier_df = serialize_df(transformed_supplier_df)
        logger.debug(
            "Serialized transformed supplier data to %d bytes.",
            len(serialized_transformed_supplier_df)
        )

        # Converting bytes → base64 string for XCom
        serialized_transformed_supplier_df_b64 = base64.b64encode(serialized_transformed_supplier_df).decode('utf-8')
        logger.debug("Encoded to base64: %d chars", len(serialized_transformed_supplier_df_b64))

        return serialized_transformed_supplier_df_b64
    # Transform data for junction tables
    # Transform data for junction tables (many-to-many relationships)


    # ========== LOADING PHASE ==========
    # Separate tasks of loading core entity and junction tables in correct sequence
    # Validate data integrity after loading


    # ========== PIPELINE ORCHESTRATION ==========
    # EXTRACT PHASE
    # All tasks of extract phase depend on main_data
    # All extract tasks receive data from main_df
    # TRANSFORM PHASE
    # Each transform task depends on the corresponding extract task
    # LOADING PHASE
    # Loading all core entity tables in one task in the correct sequence
    # Loading all the junction tables (depending on core entities)
    # Validation of results

    return


# Instantiate the DAG
BP_ETL_DAG: DAG = bp_etl_pipeline()
