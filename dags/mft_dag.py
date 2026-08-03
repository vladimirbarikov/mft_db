# pylint: disable=too-many-lines
# pylint: disable=wrong-import-position
"""
Orchestrating ETL Pipeline DAG for Material Flow Table Database.

This module defines a comprehensive Airflow DAG that orchestrates a complete
ETL (Extract, Transform, Load) pipeline for manufacturing data. The pipeline
processes Excel-based manufacturing data, transforms it, and loads it into
a PostgreSQL database with proper referential integrity.

Pipeline Architecture:
    The DAG follows a three-phase ETL pattern and is triggered by the upload API:
    1. TRIGGER: DAG is automatically triggered by upload-mft-excel endpoint with 
       file metadata passed via DAG run configuration (file_content base64, unique_id, etc.)
    2. EXTRACT: Reads the Excel content from memory (base64 decoded), extracts main DataFrame,
       and creates specialized datasets for each entity
    3. TRANSFORM: Cleans, validates, and prepares data for database loading
    4. LOAD: Bulk loads data into PostgreSQL with constraint management

Data Entities Processed:
    - Core Entities: Suppliers, Parts, Boxes, Pallets, Models, Workshops, Lines
    - Junction Tables: Many-to-many relationships between entities
    - Dimensional Data: Box and pallet dimensions with composite keys

Key Features:
    - Parallel extraction of specialized datasets from source Excel
    - Comprehensive data transformation with type conversion and cleaning
    - Referential integrity preservation through proper load sequencing
    - Composite key handling for dimensional entities (boxes, pallets)
    - Extensive logging and error handling throughout the pipeline
    - Data validation and integrity checks post-loading
    - Performance metrics collection for each task
    - STREAMING MODE: Processes Excel content from memory (no disk I/O)

Technical Implementation:
    - Uses Polars DataFrames for efficient data processing
    - Implements Airflow TaskFlow API with @task decorators
    - Manages XCom dependencies between parallel and sequential tasks
    - Integrates with custom loader module for database operations
    - Supports both standard and composite key mappings
    - Collects execution metrics for monitoring and optimization

DAG Schedule & Triggering:
    - Trigger-based execution (schedule_interval=None) - DAG runs only when triggered
    - Automatically triggered by upload API after successful file upload
    - File content passed via DAG run configuration (conf parameter) as base64
    - No catchup to prevent duplicate processing
    - Single concurrent run for data consistency
    - Configurable retry logic (3 attempts with 5-minute delays)

Integration with Upload API:
    This DAG is designed to work seamlessly with the upload_api.py module:
    
    1. File Upload: User uploads Excel file via /upload-mft-excel endpoint
    2. Validation: upload_api performs virus scan, Excel validation, security checks
    3. Encoding: File content is base64-encoded for transmission
    4. Trigger: upload_api triggers this DAG with file metadata in conf
    5. Processing: DAG decodes base64 content and processes it from memory
    6. No Disk I/O: Files are never written to disk in the Airflow container
    
    Data passed from upload_api to DAG via conf:
    - file_content: Base64-encoded Excel file content
    - file_name: Safe unique filename
    - original_filename: Original uploaded filename
    - unique_id: Unique identifier for tracking
    - file_hash: SHA-256 hash for integrity
    - upload_timestamp: When file was uploaded
    - total_rows: Total rows in main sheet
    - file_format: Excel format (xlsx/xls)
    - file_size: Size in bytes
    - sheets: List of sheets in file

Version: 2.0.0
Compatibility: Python 3.14.4+, Apache Airflow 3.0.6+
Maintainer: PLD Engineering Center
Created: 2025-01-19
Last Modified: 2026-07-27
Status: Production Ready
License: MIT
"""
# Standard library imports
import base64
from pathlib import Path
import sys
from datetime import datetime, timedelta
import pytz

# Third-party imports
import polars as pl
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
from config.columns_config import (
    # CORE ENTITY TABLES COLUMNS
    SUPPLIER_COLS, PART_COLS, BOX_COLS, PALLET_COLS,
    MODEL_COLS, CONFIGURATION_COLS, WORKSHOP_COLS, LINE_COLS,
    # JUNCTION TABLES COLUMNS
    PART_TO_BOX_COMPOSITE_COLS, BOX_TO_PALLET_COMPOSITE_COLS,
    PART_TO_MODEL_COLS, PART_TO_LINE_COLS
)

from dags.tasks.serializer import (
    serialize_df,
    deserialize_df
)
from dags.tasks.extractor import (
    create_main_df,
    create_specialized_df
)
from dags.tasks.transformer import (
    columns_to_lowercase,
    convert_to_int64,
    convert_to_string,
    convert_to_float,
    basic_clean_text,
    advanced_clean_text,
    pinyin_conversion
)
from dags.tasks.mft_loader import (
    load_core_entity_tables,
    load_junction_tables
)

# Logger setup
logger = get_logger(__name__)

# Timezone setup
moscow_tz = pytz.timezone('Europe/Moscow')

# DAG configuration
@dag(
    dag_id="mft_etl_pipeline",
    schedule=None,  # Изменено с schedule_interval на schedule
    start_date=datetime(2026, 2, 7, tzinfo=moscow_tz),  # Упрощенный способ указания timezone
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


def mft_etl_pipeline():
    """
    Manufacturing ETL pipeline triggered by file upload API.
    
    Triggered automatically after successful file upload via upload-mft-excel endpoint.
    Processes Excel files containing MFT data with comprehensive validation.
    
    Configuration passed via DAG run conf (from upload_api.py):
        - file_path: Full path to the uploaded file
        - original_filename: Original uploaded filename
        - unique_id: Unique identifier for tracking
        - file_hash: SHA-256 hash for integrity
        - upload_timestamp: When file was uploaded
        - total_rows: Total rows in main sheet
        - file_format: Excel format (xlsx/xls)
        - file_size: Size in bytes
        - sheets: List of sheets in file
    
    Processing Flow:
        1. Extract main DataFrame from file at file_path
        2. Create specialized DataFrames for each entity
        3. Transform and clean all DataFrames
        4. Load core entities in correct dependency order
        5. Load junction tables
        6. Validate data integrity
    
    DAG Configuration:
        - Schedule: None (trigger-based)
        - Catchup: False
        - Max active runs: 1
        - Retries: 3 with 5-minute delays
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
    @task(task_id="extract_supplier_data")
    def extract_supplier_data(
        serialized_main_df_b64: str
    ) -> str:
        """Task extracts raw supplier-specific data"""
        logger.info(
            "Extracting supplier data..."
        )
        # Decoding base64 → bytes
        serialized_main_df = base64.b64decode(serialized_main_df_b64)

        # Deserialize the DataFrame from bytes
        main_df = deserialize_df(serialized_main_df)

        raw_supplier_df = create_specialized_df(main_df, SUPPLIER_COLS)

        logger.info(
            "Successfully extracted supplier data.\n"
            "Shape: %d rows, %d columns.\n"
            "Columns: %s.",
            raw_supplier_df.height,
            raw_supplier_df.width,
            ', '.join(raw_supplier_df.columns),
        )

        # Serializing the DataFrame to bytes
        serialized_raw_supplier_df = serialize_df(raw_supplier_df)
        logger.debug(
            "Serialized supplier data to %d bytes.",
            len(serialized_raw_supplier_df)
        )

        # Converting bytes → base64 string for XCom
        serialized_raw_supplier_df_b64 = base64.b64encode(serialized_raw_supplier_df).decode('utf-8')
        logger.debug("Encoded to base64: %d chars", len(serialized_raw_supplier_df_b64))

        return serialized_raw_supplier_df_b64


    @task(task_id="extract_part_data")
    def extract_part_data(
        serialized_main_df_b64: str
    ) -> str:
        """Task extracts raw part-specific data"""
        logger.info(
            "Extracting part data..."
        )

        # Decoding base64 → bytes
        serialized_main_df = base64.b64decode(serialized_main_df_b64)

        # Deserialize the DataFrame from bytes
        main_df = deserialize_df(serialized_main_df)

        raw_part_df = create_specialized_df(main_df, PART_COLS)

        logger.info(
            "Successfully extracted part data.\n"
            "Shape: %d rows, %d columns.\n"
            "Columns: %s.",
            raw_part_df.height,
            raw_part_df.width,
            ', '.join(raw_part_df.columns),
        )

        # Serializing the DataFrame to bytes
        serialized_raw_part_df = serialize_df(raw_part_df)
        logger.debug(
            "Serialized part data to %d bytes.",
            len(serialized_raw_part_df)
        )

        # Converting bytes → base64 string for XCom
        serialized_raw_part_df_b64 = base64.b64encode(serialized_raw_part_df).decode('utf-8')
        logger.debug("Encoded to base64: %d chars", len(serialized_raw_part_df_b64))

        return serialized_raw_part_df_b64


    @task(task_id="extract_box_data")
    def extract_box_data(
        serialized_main_df_b64: str
    ) -> str:
        """Task extracts raw box-specific data"""
        logger.info(
            "Extracting box data..."
        )

        # Decoding base64 → bytes
        serialized_main_df = base64.b64decode(serialized_main_df_b64)

        # Deserialize the DataFrame from bytes
        main_df = deserialize_df(serialized_main_df)

        raw_box_df = create_specialized_df(main_df, BOX_COLS)

        logger.info(
            "Successfully extracted box data.\n"
            "Shape: %d rows, %d columns.\n"
            "Columns: %s.",
            raw_box_df.height,
            raw_box_df.width,
            ', '.join(raw_box_df.columns),
        )

        # Serializing the DataFrame to bytes
        serialized_raw_box_df = serialize_df(raw_box_df)
        logger.debug(
            "Serialized box data to %d bytes.",
            len(serialized_raw_box_df)
        )

        # Converting bytes → base64 string for XCom
        serialized_raw_box_df_b64 = base64.b64encode(serialized_raw_box_df).decode('utf-8')
        logger.debug("Encoded to base64: %d chars", len(serialized_raw_box_df_b64))

        return serialized_raw_box_df_b64


    @task(task_id="extract_pallet_data")
    def extract_pallet_data(
        serialized_main_df_b64: str
    ) -> str:
        """Task extracts raw pallet-specific data"""
        logger.info(
            "Extracting pallet data..."
        )

        # Decoding base64 → bytes
        serialized_main_df = base64.b64decode(serialized_main_df_b64)

        # Deserialize the DataFrame from bytes
        main_df = deserialize_df(serialized_main_df)

        raw_pallet_df = create_specialized_df(main_df, PALLET_COLS)

        logger.info(
            "Successfully extracted pallet data.\n"
            "Shape: %d rows, %d columns.\n"
            "Columns: %s.",
            raw_pallet_df.height,
            raw_pallet_df.width,
            ', '.join(raw_pallet_df.columns),
        )

        # Serializing the DataFrame to bytes
        serialized_raw_pallet_df = serialize_df(raw_pallet_df)
        logger.debug(
            "Serialized pallet data to %d bytes.",
            len(serialized_raw_pallet_df)
        )

        # Converting bytes → base64 string for XCom
        serialized_raw_pallet_df_b64 = base64.b64encode(serialized_raw_pallet_df).decode('utf-8')
        logger.debug("Encoded to base64: %d chars", len(serialized_raw_pallet_df_b64))

        return serialized_raw_pallet_df_b64


    @task(task_id="extract_model_data")
    def extract_model_data(
        serialized_main_df_b64: str
    ) -> str:
        """Task extracts raw model-specific data"""
        logger.info(
            "Extracting model data..."
        )

        # Decoding base64 → bytes
        serialized_main_df = base64.b64decode(serialized_main_df_b64)

        # Deserialize the DataFrame from bytes
        main_df = deserialize_df(serialized_main_df)

        raw_model_df = create_specialized_df(main_df, MODEL_COLS)

        logger.info(
            "Successfully extracted model data.\n"
            "Shape: %d rows, %d columns.\n"
            "Columns: %s.",
            raw_model_df.height,
            raw_model_df.width,
            ', '.join(raw_model_df.columns),
        )

        # Serializing the DataFrame to bytes
        serialized_raw_model_df = serialize_df(raw_model_df)
        logger.debug(
            "Serialized model data to %d bytes.",
            len(serialized_raw_model_df)
        )

        # Converting bytes → base64 string for XCom
        serialized_raw_model_df_b64 = base64.b64encode(serialized_raw_model_df).decode('utf-8')
        logger.debug("Encoded to base64: %d chars", len(serialized_raw_model_df_b64))

        return serialized_raw_model_df_b64


    @task(task_id="extract_configuration_data")
    def extract_configuration_data(
        serialized_main_df_b64: str
    ) -> str:
        """Task extracts raw configuration-specific data"""
        logger.info(
            "Extracting configuration data..."
        )

        # Decoding base64 → bytes
        serialized_main_df = base64.b64decode(serialized_main_df_b64)

        # Deserialize the DataFrame from bytes
        main_df = deserialize_df(serialized_main_df)

        raw_configuration_df = create_specialized_df(main_df, CONFIGURATION_COLS)

        logger.info(
            "Successfully extracted configuration data.\n"
            "Shape: %d rows, %d columns.\n"
            "Columns: %s.",
            raw_configuration_df.height,
            raw_configuration_df.width,
            ', '.join(raw_configuration_df.columns),
        )

        # Serializing the DataFrame to bytes
        serialized_raw_configuration_df = serialize_df(raw_configuration_df)
        logger.debug(
            "Serialized configuration data to %d bytes.",
            len(serialized_raw_configuration_df)
        )

        # Converting bytes → base64 string for XCom
        serialized_raw_configuration_df_b64 = base64.b64encode(serialized_raw_configuration_df).decode('utf-8')
        logger.debug("Encoded to base64: %d chars", len(serialized_raw_configuration_df_b64))

        return serialized_raw_configuration_df_b64


    @task(task_id="extract_workshop_data")
    def extract_workshop_data(
        serialized_main_df_b64: str
    ) -> str:
        """Task extracts raw workshop-specific data"""
        logger.info(
            "Extracting workshop data..."
        )

        # Decoding base64 → bytes
        serialized_main_df = base64.b64decode(serialized_main_df_b64)

        # Deserialize the DataFrame from bytes
        main_df = deserialize_df(serialized_main_df)

        raw_workshop_df = create_specialized_df(main_df, WORKSHOP_COLS)

        logger.info(
            "Successfully extracted workshop data.\n"
            "Shape: %d rows, %d columns.\n"
            "Columns: %s.",
            raw_workshop_df.height,
            raw_workshop_df.width,
            ', '.join(raw_workshop_df.columns),
        )

        # Serializing the DataFrame to bytes
        serialized_raw_workshop_df = serialize_df(raw_workshop_df)
        logger.debug(
            "Serialized workshop data to %d bytes.",
            len(serialized_raw_workshop_df)
        )

        # Converting bytes → base64 string for XCom
        serialized_raw_workshop_df_b64 = base64.b64encode(serialized_raw_workshop_df).decode('utf-8')
        logger.debug("Encoded to base64: %d chars", len(serialized_raw_workshop_df_b64))

        return serialized_raw_workshop_df_b64


    @task(task_id="extract_line_data")
    def extract_line_data(
        serialized_main_df_b64: str
    ) -> str:
        """Task extracts raw line-specific data"""
        logger.info(
            "Extracting line data..."
        )

        # Decoding base64 → bytes
        serialized_main_df = base64.b64decode(serialized_main_df_b64)

        # Deserialize the DataFrame from bytes
        main_df = deserialize_df(serialized_main_df)

        raw_line_df = create_specialized_df(main_df, LINE_COLS)

        logger.info(
            "Successfully extracted line data.\n"
            "Shape: %d rows, %d columns.\n"
            "Columns: %s.",
            raw_line_df.height,
            raw_line_df.width,
            ', '.join(raw_line_df.columns),
        )

        # Serializing the DataFrame to bytes
        serialized_raw_line_df = serialize_df(raw_line_df)
        logger.debug(
            "Serialized line data to %d bytes.",
            len(serialized_raw_line_df)
        )

        # Converting bytes → base64 string for XCom
        serialized_raw_line_df_b64 = base64.b64encode(serialized_raw_line_df).decode('utf-8')
        logger.debug("Encoded to base64: %d chars", len(serialized_raw_line_df_b64))

        return serialized_raw_line_df_b64


    # Extract data for junction tables (many-to-many relationships)
    @task(task_id="extract_part_to_box")
    def extract_part_to_box(serialized_main_df_b64: str) -> str:
        """Extract Part-to-Box junction data"""
        logger.info("Extracting Part-to-Box junction data...")

        serialized_main_df = base64.b64decode(serialized_main_df_b64)
        main_df = deserialize_df(serialized_main_df)

        # Extract Part-to-Box columns
        df = main_df.select(PART_TO_BOX_COMPOSITE_COLS)

        # Serialize to base64
        serialized_raw_part_to_box_df_b64 = base64.b64encode(serialize_df(df)).decode('utf-8')
        logger.debug("Part-to-Box serialized to %d chars", len(serialized_raw_part_to_box_df_b64))
        return serialized_raw_part_to_box_df_b64


    @task(task_id="extract_box_to_pallet")
    def extract_box_to_pallet(serialized_main_df_b64: str) -> str:
        """Extract Box-to-Pallet junction data"""
        logger.info("Extracting Box-to-Pallet junction data...")

        serialized_main_df = base64.b64decode(serialized_main_df_b64)
        main_df = deserialize_df(serialized_main_df)

        # Extract Box-to-Pallet columns
        df = main_df.select(BOX_TO_PALLET_COMPOSITE_COLS)

        # Serialize to base64
        serialized_box_to_pallet_df_b64 = base64.b64encode(serialize_df(df)).decode('utf-8')
        logger.debug("Box-to-Pallet serialized to %d chars", len(serialized_box_to_pallet_df_b64))
        return serialized_box_to_pallet_df_b64


    @task(task_id="extract_part_to_model")
    def extract_part_to_model(serialized_main_df_b64: str) -> str:
        """Extract Part-to-Model junction data"""
        logger.info("Extracting Part-to-Model junction data...")

        serialized_main_df = base64.b64decode(serialized_main_df_b64)
        main_df = deserialize_df(serialized_main_df)

        # Extract Part-to-Model columns
        df = main_df.select(PART_TO_MODEL_COLS)

        # Serialize to base64
        serialized_part_to_model_df_b64 = base64.b64encode(serialize_df(df)).decode('utf-8')
        logger.debug("Part-to-Model serialized to %d chars", len(serialized_part_to_model_df_b64))
        return serialized_part_to_model_df_b64


    @task(task_id="extract_part_to_line")
    def extract_part_to_line(serialized_main_df_b64: str) -> str:
        """Extract Part-to-Line junction data"""
        logger.info("Extracting Part-to-Line junction data...")

        serialized_main_df = base64.b64decode(serialized_main_df_b64)
        main_df = deserialize_df(serialized_main_df)

        # Extract Part-to-Line columns
        df = main_df.select(PART_TO_LINE_COLS)

        # Serialize to base64
        serialized_part_to_line_df_b64 = base64.b64encode(serialize_df(df)).decode('utf-8')
        logger.debug("Part-to-Line serialized to %d chars", len(serialized_part_to_line_df_b64))
        return serialized_part_to_line_df_b64


    # ========== TRANSFORM PHASE ==========
    # Transform data for core entity tables.
    @task(task_id="transform_supplier_data")
    def transform_supplier_data(
        serialized_raw_supplier_df_b64: str
    ) -> str:
        """Transform supplier data with text cleaning, type conversion and removing duplicates."""
        logger.info(
            "Transforming supplier data..."
        )

        # Decoding base64 → bytes
        serialized_raw_supplier_df = base64.b64decode(serialized_raw_supplier_df_b64)

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


    @task(task_id="transform_part_data")
    def transform_part_data(
        serialized_raw_part_df_b64: str
    ) -> str:
        """Transform part data with text cleaning, type conversion and removing duplicates."""
        logger.info(
            "Transforming part data..."
        )

        # Decoding base64 → bytes
        serialized_raw_part_df = base64.b64decode(serialized_raw_part_df_b64)

        # Deserialize the DataFrame from bytes
        part_df = deserialize_df(serialized_raw_part_df)

        # Drop rows where all values are missing/NaN
        part_df = part_df.filter(pl.any_horizontal(pl.all().is_not_null()))

        # Apply converting and cleaninig text
        part_df = convert_to_string(part_df, 'PART_NUMBER')
        part_df = basic_clean_text(part_df, 'PART_NUMBER')

        str_cols = ['PART_NAME', 'SUPPLIER_NAME']
        for col in str_cols:
            part_df = convert_to_string(part_df, col)
            part_df = advanced_clean_text(part_df, col)
            part_df = pinyin_conversion(part_df, col)

        # Converting weight to float with 2 decimal places
        part_df = convert_to_float(part_df, 'PART_WEIGHT_KG')

        # Removing duplicates across all PART_COLS
        transformed_part_df = part_df.unique(subset=PART_COLS, keep='first')

        # Convert all column names to lowercase
        transformed_part_df = columns_to_lowercase(transformed_part_df)

        logger.info(
            "Successfully transformed part data.\n"
            "Shape: %d rows, %d columns.\n"
            "Columns: %s.",
            transformed_part_df.height,
            transformed_part_df.width,
            ', '.join(transformed_part_df.columns),
        )

        # Serializing the DataFrame to bytes
        serialized_transformed_part_df = serialize_df(transformed_part_df)
        logger.debug(
            "Serialized transformed part data to %d bytes.",
            len(serialized_transformed_part_df)
        )

        # Converting bytes → base64 string for XCom
        serialized_transformed_part_df_b64 = base64.b64encode(serialized_transformed_part_df).decode('utf-8')
        logger.debug("Encoded to base64: %d chars", len(serialized_transformed_part_df_b64))

        return serialized_transformed_part_df_b64


    @task(task_id="transform_box_data")
    def transform_box_data(
        serialized_raw_box_df_b64: str
    ) -> str:
        """Transform box data with text cleaning, type conversion and removing duplicates."""
        logger.info(
            "Transforming box data..."
        )

        # Decoding base64 → bytes
        serialized_raw_box_df = base64.b64decode(serialized_raw_box_df_b64)

        # Deserialize the DataFrame from bytes
        box_df = deserialize_df(serialized_raw_box_df)

        # Drop rows where all values are missing/NaN
        box_df = box_df.filter(pl.any_horizontal(pl.all().is_not_null()))

        # Apply converting and cleaninig text
        box_df = convert_to_string(box_df, 'BOX_TYPE')
        box_df = basic_clean_text(box_df, 'BOX_TYPE')

        # Apply converting to Int64
        int_cols = [
            'BOX_LENGTH_MM',
            'BOX_WIDTH_MM', 
            'BOX_HEIGHT_MM',
            'BOX_STACKING'
        ]

        for col in int_cols:
            box_df = convert_to_int64(box_df, col)

        # Apply converting to float
        box_df = convert_to_float(box_df, 'BOX_WEIGHT_KG')

        # Removing duplicates across all BOX_COLS
        transformed_box_df = box_df.unique(subset=BOX_COLS, keep='first')

        # Convert all column names to lowercase
        transformed_box_df = columns_to_lowercase(transformed_box_df)

        logger.info(
            "Successfully transformed box data.\n"
            "Shape: %d rows, %d columns.\n"
            "Columns: %s.",
            transformed_box_df.height,
            transformed_box_df.width,
            ', '.join(transformed_box_df.columns),
        )

        # Serializing the DataFrame to bytes
        serialized_transformed_box_df = serialize_df(transformed_box_df)
        logger.debug(
            "Serialized transformed box data to %d bytes.",
            len(serialized_transformed_box_df)
        )

        # Converting bytes → base64 string for XCom
        serialized_transformed_box_df_b64 = base64.b64encode(serialized_transformed_box_df).decode('utf-8')
        logger.debug("Encoded to base64: %d chars", len(serialized_transformed_box_df_b64))

        return serialized_transformed_box_df_b64


    @task(task_id="transform_pallet_data")
    def transform_pallet_data(
        serialized_raw_pallet_df_b64: str
    ) -> str:
        """Transform pallet data with text cleaning, type conversion and removing duplicates."""
        logger.info(
            "Transforming pallet data..."
        )

        # Decoding base64 → bytes
        serialized_raw_pallet_df = base64.b64decode(serialized_raw_pallet_df_b64)

        # Deserialize the DataFrame from bytes
        pallet_df = deserialize_df(serialized_raw_pallet_df)

        # Drop rows where all values are missing/NaN
        pallet_df = pallet_df.filter(pl.any_horizontal(pl.all().is_not_null()))

        # Apply converting and cleaninig text
        pallet_df = convert_to_string(pallet_df, 'PALLET_TYPE')
        pallet_df = basic_clean_text(pallet_df, 'PALLET_TYPE')

        # Apply converting to Int64
        int_cols = [
            'PALLET_LENGTH_MM',
            'PALLET_WIDTH_MM',
            'PALLET_HEIGHT_MM',
            'PALLET_STACKING'
        ]

        for col in int_cols:
            pallet_df = convert_to_int64(pallet_df, col)

        # Apply converting to float
        pallet_df = convert_to_float(pallet_df, 'PALLET_WEIGHT_KG')

        # Removing duplicates across all PALLET_COLS
        transformed_pallet_df = pallet_df.unique(subset=PALLET_COLS, keep='first')

        # Convert all column names to lowercase
        transformed_pallet_df = columns_to_lowercase(transformed_pallet_df)

        logger.info(
            "Successfully transformed pallet data.\n"
            "Shape: %d rows, %d columns.\n"
            "Columns: %s.",
            transformed_pallet_df.height,
            transformed_pallet_df.width,
            ', '.join(transformed_pallet_df.columns),
        )

        # Serializing the DataFrame to bytes
        serialized_transformed_pallet_df = serialize_df(transformed_pallet_df)
        logger.debug(
            "Serialized pallet data to %d bytes.",
            len(serialized_transformed_pallet_df)
        )

        # Converting bytes → base64 string for XCom
        serialized_transformed_pallet_df_b64 = base64.b64encode(serialized_transformed_pallet_df).decode('utf-8')
        logger.debug("Encoded to base64: %d chars", len(serialized_transformed_pallet_df_b64))

        return serialized_transformed_pallet_df_b64


    @task(task_id="transform_model_data")
    def transform_model_data(
        serialized_raw_model_df_b64: str
    ) -> str:
        """Transform model data with text cleaning, type conversion and removing duplicates."""
        logger.info(
            "Transforming model data..."
        )

        # Decoding base64 → bytes
        serialized_raw_model_df = base64.b64decode(serialized_raw_model_df_b64)

        # Deserialize the DataFrame from bytes
        model_df = deserialize_df(serialized_raw_model_df)

        # Drop rows where all values are missing/NaN
        model_df = model_df.filter(pl.any_horizontal(pl.all().is_not_null()))

        # Apply converting and cleaninig text
        for col in MODEL_COLS:
            model_df = convert_to_string(model_df, col)
            model_df = basic_clean_text(model_df, col)

        # Removing duplicates across all MODEL_COLS
        transformed_model_df = model_df.unique(subset=MODEL_COLS, keep='first')

        # Convert all column names to lowercase
        transformed_model_df = columns_to_lowercase(transformed_model_df)

        logger.info(
            "Successfully transformed model data.\n"
            "Shape: %d rows, %d columns.\n"
            "Columns: %s.",
            transformed_model_df.height,
            transformed_model_df.width,
            ', '.join(transformed_model_df.columns),
        )

        # Serializing the DataFrame to bytes
        serialized_transformed_model_df = serialize_df(transformed_model_df)
        logger.debug(
            "Serialized model data to %d bytes.",
            len(serialized_transformed_model_df)
        )

        # Converting bytes → base64 string for XCom
        serialized_transformed_model_df_b64 = base64.b64encode(serialized_transformed_model_df).decode('utf-8')
        logger.debug("Encoded to base64: %d chars", len(serialized_transformed_model_df_b64))

        return serialized_transformed_model_df_b64


    @task(task_id="transform_configuration_data")
    def transform_configuration_data(
        serialized_raw_configuration_df_b64: str
    ) -> str:
        """Transform model data with text cleaning, type conversion and removing duplicates."""
        logger.info(
            "Transforming model data..."
        )

        # Decoding base64 → bytes
        serialized_raw_configuration_df = base64.b64decode(serialized_raw_configuration_df_b64)

        # Deserialize the DataFrame from bytes
        configuration_df = deserialize_df(serialized_raw_configuration_df)

        # Drop rows where all values are missing/NaN
        configuration_df = configuration_df.filter(pl.any_horizontal(pl.all().is_not_null()))

        # Apply converting text
        for col in CONFIGURATION_COLS:
            configuration_df = convert_to_string(configuration_df, col)

        # Apply cleaning text
        configuration_df = basic_clean_text(configuration_df, 'CONFIGURATION')
        configuration_df = basic_clean_text(configuration_df, 'TRANSMISSION')

        # Removing duplicates across all MODEL_COLS
        transformed_configuration_df = configuration_df.unique(
            subset=CONFIGURATION_COLS, keep='first'
        )

        # Convert all column names to lowercase
        transformed_configuration_df = columns_to_lowercase(transformed_configuration_df)

        logger.info(
            "Successfully transformed configuration data.\n"
            "Shape: %d rows, %d columns.\n"
            "Columns: %s.",
            transformed_configuration_df.height,
            transformed_configuration_df.width,
            ', '.join(transformed_configuration_df.columns),
        )

        # Serializing the DataFrame to bytes
        serialized_transformed_configuration_df = serialize_df(transformed_configuration_df)
        logger.debug(
            "Serialized model data to %d bytes.",
            len(serialized_transformed_configuration_df)
        )

        # Converting bytes → base64 string for XCom
        serialized_transformed_configuration_df_b64 = base64.b64encode(serialized_transformed_configuration_df).decode('utf-8')
        logger.debug("Encoded to base64: %d chars", len(serialized_transformed_configuration_df_b64))

        return serialized_transformed_configuration_df_b64


    @task(task_id="transform_workshop_data")
    def transform_workshop_data(
        serialized_raw_workshop_df_b64: str
    ) -> str:
        """Transform workshop data with text cleaning, type conversion and removing duplicates."""
        logger.info(
            "Transforming workshop data..."
        )

        # Decoding base64 → bytes
        serialized_raw_workshop_df = base64.b64decode(serialized_raw_workshop_df_b64)

        # Deserialize the DataFrame from bytes
        workshop_df = deserialize_df(serialized_raw_workshop_df)

        # Drop rows where all values are missing/NaN
        workshop_df = workshop_df.filter(pl.any_horizontal(pl.all().is_not_null()))

        # Apply converting and cleaninig text
        for col in WORKSHOP_COLS:
            workshop_df = convert_to_string(workshop_df, col)
            workshop_df = basic_clean_text(workshop_df, col)

        # Removing duplicates across all WORKSHOP_COLS
        transformed_workshop_df = workshop_df.unique(subset=WORKSHOP_COLS, keep='first')

        # Convert all column names to lowercase
        transformed_workshop_df = columns_to_lowercase(transformed_workshop_df)

        logger.info(
            "Successfully transformed workshop data.\n"
            "Shape: %d rows, %d columns.\n"
            "Columns: %s.",
            transformed_workshop_df.height,
            transformed_workshop_df.width,
            ', '.join(transformed_workshop_df.columns),
        )

        # Serializing the DataFrame to bytes
        serialized_transformed_workshop_df = serialize_df(transformed_workshop_df)
        logger.debug(
            "Serialized workshop data to %d bytes.",
            len(serialized_transformed_workshop_df)
        )

        # Converting bytes → base64 string for XCom
        serialized_transformed_workshop_df_b64 = base64.b64encode(serialized_transformed_workshop_df).decode('utf-8')
        logger.debug("Encoded to base64: %d chars", len(serialized_transformed_workshop_df_b64))

        return serialized_transformed_workshop_df_b64


    @task(task_id="transform_line_data")
    def transform_line_data(
        serialized_raw_line_df_b64: str
    ) -> str:
        """Transform line data with text cleaning, type conversion and removing duplicates."""
        logger.info(
            "Transforming line data..."
        )

        # Decoding base64 → bytes
        serialized_raw_line_df = base64.b64decode(serialized_raw_line_df_b64)

        # Deserialize the DataFrame from bytes
        line_df = deserialize_df(serialized_raw_line_df)

        # Drop rows where all values are missing/NaN
        line_df = line_df.filter(pl.any_horizontal(pl.all().is_not_null()))

        # Apply converting and cleaninig text
        for col in LINE_COLS:
            line_df = convert_to_string(line_df, col)
            line_df = basic_clean_text(line_df, col)

        # Removing duplicates across all LINE_COLS
        transformed_line_df = line_df.unique(subset=LINE_COLS, keep='first')

        # Convert all column names to lowercase
        transformed_line_df = columns_to_lowercase(transformed_line_df)

        logger.info(
            "Successfully transformed line data.\n"
            "Shape: %d rows, %d columns.\n"
            "Columns: %s.",
            transformed_line_df.height,
            transformed_line_df.width,
            ', '.join(transformed_line_df.columns),
        )

        # Serializing the DataFrame to bytes
        serialized_transformed_line_df = serialize_df(transformed_line_df)
        logger.debug(
            "Serialized line data to %d bytes.",
            len(serialized_transformed_line_df)
        )

        # Converting bytes → base64 string for XCom
        serialized_transformed_line_df_b64 = base64.b64encode(serialized_transformed_line_df).decode('utf-8')
        logger.debug("Encoded to base64: %d chars", len(serialized_transformed_line_df_b64))

        return serialized_transformed_line_df_b64


    # Transform data for junction tables
    @task(task_id="transform_part_to_box")
    def transform_part_to_box(part_to_box_b64: str) -> str:
        """Transform Part-to-Box junction table data."""
        logger.info("Transforming Part-to-Box junction data...")

        # Decoding the DataFrame
        df = deserialize_df(base64.b64decode(part_to_box_b64))

        # Transform
        if df is not None and not df.is_empty():
            # Drop rows where all values are missing/NaN
            df = df.filter(pl.any_horizontal(pl.all().is_not_null()))

            # Apply converting and cleaning text
            for col in ['PART_NUMBER', 'BOX_TYPE']:
                df = convert_to_string(df, col)
                df = basic_clean_text(df, col)

            # Apply converting to Int64
            for col in ['BOX_LENGTH_MM', 'BOX_WIDTH_MM', 'BOX_HEIGHT_MM', 'PART_PER_BOX']:
                df = convert_to_int64(df, col)

            # Remove duplicates
            df = df.unique(subset=PART_TO_BOX_COMPOSITE_COLS, keep='first')
            df = columns_to_lowercase(df)
        else:
            logger.warning("Part-to-Box junction table is empty")
            df = pl.DataFrame()

        # Serialize back to base64
        serialized_transformed_part_to_box_df_b64 = base64.b64encode(serialize_df(df)).decode('utf-8')
        logger.debug("Transformed Part-to-Box serialized to %d chars", len(serialized_transformed_part_to_box_df_b64))
        return serialized_transformed_part_to_box_df_b64


    @task(task_id="transform_box_to_pallet")
    def transform_box_to_pallet(box_to_pallet_b64: str) -> str:
        """Transform Box-to-Pallet junction table data."""
        logger.info("Transforming Box-to-Pallet junction data...")

        # Decoding the DataFrame
        df = deserialize_df(base64.b64decode(box_to_pallet_b64))

        # Transform
        if df is not None and not df.is_empty():
            df = df.filter(pl.any_horizontal(pl.all().is_not_null()))

            for col in ['PART_NUMBER', 'BOX_TYPE', 'PALLET_TYPE']:
                df = convert_to_string(df, col)
                df = basic_clean_text(df, col)

            for col in ['BOX_LENGTH_MM', 'BOX_WIDTH_MM', 'BOX_HEIGHT_MM',
                        'PALLET_LENGTH_MM', 'PALLET_WIDTH_MM', 'PALLET_HEIGHT_MM',
                        'BOX_PER_PALLET']:
                df = convert_to_int64(df, col)

            df = df.unique(subset=BOX_TO_PALLET_COMPOSITE_COLS, keep='first')
            df = columns_to_lowercase(df)
        else:
            logger.warning("Box-to-Pallet junction table is empty")
            df = pl.DataFrame()

        # Serialize back to base64
        serialized_transformed_box_to_pallet_df_b64 = base64.b64encode(serialize_df(df)).decode('utf-8')
        logger.debug("Transformed Box-to-Pallet serialized to %d chars", len(serialized_transformed_box_to_pallet_df_b64))
        return serialized_transformed_box_to_pallet_df_b64


    @task(task_id="transform_part_to_model")
    def transform_part_to_model(part_to_model_b64: str) -> str:
        """Transform Part-to-Model junction table data."""
        logger.info("Transforming Part-to-Model junction data...")

        # Decoding the DataFrame
        df = deserialize_df(base64.b64decode(part_to_model_b64))

        # Transform
        if df is not None and not df.is_empty():
            df = df.filter(pl.any_horizontal(pl.all().is_not_null()))

            for col in ['PART_NUMBER', 'MODEL_CODE', 'CONFIGURATION']:
                df = convert_to_string(df, col)
                df = basic_clean_text(df, col)

            df = convert_to_int64(df, 'PART_PER_VEHICLE')
            df = df.unique(subset=PART_TO_MODEL_COLS, keep='first')
            df = columns_to_lowercase(df)
        else:
            logger.warning("Part-to-Model junction table is empty")
            df = pl.DataFrame()

        # Serialize back to base64
        serialized_transformed_part_to_model_df_b64 = base64.b64encode(serialize_df(df)).decode('utf-8')
        logger.debug("Transformed Part-to-Model serialized to %d chars", len(serialized_transformed_part_to_model_df_b64))
        return serialized_transformed_part_to_model_df_b64


    @task(task_id="transform_part_to_line")
    def transform_part_to_line(part_to_line_b64: str) -> str:
        """Transform Part-to-Line junction table data."""
        logger.info("Transforming Part-to-Line junction data...")

        # Decoding the DataFrame
        df = deserialize_df(base64.b64decode(part_to_line_b64))

        # Transform
        if df is not None and not df.is_empty():
            df = df.filter(pl.any_horizontal(pl.all().is_not_null()))

            for col in PART_TO_LINE_COLS:
                df = convert_to_string(df, col)
                df = basic_clean_text(df, col)

            df = df.unique(subset=PART_TO_LINE_COLS, keep='first')
            df = columns_to_lowercase(df)
        else:
            logger.warning("Part-to-Line junction table is empty")
            df = pl.DataFrame()

        # Serialize back to base64
        serialized_transformed_part_to_line_df_b64 = base64.b64encode(serialize_df(df)).decode('utf-8')
        logger.debug("Transformed Part-to-Line serialized to %d chars", len(serialized_transformed_part_to_line_df_b64))
        return serialized_transformed_part_to_line_df_b64


    # ========== LOADING PHASE ==========
    # Separate tasks of loading core entity and junction tables in correct sequence
    @task(task_id="load_core_entity_tables")
    def load_core_tables_task(
        serialized_transformed_supplier_df_b64: str,
        serialized_transformed_workshop_df_b64: str,
        serialized_transformed_model_df_b64: str,
        serialized_transformed_configuration_df_b64: str,
        serialized_transformed_box_df_b64: str,
        serialized_transformed_pallet_df_b64: str,
        serialized_transformed_line_df_b64: str,
        serialized_transformed_part_df_b64: str
    ) -> dict[str, int]:
        """
        Task loads ALL core entity tables in the correct dependency sequence.

        The sequence of loading:
        1. Suppliers (independent)
        2. Parts (depending on Suppliers) - but it will be postponed due to Foregn Key
        3. Boxes (independent)
        4. Pallets (independent)
        5. Models (independent)
        6. Configuration (independent)
        7. Workshops (independent)
        8. Lines (depending on Workshops) - but it will be postponed due to Foregn Key
        """
        logger.info(
            "Loading all core entity tables in correct dependency sequence..."
        )

        # Deserializing all dataframes
        all_transformed_data = {
            'transformed_supplier_df': deserialize_df(base64.b64decode(serialized_transformed_supplier_df_b64)),
            'transformed_workshop_df': deserialize_df(base64.b64decode(serialized_transformed_workshop_df_b64)),
            'transformed_model_df': deserialize_df(base64.b64decode(serialized_transformed_model_df_b64)),
            'transformed_configuration_df': deserialize_df(base64.b64decode(serialized_transformed_configuration_df_b64)),
            'transformed_box_df': deserialize_df(base64.b64decode(serialized_transformed_box_df_b64)),
            'transformed_pallet_df': deserialize_df(base64.b64decode(serialized_transformed_pallet_df_b64)),
            'transformed_line_df': deserialize_df(base64.b64decode(serialized_transformed_line_df_b64)),
            'transformed_part_df': deserialize_df(base64.b64decode(serialized_transformed_part_df_b64))
        }

        # Check that all dataframes are not empty
        empty_tables = []
        for table_name, df in all_transformed_data.items():
            if df is None or df.is_empty():
                empty_tables.append(table_name)
                logger.warning(
                    "Empty DataFrame for table: %s",
                    table_name
                )

        if empty_tables:
            logger.warning(
                "Empty tables: %s",
                ', '.join(empty_tables)
            )

        # Uploading all core entities in the correct sequence with solved FK
        results = load_core_entity_tables(
            transformed_data=all_transformed_data,
            engine=None,
            resolve_foreign_keys=True  # Resolve FKs for dependent tables
        )

        # Logging the results
        total_records = sum(results.values())
        logger.info(
            "Core entity tables loading completed.\n"
            "Total records loaded: %d\n"
            "Breakdown:\n"
            "- Suppliers: %d\n"
            "- Workshops: %d\n"
            "- Models: %d\n"
            "- Configurations: %d\n"
            "- Boxes: %d\n"
            "- Pallets: %d\n"
            "- Lines: %d\n"
            "- Parts: %d",
            total_records,
            results.get('supplier_data', 0),
            results.get('workshop_data', 0),
            results.get('model_data', 0),
            results.get('configuration_data', 0),
            results.get('box_data', 0),
            results.get('pallet_data', 0),
            results.get('line_data', 0),
            results.get('part_data', 0)
        )

        return results


    @task(task_id="load_junction_tables")
    def load_junction_tables_task(
        transformed_part_to_box_b64: str,
        transformed_box_to_pallet_b64: str,
        transformed_part_to_model_b64: str,
        transformed_part_to_line_b64: str,
        core_entities_results: dict[str, int]
    ) -> dict[str, int]:
        """Load junction tables after all core entities are loaded"""
        logger.info("Loading junction tables...")

        # Decoding all DataFrames
        part_to_box_df = deserialize_df(base64.b64decode(transformed_part_to_box_b64))
        box_to_pallet_df = deserialize_df(base64.b64decode(transformed_box_to_pallet_b64))
        part_to_model_df = deserialize_df(base64.b64decode(transformed_part_to_model_b64))
        part_to_line_df = deserialize_df(base64.b64decode(transformed_part_to_line_b64))

        # Check if all required core entities were loaded
        required_entities = {
            'part_data': 'Parts',
            'box_data': 'Boxes',
            'pallet_data': 'Pallets',
            'model_data': 'Models',
            'line_data': 'Lines'
        }

        missing_entities = []
        for entity_key, entity_name in required_entities.items():
            if core_entities_results.get(entity_key, 0) == 0:
                missing_entities.append(entity_name)
                logger.warning(
                    "No %s data loaded. Related junction tables may fail.",
                    entity_name
                )

        if missing_entities:
            logger.warning(
                "Missing core entities for junction tables: %s",
                ', '.join(missing_entities)
            )

        # Подготавливаем словарь для загрузки
        junction_dict = {
            'part_to_box': part_to_box_df,
            'box_to_pallet': box_to_pallet_df,
            'part_to_model': part_to_model_df,
            'part_to_line': part_to_line_df
        }

        # Uploading data to the database
        results = load_junction_tables(
            junction_dict=junction_dict,
            engine=None,
            preserve_cache=False
        )

        # Logging of results
        if results:
            junction_total = sum(results.values())
            logger.info(
                "Junction tables loaded successfully.\n"
                "Records loaded per table:\n"
                "- PartToBox: %d\n"
                "- BoxToPallet: %d\n"
                "- PartToModel: %d\n"
                "- PartToLine: %d\n"
                "Total: %d records",
                results.get('part_to_box', 0),
                results.get('box_to_pallet', 0),
                results.get('part_to_model', 0),
                results.get('part_to_line', 0),
                junction_total
            )
        else:
            logger.error("Failed to load junction tables - empty results returned.")
            results = {}

        return results


    @task(task_id="validate_data_integrity", trigger_rule="all_done")
    def validate_data_integrity(
        core_entities_results: dict[str, int],
        junction_results: dict[str, int]
    ) -> None:
        """Validate data integrity after loading"""
        logger.info(
            "Validating data integrity..."
        )

        # Count core entity records
        core_records = sum(core_entities_results.values()) if core_entities_results else 0

        # Count junction records
        junction_records = sum(junction_results.values()) if junction_results else 0

        if core_records > 0 and junction_records > 0:
            logger.info(
                "Data loading completed successfully!\n"
                "Total records loaded:\n"
                "- Core entities: %d\n"
                "- Junction relationships: %d\n"
                "- Grand total: %d",
                core_records,
                junction_records,
                core_records + junction_records
            )

            # Log detailed breakdown
            logger.info(
                "Core entity breakdown:\n"
                "- Suppliers: %d\n"
                "- Workshops: %d\n"
                "- Models: %d\n"
                "- Configurations: %d\n"
                "- Lines: %d\n"
                "- Boxes: %d\n"
                "- Pallets: %d\n"
                "- Parts: %d",
                core_entities_results.get('supplier_data', 0),
                core_entities_results.get('workshop_data', 0),
                core_entities_results.get('model_data', 0),
                core_entities_results.get('configuration_data', 0),
                core_entities_results.get('box_data', 0),
                core_entities_results.get('pallet_data', 0),
                core_entities_results.get('line_data', 0),
                core_entities_results.get('part_data', 0)
            )

        else:
            logger.warning(
                "Data loading completed with warnings.\n"
                "Records loaded:\n"
                "- Core entities: %d\n"
                "- Junction relationships: %d",
                core_records,
                junction_records
            )

            # Checking which core tables were not uploaded
            if core_records == 0:
                logger.warning(
                    "No core entity tables were loaded!"
                )
            else:
                # Check each core table separately
                empty_tables = []
                for table_name, count in core_entities_results.items():
                    if count == 0:
                        empty_tables.append(table_name)

                if empty_tables:
                    logger.warning(
                        "Empty core entity tables: %s",
                        ', '.join(empty_tables)
                    )

            # Checking which junction tables were not uploaded
            if junction_records == 0:
                logger.warning(
                    "No junction tables were loaded!"
                )
            else:
                # Check each junction table separately
                empty_junctions = []
                for table_name, count in junction_results.items():
                    if count == 0:
                        empty_junctions.append(table_name)

                if empty_junctions:
                    logger.warning(
                        "Empty junction tables: %s",
                        ', '.join(empty_junctions)
                    )


    # ========== PIPELINE ORCHESTRATION ==========
    # EXTRACT PHASE
    # All tasks of extract phase depend on main_data
    main_task = extract_main_data()

    # All extract tasks receive data from main_df
    extract_supplier_task = extract_supplier_data(main_task)  # type: ignore
    extract_part_task = extract_part_data(main_task)  # type: ignore
    extract_box_task = extract_box_data(main_task)  # type: ignore
    extract_pallet_task = extract_pallet_data(main_task)  # type: ignore
    extract_model_task = extract_model_data(main_task)  # type: ignore
    extract_configuration_task = extract_configuration_data(main_task)  # type: ignore
    extract_workshop_task = extract_workshop_data(main_task)  # type: ignore
    extract_line_task = extract_line_data(main_task)  # type: ignore
    extract_part_to_box_task = extract_part_to_box(main_task)  # type: ignore
    extract_box_to_pallet_task = extract_box_to_pallet(main_task)  # type: ignore
    extract_part_to_model_task = extract_part_to_model(main_task)  # type: ignore
    extract_part_to_line_task = extract_part_to_line(main_task)  # type: ignore

    # TRANSFORM PHASE
    # Each transform task depends on the corresponding extract task
    transform_supplier_task = transform_supplier_data(extract_supplier_task)  # type: ignore
    transform_part_task = transform_part_data(extract_part_task)  # type: ignore
    transform_box_task = transform_box_data(extract_box_task)  # type: ignore
    transform_pallet_task = transform_pallet_data(extract_pallet_task)  # type: ignore
    transform_model_task = transform_model_data(extract_model_task)  # type: ignore
    transform_configuration_task = transform_configuration_data(extract_configuration_task)  # type: ignore
    transform_workshop_task = transform_workshop_data(extract_workshop_task)  # type: ignore
    transform_line_task = transform_line_data(extract_line_task)  # type: ignore
    transformed_part_to_box_task = transform_part_to_box(extract_part_to_box_task)  # type: ignore
    transformed_box_to_pallet_task = transform_box_to_pallet(extract_box_to_pallet_task)  # type: ignore
    transformed_part_to_model_task = transform_part_to_model(extract_part_to_model_task)  # type: ignore
    transformed_part_to_line_task = transform_part_to_line(extract_part_to_line_task)  # type: ignore


    # LOADING PHASE
    # Loading all core entity tables in one task in the correct sequence
    load_core_entities_task = load_core_tables_task(
        serialized_transformed_supplier_df_b64 = transform_supplier_task,  # type: ignore
        serialized_transformed_workshop_df_b64 = transform_workshop_task,  # type: ignore
        serialized_transformed_model_df_b64 = transform_model_task,  # type: ignore
        serialized_transformed_configuration_df_b64 = transform_configuration_task,  # type: ignore
        serialized_transformed_box_df_b64 = transform_box_task,  # type: ignore
        serialized_transformed_pallet_df_b64 = transform_pallet_task,  # type: ignore
        serialized_transformed_line_df_b64 = transform_line_task,  # type: ignore
        serialized_transformed_part_df_b64 = transform_part_task  # type: ignore
    )

    # Loading all the junction tables (depending on core entities)
    load_junction_entities_task = load_junction_tables_task(
        transformed_part_to_box_b64 = transformed_part_to_box_task,  # type: ignore
        transformed_box_to_pallet_b64 = transformed_box_to_pallet_task,  # type: ignore
        transformed_part_to_model_b64 = transformed_part_to_model_task,  # type: ignore
        transformed_part_to_line_b64 = transformed_part_to_line_task,  # type: ignore
        core_entities_results = load_core_entities_task  # type: ignore
    )

    # Validation of results
    validate_data_integrity(
        core_entities_results=load_core_entities_task,  # type: ignore
        junction_results=load_junction_entities_task  # type: ignore
    )

# Instantiate the DAG
MFT_ETL_DAG: DAG = mft_etl_pipeline()
