# pylint: disable=too-many-lines
# pylint: disable=wrong-import-position
"""
Base Display API Module for Material Flow Table Database.

This module provides a base class with common functionality for all display APIs:
- Database connection management
- Error handling with comprehensive exception catching
- Output normalization (uppercase, sentence case, title case)
- Excel export functionality using Polars
- Connection health checks

The BaseDisplayAPI class is designed to be inherited by specific display APIs:
    - MFTDisplayAPI: Active parts display with filters
    - BPDisplayAPI: Breakpoint history and information
    - PartHistoryAPI: Part change history

Version: 1.0.0
Compatibility: Python 3.14.4+, Flask 6.0.2+, SQLAlchemy 1.4.54+
Maintainer: PLD Engineering Center
Created: 2026-08-18
Last Modified: 2026-08-18
License: MIT
Status: Development
"""

# Standard library imports
from pathlib import Path
import re
import sys
import tempfile
import uuid
import zoneinfo
from datetime import datetime
from typing import Dict, Any, List, Callable
import pytz

# Third-party imports
import polars as pl
from sqlalchemy import String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import (
    SQLAlchemyError, IntegrityError, DataError, StatementError,
    OperationalError, ProgrammingError, InvalidRequestError
)

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


class BaseDisplayAPI:
    """
    Base class for all display APIs.

    Provides common functionality for querying and formatting data.
    Designed to be inherited by specific display API classes.

    Attributes:
        engine: SQLAlchemy database engine
        Session: SQLAlchemy sessionmaker instance
        ENUM_FIELDS: List of ENUM fields for exact match filtering
        UPPERCASE_COLUMNS: Columns that should be output in uppercase
        SENTENCE_CASE_COLUMNS: Columns that should be output in sentence case
        TITLE_CASE_COLUMNS: Columns that should be output in title case

    Usage:
        class MyDisplayAPI(BaseDisplayAPI):
            def get_data(self, filters):
                def query(session):
                    return session.query(MyModel).filter(...).all()
                return self._safe_query(query)
    """

    # List of ENUM fields (for exact match, case-insensitive)
    ENUM_FIELDS = [
        'workshop_code', 'workshop_name', 'model_code', 'model_name',
        'localization', 'box_type', 'pallet_type', 'configuration'
    ]

    # Output normalization rules
    UPPERCASE_COLUMNS = [
        'PART_NUMBER', 'CONFIGURATION', 'MODEL_CODE',
        'LINE_CODE', 'WORKSHOP_CODE', 'BUILDING'
    ]
    SENTENCE_CASE_COLUMNS = [
        'PART_NAME', 'MODEL_NAME', 'LINE_NAME', 'WORKSHOP_NAME',
        'BOX_TYPE', 'PALLET_TYPE', 'SUPPLIER_NAME', 'LOCALIZATION'
    ]
    TITLE_CASE_COLUMNS = ['LOCATION', 'CITY', 'STREET']

    # Timezone settings
    MOSCOW_TZ = None  # Will be initialized in __init__

    def __init__(self, engine):
        """
        Initialize with database engine.

        Args:
            engine: SQLAlchemy engine from connector.py

        Raises:
            ValueError: If engine is None
        """
        if engine is None:
            logger.error("Cannot initialize BaseDisplayAPI: engine is None")
            raise ValueError("Database engine cannot be None")

        self.engine = engine
        self.Session = sessionmaker(bind=self.engine)

        # Initialize timezone
        try:
            self.MOSCOW_TZ = zoneinfo.ZoneInfo("Europe/Moscow")
        except ImportError:
            self.MOSCOW_TZ = pytz.timezone('Europe/Moscow')

        logger.info("%s initialized successfully", self.__class__.__name__)

    # ========================================================================
    # SESSION MANAGEMENT
    # ========================================================================

    def _get_session(self):
        """
        Create and return a new database session.

        Returns:
            SQLAlchemy session object

        Note:
            Session should be closed by the caller or by _safe_query
        """
        return self.Session()

    # ========================================================================
    # ERROR HANDLING
    # ========================================================================

    def _safe_query(self, query_func: Callable) -> Any:
        """
        Execute query with proper error handling and session management.

        This method wraps any database query in a try/except block with
        comprehensive error handling for all SQLAlchemy exceptions.

        Args:
            query_func: Function that executes the query and returns results

        Returns:
            Query results or error dict with 'success': False

        Error handling:
            - IntegrityError: Duplicate key or foreign key violation
            - DataError: Invalid data format or type
            - OperationalError: Database connection or transaction error
            - ProgrammingError: Invalid table/column or SQL syntax
            - InvalidRequestError: ORM error
            - StatementError: Invalid SQL statement
            - SQLAlchemyError: General database error
            - Exception: Unexpected error (logged with exc_info=True)

        Example:
            >>> def query(session):
            ...     return session.query(MyModel).all()
            >>> result = self._safe_query(query)
        """
        session = self._get_session()
        try:
            result = query_func(session)
            session.commit()
            return result

        except IntegrityError as e:
            session.rollback()
            logger.error("Integrity error in database query: %s", e)
            return {
                "error": "Data integrity violation (duplicate key or foreign key)",
                "detail": str(e.orig) if e.orig else str(e),
                "status": "integrity_error",
                "success": False
            }

        except DataError as e:
            session.rollback()
            logger.error("Data error in database query: %s", e)
            return {
                "error": "Invalid data format or type",
                "detail": str(e.orig) if e.orig else str(e),
                "status": "data_error",
                "success": False
            }

        except OperationalError as e:
            session.rollback()
            logger.error("Operational error in database query: %s", e)
            return {
                "error": "Database connection or transaction error",
                "detail": str(e.orig) if e.orig else str(e),
                "status": "operational_error",
                "success": False
            }

        except ProgrammingError as e:
            session.rollback()
            error_msg = str(e.orig) if e.orig else str(e)

            # Check if the error is related to missing tables
            if 'does not exist' in error_msg.lower() or (
                'relation' in error_msg.lower() and 'does not exist' in error_msg.lower()
            ):
                logger.info("Database tables not yet created")
                return {
                    "error": "Database tables not created yet",
                    "detail": error_msg,
                    "status": "tables_not_created",
                    "success": False
                }

            logger.error("Programming error in database query: %s", e)
            return {
                "error": "Database programming error (invalid table/column or syntax)",
                "detail": error_msg,
                "status": "programming_error",
                "success": False
            }

        except InvalidRequestError as e:
            session.rollback()
            logger.error("Invalid request error in database query: %s", e)
            return {
                "error": "Invalid database request (ORM error)",
                "detail": str(e),
                "status": "invalid_request",
                "success": False
            }

        except StatementError as e:
            session.rollback()
            logger.error("Statement error in database query: %s", e)
            return {
                "error": "Invalid SQL statement",
                "detail": str(e.orig) if e.orig else str(e),
                "status": "statement_error",
                "success": False
            }

        except SQLAlchemyError as e:
            session.rollback()
            logger.error("SQLAlchemy error in database query: %s", e)
            return {
                "error": "Database error occurred",
                "detail": str(e),
                "status": "database_error",
                "success": False
            }

        except Exception as unexpected_error:
            session.rollback()
            logger.error(
                "Unexpected error in database query: %s",
                unexpected_error,
                exc_info=True
            )
            return {
                "error": f"Unexpected error: {str(unexpected_error)}",
                "status": "unexpected_error",
                "success": False
            }

        finally:
            session.close()

    # ========================================================================
    # OUTPUT NORMALIZATION
    # ========================================================================

    def normalize_output(self, column_name: str, value: Any) -> Any:
        """
        Normalize output value based on column name rules.

        Normalization rules:
            - UPPERCASE: PART_NUMBER, CONFIGURATION, MODEL_CODE,
                         LINE_CODE, WORKSHOP_CODE, BUILDING
            - Sentence case: PART_NAME, MODEL_NAME, LINE_NAME, WORKSHOP_NAME,
                             BOX_TYPE, PALLET_TYPE, SUPPLIER_NAME, LOCALIZATION
            - Title case: LOCATION, CITY, STREET

        Args:
            column_name: Name of the column (case-insensitive)
            value: Value to normalize

        Returns:
            Normalized value or None if value is None

        Example:
            >>> normalize_output("PART_NUMBER", "abc-123")
            "ABC-123"
            >>> normalize_output("PART_NAME", "BOLT M8")
            "Bolt m8"
            >>> normalize_output("LOCATION", "NEW YORK")
            "New York"
        """
        if value is None:
            return None

        upper_col = column_name.upper()

        if upper_col in self.UPPERCASE_COLUMNS:
            return str(value).upper()

        if upper_col in self.SENTENCE_CASE_COLUMNS:
            s = str(value)
            if not s:
                return s
            return s[0].upper() + s[1:].lower()

        if upper_col in self.TITLE_CASE_COLUMNS:
            s = str(value)
            return re.sub(
                r"[A-Za-z]+('[A-Za-z]+)?",
                lambda mo: mo.group(0)[0].upper() + mo.group(0)[1:].lower(),
                s
            )

        # Default: return as is
        return value

    # ========================================================================
    # FILTER HELPERS
    # ========================================================================

    def _apply_filter(self, query, field, value, is_enum: bool = False):
        """
        Apply filter to query with case-insensitive search.

        For ENUM fields: exact match (case-insensitive)
        For non-ENUM fields: partial match (case-insensitive)

        Args:
            query: SQLAlchemy query object
            field: Field to filter on
            value: Value to search for
            is_enum: Whether the field is an ENUM type

        Returns:
            Modified query with filter applied

        Example:
            >>> query = session.query(MyModel)
            >>> query = self._apply_filter(query, MyModel.name, "test", is_enum=False)
            >>> # WHERE name ILIKE '%test%'
        """
        if value is None or value == "":
            return query

        str_value = str(value)

        if is_enum:
            # For ENUM fields - exact match (case-insensitive)
            return query.filter(field.cast(String).ilike(str_value))
        else:
            # For non-ENUM fields - partial match
            return query.filter(field.ilike(f"%{str_value}%"))

    # ========================================================================
    # FILTER PROCESSING
    # ========================================================================

    def process_filters(self, raw_filters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process raw filters with support for range queries (_min/_max suffixes).

        Range query syntax:
            - {field}_min: Minimum value for range filter
            - {field}_max: Maximum value for range filter

        Args:
            raw_filters: Dictionary of raw filters from request

        Returns:
            Processed filters with range queries converted to dict

        Example:
            >>> raw = {"part_weight_kg_min": "1", "part_weight_kg_max": "3"}
            >>> process_filters(raw)
            {"part_weight_kg": {"min": 1, "max": 3}}
        """
        processed_filters = {}

        for key, value in raw_filters.items():
            if value is None or value == "":
                continue

            # Check if this is a range filter (ends with _min or _max)
            if key.endswith('_min') or key.endswith('_max'):
                base_key = key[:-4]  # Remove _min or _max
                range_type = key[-3:]  # 'min' or 'max'

                if base_key not in processed_filters:
                    processed_filters[base_key] = {}

                try:
                    if '.' in str(value):
                        processed_filters[base_key][range_type] = float(value)
                    else:
                        processed_filters[base_key][range_type] = int(value)
                except (ValueError, TypeError):
                    processed_filters[base_key][range_type] = value

            else:
                # Regular filter
                try:
                    if isinstance(value, str) and '.' in value:
                        processed_filters[key] = float(value)
                    elif isinstance(value, str):
                        try:
                            processed_filters[key] = int(value)
                        except ValueError:
                            processed_filters[key] = value
                    else:
                        processed_filters[key] = value
                except (ValueError, TypeError):
                    processed_filters[key] = value

        return processed_filters

    # ========================================================================
    # CONNECTION MANAGEMENT
    # ========================================================================

    def check_connection(self) -> bool:
        """
        Check if database connection is alive.

        Returns:
            bool: True if connection is working, False otherwise

        Example:
            >>> if api.check_connection():
            ...     data = api.get_data()
        """
        def _check(session):
            session.execute('SELECT 1').scalar()
            return True

        result = self._safe_query(_check)
        return result is True

    # ========================================================================
    # EXCEL EXPORT
    # ========================================================================

    def export_to_excel(
        self,
        data: List[Dict[str, Any]],
        filters: Dict[str, Any],
        prefix: str = "export"
    ) -> Dict[str, Any]:
        """
        Export data to Excel file using Polars.

        Args:
            data: List of dictionaries with data to export
            filters: Applied filters (for metadata and filename)
            prefix: Prefix for filename (default: "export")

        Returns:
            Dictionary with export information:
                - success: bool
                - file_path: str (path to temporary file)
                - filename: str (generated filename)
                - row_count: int
                - applied_filters: dict

        Status codes:
            - success: True on success
            - status: "no_data" if data is empty
            - status: "export_error" on export failure

        Example:
            >>> result = api.export_to_excel(data, {"part_number": "ABC-123"})
            >>> if result['success']:
            ...     print(f"Exported to {result['file_path']}")
        """
        if not data:
            logger.warning("Export requested with empty data")
            return {
                "success": False,
                "error": "No data to export",
                "status": "no_data"
            }

        try:
            # Generate unique filename
            moscow_time = datetime.now(self.MOSCOW_TZ)
            timestamp = moscow_time.strftime("%Y%m%d_%H%M%S")
            unique_id = uuid.uuid4().hex[:8]
            filename = f"{prefix}_{timestamp}_{unique_id}.xlsx"

            # Create temporary directory and file
            temp_dir = Path(tempfile.gettempdir()) / "mft_exports"
            temp_dir.mkdir(parents=True, exist_ok=True)
            file_path = temp_dir / filename

            # Create Polars DataFrame
            df = pl.DataFrame(data)

            # Export to Excel
            df.write_excel(
                file_path,
                worksheet="Data",
                autofit=True,
                table_style="Table Style Medium 2",
                column_widths=None
            )

            logger.info(
                "Exported %d rows to %s",
                len(data),
                file_path
            )

            return {
                "success": True,
                "file_path": str(file_path),
                "filename": filename,
                "row_count": len(data),
                "applied_filters": filters
            }

        except ImportError as e:
            logger.error(
                "Polars Excel export failed - missing dependency: %s",
                e
            )
            return {
                "success": False,
                "error": "Excel export requires polars[excel] or xlsxwriter",
                "status": "missing_dependency"
            }

        except PermissionError as e:
            logger.error("Permission denied when creating export file: %s", e)
            return {
                "success": False,
                "error": f"Permission denied: {str(e)}",
                "status": "export_error"
            }

        except OSError as e:
            logger.error("OS error during export: %s", e)
            return {
                "success": False,
                "error": f"File system error: {str(e)}",
                "status": "export_error"
            }

        except ValueError as e:
            logger.error("Value error during export: %s", e)
            return {
                "success": False,
                "error": f"Invalid data for export: {str(e)}",
                "status": "export_error"
            }

        except TypeError as e:
            logger.error("Type error during export: %s", e)
            return {
                "success": False,
                "error": f"Data type error: {str(e)}",
                "status": "export_error"
            }

        except AttributeError as e:
            logger.error("Attribute error during export: %s", e)
            return {
                "success": False,
                "error": f"Missing attribute: {str(e)}",
                "status": "export_error"
            }

        except Exception as unexpected_error:
            logger.error(
                "Unexpected error exporting to Excel: %s",
                unexpected_error,
                exc_info=True
            )
            return {
                "success": False,
                "error": f"Failed to export to Excel: {str(unexpected_error)}",
                "status": "export_error"
            }


# ============================================================================
# PUBLIC INTERFACE
# ============================================================================

__all__ = [
    'BaseDisplayAPI',
]
