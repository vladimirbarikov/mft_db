# pylint: disable=too-many-lines
# pylint: disable=wrong-import-position
"""
Display API Module for Material Flow Table Database.

This module provides:
1. DatabaseAPI class with methods for querying data from the database
2. Flask endpoints for frontend communication with universal search
3. Case-insensitive search and output normalization
4. Range search for numeric fields (weight, dimensions, volume, area)
5. Excel export functionality using Polars

Version: 1.0.0
Compatibility: Python 3.12.3
Maintainer: PLD Engineering Center
Created: 2026-03-02
Last Modified: 2026-03-06
License: MIT
Status: Production
"""
# Standard library imports
from pathlib import Path
import sys
import re
import os
import tempfile
from datetime import datetime
from typing import Dict, Any, Optional
from functools import wraps

# Third-party imports
import polars as pl
from flask import Blueprint, Flask, request, jsonify, current_app, send_file
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from sqlalchemy import String, and_
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
from dags.tasks.connector import initialize_database
from database.database import (
    # Entity tables
    SupplierData, PartData, BoxData, PalletData,
    ModelData, ConfigurationData, WorkshopData, LineData,
    # Junction tables
    PartToBox, BoxToPallet, PartToModel, PartToLine
)

# Logger setup
logger = get_logger("endpoints.mft_display_api")

# ========== CONFIGURATION ==========

# Flask/Upload API configuration
FLASK_SECRET_KEY = os.getenv('FLASK_SECRET_KEY')
if not FLASK_SECRET_KEY:
    raise RuntimeError("FLASK_SECRET_KEY must be set in .env file")

FLASK_HOST = os.getenv('FLASK_HOST', '0.0.0.0')
FLASK_PORT = int(os.getenv('MFT_DISPLAY_API_PORT', '5000'))
FLASK_DEBUG = os.getenv('FLASK_DEBUG', 'false').lower() == 'true'

# Defining the environment (default is development)
FLASK_ENV = os.getenv('FLASK_ENV', 'development')
IS_PRODUCTION = FLASK_ENV == 'production'

# CORS configuration
# For production, replace "*" with actual domains
ALLOWED_ORIGINS = os.getenv('ALLOWED_ORIGINS', '*')

# Rate limiting settings
RATE_LIMIT = os.getenv('RATE_LIMIT', '10 per minute')
RATE_LIMIT_STORAGE_URL = os.getenv('RATE_LIMIT_STORAGE_URL', 'memory://')

# ========== CREATING BLUEPRINT ==========
display_bp = Blueprint('display', __name__)

# ========== RATE LIMITING SETUP ==========
limiter = Limiter(
    key_func=get_remote_address,
    storage_uri=RATE_LIMIT_STORAGE_URL,
    default_limits=["200 per day", "50 per hour"],
    strategy="fixed-window"
)


# ========== RATE LIMITING DECORATOR ==========
def rate_limit(limit_string: Optional[str] = None):
    """
    Decorator factory for applying rate limits to endpoints.
    
    Wraps Flask routes with Flask-Limiter's rate limiting functionality.
    
    Args:
        limit_string (Optional[str]): Rate limit string (e.g., "10 per minute").
                                     If None, uses default RATE_LIMIT setting.
                                     
    Returns:
        Callable: Decorated function with rate limiting applied
        
    Example:
        >>> @rate_limit("5 per minute")
        ... def my_endpoint():
        ...     return jsonify({"message": "ok"})
    """
    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            return limiter.limit(limit_string or RATE_LIMIT)(f)(*args, **kwargs)
        return wrapped
    return decorator


# ========== FUNCTIONS FOR CASE NORMALIZATION ==========
def to_uppercase(value: Any) -> Any:
    """Convert to uppercase string if not None."""
    if value is None:
        return None
    return str(value).upper()

def to_title_case(value: Any) -> Any:
    """Convert to title case (first letter of each word uppercase) if not None."""
    if value is None:
        return None
    # Handle None and convert to string
    s = str(value)
    # Use regex to handle words properly
    return re.sub(r"[A-Za-z]+('[A-Za-z]+)?",
                  lambda mo: mo.group(0)[0].upper() +
                  mo.group(0)[1:].lower(), s)

def to_sentence_case(value: Any) -> Any:
    """Convert to sentence case (first letter uppercase, rest lowercase) if not None."""
    if value is None:
        return None
    s = str(value)
    if not s:
        return s
    return s[0].upper() + s[1:].lower()

def normalize_output(
        column_name: str,
        value: Any
    ) -> Any:
    """
    Normalize output value based on column name rules.
    
    Rules:
    - UPPERCASE: PART_NUMBER, CONFIGURATION, MODEL_CODE, LINE_CODE, WORKSHOP_CODE, BUILDING
    - Sentence case: PART_NAME, MODEL_NAME, LINE_NAME, WORKSHOP_NAME,
                     BOX_TYPE, PALLET_TYPE, SUPPLIER_NAME, LOCALIZATION, DESCRIPTION
    - Title case: LOCATION, CITY, STREET
    """
    if value is None:
        return None

    uppercase_columns = [
        'PART_NUMBER', 'CONFIGURATION', 'MODEL_CODE', 
        'LINE_CODE', 'WORKSHOP_CODE', 'BUILDING'
    ]

    sentence_case_columns = [
        'PART_NAME', 'MODEL_NAME', 'LINE_NAME', 'WORKSHOP_NAME', 'BOX_TYPE',
        'PALLET_TYPE', 'SUPPLIER_NAME', 'LOCALIZATION', 'DESCRIPTION'
    ]

    title_case_columns = ['LOCATION', 'CITY', 'STREET']

    if column_name in uppercase_columns:
        return to_uppercase(value)
    elif column_name in sentence_case_columns:
        return to_sentence_case(value)
    elif column_name in title_case_columns:
        return to_title_case(value)
    else:
        # Default: return as is
        return value


# ========== DATABASE API CLASS ==========
class DatabaseAPI:
    """
    Main API class for database operations.
    
    Provides methods for querying and filtering data across all tables.
    Handles session management and error handling with case-insensitive search.
    Supports range queries for numeric fields.
    """

    # List of ENUM fields
    ENUM_FIELDS = [
        'workshop_code', 'workshop_name', 'model_code', 'model_name',
        'localization', 'box_type', 'pallet_type', 'configuration'
    ]

    def __init__(self, engine):
        """
        Initialize with database engine.
        
        Args:
            engine: SQLAlchemy engine from connector.py
            
        Raises:
            ValueError: If engine is None
        """
        if engine is None:
            raise ValueError("Database engine cannot be None")

        self.engine = engine
        self.Session = sessionmaker(bind=self.engine)
        logger.info("DatabaseAPI initialized with database connection")

    def _get_session(self):
        """Create and return a new database session."""
        return self.Session()

    def _safe_query(self, query_func):
        """
        Execute query with proper error handling and session management.
        
        Args:
            query_func: Function that executes the query
        
        Returns:
            Query results or error dict
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
            logger.error("Programming error in database query: %s", e)
            return {
                "error": "Database programming error (invalid table/column or syntax)",
                "detail": str(e.orig) if e.orig else str(e),
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
            logger.error("Unexpected error in database query: %s", unexpected_error, exc_info=True)
            return {
                "error": f"Unexpected error: {str(unexpected_error)}",
                "status": "unexpected_error",
                "success": False
            }
        finally:
            session.close()

    def _apply_filter(self, query, field, value, is_enum=False):
        """
        Smart application of the filter, taking into account the type of field.
        
        Args:
            query: SQLAlchemy query object
            field: A field for filtering
            value: The value for the search
            is_enum: Is the ENUM field a type
            
        Returns:
            Modified query
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

    def universal_search(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Universal search - returns ONLY rows that match ALL provided filters.
        Each column in result contains ONLY values that satisfy the filters.
        Filters are combined with AND logic - each additional filter narrows the search.
        
        Args:
        filters: Dictionary with filter parameters (any column from any table)
                Example: {
                    "PART_NUMBER": "6804100XKN01A8P",
                    "MODEL_NAME": "Jolion",
                    "LINE_CODE": "HZZ102",
                    "LOCATION": "China",
                    "BOX_TYPE": "Non-returnable"
                }
        
        Returns:
            Dictionary with complete information for all matching parts
        """
        def query(session):

            # Base query - start from the most granular level to get all combinations
            # This ensures that each row is a real combination.
            query = session.query(
                PartData.part_number,
                PartData.part_name,
                PartData.part_weight_kg,

                PartToModel.part_per_vehicle,
                ConfigurationData.configuration,
                ModelData.model_code,
                ModelData.model_name,

                LineData.line_code,
                LineData.line_name,
                WorkshopData.workshop_code,
                WorkshopData.workshop_name,

                PartToBox.part_per_box,
                BoxData.box_type,
                BoxData.box_weight_kg,
                BoxData.box_length_mm,
                BoxData.box_width_mm,
                BoxData.box_height_mm,
                BoxData.box_vol_m3,
                BoxData.box_area_m2,
                BoxData.box_stacking,

                BoxToPallet.box_per_pallet,
                PalletData.pallet_type,
                PalletData.pallet_weight_kg,
                PalletData.pallet_length_mm,
                PalletData.pallet_width_mm,
                PalletData.pallet_height_mm,
                PalletData.pallet_vol_m3,
                PalletData.pallet_area_m2,
                PalletData.pallet_stacking,

                SupplierData.supplier_name,
                SupplierData.location,
                SupplierData.city,
                SupplierData.street,
                SupplierData.building,
                SupplierData.localization

            ).select_from(PartToBox)

            # Add all joins sequentially
            query = query.join(PartData, PartData.part_id == PartToBox.part_id)
            query = query.join(SupplierData, SupplierData.supplier_id == PartData.supplier_id)
            query = query.join(BoxData, BoxData.box_id == PartToBox.box_id)

            # Join PartToModel and related tables
            query = query.join(PartToModel, PartData.part_id == PartToModel.part_id)
            query = query.join(ModelData, ModelData.model_id == PartToModel.model_id)
            query = query.join(ConfigurationData, ConfigurationData.configuration_id == PartToModel.configuration_id)

            # Join PartToLine and related tables
            query = query.join(PartToLine, PartData.part_id == PartToLine.part_id)
            query = query.join(LineData, LineData.line_id == PartToLine.line_id)
            query = query.join(WorkshopData, WorkshopData.workshop_id == LineData.workshop_id)

            # Outer joins for optional relationships
            query = query.outerjoin(BoxToPallet, and_(
                BoxData.box_id == BoxToPallet.box_id,
                PartData.part_id == BoxToPallet.part_id
            ))
            query = query.outerjoin(PalletData, PalletData.pallet_id == BoxToPallet.pallet_id)

            # Building WHERE conditions based on filled filters
            conditions = []

            for key, value in filters.items():
                if value is None or value == "":
                    continue

                # Convert string values to lowercase for case-insensitive search
                # because database stores everything in lowercase
                if isinstance(value, str):
                    str_value = value.lower()
                elif isinstance(value, dict):
                    # For range filters, convert string values inside the dict
                    str_value = {}
                    for range_key, range_value in value.items():
                        if isinstance(range_value, str):
                            str_value[range_key] = range_value.lower()
                        else:
                            str_value[range_key] = range_value
                else:
                    str_value = value

                # ===== PART =====
                if key == "PART_NUMBER":
                    conditions.append(PartData.part_number.ilike(f"%{str_value}%"))
                elif key == "PART_NAME":
                    conditions.append(PartData.part_name.ilike(f"%{str_value}%"))
                elif key == "PART_WEIGHT_KG":
                    if isinstance(value, dict) and ('min' in value or 'max' in value):
                        if 'min' in value:
                            conditions.append(PartData.part_weight_kg >= float(value['min']))
                        if 'max' in value:
                            conditions.append(PartData.part_weight_kg <= float(value['max']))
                    else:
                        if not isinstance(value, dict):
                            try:
                                conditions.append(PartData.part_weight_kg == float(value))
                            except (TypeError, ValueError):
                                pass

                # ===== MODEL =====
                elif key == "PART_PER_VEHICLE":
                    if isinstance(value, dict) and ('min' in value or 'max' in value):
                        if 'min' in value:
                            conditions.append(PartToModel.part_per_vehicle >= int(value['min']))
                        if 'max' in value:
                            conditions.append(PartToModel.part_per_vehicle <= int(value['max']))
                    else:
                        if not isinstance(value, dict):
                            try:
                                conditions.append(PartToModel.part_per_vehicle == int(value))
                            except (TypeError, ValueError):
                                pass
                elif key == "CONFIGURATION":
                    conditions.append(ConfigurationData.configuration.cast(String).ilike(str_value))
                elif key == "MODEL_CODE":
                    conditions.append(ModelData.model_code.cast(String).ilike(str_value))
                elif key == "MODEL_NAME":
                    conditions.append(ModelData.model_name.cast(String).ilike(str_value))

                # ===== LINE & WORKSHOP =====
                elif key == "LINE_CODE":
                    conditions.append(LineData.line_code.ilike(f"%{str_value}%"))
                elif key == "LINE_NAME":
                    conditions.append(LineData.line_name.ilike(f"%{str_value}%"))
                elif key == "WORKSHOP_CODE":
                    conditions.append(WorkshopData.workshop_code.cast(String).ilike(str_value))
                elif key == "WORKSHOP_NAME":
                    conditions.append(WorkshopData.workshop_name.cast(String).ilike(str_value))

                # ===== BOX =====
                elif key == "PART_PER_BOX":
                    if isinstance(value, dict) and ('min' in value or 'max' in value):
                        if 'min' in value:
                            conditions.append(PartToBox.part_per_box >= int(value['min']))
                        if 'max' in value:
                            conditions.append(PartToBox.part_per_box <= int(value['max']))
                    else:
                        if not isinstance(value, dict):
                            try:
                                conditions.append(PartToBox.part_per_box == int(value))
                            except (TypeError, ValueError):
                                pass
                elif key == "BOX_TYPE":
                    conditions.append(BoxData.box_type.cast(String).ilike(str_value))
                elif key == "BOX_WEIGHT_KG":
                    if isinstance(value, dict) and ('min' in value or 'max' in value):
                        if 'min' in value:
                            conditions.append(BoxData.box_weight_kg >= float(value['min']))
                        if 'max' in value:
                            conditions.append(BoxData.box_weight_kg <= float(value['max']))
                    else:
                        if not isinstance(value, dict):
                            try:
                                conditions.append(BoxData.box_weight_kg == float(value))
                            except (TypeError, ValueError):
                                pass
                elif key == "BOX_LENGTH_MM":
                    if isinstance(value, dict) and ('min' in value or 'max' in value):
                        if 'min' in value:
                            conditions.append(BoxData.box_length_mm >= int(value['min']))
                        if 'max' in value:
                            conditions.append(BoxData.box_length_mm <= int(value['max']))
                    else:
                        if not isinstance(value, dict):
                            try:
                                conditions.append(BoxData.box_length_mm == int(value))
                            except (TypeError, ValueError):
                                pass
                elif key == "BOX_WIDTH_MM":
                    if isinstance(value, dict) and ('min' in value or 'max' in value):
                        if 'min' in value:
                            conditions.append(BoxData.box_width_mm >= int(value['min']))
                        if 'max' in value:
                            conditions.append(BoxData.box_width_mm <= int(value['max']))
                    else:
                        if not isinstance(value, dict):
                            try:
                                conditions.append(BoxData.box_width_mm == int(value))
                            except (TypeError, ValueError):
                                pass
                elif key == "BOX_HEIGHT_MM":
                    if isinstance(value, dict) and ('min' in value or 'max' in value):
                        if 'min' in value:
                            conditions.append(BoxData.box_height_mm >= int(value['min']))
                        if 'max' in value:
                            conditions.append(BoxData.box_height_mm <= int(value['max']))
                    else:
                        if not isinstance(value, dict):
                            try:
                                conditions.append(BoxData.box_height_mm == int(value))
                            except (TypeError, ValueError):
                                pass
                elif key == "BOX_VOL_M3":
                    if isinstance(value, dict) and ('min' in value or 'max' in value):
                        if 'min' in value:
                            conditions.append(BoxData.box_vol_m3 >= float(value['min']))
                        if 'max' in value:
                            conditions.append(BoxData.box_vol_m3 <= float(value['max']))
                    else:
                        if not isinstance(value, dict):
                            try:
                                conditions.append(BoxData.box_vol_m3 == float(value))
                            except (TypeError, ValueError):
                                pass
                elif key == "BOX_AREA_M2":
                    if isinstance(value, dict) and ('min' in value or 'max' in value):
                        if 'min' in value:
                            conditions.append(BoxData.box_area_m2 >= float(value['min']))
                        if 'max' in value:
                            conditions.append(BoxData.box_area_m2 <= float(value['max']))
                    else:
                        if not isinstance(value, dict):
                            try:
                                conditions.append(BoxData.box_area_m2 == float(value))
                            except (TypeError, ValueError):
                                pass
                elif key == "BOX_STACKING":
                    if isinstance(value, dict) and ('min' in value or 'max' in value):
                        if 'min' in value:
                            conditions.append(BoxData.box_stacking >= int(value['min']))
                        if 'max' in value:
                            conditions.append(BoxData.box_stacking <= int(value['max']))
                    else:
                        if not isinstance(value, dict):
                            try:
                                conditions.append(BoxData.box_stacking == int(value))
                            except (TypeError, ValueError):
                                pass

                # ===== PALLET =====
                elif key == "BOX_PER_PALLET":
                    if isinstance(value, dict) and ('min' in value or 'max' in value):
                        if 'min' in value:
                            conditions.append(BoxToPallet.box_per_pallet >= int(value['min']))
                        if 'max' in value:
                            conditions.append(BoxToPallet.box_per_pallet <= int(value['max']))
                    else:
                        if not isinstance(value, dict):
                            try:
                                conditions.append(BoxToPallet.box_per_pallet == int(value))
                            except (TypeError, ValueError):
                                pass
                elif key == "PALLET_TYPE":
                    conditions.append(PalletData.pallet_type.cast(String).ilike(str_value))
                elif key == "PALLET_WEIGHT_KG":
                    if isinstance(value, dict) and ('min' in value or 'max' in value):
                        if 'min' in value:
                            conditions.append(PalletData.pallet_weight_kg >= float(value['min']))
                        if 'max' in value:
                            conditions.append(PalletData.pallet_weight_kg <= float(value['max']))
                    else:
                        if not isinstance(value, dict):
                            try:
                                conditions.append(PalletData.pallet_weight_kg == float(value))
                            except (TypeError, ValueError):
                                pass
                elif key == "PALLET_LENGTH_MM":
                    if isinstance(value, dict) and ('min' in value or 'max' in value):
                        if 'min' in value:
                            conditions.append(PalletData.pallet_length_mm >= int(value['min']))
                        if 'max' in value:
                            conditions.append(PalletData.pallet_length_mm <= int(value['max']))
                    else:
                        if not isinstance(value, dict):
                            try:
                                conditions.append(PalletData.pallet_length_mm == int(value))
                            except (TypeError, ValueError):
                                pass
                elif key == "PALLET_WIDTH_MM":
                    if isinstance(value, dict) and ('min' in value or 'max' in value):
                        if 'min' in value:
                            conditions.append(PalletData.pallet_width_mm >= int(value['min']))
                        if 'max' in value:
                            conditions.append(PalletData.pallet_width_mm <= int(value['max']))
                    else:
                        if not isinstance(value, dict):
                            try:
                                conditions.append(PalletData.pallet_width_mm == int(value))
                            except (TypeError, ValueError):
                                pass
                elif key == "PALLET_HEIGHT_MM":
                    if isinstance(value, dict) and ('min' in value or 'max' in value):
                        if 'min' in value:
                            conditions.append(PalletData.pallet_height_mm >= int(value['min']))
                        if 'max' in value:
                            conditions.append(PalletData.pallet_height_mm <= int(value['max']))
                    else:
                        if not isinstance(value, dict):
                            try:
                                conditions.append(PalletData.pallet_height_mm == int(value))
                            except (TypeError, ValueError):
                                pass
                elif key == "PALLET_VOL_M3":
                    if isinstance(value, dict) and ('min' in value or 'max' in value):
                        if 'min' in value:
                            conditions.append(PalletData.pallet_vol_m3 >= float(value['min']))
                        if 'max' in value:
                            conditions.append(PalletData.pallet_vol_m3 <= float(value['max']))
                    else:
                        if not isinstance(value, dict):
                            try:
                                conditions.append(PalletData.pallet_vol_m3 == float(value))
                            except (TypeError, ValueError):
                                pass
                elif key == "PALLET_AREA_M2":
                    if isinstance(value, dict) and ('min' in value or 'max' in value):
                        if 'min' in value:
                            conditions.append(PalletData.pallet_area_m2 >= float(value['min']))
                        if 'max' in value:
                            conditions.append(PalletData.pallet_area_m2 <= float(value['max']))
                    else:
                        if not isinstance(value, dict):
                            try:
                                conditions.append(PalletData.pallet_area_m2 == float(value))
                            except (TypeError, ValueError):
                                pass
                elif key == "PALLET_STACKING":
                    if isinstance(value, dict) and ('min' in value or 'max' in value):
                        if 'min' in value:
                            conditions.append(PalletData.pallet_stacking >= int(value['min']))
                        if 'max' in value:
                            conditions.append(PalletData.pallet_stacking <= int(value['max']))
                    else:
                        if not isinstance(value, dict):
                            try:
                                conditions.append(PalletData.pallet_stacking == int(value))
                            except (TypeError, ValueError):
                                pass

                # ===== SUPPLIER =====
                elif key == "SUPPLIER_NAME":
                    conditions.append(SupplierData.supplier_name.ilike(f"%{str_value}%"))
                elif key == "LOCATION":
                    conditions.append(SupplierData.location.ilike(f"%{str_value}%"))
                elif key == "CITY":
                    conditions.append(SupplierData.city.ilike(f"%{str_value}%"))
                elif key == "STREET":
                    conditions.append(SupplierData.street.ilike(f"%{str_value}%"))
                elif key == "BUILDING":
                    conditions.append(SupplierData.building.ilike(f"%{str_value}%"))
                elif key == "LOCALIZATION":
                    conditions.append(SupplierData.localization.cast(String).ilike(str_value))

            # Apply all the conditions with AND
            if conditions:
                query = query.filter(and_(*conditions))

            # Removing duplicates
            query = query.distinct()

            # Executing the request
            results = query.all()

            # If nothing is found, return the message
            if not results:
                return {
                    "success": True,
                    "found": False,
                    "message": "No parts found matching the criteria",
                    "data": []
                }

            # Convert the results to a flat table
            result_data = []
            for row in results:
                row_dict = dict(zip(row.keys(), row))

                # Normalize the output
                normalized_row = {}
                for col_name, value in row_dict.items():
                    upper_col_name = col_name.upper()
                    normalized_row[upper_col_name] = normalize_output(upper_col_name, value)

                result_data.append(normalized_row)

            return {
                "success": True,
                "found": True,
                "total_records": len(result_data),
                "applied_filters": filters,
                "data": result_data
            }

        return self._safe_query(query)

    def _create_result_row(
            self, part, supplier,
            ptm, ptl, ptb, box
        ):
        """Create a flat result row with all information and normalized output."""
        # Get configuration from part_to_model if available
        configuration = None
        if ptm and ptm.configuration:
            configuration = ptm.configuration.configuration

        # Basic part info with normalization
        row = {
            # Part information
            "PART_NUMBER": normalize_output("PART_NUMBER", part.part_number),
            "PART_NAME": normalize_output("PART_NAME", part.part_name),
            "PART_WEIGHT_KG": float(part.part_weight_kg) if part.part_weight_kg else None,

            # Model information (if available)
            "PART_PER_VEHICLE": ptm.part_per_vehicle if ptm else None,
            "CONFIGURATION": normalize_output("CONFIGURATION", configuration),
            "MODEL_CODE": normalize_output(
                "MODEL_CODE", ptm.model.model_code if ptm and ptm.model else None
            ),
            "MODEL_NAME": normalize_output(
                "MODEL_NAME", ptm.model.model_name if ptm and ptm.model else None
            ),

            # Line information (if available)
            "LINE_CODE": normalize_output(
                "LINE_CODE", ptl.line.line_code if ptl and ptl.line else None
            ),
            "LINE_NAME": normalize_output(
                "LINE_NAME", ptl.line.line_name if ptl and ptl.line else None
            ),

            # Workshop information (if available)
            "WORKSHOP_CODE": normalize_output(
                "WORKSHOP_CODE", ptl.line.workshop.workshop_code if ptl and ptl.line and ptl.line.workshop else None
            ),
            "WORKSHOP_NAME": normalize_output(
                "WORKSHOP_NAME", ptl.line.workshop.workshop_name if ptl and ptl.line and ptl.line.workshop else None
            ),

            # Box information (if available)
            "PART_PER_BOX": ptb.part_per_box if ptb else None,
            "BOX_TYPE": normalize_output("BOX_TYPE", box.box_type if box else None),
            "BOX_WEIGHT_KG": float(box.box_weight_kg) if box and box.box_weight_kg else None,
            "BOX_LENGTH_MM": box.box_length_mm if box else None,
            "BOX_WIDTH_MM": box.box_width_mm if box else None,
            "BOX_HEIGHT_MM": box.box_height_mm if box else None,
            "BOX_VOL_M3": float(box.box_vol_m3) if box and box.box_vol_m3 else None,
            "BOX_AREA_M2": float(box.box_area_m2) if box and box.box_area_m2 else None,
            "BOX_STACKING": box.box_stacking if box else None,

            # Pallet information (if available)
            "BOX_PER_PALLET": None,
            "PALLET_TYPE": None,
            "PALLET_WEIGHT_KG": None,
            "PALLET_LENGTH_MM": None,
            "PALLET_WIDTH_MM": None,
            "PALLET_HEIGHT_MM": None,
            "PALLET_VOL_M3": None,
            "PALLET_AREA_M2": None,
            "PALLET_STACKING": None
        }

        # Add pallet information if box has pallets and the pallet combination matches this part
        if box and box.pallets:
            # Find pallet combinations that match this part
            matching_pallets = [btp for btp in box.pallets if btp.part_id == part.part_id]
            if matching_pallets:
                btp = matching_pallets[0]
                if btp and btp.pallet:
                    pallet = btp.pallet
                    row["BOX_PER_PALLET"] = btp.box_per_pallet
                    row["PALLET_TYPE"] = normalize_output("PALLET_TYPE", pallet.pallet_type)
                    row["PALLET_WEIGHT_KG"] = float(pallet.pallet_weight_kg) if pallet.pallet_weight_kg else None
                    row["PALLET_LENGTH_MM"] = pallet.pallet_length_mm
                    row["PALLET_WIDTH_MM"] = pallet.pallet_width_mm
                    row["PALLET_HEIGHT_MM"] = pallet.pallet_height_mm
                    row["PALLET_VOL_M3"] = float(pallet.pallet_vol_m3) if pallet.pallet_vol_m3 else None
                    row["PALLET_AREA_M2"] = float(pallet.pallet_area_m2) if pallet.pallet_area_m2 else None
                    row["PALLET_STACKING"] = pallet.pallet_stacking

        # Supplier information with normalization
        row.update({
            "SUPPLIER_NAME": normalize_output(
                "SUPPLIER_NAME", supplier.supplier_name if supplier else None
            ),
            "LOCATION": normalize_output(
                "LOCATION", supplier.location if supplier else None
            ),
            "CITY": normalize_output(
                "CITY", supplier.city if supplier else None
            ),
            "STREET": normalize_output(
                "STREET", supplier.street if supplier else None
            ),
            "BUILDING": normalize_output(
                "BUILDING", supplier.building if supplier else None
            ),
            "LOCALIZATION": normalize_output(
                "LOCALIZATION", supplier.localization if supplier else None
            )
        })

        return row

    def export_to_excel(
            self,
            filters: Dict[str, Any],
            export_path: Optional[str] = None
        ) -> Dict[str, Any]:
        """
        Export search results to Excel file using Polars.
        
        Args:
            filters: Dictionary with filter parameters (same as universal_search)
            export_path: Optional path to save the file. If None, creates a temporary file.
            
        Returns:
            Dictionary with export information including file path
        """
        # First, get the search results
        search_result = self.universal_search(filters)

        if not search_result.get('success'):
            return search_result

        if not search_result.get('found'):
            return {
                "success": False,
                "error": "No data to export",
                "status": "no_data"
            }

        try:
            # Convert data to Polars DataFrame
            data = search_result['data']

            if not data:
                return {
                    "success": False,
                    "error": "No data to export",
                    "status": "no_data"
                }

            # Create Polars DataFrame
            df = pl.DataFrame(data)

            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Create filter description for filename
            filter_desc = []
            for key, value in filters.items():
                if value:
                    # Clean key for filename
                    clean_key = key.replace('_', '').replace('-', '')
                    if isinstance(value, dict):
                        # Handle range filters
                        range_parts = []
                        if 'min' in value:
                            range_parts.append(f"min{value['min']}")
                        if 'max' in value:
                            range_parts.append(f"max{value['max']}")
                        if range_parts:
                            filter_desc.append(f"{clean_key}_{'_'.join(range_parts)}")
                    else:
                        filter_desc.append(f"{clean_key}_{value}")

            filter_str = "_".join(filter_desc)[:50]  # Limit length

            if filter_str:
                filename = f"mft_export_{filter_str}_{timestamp}.xlsx"
            else:
                filename = f"mft_export_all_{timestamp}.xlsx"

            # Determine export path
            if export_path:
                # Ensure directory exists
                export_dir = Path(export_path)
                export_dir.mkdir(parents=True, exist_ok=True)
                file_path = export_dir / filename
            else:
                # Create temporary file
                temp_dir = Path(tempfile.gettempdir()) / "mft_exports"
                temp_dir.mkdir(parents=True, exist_ok=True)
                file_path = temp_dir / filename

            # Export to Excel using Polars
            df.write_excel(
                file_path,
                worksheet="Material Flow Data",
                autofit=True,
                table_style="Table Style Medium 2",
                column_widths=None  # Let polars handle column widths
            )

            logger.info("Exported %d rows to %s", len(data), file_path)

            return {
                "success": True,
                "file_path": str(file_path),
                "filename": filename,
                "row_count": len(data),
                "applied_filters": filters
            }

        except ImportError as e:
            logger.error("Polars Excel export failed - missing dependency: %s", e)
            return {
                "success": False,
                "error": "Excel export requires polars[excel] or xlsxwriter",
                "status": "missing_dependency"
            }

        except Exception as e:
            logger.error("Error exporting to Excel: %s", e, exc_info=True)
            return {
                "success": False,
                "error": f"Failed to export to Excel: {str(e)}",
                "status": "export_error"
            }

    def check_connection(self) -> bool:
        """
        Check if database connection is alive.
        
        Returns:
            bool: True if connection is working, False otherwise
        """
        def _check(session):
            try:
                session.execute('SELECT 1').scalar()
                return True
            except Exception:
                return False

        result = self._safe_query(_check)
        return result is True


# ========== FLASK ENDPOINTS ==========
def get_db_api() -> Optional[DatabaseAPI]:
    """
    Get DatabaseAPI instance from Flask application context.
    If not initialized or connection lost, attempt to reconnect.
    
    Returns:
        DatabaseAPI instance or None if not available
        
    Note:
        Attempts to reconnect if connection is lost or was never established.
        Only logs errors without raising exceptions to keep the API responsive.
    """
    try:
        # Check existing connection
        if 'db_api' in current_app.extensions and current_app.extensions['db_api'] is not None:
            # Check if the session is still alive
            db_api = current_app.extensions['db_api']

            if db_api.check_connection():
                return db_api
            else:
                logger.warning("Database connection check failed, will attempt to reconnect")
                current_app.extensions['db_api'] = None

        # Try to reconnect (if there is no connection or it has been reset)
        logger.info("Attempting to (re)connect to database...")
        engine = initialize_database(create_tables=False)

        if engine:
            try:
                db_api = DatabaseAPI(engine)
                current_app.extensions['db_api'] = db_api
                logger.info("Successfully (re)connected to database")
                return db_api

            except ValueError as e:
                logger.error("Failed to create DatabaseAPI instance: %s", e)
                current_app.extensions['db_api'] = None
                return None

            except SQLAlchemyError as e:
                logger.error("SQLAlchemy error creating DatabaseAPI: %s", e)
                current_app.extensions['db_api'] = None
                return None

            except Exception as unexpected_error:
                logger.error(
                    "Unexpected error creating DatabaseAPI: %s",
                    unexpected_error, exc_info=True
                )
                current_app.extensions['db_api'] = None
                return None

        else:
            logger.debug("Database engine not available (this is normal if DB is starting up)")
            current_app.extensions['db_api'] = None
            return None

    except OperationalError as e:
        logger.debug("Database not ready yet: %s", e)
        current_app.extensions['db_api'] = None
        return None

    except ProgrammingError as e:
        # Check if the error is related to missing tables
        error_msg = str(e.orig) if e.orig else str(e)
        if 'does not exist' in error_msg.lower() or ('relation' in error_msg.lower() and 'does not exist' in error_msg.lower()):
            logger.debug("Tables not yet created")
        else:
            logger.warning("Programming error during database initialization: %s", e)
        current_app.extensions['db_api'] = None
        return None

    except ConnectionError as e:
        logger.debug("Connection error to database: %s", e)
        current_app.extensions['db_api'] = None
        return None

    except SQLAlchemyError as e:
        logger.warning("SQLAlchemy error during database initialization: %s", e)
        current_app.extensions['db_api'] = None
        return None

    except RuntimeError as e:
        logger.error("Called outside of application context: %s", e)
        return None

    except KeyError as e:
        logger.error("KeyError accessing app.extensions: %s", e)
        return None

    except AttributeError as e:
        logger.error("AttributeError accessing current_app: %s", e)
        return None

    except Exception as unexpected_error:
        logger.error("Unexpected global error in get_db_api: %s", unexpected_error, exc_info=True)
        return None


def handle_api_response(f):
    """Decorator to handle API responses and errors."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            result = f(*args, **kwargs)

            # If it is already an HTTP response (send_file, jsonify, redirect, tuple)
            if isinstance(result, tuple) or hasattr(result, 'get_data'):
                return result

            # Check that result is a dictionary before calling .get()
            if isinstance(result, dict):
                if result.get('error'):
                    status_code = 500
                    if result.get('status') in ['integrity_error', 'data_error']:
                        status_code = 400
                    elif result.get('status') == 'operational_error':
                        status_code = 503
                    elif result.get('status') == 'not_found':
                        status_code = 404
                    elif result.get('status') == 'no_data':
                        status_code = 404
                    return jsonify(result), status_code

                # If the dictionary has no error, we return it as JSON
                return jsonify(result)
            # For all other types (None, list, number, etc.)
            return result

        except (ValueError, TypeError) as e:
            logger.warning("Validation error in API request: %s", e)
            return jsonify({
                'error': f'Invalid request data: {str(e)}',
                'success': False,
                'status': 'bad_request'
            }), 400

        except IntegrityError as e:
            logger.error("Integrity error in API request: %s", e)
            return jsonify({
                'error': 'Data integrity violation',
                'detail': str(e.orig) if e.orig else str(e),
                'success': False,
                'status': 'integrity_error'
            }), 400

        except DataError as e:
            logger.error("Data error in API request: %s", e)
            return jsonify({
                'error': 'Invalid data format',
                'detail': str(e.orig) if e.orig else str(e),
                'success': False,
                'status': 'data_error'
            }), 400

        except OperationalError as e:
            logger.error("Operational error in API request: %s", e)
            return jsonify({
                'error': 'Database operation failed (connection or transaction)',
                'success': False,
                'status': 'operational_error'
            }), 503

        except ProgrammingError as e:
            # Check if the error is related to the missing table
            error_msg = str(e.orig) if e.orig else str(e)
            if 'does not exist' in error_msg.lower() or ('relation' in error_msg.lower() and 'does not exist' in error_msg.lower()):
                logger.info("Database tables not yet created, returning empty result")
                return jsonify({
                    'success': True,
                    'found': False,
                    'message': 'No data available – database tables are not created yet. Please run ETL first.',
                    'data': []
                }), 200
            else:
                logger.error("Programming error in API request: %s", e)
                return jsonify({
                    'error': 'Database programming error',
                    'detail': error_msg,
                    'success': False,
                    'status': 'programming_error'
                }), 500

        except InvalidRequestError as e:
            logger.error("Invalid request error in API request: %s", e)
            return jsonify({
                'error': 'Invalid database request',
                'success': False,
                'status': 'invalid_request'
            }), 500

        except StatementError as e:
            logger.error("Statement error in API request: %s", e)
            return jsonify({
                'error': 'SQL statement error',
                'success': False,
                'status': 'statement_error'
            }), 500

        except SQLAlchemyError as e:
            logger.error("SQLAlchemy error in API request: %s", e)
            return jsonify({
                'error': 'Database error occurred',
                'success': False,
                'status': 'database_error'
            }), 500

        except KeyError as e:
            logger.warning("Missing required key in request: %s", e)
            return jsonify({
                'error': f'Missing required parameter: {str(e)}',
                'success': False,
                'status': 'bad_request'
            }), 400

        except AttributeError as e:
            logger.error("Attribute error in API request: %s", e)
            return jsonify({
                'error': 'Internal server error',
                'success': False,
                'status': 'error'
            }), 500

        except RuntimeError as e:
            logger.error("Runtime error in API request: %s", e)
            return jsonify({
                'error': 'Application context error',
                'success': False,
                'status': 'error'
            }), 500

        except Exception as unexpected_error:
            logger.error("Unexpected API error: %s", unexpected_error, exc_info=True)
            return jsonify({
                'error': 'An unexpected error occurred',
                'success': False,
                'status': 'error'
            }), 500

    return wrapper


# ========== UNIVERSAL SEARCH ENDPOINT ==========
@display_bp.route('/search', methods=['GET', 'POST'])
@rate_limit()
@handle_api_response
def universal_search_endpoint():
    """
    Universal search endpoint - accepts any filters and returns complete data.
    All text searches are case-insensitive.
    Numeric fields support range queries with _min and _max suffixes.

    GET: /api/search?part_number=999&localization=yes&workshop_code=as&part_weight_kg_min=1&part_weight_kg_max=3
    POST: /api/search with JSON body containing filters

    Returns flat table with all columns, normalized according to rules.
    """
    api = get_db_api()
    if not api:
        return jsonify({
            'error': 'Database connection not available',
            'success': False,
            'status': 'service_unavailable'
        }), 503

    # Collect filters from request
    filters = {}

    if request.method == 'POST':
        # JSON body
        filters = request.get_json(silent=True) or {}
        if not isinstance(filters, dict):
            raise ValueError("POST body must be a JSON object")
    else:
        # Query parameters
        filters = request.args.to_dict()

    # Process filters to support range queries with _min and _max suffixes
    processed_filters = {}

    # First, collect all filters
    for key, value in filters.items():
        if value is None or value == "":
            continue

        # Check if this is a range filter (ends with _min or _max)
        if key.endswith('_min') or key.endswith('_max'):
            base_key = key[:-4]  # Remove _min or _max
            range_type = key[-3:]  # 'min' or 'max'

            if base_key not in processed_filters:
                processed_filters[base_key] = {}

            try:
                # Try to convert to float or int
                if '.' in value:
                    processed_filters[base_key][range_type] = float(value)
                else:
                    processed_filters[base_key][range_type] = int(value)
            except (ValueError, TypeError):
                processed_filters[base_key][range_type] = value
        else:
            # Regular filter
            try:
                # Try to convert numeric strings to appropriate types
                if isinstance(value, str) and '.' in value:
                    processed_filters[key] = float(value)
                elif isinstance(value, str):
                    try:
                        processed_filters[key] = int(value)
                    except ValueError:
                        processed_filters[key] = value
                else:
                    processed_filters[key] = value
            except (ValueError, TypeError) as e:
                logger.warning(
                    "Failed to convert filter value '%s' for key '%s': %s",
                    value, key, e
                )
                processed_filters[key] = value

    logger.info("Universal search with filters: %s", processed_filters)

    return api.universal_search(processed_filters)


# ========== EXPORT TO EXCEL ENDPOINT ==========
@display_bp.route('/export', methods=['POST'])
@rate_limit()
@handle_api_response
def export_to_excel_endpoint():
    """
    Export search results to Excel file.
    
    POST /export with JSON body containing filters and optional export_path
    
    Expected JSON body:
        {
            "filters": {                    # Dictionary with search filters
                "part_number": "999",        # Case-insensitive partial match
                "localization": "yes",       # Exact match for enum fields
                "workshop_code": "as",       # Can use _min/_max for range queries
                "box_length_mm_min": 500,    # Minimum value for range filter
                "box_length_mm_max": 1200    # Maximum value for range filter
            }
        }

    Note: export_path parameter is no longer supported. File is always sent for download
    and user chooses save location in their browser/Postman save dialog.

    Workflow:
        1. Client sends POST request with filters
        2. Server validates filters and queries database
        3. Server creates temporary Excel file using Polars
        4. Server sends file to client with as_attachment=True
        5. Browser shows save dialog
        6. User selects folder on the local machine
        7. File is saved to user-specified location
        8. Temporary file is automatically cleaned up

    Returns:
        flask.Response: Excel file as attachment with:
            - Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
            - Content-Disposition: attachment; filename=mft_export_*.xlsx
            - File is sent for download, user chooses save location in their browser/Postman

    Raises:
        ValueError: If request body is not a JSON object or filters is not a dictionary
        Various SQLAlchemy errors: Handled by @handle_api_response decorator

    Status Codes:
        200: OK - Excel file successfully sent
        400: Bad Request - Invalid JSON format or filters not a dictionary
        503: Service Unavailable - Database connection not available
        500: Internal Server Error - Unexpected error during export
    """
    api = get_db_api()
    if not api:
        return jsonify({
            'error': 'Database connection not available',
            'success': False,
            'status': 'service_unavailable'
        }), 503

    # Get request data
    data = request.get_json(silent=True)
    if not data or not isinstance(data, dict):
        logger.warning("Invalid request body: not a JSON object")
        raise ValueError("Request body must be a JSON object")

    filters = data.get('filters', {})

    # Log but ignore export_path
    if 'export_path' in data:
        logger.info(
            "export_path parameter is ignored. User will choose save location in the browser"
        )

    if not isinstance(filters, dict):
        logger.warning("Filters parameter is not a dictionary: %s", type(filters))
        raise ValueError("'filters' must be a JSON object")

    logger.info("Export request with filters: %s", filters)

    # Process filters (same as in search endpoint)
    processed_filters = {}
    for key, value in filters.items():
        if value is None or value == "":
            continue

        if isinstance(value, dict):
            # Range filter
            processed_filters[key] = {}
            for range_key, range_value in value.items():
                if range_key in ['min', 'max']:
                    try:
                        if isinstance(range_value, str) and '.' in range_value:
                            processed_filters[key][range_key] = float(range_value)
                        elif isinstance(range_value, str):
                            try:
                                processed_filters[key][range_key] = int(range_value)
                            except ValueError:
                                processed_filters[key][range_key] = range_value
                        else:
                            processed_filters[key][range_key] = range_value
                    except (ValueError, TypeError) as conv_error:
                        logger.warning(
                            "Failed to convert range value '%s' for key '%s': %s",
                            range_value, key, conv_error
                        )
                        processed_filters[key][range_key] = range_value
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
            except (ValueError, TypeError) as conv_error:
                logger.warning(
                    "Failed to convert filter value '%s' for key '%s': %s",
                    value, key, conv_error
                )
                processed_filters[key] = value

    # Export to Excel - ALWAYS creating a temporary file (export_path=None)
    result = api.export_to_excel(processed_filters, export_path=None)

    if not result.get('success'):
        logger.error("Export failed: %s", result.get('error', 'Unknown error'))
        return result

    # Send file to the user for download
    file_path = result['file_path']
    filename = result['filename']

    try:
        if not os.path.exists(file_path):
            logger.error("Export file not found: %s", file_path)
            return {
                'success': False,
                'error': 'Export file not found',
                'status': 'export_error'
            }

        # send_file sends file to client, browser shows save dialog
        response = send_file(
            file_path,
            as_attachment=True,
            download_name=result['filename'],
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

        # Clean up temporary file AFTER sending
        @response.call_on_close
        def cleanup():
            try:
                if os.path.exists(file_path):
                    os.unlink(file_path)
                    logger.debug("Temporary file %s cleaned up", file_path)
                else:
                    logger.debug("File %s already removed", file_path)

            except OSError as cleanup_error:
                logger.warning("Failed to cleanup temp file %s: %s", file_path, cleanup_error)

            except Exception as unexpected_error:
                logger.error(
                    "Unexpected error during cleanup of %s: %s",
                    file_path, unexpected_error, exc_info=True
                )

        logger.info("File %s sent to user, will be cleaned up after download", filename)
        return response

    except PermissionError as perm_error:
        logger.error("Permission denied when accessing file %s: %s", file_path, perm_error)
        # Clean up temp file
        try:
            if os.path.exists(file_path):
                os.unlink(file_path)
        except Exception:
            pass
        return {
            'success': False,
            'error': 'File access denied',
            'status': 'export_error'
        }

    except OSError as os_error:
        logger.error("OS error when sending file %s: %s", file_path, os_error)
        # Clean up temp file
        try:
            if os.path.exists(file_path):
                os.unlink(file_path)
        except Exception:
            pass
        return {
            'success': False,
            'error': f'File system error: {str(os_error)}',
            'status': 'export_error'
        }

    except Exception as unexpected_error:
        logger.error("Unexpected error sending file: %s", unexpected_error, exc_info=True)

        # Clean up temp file if it exists
        try:
            if 'file_path' in result and os.path.exists(result['file_path']):
                os.unlink(result['file_path'])
                logger.debug("Temporary file %s cleaned up after error", result['file_path'])
        except Exception as cleanup_error:
            logger.warning("Failed to cleanup temp file after error: %s", cleanup_error)

        return {
            'success': False,
            'error': f'Failed to send file: {str(unexpected_error)}',
            'status': 'export_error'
        }


# ========== ENDPOINTS FOR REFERENCE INFORMATION ==========
@display_bp.route('/info/columns', methods=['GET'])
def get_available_columns():
    """GET /api/info/columns - Get list of all available filter columns with range support."""
    return jsonify({
        'success': True,
        'columns': {
            # Part columns
            'part': {
                'exact': ['part_number', 'part_name'],
                'range': ['part_weight_kg']
            },

            # Supplier columns
            'supplier': {
                'exact': ['supplier_name', 'location', 'city', 'street', 'building', 'localization'],
                'range': []
            },

            # Model columns
            'model': {
                'exact': ['model_code', 'model_name', 'configuration'],
                'range': ['part_per_vehicle']
            },

            # Line columns
            'line': {
                'exact': ['line_code', 'line_name'],
                'range': []
            },

            # Workshop columns
            'workshop': {
                'exact': ['workshop_code', 'workshop_name'],
                'range': []
            },

            # Box columns
            'box': {
                'exact': ['part_per_box', 'box_type'],
                'range': [
                    'box_weight_kg', 'box_length_mm', 'box_width_mm',
                    'box_height_mm', 'box_vol_m3', 'box_area_m2', 'box_stacking'
                ]
            },

            # Pallet columns
            'pallet': {
                'exact': ['box_per_pallet', 'pallet_type'],
                'range': [
                    'pallet_weight_kg', 'pallet_length_mm', 'pallet_width_mm',
                    'pallet_height_mm', 'pallet_vol_m3', 'pallet_area_m2', 'pallet_stacking'
                ]
            }
        },
        'filter_syntax': {
            'exact': 'Use column name directly: ?part_number=999',
            'range': 'Use _min and _max suffixes: ?part_weight_kg_min=1&part_weight_kg_max=3',
            'text_search': 'Case-insensitive partial matching for text fields'
        },
        'output_columns': [
            'PART_NUMBER', 'PART_NAME', 'PART_WEIGHT_KG',
            'PART_PER_VEHICLE', 'CONFIGURATION', 'MODEL_CODE', 'MODEL_NAME',
            'LINE_CODE', 'LINE_NAME', 'WORKSHOP_CODE', 'WORKSHOP_NAME',
            'PART_PER_BOX', 'BOX_TYPE', 'BOX_WEIGHT_KG', 'BOX_LENGTH_MM', 
            'BOX_WIDTH_MM', 'BOX_HEIGHT_MM', 'BOX_VOL_M3', 'BOX_AREA_M2', 'BOX_STACKING',
            'BOX_PER_PALLET', 'PALLET_TYPE', 'PALLET_WEIGHT_KG', 
            'PALLET_LENGTH_MM', 'PALLET_WIDTH_MM', 'PALLET_HEIGHT_MM',
            'PALLET_VOL_M3', 'PALLET_AREA_M2', 'PALLET_STACKING',
            'SUPPLIER_NAME', 'LOCATION', 'CITY', 'STREET', 'BUILDING', 'LOCALIZATION'
        ],
        'normalization_rules': {
            'uppercase': [
                'PART_NUMBER', 'CONFIGURATION', 'MODEL_CODE',
                'LINE_CODE', 'WORKSHOP_CODE', 'BUILDING'
            ],
            'sentence_case': [
                'PART_NAME', 'MODEL_NAME', 'LINE_NAME', 'WORKSHOP_NAME',
                'BOX_TYPE', 'PALLET_TYPE', 'SUPPLIER_NAME', 'LOCALIZATION'
            ],
            'title_case': ['LOCATION', 'CITY', 'STREET']
        }
    })


@display_bp.route('/health', methods=['GET'])
def health_check():
    """
    GET /api/health - Health check endpoint.
    Health check endpoint – always returns 200, even if DB is not ready.
    """
    try:
        api = get_db_api()

        # Test database connection
        db_status = 'disconnected'
        if api:
            if api.check_connection():
                db_status = 'connected'
            else:
                db_status = 'connection_failed'

        return jsonify({
            'status': 'healthy',
            'service': 'Display API',
            'timestamp': datetime.now().isoformat(),
            'environment': FLASK_ENV,
            'cors_mode': 'restricted' if ALLOWED_ORIGINS != '*' else 'open',
            'cors_origins': ALLOWED_ORIGINS if ALLOWED_ORIGINS != '*' else 'all',
            'rate_limit': RATE_LIMIT,
            'database_status': db_status,
            'features': {
                'case_insensitive_search': True,
                'output_normalization': True,
                'range_queries': True,
                'excel_export': True
            }
        }), 200

    except Exception as e:
        logger.error("Health check failed: %s", e)
        return jsonify({
            'status': 'degraded',
            'service': 'Display API',
            'error': str(e)
        }), 200


@display_bp.route('/', methods=['GET'])
def api_documentation():
    """GET /api/ - API documentation."""
    return jsonify({
        'name': 'Material Flow Database API',
        'version': '1.0.0',
        'description': 'Universal search API for material flow database with case-insensitive search, output normalization, range queries, and Excel export',
        'features': {
            'case_insensitive_search': True,
            'output_normalization': True,
            'range_queries': True,
            'excel_export': True
        },
        'normalization_rules': {
            'uppercase': [
                'PART_NUMBER', 'CONFIGURATION', 'MODEL_CODE',
                'LINE_CODE', 'WORKSHOP_CODE', 'BUILDING'
            ],
            'sentence_case': [
                'PART_NAME', 'MODEL_NAME', 'LINE_NAME', 'WORKSHOP_NAME',
                'BOX_TYPE', 'PALLET_TYPE', 'SUPPLIER_NAME', 'LOCALIZATION'
            ],
            'title_case': ['LOCATION', 'CITY', 'STREET']
        },
        'range_query_examples': {
            'Part weight': '/api/search?part_weight_kg_min=1&part_weight_kg_max=3',
            'Box dimensions': '/api/search?box_length_mm_min=500&box_length_mm_max=1200&box_width_mm_min=300&box_width_mm_max=800',
            'Box volume': '/api/search?box_vol_m3_min=1&box_vol_m3_max=5',
            'Pallet area': '/api/search?pallet_area_m2_min=1&pallet_area_m2_max=2.5',
            'Stacking factor': '/api/search?box_stacking_min=3&box_stacking_max=6'
        },
        'export_examples': {
            'Export to custom location': 'POST /api/export with JSON: {"filters": {"part_number": "999"}, "export_path": "/path/to/save"}',
            'Download directly': 'POST /api/export with JSON: {"filters": {"localization": "yes", "workshop_code": "as"}}'
        },
        'usage': {
            'search': {
                'endpoint': '/api/search',
                'methods': ['GET', 'POST'],
                'description': 'Accepts any filters (case-insensitive) and returns complete part information with normalized output'
            },
            'export': {
                'endpoint': '/api/export',
                'methods': ['POST'],
                'description': 'Export search results to Excel file (Polars)'
            }
        },
        'examples': {
            'Simple Search': '/api/search?part_number=999',
            'Search with a range': '/api/search?part_weight_kg_min=1&part_weight_kg_max=3&localization=yes',
            'Complex search': '/api/search?workshop_code=as&box_length_mm_min=500&box_length_mm_max=1200&supplier_name=bosch',
            'Export': 'POST /api/export with JSON body: {"filters": {"part_number": "999", "part_weight_kg_min": 1, "part_weight_kg_max": 3}}'
        },
        'available_filters': {
            'exact_match': [
                'part_number', 'part_name', 'supplier_name', 'location', 'city', 'street', 
                'building', 'localization', 'model_code', 'model_name', 'configuration',
                'line_code', 'line_name', 'workshop_code', 'workshop_name', 'part_per_box',
                'box_type', 'box_per_pallet', 'pallet_type'
            ],
            'range_queries': [
                'part_weight_kg', 'part_per_vehicle', 'box_weight_kg', 'box_length_mm',
                'box_width_mm', 'box_height_mm', 'box_vol_m3', 'box_area_m2', 'box_stacking',
                'pallet_weight_kg', 'pallet_length_mm', 'pallet_width_mm', 'pallet_height_mm',
                'pallet_vol_m3', 'pallet_area_m2', 'pallet_stacking'
            ]
        },
        'output_columns': [
            'PART_NUMBER', 'PART_NAME', 'PART_WEIGHT_KG',
            'PART_PER_VEHICLE', 'CONFIGURATION', 'MODEL_CODE', 'MODEL_NAME',
            'LINE_CODE', 'LINE_NAME', 'WORKSHOP_CODE', 'WORKSHOP_NAME',
            'PART_PER_BOX', 'BOX_TYPE', 'BOX_WEIGHT_KG', 'BOX_LENGTH_MM', 
            'BOX_WIDTH_MM', 'BOX_HEIGHT_MM', 'BOX_VOL_M3', 'BOX_AREA_M2', 'BOX_STACKING',
            'BOX_PER_PALLET', 'PALLET_TYPE', 'PALLET_WEIGHT_KG', 
            'PALLET_LENGTH_MM', 'PALLET_WIDTH_MM', 'PALLET_HEIGHT_MM',
            'PALLET_VOL_M3', 'PALLET_AREA_M2', 'PALLET_STACKING',
            'SUPPLIER_NAME', 'LOCATION', 'CITY', 'STREET', 'BUILDING', 'LOCALIZATION'
        ]
    })


# ========== FLASK APP SETUP ==========
def create_app():
    """
    Create and configure the Flask application instance.
    
    Sets up:
    - Secret key for sessions
    - CORS for Browser Security Policy
    - Security headers
    - Blueprint registration
    - Rate limiter initialization
    - Database connection

    Returns:
        Flask: Configured Flask application instance
    """
    app = Flask(__name__)
    app.secret_key = FLASK_SECRET_KEY

    # ========== CORS CONFIGURATION ==========
    if ALLOWED_ORIGINS == "*":
        CORS(app)
        logger.debug("CORS: Allowing all origins (development mode)")
    else:
        allowed_origins_list = [origin.strip() for origin in ALLOWED_ORIGINS.split(',')]
        CORS(app, origins=allowed_origins_list, supports_credentials=True)
        logger.info("CORS: Restricted to %d origins", len(allowed_origins_list))

    # ========== SECURITY HEADERS ==========
    @app.after_request
    def add_security_headers(response):
        if IS_PRODUCTION:
            response.headers.add('X-Content-Type-Options', 'nosniff')
            response.headers.add('X-Frame-Options', 'DENY')
            response.headers.add('X-XSS-Protection', '1; mode=block')
        return response

    # ========== REGISTER BLUEPRINT ==========
    app.register_blueprint(display_bp)

    # ========== RATE LIMITING ==========
    limiter.init_app(app)

    # ========== DATABASE CONNECTION ==========
    try:
        engine = initialize_database(create_tables=False)

        if engine:
            try:
                db_api = DatabaseAPI(engine)
                app.extensions['db_api'] = db_api
                logger.info("DatabaseAPI initialized and stored in app.extensions")

            except (ValueError, SQLAlchemyError) as e:
                logger.error("Failed to create DatabaseAPI instance: %s", e)
                app.extensions['db_api'] = None

        else:
            logger.error("Failed to initialize database connection (engine is None)")
            app.extensions['db_api'] = None

    except (OperationalError, ProgrammingError) as e:
        logger.error("Database connection error during initialization: %s", e)
        app.extensions['db_api'] = None

    except Exception as unexpected_error:
        logger.error(
            "Unexpected error during database initialization: %s",
            unexpected_error, exc_info=True
        )
        app.extensions['db_api'] = None

    # ========== LOG REGISTERED ROUTES ==========
    logger.info("Display API endpoints registered:")
    for rule in app.url_map.iter_rules():
        if rule.endpoint and rule.endpoint.startswith('display'):
            if rule.methods:
                methods = sorted([m for m in rule.methods if m not in {'HEAD', 'OPTIONS'}])
                methods_str = ','.join(methods) if methods else 'NONE'
            else:
                methods_str = 'NONE'

            logger.info("  %-50s %s -> [%s]", rule, rule.endpoint, methods_str)

    return app

# ========== CREATE APP INSTANCE ==========
app = create_app()


# ========== MAIN ENTRY POINT ==========
if __name__ == '__main__':
    logger.info("="*60)
    logger.info("Starting Display API on %s:%s", FLASK_HOST, FLASK_PORT)
    logger.info("Environment: %s", FLASK_ENV)
    logger.info("CORS mode: %s", 'restricted' if ALLOWED_ORIGINS != '*' else 'open')
    logger.info("CORS origins: %s", ALLOWED_ORIGINS)
    logger.info("Rate limit: %s", RATE_LIMIT)
    logger.info("Debug mode: %s", FLASK_DEBUG)
    logger.info("="*60)

    app.run(
        host=FLASK_HOST,
        port=FLASK_PORT,
        debug=FLASK_DEBUG,
        threaded=True
    )
