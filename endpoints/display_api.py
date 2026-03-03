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
Last Modified: 2026-03-02
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
from flask import Blueprint, request, jsonify, current_app, send_file
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker, joinedload, selectinload
from sqlalchemy.exc import (
    SQLAlchemyError, IntegrityError, DataError, StatementError,
    OperationalError, ProgrammingError, InvalidRequestError
)
import polars as pl

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
    ModelData, WorkshopData, LineData,
    # Junction tables
    PartToBox, BoxToPallet, PartToModel, PartToLine
)

# Logger setup
logger = get_logger(__name__)

# ============================================================================
# FUNCTIONS FOR CASE NORMALIZATION
# ============================================================================

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
                     BOX_TYPE, PALLET_TYPE, SUPPLIER_NAME, LOCALIZATION
    - Title case: LOCATION, CITY, STREET
    """
    if value is None:
        return None

    uppercase_columns = [
        'PART_NUMBER', 'CONFIGURATION', 'MODEL_CODE', 
        'LINE_CODE', 'WORKSHOP_CODE', 'BUILDING'
    ]

    sentence_case_columns = [
        'PART_NAME', 'MODEL_NAME', 'LINE_NAME', 'WORKSHOP_NAME',
        'BOX_TYPE', 'PALLET_TYPE', 'SUPPLIER_NAME', 'LOCALIZATION'
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


# ============================================================================
# DATABASE API CLASS
# ============================================================================

class DatabaseAPI:
    """
    Main API class for database operations.
    
    Provides methods for querying and filtering data across all tables.
    Handles session management and error handling with case-insensitive search.
    Supports range queries for numeric fields.
    """

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
        except Exception as e:
            session.rollback()
            logger.error("Unexpected error in database query: %s", e, exc_info=True)
            return {
                "error": f"Unexpected error: {str(e)}",
                "status": "unexpected_error",
                "success": False
            }
        finally:
            session.close()

    # ============================================================================
    # UNIVERSAL SEARCH (WITH CASE-INSENSITIVE FILTERING AND RANGES)
    # ============================================================================

    def universal_search(
            self,
            filters: Dict[str, Any]
        ) -> Dict[str, Any]:
        """
        Universal search - accepts any filters and returns complete part information.
        All text searches are case-insensitive.
        Numeric fields support range queries with _min and _max suffixes.
        
        Args:
            filters: Dictionary with filter parameters (any column from any table)
                    Example: {
                        "part_number": "999",
                        "localization": "yes",
                        "workshop_code": "as",
                        "supplier_name": "bosch",
                        "part_weight_kg_min": 1.0,
                        "part_weight_kg_max": 3.0,
                        "box_length_mm_min": 500,
                        "box_length_mm_max": 1200,
                        "box_vol_m3_min": 1.0,
                        "box_vol_m3_max": 5.0
                    }
        
        Returns:
            Dictionary with complete information for all matching parts
        """
        def query(session):
            # Start with PartData
            query = session.query(PartData).distinct()

            # Track which joins we've already made to avoid duplicates
            joined_supplier = False
            joined_part_to_model = False
            joined_model = False
            joined_part_to_line = False
            joined_line = False
            joined_workshop = False
            joined_part_to_box = False
            joined_box = False
            joined_box_to_pallet = False
            joined_pallet = False

            # Apply filters dynamically (case-insensitive and range support)
            for key, value in filters.items():
                if value is None or value == "":
                    continue

                # Convert value to string for case-insensitive comparison where needed
                str_value = str(value)

                # ===== PART DATA filters =====
                if key == "part_number":
                    query = query.filter(PartData.part_number.ilike(f"%{str_value}%"))
                elif key == "part_name":
                    query = query.filter(PartData.part_name.ilike(f"%{str_value}%"))
                elif key == "part_weight_kg_min":
                    query = query.filter(PartData.part_weight_kg >= float(value))
                elif key == "part_weight_kg_max":
                    query = query.filter(PartData.part_weight_kg <= float(value))

                # ===== SUPPLIER filters =====
                elif key in [
                    "supplier_name", "location", "city", "street", "building", "localization"
                ]:
                    if not joined_supplier:
                        query = query.join(PartData.supplier)
                        joined_supplier = True

                    if key == "supplier_name":
                        query = query.filter(
                            SupplierData.supplier_name.ilike(f"%{str_value}%")
                        )
                    elif key == "location":
                        query = query.filter(
                            SupplierData.location.ilike(f"%{str_value}%")
                        )
                    elif key == "city":
                        query = query.filter(
                            SupplierData.city.ilike(f"%{str_value}%")
                        )
                    elif key == "street":
                        query = query.filter(
                            SupplierData.street.ilike(f"%{str_value}%")
                        )
                    elif key == "building":
                        query = query.filter(
                            SupplierData.building.ilike(f"%{str_value}%")
                        )
                    elif key == "localization":
                        query = query.filter(
                            func.lower(SupplierData.localization) == str_value.lower()
                        )

                # ===== MODEL filters =====
                elif key in ["model_code", "model_name", "configuration", "part_per_vehicle"]:
                    if not joined_part_to_model:
                        query = query.join(PartToModel, PartData.part_id == PartToModel.part_id)
                        joined_part_to_model = True

                    if not joined_model and key in ["model_code", "model_name"]:
                        query = query.join(PartToModel.model)
                        joined_model = True

                    if key == "model_code":
                        query = query.filter(func.lower(ModelData.model_code) == str_value.lower())
                    elif key == "model_name":
                        query = query.filter(func.lower(ModelData.model_name) == str_value.lower())
                    elif key == "configuration":
                        query = query.filter(PartToModel.configuration.ilike(f"%{str_value}%"))
                    elif key == "part_per_vehicle":
                        query = query.filter(PartToModel.part_per_vehicle == int(value))

                # ===== LINE & WORKSHOP filters =====
                elif key in ["line_code", "line_name", "workshop_code", "workshop_name"]:
                    if not joined_part_to_line:
                        query = query.join(PartToLine, PartData.part_id == PartToLine.part_id)
                        joined_part_to_line = True

                    if not joined_line and key in ["line_code", "line_name"]:
                        query = query.join(PartToLine.line)
                        joined_line = True

                    if not joined_workshop and key in ["workshop_code", "workshop_name"]:
                        if not joined_line:
                            query = query.join(PartToLine.line)
                            joined_line = True
                        query = query.join(LineData.workshop)
                        joined_workshop = True

                    if key == "line_code":
                        query = query.filter(func.lower(LineData.line_code) == str_value.lower())
                    elif key == "line_name":
                        query = query.filter(LineData.line_name.ilike(f"%{str_value}%"))
                    elif key == "workshop_code":
                        query = query.filter(
                            func.lower(WorkshopData.workshop_code) == str_value.lower()
                        )
                    elif key == "workshop_name":
                        query = query.filter(
                            func.lower(WorkshopData.workshop_name) == str_value.lower()
                        )

                # ===== BOX filters =====
                elif key in ["part_per_box", "box_type",
                           "box_weight_kg_min", "box_weight_kg_max",
                           "box_length_mm_min", "box_length_mm_max",
                           "box_width_mm_min", "box_width_mm_max",
                           "box_height_mm_min", "box_height_mm_max",
                           "box_vol_m3_min", "box_vol_m3_max",
                           "box_area_m2_min", "box_area_m2_max",
                           "box_stacking_min", "box_stacking_max"]:

                    if not joined_part_to_box:
                        query = query.join(PartToBox, PartData.part_id == PartToBox.part_id)
                        joined_part_to_box = True

                    if not joined_box:
                        query = query.join(PartToBox.box)
                        joined_box = True

                    if key == "part_per_box":
                        query = query.filter(PartToBox.part_per_box == int(value))
                    elif key == "box_type":
                        query = query.filter(func.lower(BoxData.box_type) == str_value.lower())

                    # Box weight ranges
                    elif key == "box_weight_kg_min":
                        query = query.filter(BoxData.box_weight_kg >= float(value))
                    elif key == "box_weight_kg_max":
                        query = query.filter(BoxData.box_weight_kg <= float(value))

                    # Box dimension ranges
                    elif key == "box_length_mm_min":
                        query = query.filter(BoxData.box_length_mm >= int(value))
                    elif key == "box_length_mm_max":
                        query = query.filter(BoxData.box_length_mm <= int(value))
                    elif key == "box_width_mm_min":
                        query = query.filter(BoxData.box_width_mm >= int(value))
                    elif key == "box_width_mm_max":
                        query = query.filter(BoxData.box_width_mm <= int(value))
                    elif key == "box_height_mm_min":
                        query = query.filter(BoxData.box_height_mm >= int(value))
                    elif key == "box_height_mm_max":
                        query = query.filter(BoxData.box_height_mm <= int(value))

                    # Box volume/area ranges (computed columns)
                    elif key == "box_vol_m3_min":
                        query = query.filter(BoxData.box_vol_m3 >= float(value))
                    elif key == "box_vol_m3_max":
                        query = query.filter(BoxData.box_vol_m3 <= float(value))
                    elif key == "box_area_m2_min":
                        query = query.filter(BoxData.box_area_m2 >= float(value))
                    elif key == "box_area_m2_max":
                        query = query.filter(BoxData.box_area_m2 <= float(value))

                    # Box stacking ranges
                    elif key == "box_stacking_min":
                        query = query.filter(BoxData.box_stacking >= int(value))
                    elif key == "box_stacking_max":
                        query = query.filter(BoxData.box_stacking <= int(value))

                # ===== PALLET filters =====
                elif key in ["box_per_pallet", "pallet_type",
                           "pallet_weight_kg_min", "pallet_weight_kg_max",
                           "pallet_length_mm_min", "pallet_length_mm_max",
                           "pallet_width_mm_min", "pallet_width_mm_max",
                           "pallet_height_mm_min", "pallet_height_mm_max",
                           "pallet_vol_m3_min", "pallet_vol_m3_max",
                           "pallet_area_m2_min", "pallet_area_m2_max",
                           "pallet_stacking_min", "pallet_stacking_max"]:

                    if not joined_part_to_box:
                        query = query.join(PartToBox, PartData.part_id == PartToBox.part_id)
                        joined_part_to_box = True

                    if not joined_box:
                        query = query.join(PartToBox.box)
                        joined_box = True

                    if not joined_box_to_pallet:
                        query = query.join(BoxToPallet, BoxData.box_id == BoxToPallet.box_id)
                        joined_box_to_pallet = True

                    if not joined_pallet:
                        query = query.join(BoxToPallet.pallet)
                        joined_pallet = True

                    if key == "box_per_pallet":
                        query = query.filter(BoxToPallet.box_per_pallet == int(value))
                    elif key == "pallet_type":
                        query = query.filter(
                            func.lower(PalletData.pallet_type) == str_value.lower()
                        )

                    # Pallet weight ranges
                    elif key == "pallet_weight_kg_min":
                        query = query.filter(PalletData.pallet_weight_kg >= float(value))
                    elif key == "pallet_weight_kg_max":
                        query = query.filter(PalletData.pallet_weight_kg <= float(value))

                    # Pallet dimension ranges
                    elif key == "pallet_length_mm_min":
                        query = query.filter(PalletData.pallet_length_mm >= int(value))
                    elif key == "pallet_length_mm_max":
                        query = query.filter(PalletData.pallet_length_mm <= int(value))
                    elif key == "pallet_width_mm_min":
                        query = query.filter(PalletData.pallet_width_mm >= int(value))
                    elif key == "pallet_width_mm_max":
                        query = query.filter(PalletData.pallet_width_mm <= int(value))
                    elif key == "pallet_height_mm_min":
                        query = query.filter(PalletData.pallet_height_mm >= int(value))
                    elif key == "pallet_height_mm_max":
                        query = query.filter(PalletData.pallet_height_mm <= int(value))

                    # Pallet volume/area ranges (computed columns)
                    elif key == "pallet_vol_m3_min":
                        query = query.filter(PalletData.pallet_vol_m3 >= float(value))
                    elif key == "pallet_vol_m3_max":
                        query = query.filter(PalletData.pallet_vol_m3 <= float(value))
                    elif key == "pallet_area_m2_min":
                        query = query.filter(PalletData.pallet_area_m2 >= float(value))
                    elif key == "pallet_area_m2_max":
                        query = query.filter(PalletData.pallet_area_m2 <= float(value))

                    # Pallet stacking ranges
                    elif key == "pallet_stacking_min":
                        query = query.filter(PalletData.pallet_stacking >= int(value))
                    elif key == "pallet_stacking_max":
                        query = query.filter(PalletData.pallet_stacking <= int(value))

            # Execute query with all necessary eager loading
            parts = query.options(
                joinedload(PartData.supplier),
                selectinload(PartData.models).joinedload(PartToModel.model),
                selectinload(PartData.lines).joinedload(PartToLine.line).joinedload(LineData.workshop),
                selectinload(PartData.boxes).joinedload(PartToBox.box).selectinload(BoxData.pallets).joinedload(BoxToPallet.pallet)
            ).all()

            if not parts:
                return {
                    "success": True,
                    "found": False,
                    "message": "No parts found matching the criteria",
                    "data": []
                }

            # Build complete result with all information flattened and normalized
            result_data = []
            for part in parts:
                # Get part-supplier information
                supplier = part.supplier

                # For each part, there might be multiple models, lines, boxes
                # We need to create a row for each unique combination

                # If part has no models/lines/boxes, create at least one row
                if not part.models and not part.lines and not part.boxes:
                    row = self._create_result_row(part, supplier, None, None, None, None)
                    result_data.append(row)
                else:
                    # Iterate through models
                    model_combinations = part.models if part.models else [None]
                    line_combinations = part.lines if part.lines else [None]
                    box_combinations = part.boxes if part.boxes else [None]

                    for ptm in model_combinations:
                        for ptl in line_combinations:
                            for ptb in box_combinations:
                                row = self._create_result_row(
                                    part, supplier, ptm, ptl, ptb,
                                    ptb.box if ptb else None
                                )
                                result_data.append(row)

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

        # Basic part info with normalization
        row = {
            # Part information
            "PART_NUMBER": normalize_output("PART_NUMBER", part.part_number),
            "PART_NAME": normalize_output("PART_NAME", part.part_name),
            "PART_WEIGHT_KG": float(part.part_weight_kg) if part.part_weight_kg else None,

            # Model information (if available)
            "PART_PER_VEHICLE": ptm.part_per_vehicle if ptm else None,
            "CONFIGURATION": normalize_output(
                "CONFIGURATION", ptm.configuration if ptm else None
            ),
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

            # Pallet information (if available) - take first pallet if multiple
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

        # Add pallet information if box has pallets
        if box and box.pallets:
            # Get the first pallet relationship (or handle multiple appropriately)
            btp = box.pallets[0] if box.pallets else None
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

    # ============================================================================
    # EXPORT TO EXCEL USING POLARS
    # ============================================================================

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


# ============================================================================
# FLASK ENDPOINTS
# ============================================================================

# Create blueprint
display_api_bp = Blueprint('display_api', __name__, url_prefix='/api')


def get_db_api() -> Optional[DatabaseAPI]:
    """
    Get DatabaseAPI instance from Flask application context.
    
    Returns:
        DatabaseAPI instance or None if not initialized
        
    Raises:
        RuntimeError: If called outside of application context
    """
    try:
        if 'db_api' not in current_app.extensions:
            logger.error("DatabaseAPI not initialized in application context")
            return None

        return current_app.extensions['db_api']
    except RuntimeError as e:
        logger.error("Called outside of application context: %s", e)
        return None


def handle_api_response(func):
    """Decorator to handle API responses and errors."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)

            if isinstance(result, tuple):
                return result

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

            return jsonify(result)

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
            logger.error("Programming error in API request: %s", e)
            return jsonify({
                'error': 'Database programming error (invalid syntax or object)',
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

        except Exception as e:
            logger.error("Unexpected API error: %s", e, exc_info=True)
            return jsonify({
                'error': 'An unexpected error occurred',
                'success': False,
                'status': 'error'
            }), 500

    return wrapper


# ============================================================================
# UNIVERSAL SEARCH ENDPOINT
# ============================================================================

@display_api_bp.route('/search', methods=['GET', 'POST'])
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

    # Convert string numbers to appropriate types
    processed_filters = {}
    for key, value in filters.items():
        if value is None or value == "":
            continue

        # Try to convert numeric strings to appropriate types
        try:
            # Check if it's a float
            if isinstance(value, str) and '.' in value:
                processed_filters[key] = float(value)
            elif isinstance(value, str):
                # Try int first
                try:
                    processed_filters[key] = int(value)
                except ValueError:
                    # Keep as string
                    processed_filters[key] = value
            else:
                # Already a number or other type
                processed_filters[key] = value

        except (ValueError, TypeError) as e:
            logger.warning("Failed to convert filter value '%s' for key '%s': %s", value, key, e)
            # Keep as string
            processed_filters[key] = value

    logger.info("Universal search with filters: %s", processed_filters)

    return api.universal_search(processed_filters)


# ============================================================================
# EXPORT TO EXCEL ENDPOINT
# ============================================================================

@display_api_bp.route('/export', methods=['POST'])
@handle_api_response
def export_to_excel_endpoint():
    """
    Export search results to Excel file.
    
    POST /api/export with JSON body containing filters and optional export_path
    
    Request body example:
    {
        "filters": {
            "part_number": "999",
            "localization": "yes",
            "workshop_code": "as"
        },
        "export_path": "/path/to/save"  # optional
    }
    
    Returns:
        If export_path provided: JSON with file info
        If no export_path: Downloads the Excel file
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
        raise ValueError("Request body must be a JSON object")

    filters = data.get('filters', {})
    export_path = data.get('export_path')

    if not isinstance(filters, dict):
        raise ValueError("'filters' must be a JSON object")

    if export_path is not None and not isinstance(export_path, str):
        raise ValueError("'export_path' must be a string")

    logger.info("Export request with filters: %s, path: %s", filters, export_path)

    # Process filters (same as in search endpoint)
    processed_filters = {}
    for key, value in filters.items():
        if value is None or value == "":
            continue

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

    # Export to Excel
    result = api.export_to_excel(processed_filters, export_path)

    if not result.get('success'):
        return result

    # If export_path was provided, return file info
    if export_path:
        return jsonify({
            'success': True,
            'message': f'Successfully exported {result["row_count"]} rows',
            'file_path': result['file_path'],
            'filename': result['filename'],
            'row_count': result['row_count']
        })

    # Otherwise, download the file
    try:
        return send_file(
            result['file_path'],
            as_attachment=True,
            download_name=result['filename'],
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        logger.error("Error sending file: %s", e)

        # Clean up temp file if it exists
        if 'file_path' in result:
            try:
                os.unlink(result['file_path'])
                logger.debug("Temporary file %s cleaned up successfully", result['file_path'])

            except FileNotFoundError:
                # File already deleted, that's fine
                logger.debug("Temporary file %s already removed", result['file_path'])

            except PermissionError as e1:
                logger.warning("Permission denied when cleaning up %s: %s", result['file_path'], e1)

            except OSError as e2:
                logger.warning("OS error when cleaning up %s: %s", result['file_path'], e2)
        raise

# ============================================================================
# SPECIALIZED ENDPOINTS
# ============================================================================

@display_api_bp.route('/part/<path:part_number>', methods=['GET'])
@handle_api_response
def get_part_by_number_endpoint(part_number):
    """
    GET /api/part/{part_number}
    Shortcut for searching by part number (case-insensitive).
    """
    api = get_db_api()
    if not api:
        return jsonify({
            'error': 'Database connection not available',
            'success': False,
            'status': 'service_unavailable'
        }), 503

    if not part_number or not isinstance(part_number, str):
        raise ValueError("Part number must be a non-empty string")

    filters = {'part_number': part_number}
    return api.universal_search(filters)


@display_api_bp.route('/line/<path:line_identifier>/parts', methods=['GET'])
@handle_api_response
def get_parts_by_line_endpoint(line_identifier):
    """
    GET /api/line/{line_identifier}/parts?localization=yes|no&weight_min=1&weight_max=3
    Shortcut for searching by line (case-insensitive) with optional ranges.
    """
    api = get_db_api()
    if not api:
        return jsonify({
            'error': 'Database connection not available',
            'success': False,
            'status': 'service_unavailable'
        }), 503

    if not line_identifier or not isinstance(line_identifier, str):
        raise ValueError("Line identifier must be a non-empty string")

    filters = {}

    # Try to interpret as line code or name (search both)
    filters['line_code'] = line_identifier
    filters['line_name'] = line_identifier

    # Add optional filters
    optional_filters = [
        'localization', 'part_weight_kg_min', 'part_weight_kg_max',
        'box_length_mm_min', 'box_length_mm_max', 'box_width_mm_min',
        'box_width_mm_max', 'box_height_mm_min', 'box_height_mm_max',
        'box_vol_m3_min', 'box_vol_m3_max', 'box_area_m2_min', 'box_area_m2_max'
    ]

    for opt in optional_filters:
        value = request.args.get(opt)
        if value:
            filters[opt] = value

    return api.universal_search(filters)


@display_api_bp.route('/workshop/<path:workshop_identifier>/parts', methods=['GET'])
@handle_api_response
def get_parts_by_workshop_endpoint(workshop_identifier):
    """
    GET /api/workshop/{workshop_identifier}/parts?localization=yes|no&weight_min=1&weight_max=3
    Shortcut for searching by workshop (case-insensitive) with optional ranges.
    """
    api = get_db_api()
    if not api:
        return jsonify({
            'error': 'Database connection not available',
            'success': False,
            'status': 'service_unavailable'
        }), 503

    if not workshop_identifier or not isinstance(workshop_identifier, str):
        raise ValueError("Workshop identifier must be a non-empty string")

    filters = {}

    # Try to interpret as workshop code or name (search both)
    filters['workshop_code'] = workshop_identifier
    filters['workshop_name'] = workshop_identifier

    # Add optional filters
    optional_filters = [
        'localization', 'part_weight_kg_min', 'part_weight_kg_max',
        'box_length_mm_min', 'box_length_mm_max', 'box_width_mm_min',
        'box_width_mm_max', 'box_height_mm_min', 'box_height_mm_max',
        'box_vol_m3_min', 'box_vol_m3_max', 'box_area_m2_min', 'box_area_m2_max'
    ]

    for opt in optional_filters:
        value = request.args.get(opt)
        if value:
            filters[opt] = value

    return api.universal_search(filters)


# ============================================================================
# ENDPOINTS FOR REFERENCE INFORMATION
# ============================================================================

@display_api_bp.route('/info/columns', methods=['GET'])
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


@display_api_bp.route('/health', methods=['GET'])
def health_check():
    """GET /api/health - Health check endpoint."""
    try:
        api = get_db_api()
        return jsonify({
            'status': 'healthy',
            'service': 'Display API',
            'database_connected': api is not None,
            'features': {
                'case_insensitive_search': True,
                'output_normalization': True,
                'range_queries': True,
                'excel_export': True
            }
        })
    except Exception as e:
        logger.error("Health check failed: %s", e)
        return jsonify({
            'status': 'unhealthy',
            'service': 'Display API',
            'error': str(e)
        }), 500


@display_api_bp.route('/', methods=['GET'])
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
        ],
        'shortcuts': {
            'GET /api/part/{part_number}': 'Search by part number',
            'GET /api/line/{line}/parts': 'Search by line (with optional ranges)',
            'GET /api/workshop/{workshop}/parts': 'Search by workshop (with optional ranges)'
        }
    })


# ============================================================================
# INITIALIZATION FUNCTION
# ============================================================================

def init_display_api(app):
    """
    Initialize Display API with Flask app.
    
    This function:
    1. Creates DatabaseAPI instance
    2. Stores it in app.extensions['db_api']
    3. Registers the blueprint
    
    Args:
        app: Flask application instance
    
    Returns:
        Flask app with registered blueprint
        
    Raises:
        TypeError: If app is not a Flask application instance
        ValueError: If app is None
    """
    if app is None:
        raise ValueError("Flask application instance cannot be None")

    if not hasattr(app, 'extensions'):
        raise TypeError("Invalid Flask application instance")

    # Initialize database connection
    try:
        engine = initialize_database(create_tables=False)

        if engine:
            # Create DatabaseAPI instance
            try:
                db_api = DatabaseAPI(engine)
                # Store in app.extensions (Flask's extension storage)
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

    except Exception as e:
        logger.error("Unexpected error during database initialization: %s", e, exc_info=True)
        app.extensions['db_api'] = None

    # Register blueprint
    try:
        app.register_blueprint(display_api_bp)

    except (ValueError, TypeError, RuntimeError) as e:
        logger.error("Failed to register blueprint: %s", e)
        raise

    # Log registered routes
    logger.info("Display API endpoints registered:")
    for rule in app.url_map.iter_rules():
        if rule.endpoint and rule.endpoint.startswith('display_api'):
            logger.info("  %s -> %s", rule, rule.endpoint)

    return app
