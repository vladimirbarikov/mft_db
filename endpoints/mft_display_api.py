# pylint: disable=too-many-lines
# pylint: disable=wrong-import-position
"""
MFT Display API Module for Material Flow Table Database.

This module provides endpoints for displaying active parts with filtering.
Uses BaseDisplayAPI for common functionality and database views for performance.

ENDPOINTS:
    GET/POST /api/search - Search active parts with filters
    POST /api/export - Export search results to Excel
    GET /api/info/columns - Get available filter columns
    GET /api/health - Health check
    GET /api/ - API documentation

Version: 1.0.0
Compatibility: Python 3.14.4+, Flask 6.0.2+
Maintainer: PLD Engineering Center
Created: 2026-03-02
Last Modified: 2026-08-18
License: MIT
Status: Production
"""

# Standard library imports
from pathlib import Path
import sys
import os
from datetime import datetime
from typing import Dict, Any, Optional
from functools import wraps

# Third-party imports
from flask import Blueprint, Flask, request, jsonify, send_file
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from sqlalchemy import and_
from sqlalchemy.exc import (
    SQLAlchemyError, IntegrityError, DataError, StatementError,
    OperationalError, ProgrammingError, InvalidRequestError
)

# The relative path to the root project directory
try:
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
except NameError:
    PROJECT_ROOT = Path("/opt/airflow")

# Add the project path to sys.path
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Local imports
from config import get_logger
from dags.tasks.connector import initialize_database
from endpoints.base_display_api import BaseDisplayAPI
from database.views import ActivePartsFullView, PartHistoryView

# Logger setup
logger = get_logger("endpoints.mft_display_api")

# ========== CONFIGURATION ==========

FLASK_SECRET_KEY = os.getenv('FLASK_SECRET_KEY')
if not FLASK_SECRET_KEY:
    raise RuntimeError("FLASK_SECRET_KEY must be set in .env file")

FLASK_HOST = os.getenv('FLASK_HOST', '0.0.0.0')
FLASK_PORT = int(os.getenv('MFT_DISPLAY_API_PORT', '5000'))
FLASK_DEBUG = os.getenv('FLASK_DEBUG', 'false').lower() == 'true'
FLASK_ENV = os.getenv('FLASK_ENV', 'development')
IS_PRODUCTION = FLASK_ENV == 'production'

ALLOWED_ORIGINS = os.getenv('ALLOWED_ORIGINS', '*')
RATE_LIMIT = os.getenv('RATE_LIMIT', '10 per minute')
RATE_LIMIT_STORAGE_URL = os.getenv('RATE_LIMIT_STORAGE_URL', 'memory://')

# ========== CREATING BLUEPRINT ==========
display_bp = Blueprint('display', __name__, url_prefix='/api')

# ========== RATE LIMITING SETUP ==========
limiter = Limiter(
    key_func=get_remote_address,
    storage_uri=RATE_LIMIT_STORAGE_URL,
    default_limits=["200 per day", "50 per hour"],
    strategy="fixed-window"
)


# ========== RATE LIMITING DECORATOR ==========
def rate_limit(limit_string: Optional[str] = None):
    """Decorator factory for applying rate limits to endpoints."""
    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            return limiter.limit(limit_string or RATE_LIMIT)(f)(*args, **kwargs)
        return wrapped
    return decorator


# ========== API CLASS ==========

class MFTDisplayAPI(BaseDisplayAPI):
    """
    API for active parts display using database views.
    
    Uses v_active_parts_full view for optimized queries.
    Shows only active versions (is_active = true).
    One row per part.
    """

    def __init__(self, engine):
        """Initialize with database engine."""
        try:
            super().__init__(engine)
            logger.info("MFTDisplayAPI initialized successfully")
        except ValueError as e:
            logger.error("ValueError initializing MFTDisplayAPI: %s", e)
            raise
        except TypeError as e:
            logger.error("TypeError initializing MFTDisplayAPI: %s", e)
            raise
        except AttributeError as e:
            logger.error("AttributeError initializing MFTDisplayAPI: %s", e)
            raise
        except Exception as unexpected_error:
            logger.error(
                "Unexpected error initializing MFTDisplayAPI: %s",
                unexpected_error,
                exc_info=True
            )
            raise RuntimeError(f"Unexpected error initializing MFTDisplayAPI: {unexpected_error}") from unexpected_error

    def universal_search(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Search active parts with filters.
        
        Uses v_active_parts_full view for optimized performance.
        Returns ONLY active parts (is_active = true).
        
        Args:
            filters: Dictionary with filter parameters
            
        Returns:
            Dictionary with search results
        """
        def query(session):
            try:
                # Start with ActivePartsFullView
                query = session.query(ActivePartsFullView)

                # Apply filters
                conditions = []

                for key, value in filters.items():
                    if value is None or value == "":
                        continue

                    try:
                        str_value = str(value).lower() if isinstance(value, str) else value

                        # ===== PART =====
                        if key == "part_number":
                            conditions.append(ActivePartsFullView.part_number.ilike(f"%{str_value}%"))
                        elif key == "part_name":
                            conditions.append(ActivePartsFullView.part_name.ilike(f"%{str_value}%"))
                        elif key == "part_weight_kg":
                            if isinstance(value, dict):
                                if 'min' in value:
                                    conditions.append(ActivePartsFullView.part_weight_kg >= float(value['min']))
                                if 'max' in value:
                                    conditions.append(ActivePartsFullView.part_weight_kg <= float(value['max']))
                            else:
                                try:
                                    conditions.append(ActivePartsFullView.part_weight_kg == float(value))
                                except (TypeError, ValueError) as conv_error:
                                    logger.debug(
                                        "Failed to convert part_weight_kg value '%s': %s",
                                        value, conv_error
                                    )
                                    continue

                        # ===== MODEL =====
                        elif key == "model_code":
                            conditions.append(ActivePartsFullView.model_code.ilike(str_value))
                        elif key == "model_name":
                            conditions.append(ActivePartsFullView.model_name.ilike(str_value))
                        elif key == "configuration":
                            conditions.append(ActivePartsFullView.configuration.ilike(str_value))
                        elif key == "part_per_vehicle":
                            if isinstance(value, dict):
                                if 'min' in value:
                                    conditions.append(ActivePartsFullView.part_per_vehicle >= int(value['min']))
                                if 'max' in value:
                                    conditions.append(ActivePartsFullView.part_per_vehicle <= int(value['max']))
                            else:
                                try:
                                    conditions.append(ActivePartsFullView.part_per_vehicle == int(value))
                                except (TypeError, ValueError) as conv_error:
                                    logger.debug(
                                        "Failed to convert part_per_vehicle value '%s': %s",
                                        value, conv_error
                                    )
                                    continue

                        # ===== SUPPLIER =====
                        elif key == "supplier_name":
                            conditions.append(ActivePartsFullView.supplier_name.ilike(f"%{str_value}%"))
                        elif key == "localization":
                            conditions.append(ActivePartsFullView.localization.ilike(str_value))
                        elif key == "city":
                            conditions.append(ActivePartsFullView.city.ilike(f"%{str_value}%"))
                        elif key == "street":
                            conditions.append(ActivePartsFullView.street.ilike(f"%{str_value}%"))

                        # ===== LINE & WORKSHOP =====
                        elif key == "line_code":
                            conditions.append(ActivePartsFullView.line_code.ilike(f"%{str_value}%"))
                        elif key == "line_name":
                            conditions.append(ActivePartsFullView.line_name.ilike(f"%{str_value}%"))
                        elif key == "workshop_code":
                            conditions.append(ActivePartsFullView.workshop_code.ilike(str_value))
                        elif key == "workshop_name":
                            conditions.append(ActivePartsFullView.workshop_name.ilike(str_value))

                        # ===== BOX =====
                        elif key == "box_type":
                            conditions.append(ActivePartsFullView.box_type.ilike(str_value))
                        elif key == "box_length_mm":
                            if isinstance(value, dict):
                                if 'min' in value:
                                    conditions.append(ActivePartsFullView.box_length_mm >= int(value['min']))
                                if 'max' in value:
                                    conditions.append(ActivePartsFullView.box_length_mm <= int(value['max']))
                            else:
                                try:
                                    conditions.append(ActivePartsFullView.box_length_mm == int(value))
                                except (TypeError, ValueError) as conv_error:
                                    logger.debug(
                                        "Failed to convert box_length_mm value '%s': %s",
                                        value, conv_error
                                    )
                                    continue
                        elif key == "box_width_mm":
                            if isinstance(value, dict):
                                if 'min' in value:
                                    conditions.append(ActivePartsFullView.box_width_mm >= int(value['min']))
                                if 'max' in value:
                                    conditions.append(ActivePartsFullView.box_width_mm <= int(value['max']))
                            else:
                                try:
                                    conditions.append(ActivePartsFullView.box_width_mm == int(value))
                                except (TypeError, ValueError) as conv_error:
                                    logger.debug(
                                        "Failed to convert box_width_mm value '%s': %s",
                                        value, conv_error
                                    )
                                    continue
                        elif key == "box_height_mm":
                            if isinstance(value, dict):
                                if 'min' in value:
                                    conditions.append(ActivePartsFullView.box_height_mm >= int(value['min']))
                                if 'max' in value:
                                    conditions.append(ActivePartsFullView.box_height_mm <= int(value['max']))
                            else:
                                try:
                                    conditions.append(ActivePartsFullView.box_height_mm == int(value))
                                except (TypeError, ValueError) as conv_error:
                                    logger.debug(
                                        "Failed to convert box_height_mm value '%s': %s",
                                        value, conv_error
                                    )
                                    continue
                        elif key == "part_per_box":
                            if isinstance(value, dict):
                                if 'min' in value:
                                    conditions.append(ActivePartsFullView.part_per_box >= int(value['min']))
                                if 'max' in value:
                                    conditions.append(ActivePartsFullView.part_per_box <= int(value['max']))
                            else:
                                try:
                                    conditions.append(ActivePartsFullView.part_per_box == int(value))
                                except (TypeError, ValueError) as conv_error:
                                    logger.debug(
                                        "Failed to convert part_per_box value '%s': %s",
                                        value, conv_error
                                    )
                                    continue

                        # ===== PALLET =====
                        elif key == "pallet_type":
                            conditions.append(ActivePartsFullView.pallet_type.ilike(str_value))
                        elif key == "pallet_length_mm":
                            if isinstance(value, dict):
                                if 'min' in value:
                                    conditions.append(ActivePartsFullView.pallet_length_mm >= int(value['min']))
                                if 'max' in value:
                                    conditions.append(ActivePartsFullView.pallet_length_mm <= int(value['max']))
                            else:
                                try:
                                    conditions.append(ActivePartsFullView.pallet_length_mm == int(value))
                                except (TypeError, ValueError) as conv_error:
                                    logger.debug(
                                        "Failed to convert pallet_length_mm value '%s': %s",
                                        value, conv_error
                                    )
                                    continue
                        elif key == "pallet_width_mm":
                            if isinstance(value, dict):
                                if 'min' in value:
                                    conditions.append(ActivePartsFullView.pallet_width_mm >= int(value['min']))
                                if 'max' in value:
                                    conditions.append(ActivePartsFullView.pallet_width_mm <= int(value['max']))
                            else:
                                try:
                                    conditions.append(ActivePartsFullView.pallet_width_mm == int(value))
                                except (TypeError, ValueError) as conv_error:
                                    logger.debug(
                                        "Failed to convert pallet_width_mm value '%s': %s",
                                        value, conv_error
                                    )
                                    continue
                        elif key == "pallet_height_mm":
                            if isinstance(value, dict):
                                if 'min' in value:
                                    conditions.append(ActivePartsFullView.pallet_height_mm >= int(value['min']))
                                if 'max' in value:
                                    conditions.append(ActivePartsFullView.pallet_height_mm <= int(value['max']))
                            else:
                                try:
                                    conditions.append(ActivePartsFullView.pallet_height_mm == int(value))
                                except (TypeError, ValueError) as conv_error:
                                    logger.debug(
                                        "Failed to convert pallet_height_mm value '%s': %s",
                                        value, conv_error
                                    )
                                    continue
                        elif key == "box_per_pallet":
                            if isinstance(value, dict):
                                if 'min' in value:
                                    conditions.append(ActivePartsFullView.box_per_pallet >= int(value['min']))
                                if 'max' in value:
                                    conditions.append(ActivePartsFullView.box_per_pallet <= int(value['max']))
                            else:
                                try:
                                    conditions.append(ActivePartsFullView.box_per_pallet == int(value))
                                except (TypeError, ValueError) as conv_error:
                                    logger.debug(
                                        "Failed to convert box_per_pallet value '%s': %s",
                                        value, conv_error
                                    )
                                    continue

                        # ===== BREAKPOINT =====
                        elif key == "breakpoint_number":
                            conditions.append(ActivePartsFullView.latest_breakpoint_number.ilike(f"%{str_value}%"))
                        elif key == "change_domain":
                            conditions.append(ActivePartsFullView.latest_change_domain.ilike(str_value))
                        elif key == "change_nature":
                            conditions.append(ActivePartsFullView.latest_change_nature.ilike(str_value))

                    except (ValueError, TypeError, AttributeError) as e:
                        logger.debug(
                            "Error processing filter key '%s' with value '%s': %s",
                            key, value, e
                        )
                        continue
                    except Exception as unexpected_error:
                        logger.warning(
                            "Unexpected error processing filter key '%s': %s",
                            key, unexpected_error
                        )
                        continue

                # Apply all conditions with AND
                if conditions:
                    try:
                        query = query.filter(and_(*conditions))
                    except (ValueError, TypeError, AttributeError) as e:
                        logger.error("Error applying filter conditions: %s", e)
                        return {
                            "success": False,
                            "error": f"Invalid filter conditions: {str(e)}",
                            "status": "filter_error"
                        }
                    except Exception as unexpected_error:
                        logger.error(
                            "Unexpected error applying filter conditions: %s",
                            unexpected_error,
                            exc_info=True
                        )
                        return {
                            "success": False,
                            "error": f"Unexpected error applying filters: {str(unexpected_error)}",
                            "status": "filter_error"
                        }

                # Execute query
                try:
                    results = query.all()
                except SQLAlchemyError as e:
                    logger.error("SQLAlchemy error executing query: %s", e)
                    return {
                        "success": False,
                        "error": f"Database query error: {str(e)}",
                        "status": "query_error"
                    }
                except Exception as unexpected_error:
                    logger.error(
                        "Unexpected error executing query: %s",
                        unexpected_error,
                        exc_info=True
                    )
                    return {
                        "success": False,
                        "error": f"Unexpected query error: {str(unexpected_error)}",
                        "status": "query_error"
                    }

                if not results:
                    return {
                        "success": True,
                        "found": False,
                        "message": "No parts found matching the criteria",
                        "data": []
                    }

                # Format results with normalization
                result_data = []
                for row in results:
                    try:
                        row_dict = {
                            "PART_NUMBER": self.normalize_output("PART_NUMBER", row.part_number),
                            "PART_NAME": self.normalize_output("PART_NAME", row.part_name),
                            "PART_WEIGHT_KG": float(row.part_weight_kg) if row.part_weight_kg else None,
                            "VERSION": row.version_number,
                            "IS_ACTIVE": row.is_active,

                            "SUPPLIER_NAME": self.normalize_output("SUPPLIER_NAME", row.supplier_name),
                            "LOCALIZATION": self.normalize_output("LOCALIZATION", row.localization),
                            "CITY": self.normalize_output("CITY", row.city),
                            "STREET": self.normalize_output("STREET", row.street),

                            "MODEL_CODE": self.normalize_output("MODEL_CODE", row.model_code),
                            "MODEL_NAME": self.normalize_output("MODEL_NAME", row.model_name),
                            "CONFIGURATION": self.normalize_output("CONFIGURATION", row.configuration),
                            "PART_PER_VEHICLE": row.part_per_vehicle,

                            "LINE_CODE": self.normalize_output("LINE_CODE", row.line_code),
                            "LINE_NAME": self.normalize_output("LINE_NAME", row.line_name),
                            "WORKSHOP_CODE": self.normalize_output("WORKSHOP_CODE", row.workshop_code),
                            "WORKSHOP_NAME": self.normalize_output("WORKSHOP_NAME", row.workshop_name),

                            "BOX_TYPE": self.normalize_output("BOX_TYPE", row.box_type),
                            "BOX_WEIGHT_KG": float(row.box_weight_kg) if row.box_weight_kg else None,
                            "BOX_LENGTH_MM": row.box_length_mm,
                            "BOX_WIDTH_MM": row.box_width_mm,
                            "BOX_HEIGHT_MM": row.box_height_mm,
                            "BOX_VOL_M3": float(row.box_vol_m3) if row.box_vol_m3 else None,
                            "BOX_AREA_M2": float(row.box_area_m2) if row.box_area_m2 else None,
                            "BOX_STACKING": row.box_stacking,
                            "PART_PER_BOX": row.part_per_box,

                            "PALLET_TYPE": self.normalize_output("PALLET_TYPE", row.pallet_type),
                            "PALLET_WEIGHT_KG": float(row.pallet_weight_kg) if row.pallet_weight_kg else None,
                            "PALLET_LENGTH_MM": row.pallet_length_mm,
                            "PALLET_WIDTH_MM": row.pallet_width_mm,
                            "PALLET_HEIGHT_MM": row.pallet_height_mm,
                            "PALLET_VOL_M3": float(row.pallet_vol_m3) if row.pallet_vol_m3 else None,
                            "PALLET_AREA_M2": float(row.pallet_area_m2) if row.pallet_area_m2 else None,
                            "PALLET_STACKING": row.pallet_stacking,
                            "BOX_PER_PALLET": row.box_per_pallet,

                            "BREAKPOINT_NUMBER": row.latest_breakpoint_number,
                            "BREAKPOINT_DATE": row.latest_breakpoint_date.isoformat() if row.latest_breakpoint_date else None,
                            "CHANGE_DOMAIN": row.latest_change_domain,
                            "CHANGE_NATURE": row.latest_change_nature,
                            "BREAKPOINT_STATUS": row.latest_breakpoint_status
                        }
                        result_data.append(row_dict)
                    except (ValueError, TypeError, AttributeError) as e:
                        logger.warning("Error formatting result row: %s", e)
                        continue
                    except Exception as unexpected_error:
                        logger.warning(
                            "Unexpected error formatting result row: %s",
                            unexpected_error
                        )
                        continue

                return {
                    "success": True,
                    "found": True,
                    "total_records": len(result_data),
                    "applied_filters": filters,
                    "data": result_data
                }

            except SQLAlchemyError as e:
                logger.error("SQLAlchemy error in universal_search: %s", e)
                return {
                    "success": False,
                    "error": f"Database error: {str(e)}",
                    "status": "database_error"
                }
            except (ValueError, TypeError, AttributeError) as e:
                logger.error("Data error in universal_search: %s", e)
                return {
                    "success": False,
                    "error": f"Data error: {str(e)}",
                    "status": "data_error"
                }
            except Exception as unexpected_error:
                logger.error(
                    "Unexpected error in universal_search: %s",
                    unexpected_error,
                    exc_info=True
                )
                return {
                    "success": False,
                    "error": f"Unexpected error: {str(unexpected_error)}",
                    "status": "unexpected_error"
                }

        return self._safe_query(query)

    def get_part_history(self, part_number: str) -> Dict[str, Any]:
        """
        Get full history for a specific part.
        
        Uses v_part_history view for optimized queries.
        
        Args:
            part_number: Part number to get history for
            
        Returns:
            Dictionary with part history
        """
        if not part_number or not part_number.strip():
            logger.warning("get_part_history called with empty part_number")
            return {
                "success": False,
                "error": "Part number cannot be empty",
                "status": "invalid_parameter"
            }

        def query(session):
            try:
                history = session.query(PartHistoryView).filter(
                    PartHistoryView.part_number == part_number
                ).order_by(PartHistoryView.version_number.desc()).all()

                if not history:
                    return {
                        "success": False,
                        "error": f"Part {part_number} not found",
                        "status": "not_found"
                    }

                result = {
                    "success": True,
                    "part_number": self.normalize_output("PART_NUMBER", part_number),
                    "total_versions": len(history),
                    "history": []
                }

                for h in history:
                    try:
                        entry = {
                            "version": h.version_number,
                            "part_name": self.normalize_output("PART_NAME", h.part_name),
                            "supplier_name": self.normalize_output("SUPPLIER_NAME", h.supplier_name),
                            "configuration": self.normalize_output("CONFIGURATION", h.configuration),
                            "is_active": h.is_active,
                            "breakpoint_number": h.breakpoint_number,
                            "breakpoint_date": h.breakpoint_date.isoformat() if h.breakpoint_date else None,
                            "change_source": h.change_source,
                            "change_domain": h.change_domain,
                            "change_nature": h.change_nature,
                            "change_action_type": h.change_action_type,
                            "created_at": h.created_at.isoformat() if h.created_at else None,
                        }
                        result["history"].append(entry)
                    except (ValueError, TypeError, AttributeError) as e:
                        logger.warning(
                            "Error formatting history entry for part %s version %s: %s",
                            part_number, h.version_number, e
                        )
                        continue
                    except Exception as unexpected_error:
                        logger.warning(
                            "Unexpected error formatting history entry for part %s version %s: %s",
                            part_number, h.version_number, unexpected_error
                        )
                        continue

                return result

            except SQLAlchemyError as e:
                logger.error("SQLAlchemy error getting part history for %s: %s", part_number, e)
                return {
                    "success": False,
                    "error": f"Database error: {str(e)}",
                    "status": "database_error"
                }
            except (ValueError, TypeError, AttributeError) as e:
                logger.error("Data error getting part history for %s: %s", part_number, e)
                return {
                    "success": False,
                    "error": f"Data error: {str(e)}",
                    "status": "data_error"
                }
            except Exception as unexpected_error:
                logger.error(
                    "Unexpected error getting part history for %s: %s",
                    part_number,
                    unexpected_error,
                    exc_info=True
                )
                return {
                    "success": False,
                    "error": f"Unexpected error: {str(unexpected_error)}",
                    "status": "unexpected_error"
                }

        return self._safe_query(query)

    def get_active_part_details(self, part_number: str) -> Dict[str, Any]:
        """
        Get details for a specific active part.
        
        Args:
            part_number: Part number to get details for
            
        Returns:
            Dictionary with part details
        """
        if not part_number or not part_number.strip():
            logger.warning("get_active_part_details called with empty part_number")
            return {
                "success": False,
                "error": "Part number cannot be empty",
                "status": "invalid_parameter"
            }

        def query(session):
            try:
                part = session.query(ActivePartsFullView).filter(
                    ActivePartsFullView.part_number == part_number,
                    ActivePartsFullView.is_active.is_(True)
                ).first()

                if not part:
                    return {
                        "success": False,
                        "error": f"Active part {part_number} not found",
                        "status": "not_found"
                    }

                return {
                    "success": True,
                    "data": {
                        "part_number": self.normalize_output("PART_NUMBER", part.part_number),
                        "part_name": self.normalize_output("PART_NAME", part.part_name),
                        "part_weight_kg": float(part.part_weight_kg) if part.part_weight_kg else None,
                        "version": part.version_number,
                        "supplier_name": self.normalize_output("SUPPLIER_NAME", part.supplier_name),
                        "model_code": self.normalize_output("MODEL_CODE", part.model_code),
                        "model_name": self.normalize_output("MODEL_NAME", part.model_name),
                        "configuration": self.normalize_output("CONFIGURATION", part.configuration),
                        "line_code": self.normalize_output("LINE_CODE", part.line_code),
                        "workshop_code": self.normalize_output("WORKSHOP_CODE", part.workshop_code),
                        "box_type": self.normalize_output("BOX_TYPE", part.box_type),
                        "pallet_type": self.normalize_output("PALLET_TYPE", part.pallet_type),
                        "breakpoint_number": part.latest_breakpoint_number,
                        "change_domain": part.latest_change_domain,
                        "change_nature": part.latest_change_nature,
                    }
                }

            except SQLAlchemyError as e:
                logger.error("SQLAlchemy error getting active part details for %s: %s", part_number, e)
                return {
                    "success": False,
                    "error": f"Database error: {str(e)}",
                    "status": "database_error"
                }
            except (ValueError, TypeError, AttributeError) as e:
                logger.error("Data error getting active part details for %s: %s", part_number, e)
                return {
                    "success": False,
                    "error": f"Data error: {str(e)}",
                    "status": "data_error"
                }
            except Exception as unexpected_error:
                logger.error(
                    "Unexpected error getting active part details for %s: %s",
                    part_number,
                    unexpected_error,
                    exc_info=True
                )
                return {
                    "success": False,
                    "error": f"Unexpected error: {str(unexpected_error)}",
                    "status": "unexpected_error"
                }

        return self._safe_query(query)


# ========== FLASK ENDPOINTS ==========

def get_mft_api():
    """Get MFTDisplayAPI instance."""
    try:
        engine = initialize_database(create_tables=False)
        if engine:
            try:
                return MFTDisplayAPI(engine)
            except (ValueError, TypeError, AttributeError) as e:
                logger.error("Error creating MFTDisplayAPI instance: %s", e)
                return None
            except Exception as unexpected_error:
                logger.error(
                    "Unexpected error creating MFTDisplayAPI instance: %s",
                    unexpected_error,
                    exc_info=True
                )
                return None
        logger.warning("Database engine is None")
        return None
    except SQLAlchemyError as e:
        logger.error("SQLAlchemy error initializing database: %s", e)
        return None
    except (ValueError, TypeError, AttributeError) as e:
        logger.error("Error initializing database: %s", e)
        return None
    except Exception as unexpected_error:
        logger.error(
            "Unexpected error initializing database: %s",
            unexpected_error,
            exc_info=True
        )
        return None


def handle_api_response(f):
    """Decorator to handle API responses and errors."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            result = f(*args, **kwargs)

            if isinstance(result, tuple) or hasattr(result, 'get_data'):
                return result

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
                    elif result.get('status') == 'invalid_parameter':
                        status_code = 400
                    return jsonify(result), status_code
                return jsonify(result)
            return result

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
                'error': 'Database operation failed',
                'success': False,
                'status': 'operational_error'
            }), 503

        except ProgrammingError as e:
            error_msg = str(e.orig) if e.orig else str(e)
            if 'does not exist' in error_msg.lower() or (
                'relation' in error_msg.lower() and 'does not exist' in error_msg.lower()
            ):
                logger.info("Database tables not yet created")
                return jsonify({
                    'success': True,
                    'found': False,
                    'message': 'No data available – database tables are not created yet.',
                    'data': []
                }), 200
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

        except (ValueError, TypeError) as e:
            logger.warning("Validation error in API request: %s", e)
            return jsonify({
                'error': f'Invalid request data: {str(e)}',
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

        except KeyError as e:
            logger.warning("Missing required key in request: %s", e)
            return jsonify({
                'error': f'Missing required parameter: {str(e)}',
                'success': False,
                'status': 'bad_request'
            }), 400

        except RuntimeError as e:
            logger.error("Runtime error in API request: %s", e)
            return jsonify({
                'error': 'Application context error',
                'success': False,
                'status': 'error'
            }), 500

        except Exception as unexpected_error:
            logger.error(
                "Unexpected API error: %s",
                unexpected_error,
                exc_info=True
            )
            return jsonify({
                'error': 'An unexpected error occurred',
                'success': False,
                'status': 'error'
            }), 500

    return wrapper


# ========== ENDPOINTS ==========

@display_bp.route('/search', methods=['GET', 'POST'])
@rate_limit()
@handle_api_response
def universal_search_endpoint():
    """Search active parts with filters."""
    try:
        api = get_mft_api()
        if not api:
            return jsonify({
                'error': 'Database connection not available',
                'success': False,
                'status': 'service_unavailable'
            }), 503

        if request.method == 'POST':
            try:
                filters = request.get_json(silent=True) or {}
            except (ValueError, TypeError) as e:
                logger.warning("Invalid JSON in request: %s", e)
                return jsonify({
                    'error': 'Invalid JSON body',
                    'success': False,
                    'status': 'bad_request'
                }), 400
        else:
            filters = request.args.to_dict()

        # Process filters
        try:
            processed_filters = api.process_filters(filters)
        except (ValueError, TypeError, AttributeError) as e:
            logger.warning("Error processing filters: %s", e)
            return jsonify({
                'error': f'Invalid filter parameters: {str(e)}',
                'success': False,
                'status': 'bad_request'
            }), 400
        except Exception as unexpected_error:
            logger.error("Unexpected error processing filters: %s", unexpected_error, exc_info=True)
            return jsonify({
                'error': 'Error processing filters',
                'success': False,
                'status': 'internal_error'
            }), 500

        logger.info("Search with filters: %s", processed_filters)
        return api.universal_search(processed_filters)

    except SQLAlchemyError as e:
        logger.error("SQLAlchemy error in search endpoint: %s", e)
        return jsonify({
            'error': 'Database error',
            'success': False,
            'status': 'database_error'
        }), 500
    except Exception as unexpected_error:
        logger.error("Unexpected error in search endpoint: %s", unexpected_error, exc_info=True)
        return jsonify({
            'error': 'Internal server error',
            'success': False,
            'status': 'error'
        }), 500


@display_bp.route('/parts/<string:part_number>/history', methods=['GET'])
@rate_limit()
@handle_api_response
def get_part_history_endpoint(part_number):
    """Get full history for a specific part."""
    try:
        api = get_mft_api()
        if not api:
            return jsonify({
                'error': 'Database connection not available',
                'success': False,
                'status': 'service_unavailable'
            }), 503

        if not part_number or not part_number.strip():
            return jsonify({
                'error': 'Part number cannot be empty',
                'success': False,
                'status': 'bad_request'
            }), 400

        return api.get_part_history(part_number)

    except SQLAlchemyError as e:
        logger.error("SQLAlchemy error in part history endpoint: %s", e)
        return jsonify({
            'error': 'Database error',
            'success': False,
            'status': 'database_error'
        }), 500
    except Exception as unexpected_error:
        logger.error("Unexpected error in part history endpoint: %s", unexpected_error, exc_info=True)
        return jsonify({
            'error': 'Internal server error',
            'success': False,
            'status': 'error'
        }), 500


@display_bp.route('/parts/<string:part_number>/details', methods=['GET'])
@rate_limit()
@handle_api_response
def get_active_part_details_endpoint(part_number):
    """Get details for a specific active part."""
    try:
        api = get_mft_api()
        if not api:
            return jsonify({
                'error': 'Database connection not available',
                'success': False,
                'status': 'service_unavailable'
            }), 503

        if not part_number or not part_number.strip():
            return jsonify({
                'error': 'Part number cannot be empty',
                'success': False,
                'status': 'bad_request'
            }), 400

        return api.get_active_part_details(part_number)

    except SQLAlchemyError as e:
        logger.error("SQLAlchemy error in part details endpoint: %s", e)
        return jsonify({
            'error': 'Database error',
            'success': False,
            'status': 'database_error'
        }), 500
    except Exception as unexpected_error:
        logger.error("Unexpected error in part details endpoint: %s", unexpected_error, exc_info=True)
        return jsonify({
            'error': 'Internal server error',
            'success': False,
            'status': 'error'
        }), 500


@display_bp.route('/export', methods=['POST'])
@rate_limit()
@handle_api_response
def export_to_excel_endpoint():
    """Export search results to Excel."""
    try:
        api = get_mft_api()
        if not api:
            return jsonify({
                'error': 'Database connection not available',
                'success': False,
                'status': 'service_unavailable'
            }), 503

        try:
            data = request.get_json(silent=True) or {}
        except (ValueError, TypeError) as e:
            logger.warning("Invalid JSON in export request: %s", e)
            return jsonify({
                'error': 'Invalid JSON body',
                'success': False,
                'status': 'bad_request'
            }), 400

        filters = data.get('filters', {})

        if not filters:
            return jsonify({
                'error': 'No filters provided',
                'success': False,
                'status': 'bad_request'
            }), 400

        # Process filters
        try:
            processed_filters = api.process_filters(filters)
        except (ValueError, TypeError, AttributeError) as e:
            logger.warning("Error processing filters in export: %s", e)
            return jsonify({
                'error': f'Invalid filter parameters: {str(e)}',
                'success': False,
                'status': 'bad_request'
            }), 400

        # Get search results
        search_result = api.universal_search(processed_filters)

        if not search_result.get('success'):
            return search_result

        if not search_result.get('found'):
            return {
                "success": False,
                "error": "No data to export",
                "status": "no_data"
            }

        # Export to Excel
        try:
            result = api.export_to_excel(
                search_result['data'],
                processed_filters,
                prefix="mft_export"
            )
        except (ValueError, TypeError, AttributeError) as e:
            logger.error("Error in export_to_excel: %s", e)
            return {
                "success": False,
                "error": f"Export error: {str(e)}",
                "status": "export_error"
            }
        except Exception as unexpected_error:
            logger.error("Unexpected error in export_to_excel: %s", unexpected_error, exc_info=True)
            return {
                "success": False,
                "error": f"Export error: {str(unexpected_error)}",
                "status": "export_error"
            }

        if not result.get('success'):
            return result

        # Send file
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

            response = send_file(
                file_path,
                as_attachment=True,
                download_name=filename,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )

            @response.call_on_close
            def cleanup():
                try:
                    if os.path.exists(file_path):
                        os.unlink(file_path)
                        logger.debug("Temporary file %s cleaned up", file_path)
                except OSError as cleanup_error:
                    logger.warning("OS error cleaning up temp file %s: %s", file_path, cleanup_error)
                except Exception as cleanup_error:
                    logger.warning("Failed to cleanup temp file %s: %s", file_path, cleanup_error)

            return response

        except PermissionError as e:
            logger.error("Permission error sending file: %s", e)
            try:
                if os.path.exists(file_path):
                    os.unlink(file_path)
            except Exception:
                pass
            return {
                'success': False,
                'error': f'Permission denied: {str(e)}',
                'status': 'export_error'
            }

        except OSError as e:
            logger.error("OS error sending file: %s", e)
            try:
                if os.path.exists(file_path):
                    os.unlink(file_path)
            except Exception:
                pass
            return {
                'success': False,
                'error': f'File system error: {str(e)}',
                'status': 'export_error'
            }

        except Exception as e:
            logger.error("Error sending file: %s", e, exc_info=True)
            try:
                if os.path.exists(file_path):
                    os.unlink(file_path)
            except Exception:
                pass
            return {
                'success': False,
                'error': f'Failed to send file: {str(e)}',
                'status': 'export_error'
            }

    except SQLAlchemyError as e:
        logger.error("SQLAlchemy error in export endpoint: %s", e)
        return jsonify({
            'error': 'Database error',
            'success': False,
            'status': 'database_error'
        }), 500
    except Exception as unexpected_error:
        logger.error("Unexpected error in export endpoint: %s", unexpected_error, exc_info=True)
        return jsonify({
            'error': 'Internal server error',
            'success': False,
            'status': 'error'
        }), 500


# ========== HEALTH AND DOCUMENTATION ENDPOINTS ==========

@display_bp.route('/info/columns', methods=['GET'])
def get_available_columns():
    """Get list of all available filter columns with range support."""
    try:
        return jsonify({
            'success': True,
            'columns': {
                'part': {
                    'exact': ['part_number', 'part_name'],
                    'range': ['part_weight_kg']
                },
                'supplier': {
                    'exact': ['supplier_name', 'localization', 'city', 'street'],
                    'range': []
                },
                'model': {
                    'exact': ['model_code', 'model_name', 'configuration'],
                    'range': ['part_per_vehicle']
                },
                'line': {
                    'exact': ['line_code', 'line_name'],
                    'range': []
                },
                'workshop': {
                    'exact': ['workshop_code', 'workshop_name'],
                    'range': []
                },
                'box': {
                    'exact': ['box_type', 'part_per_box'],
                    'range': [
                        'box_weight_kg', 'box_length_mm', 'box_width_mm',
                        'box_height_mm', 'box_vol_m3', 'box_area_m2', 'box_stacking'
                    ]
                },
                'pallet': {
                    'exact': ['pallet_type', 'box_per_pallet'],
                    'range': [
                        'pallet_weight_kg', 'pallet_length_mm', 'pallet_width_mm',
                        'pallet_height_mm', 'pallet_vol_m3', 'pallet_area_m2', 'pallet_stacking'
                    ]
                },
                'breakpoint': {
                    'exact': ['breakpoint_number', 'change_domain', 'change_nature'],
                    'range': []
                }
            },
            'filter_syntax': {
                'exact': 'Use column name directly: ?part_number=999',
                'range': 'Use _min and _max suffixes: ?part_weight_kg_min=1&part_weight_kg_max=3',
                'text_search': 'Case-insensitive partial matching for text fields'
            },
            'output_columns': [
                'PART_NUMBER', 'PART_NAME', 'PART_WEIGHT_KG', 'VERSION', 'IS_ACTIVE',
                'SUPPLIER_NAME', 'LOCALIZATION', 'CITY', 'STREET',
                'MODEL_CODE', 'MODEL_NAME', 'CONFIGURATION', 'PART_PER_VEHICLE',
                'LINE_CODE', 'LINE_NAME', 'WORKSHOP_CODE', 'WORKSHOP_NAME',
                'BOX_TYPE', 'BOX_WEIGHT_KG', 'BOX_LENGTH_MM', 'BOX_WIDTH_MM',
                'BOX_HEIGHT_MM', 'BOX_VOL_M3', 'BOX_AREA_M2', 'BOX_STACKING', 'PART_PER_BOX',
                'PALLET_TYPE', 'PALLET_WEIGHT_KG', 'PALLET_LENGTH_MM', 'PALLET_WIDTH_MM',
                'PALLET_HEIGHT_MM', 'PALLET_VOL_M3', 'PALLET_AREA_M2', 'PALLET_STACKING', 'BOX_PER_PALLET',
                'BREAKPOINT_NUMBER', 'BREAKPOINT_DATE', 'CHANGE_DOMAIN', 'CHANGE_NATURE', 'BREAKPOINT_STATUS'
            ],
            'normalization_rules': {
                'uppercase': ['PART_NUMBER', 'CONFIGURATION', 'MODEL_CODE', 'LINE_CODE', 'WORKSHOP_CODE', 'BUILDING'],
                'sentence_case': ['PART_NAME', 'MODEL_NAME', 'LINE_NAME', 'WORKSHOP_NAME', 'BOX_TYPE', 'PALLET_TYPE', 'SUPPLIER_NAME', 'LOCALIZATION'],
                'title_case': ['LOCATION', 'CITY', 'STREET']
            }
        })
    except Exception as unexpected_error:
        logger.error("Unexpected error in get_available_columns: %s", unexpected_error, exc_info=True)
        return jsonify({
            'success': False,
            'error': 'Internal server error',
            'status': 'error'
        }), 500


@display_bp.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    try:
        api = get_mft_api()
        db_status = 'disconnected'
        if api:
            try:
                if api.check_connection():
                    db_status = 'connected'
                else:
                    db_status = 'connection_failed'
            except SQLAlchemyError as e:
                logger.warning("Database connection check error: %s", e)
                db_status = 'connection_error'
            except Exception as e:
                logger.warning("Unexpected error in connection check: %s", e)
                db_status = 'connection_error'

        return jsonify({
            'status': 'healthy' if db_status == 'connected' else 'degraded',
            'service': 'MFT Display API',
            'timestamp': datetime.now().isoformat(),
            'environment': FLASK_ENV,
            'database_status': db_status,
            'features': {
                'case_insensitive_search': True,
                'output_normalization': True,
                'range_queries': True,
                'excel_export': True
            }
        }), 200
    except Exception as unexpected_error:
        logger.error("Health check failed: %s", unexpected_error, exc_info=True)
        return jsonify({
            'status': 'degraded',
            'service': 'MFT Display API',
            'error': str(unexpected_error)
        }), 200


@display_bp.route('/', methods=['GET'])
def api_documentation():
    """API documentation."""
    try:
        return jsonify({
            'name': 'MFT Display API',
            'version': '1.0.0',
            'description': 'Universal search API for material flow database',
            'endpoints': {
                '/api/search': {
                    'methods': ['GET', 'POST'],
                    'description': 'Search active parts with filters'
                },
                '/api/parts/{part_number}/history': {
                    'methods': ['GET'],
                    'description': 'Get full history for a specific part'
                },
                '/api/parts/{part_number}/details': {
                    'methods': ['GET'],
                    'description': 'Get details for a specific active part'
                },
                '/api/export': {
                    'methods': ['POST'],
                    'description': 'Export search results to Excel'
                },
                '/api/info/columns': {
                    'methods': ['GET'],
                    'description': 'Get available filter columns'
                },
                '/api/health': {
                    'methods': ['GET'],
                    'description': 'Health check'
                }
            }
        })
    except Exception as unexpected_error:
        logger.error("Unexpected error in API documentation: %s", unexpected_error, exc_info=True)
        return jsonify({
            'success': False,
            'error': 'Internal server error',
            'status': 'error'
        }), 500


# ========== FLASK APP SETUP ==========

def create_app():
    """Create and configure the Flask application instance."""
    app = Flask(__name__)
    app.secret_key = FLASK_SECRET_KEY

    # CORS
    try:
        if ALLOWED_ORIGINS == "*":
            CORS(app)
            logger.debug("CORS: Allowing all origins")
        else:
            allowed_origins_list = [origin.strip() for origin in ALLOWED_ORIGINS.split(',')]
            CORS(app, origins=allowed_origins_list, supports_credentials=True)
            logger.info("CORS: Restricted to %d origins", len(allowed_origins_list))
    except (ValueError, TypeError, AttributeError) as e:
        logger.error("Error configuring CORS: %s", e)
        CORS(app)  # Fallback to allow all

    # Security headers
    @app.after_request
    def add_security_headers(response):
        if IS_PRODUCTION:
            try:
                response.headers.add('X-Content-Type-Options', 'nosniff')
                response.headers.add('X-Frame-Options', 'DENY')
                response.headers.add('X-XSS-Protection', '1; mode=block')
            except (ValueError, TypeError, AttributeError) as e:
                logger.warning("Error adding security headers: %s", e)
        return response

    # Register blueprint
    app.register_blueprint(display_bp)

    # Rate limiting
    try:
        limiter.init_app(app)
    except (ValueError, TypeError, AttributeError) as e:
        logger.error("Error initializing rate limiter: %s", e)

    # Database connection check
    try:
        engine = initialize_database(create_tables=False)
        if engine:
            app.extensions['db_engine'] = engine
            logger.info("Database engine initialized")
        else:
            logger.warning("Database engine is None")
    except SQLAlchemyError as e:
        logger.error("SQLAlchemy error initializing database: %s", e)
    except (ValueError, TypeError, AttributeError) as e:
        logger.error("Error initializing database: %s", e)
    except Exception as unexpected_error:
        logger.error(
            "Unexpected error during database initialization: %s",
            unexpected_error,
            exc_info=True
        )

    return app


flask_app = create_app()


if __name__ == '__main__':
    logger.info("=" * 60)
    logger.info("Starting MFT Display API on %s:%s", FLASK_HOST, FLASK_PORT)
    logger.info("Environment: %s", FLASK_ENV)
    logger.info("=" * 60)

    try:
        flask_app.run(
            host=FLASK_HOST,
            port=FLASK_PORT,
            debug=FLASK_DEBUG,
            threaded=True
        )
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    except Exception as unexpected_error:
        logger.error("Failed to start application: %s", unexpected_error, exc_info=True)
        sys.exit(1)
