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
        super().__init__(engine)
        logger.info("MFTDisplayAPI initialized")

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
            # Start with ActivePartsFullView
            query = session.query(ActivePartsFullView)

            # Apply filters
            conditions = []

            for key, value in filters.items():
                if value is None or value == "":
                    continue

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
                        except (TypeError, ValueError):
                            pass

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
                        except (TypeError, ValueError):
                            pass

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
                        except (TypeError, ValueError):
                            pass
                elif key == "box_width_mm":
                    if isinstance(value, dict):
                        if 'min' in value:
                            conditions.append(ActivePartsFullView.box_width_mm >= int(value['min']))
                        if 'max' in value:
                            conditions.append(ActivePartsFullView.box_width_mm <= int(value['max']))
                    else:
                        try:
                            conditions.append(ActivePartsFullView.box_width_mm == int(value))
                        except (TypeError, ValueError):
                            pass
                elif key == "box_height_mm":
                    if isinstance(value, dict):
                        if 'min' in value:
                            conditions.append(ActivePartsFullView.box_height_mm >= int(value['min']))
                        if 'max' in value:
                            conditions.append(ActivePartsFullView.box_height_mm <= int(value['max']))
                    else:
                        try:
                            conditions.append(ActivePartsFullView.box_height_mm == int(value))
                        except (TypeError, ValueError):
                            pass
                elif key == "part_per_box":
                    if isinstance(value, dict):
                        if 'min' in value:
                            conditions.append(ActivePartsFullView.part_per_box >= int(value['min']))
                        if 'max' in value:
                            conditions.append(ActivePartsFullView.part_per_box <= int(value['max']))
                    else:
                        try:
                            conditions.append(ActivePartsFullView.part_per_box == int(value))
                        except (TypeError, ValueError):
                            pass

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
                        except (TypeError, ValueError):
                            pass
                elif key == "pallet_width_mm":
                    if isinstance(value, dict):
                        if 'min' in value:
                            conditions.append(ActivePartsFullView.pallet_width_mm >= int(value['min']))
                        if 'max' in value:
                            conditions.append(ActivePartsFullView.pallet_width_mm <= int(value['max']))
                    else:
                        try:
                            conditions.append(ActivePartsFullView.pallet_width_mm == int(value))
                        except (TypeError, ValueError):
                            pass
                elif key == "pallet_height_mm":
                    if isinstance(value, dict):
                        if 'min' in value:
                            conditions.append(ActivePartsFullView.pallet_height_mm >= int(value['min']))
                        if 'max' in value:
                            conditions.append(ActivePartsFullView.pallet_height_mm <= int(value['max']))
                    else:
                        try:
                            conditions.append(ActivePartsFullView.pallet_height_mm == int(value))
                        except (TypeError, ValueError):
                            pass
                elif key == "box_per_pallet":
                    if isinstance(value, dict):
                        if 'min' in value:
                            conditions.append(ActivePartsFullView.box_per_pallet >= int(value['min']))
                        if 'max' in value:
                            conditions.append(ActivePartsFullView.box_per_pallet <= int(value['max']))
                    else:
                        try:
                            conditions.append(ActivePartsFullView.box_per_pallet == int(value))
                        except (TypeError, ValueError):
                            pass

                # ===== BREAKPOINT =====
                elif key == "breakpoint_number":
                    conditions.append(ActivePartsFullView.latest_breakpoint_number.ilike(f"%{str_value}%"))
                elif key == "change_domain":
                    conditions.append(ActivePartsFullView.latest_change_domain.ilike(str_value))
                elif key == "change_nature":
                    conditions.append(ActivePartsFullView.latest_change_nature.ilike(str_value))

            # Apply all conditions with AND
            if conditions:
                query = query.filter(and_(*conditions))

            # Execute query
            results = query.all()

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

            return {
                "success": True,
                "found": True,
                "total_records": len(result_data),
                "applied_filters": filters,
                "data": result_data
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
        def query(session):
            history = session.query(PartHistoryView).filter(
                PartHistoryView.part_number == part_number
            ).order_by(PartHistoryView.version_number.desc()).all()

            if not history:
                return {
                    "success": False,
                    "error": f"Part {part_number} not found"
                }

            result = {
                "success": True,
                "part_number": self.normalize_output("PART_NUMBER", part_number),
                "total_versions": len(history),
                "history": [
                    {
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
                    for h in history
                ]
            }
            return result

        return self._safe_query(query)

    def get_active_part_details(self, part_number: str) -> Dict[str, Any]:
        """
        Get details for a specific active part.
        
        Args:
            part_number: Part number to get details for
            
        Returns:
            Dictionary with part details
        """
        def query(session):
            part = session.query(ActivePartsFullView).filter(
                ActivePartsFullView.part_number == part_number,
                ActivePartsFullView.is_active == True
            ).first()

            if not part:
                return {
                    "success": False,
                    "error": f"Active part {part_number} not found"
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

        return self._safe_query(query)


# ========== FLASK ENDPOINTS ==========

def get_mft_api():
    """Get MFTDisplayAPI instance."""
    try:
        engine = initialize_database(create_tables=False)
        if engine:
            return MFTDisplayAPI(engine)
        return None
    except Exception as e:
        logger.error("Error creating MFTDisplayAPI: %s", e)
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
                    return jsonify(result), status_code
                return jsonify(result)
            return result

        except Exception as e:
            logger.error("API error: %s", e, exc_info=True)
            return jsonify({
                'error': f'Internal server error: {str(e)}',
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
    api = get_mft_api()
    if not api:
        return jsonify({
            'error': 'Database connection not available',
            'success': False,
            'status': 'service_unavailable'
        }), 503

    if request.method == 'POST':
        filters = request.get_json(silent=True) or {}
    else:
        filters = request.args.to_dict()

    # Process filters
    processed_filters = api.process_filters(filters)
    logger.info("Search with filters: %s", processed_filters)

    return api.universal_search(processed_filters)


@display_bp.route('/parts/<string:part_number>/history', methods=['GET'])
@rate_limit()
@handle_api_response
def get_part_history_endpoint(part_number):
    """Get full history for a specific part."""
    api = get_mft_api()
    if not api:
        return jsonify({
            'error': 'Database connection not available',
            'success': False,
            'status': 'service_unavailable'
        }), 503

    return api.get_part_history(part_number)


@display_bp.route('/parts/<string:part_number>/details', methods=['GET'])
@rate_limit()
@handle_api_response
def get_active_part_details_endpoint(part_number):
    """Get details for a specific active part."""
    api = get_mft_api()
    if not api:
        return jsonify({
            'error': 'Database connection not available',
            'success': False,
            'status': 'service_unavailable'
        }), 503

    return api.get_active_part_details(part_number)


@display_bp.route('/export', methods=['POST'])
@rate_limit()
@handle_api_response
def export_to_excel_endpoint():
    """Export search results to Excel."""
    api = get_mft_api()
    if not api:
        return jsonify({
            'error': 'Database connection not available',
            'success': False,
            'status': 'service_unavailable'
        }), 503

    data = request.get_json(silent=True) or {}
    filters = data.get('filters', {})

    if not filters:
        return jsonify({
            'error': 'No filters provided',
            'success': False,
            'status': 'bad_request'
        }), 400

    # Get search results
    processed_filters = api.process_filters(filters)
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
    result = api.export_to_excel(
        search_result['data'],
        processed_filters,
        prefix="mft_export"
    )

    if not result.get('success'):
        return result

    # Send file
    file_path = result['file_path']
    filename = result['filename']

    try:
        if not os.path.exists(file_path):
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
            except Exception as e:
                logger.warning("Failed to cleanup temp file: %s", e)

        return response

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


@display_bp.route('/info/columns', methods=['GET'])
def get_available_columns():
    """Get list of all available filter columns with range support."""
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


@display_bp.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    try:
        api = get_mft_api()
        db_status = 'disconnected'
        if api and api.check_connection():
            db_status = 'connected'

        return jsonify({
            'status': 'healthy',
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
    except Exception as e:
        logger.error("Health check failed: %s", e)
        return jsonify({
            'status': 'degraded',
            'service': 'MFT Display API',
            'error': str(e)
        }), 200


@display_bp.route('/', methods=['GET'])
def api_documentation():
    """API documentation."""
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


# ========== FLASK APP SETUP ==========

def create_app():
    """Create and configure the Flask application instance."""
    app = Flask(__name__)
    app.secret_key = FLASK_SECRET_KEY

    # CORS
    if ALLOWED_ORIGINS == "*":
        CORS(app)
    else:
        allowed_origins_list = [origin.strip() for origin in ALLOWED_ORIGINS.split(',')]
        CORS(app, origins=allowed_origins_list, supports_credentials=True)

    # Security headers
    @app.after_request
    def add_security_headers(response):
        if IS_PRODUCTION:
            response.headers.add('X-Content-Type-Options', 'nosniff')
            response.headers.add('X-Frame-Options', 'DENY')
            response.headers.add('X-XSS-Protection', '1; mode=block')
        return response

    # Register blueprint
    app.register_blueprint(display_bp)

    # Rate limiting
    limiter.init_app(app)

    # Database connection check
    try:
        engine = initialize_database(create_tables=False)
        if engine:
            app.extensions['db_engine'] = engine
            logger.info("Database engine initialized")
        else:
            logger.warning("Database engine is None")
    except Exception as e:
        logger.error("Database initialization error: %s", e)
    
    return app


flask_app = create_app()


if __name__ == '__main__':
    logger.info("=" * 60)
    logger.info("Starting MFT Display API on %s:%s", FLASK_HOST, FLASK_PORT)
    logger.info("Environment: %s", FLASK_ENV)
    logger.info("=" * 60)

    flask_app.run(
        host=FLASK_HOST,
        port=FLASK_PORT,
        debug=FLASK_DEBUG,
        threaded=True
    )
