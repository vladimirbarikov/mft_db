# pylint: disable=too-many-lines
# pylint: disable=wrong-import-position
"""
Breakpoint Display API Module for Material Flow Table Database.

This module provides endpoints for displaying breakpoint information and history.
Uses BaseDisplayAPI for common functionality and database views for performance.

ENDPOINTS:
    GET /api/breakpoints/{breakpoint_number}/history - Get breakpoint history
    GET /api/breakpoints/search - Search breakpoints with filters
    GET /api/breakpoints/stats - Get breakpoint statistics
    GET /api/breakpoints/{breakpoint_number}/details - Get breakpoint details
    GET /api/health - Health check
    GET /api/ - API documentation

Version: 1.0.0
Compatibility: Python 3.14.4+, Flask 6.0.2+
Maintainer: PLD Engineering Center
Created: 2026-08-18
Last Modified: 2026-08-18
License: MIT
Status: Development
"""

# Standard library imports
from pathlib import Path
import sys
import os
from datetime import datetime
from typing import Dict, Any, Optional
from functools import wraps

# Third-party imports
from flask import Blueprint, Flask, request, jsonify
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from sqlalchemy import and_, desc
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
from database.views import (
    BreakpointDetailsView,
    PartsByBreakpointView,
    BreakpointSummaryView,
    BreakpointChangesSummaryView
)

# Logger setup
logger = get_logger("endpoints.bp_display_api")

# ========== CONFIGURATION ==========

FLASK_SECRET_KEY = os.getenv('FLASK_SECRET_KEY')
if not FLASK_SECRET_KEY:
    raise RuntimeError("FLASK_SECRET_KEY must be set in .env file")

FLASK_HOST = os.getenv('FLASK_HOST', '0.0.0.0')
FLASK_PORT = int(os.getenv('BP_DISPLAY_API_PORT', '5001'))
FLASK_DEBUG = os.getenv('FLASK_DEBUG', 'false').lower() == 'true'
FLASK_ENV = os.getenv('FLASK_ENV', 'development')
IS_PRODUCTION = FLASK_ENV == 'production'

ALLOWED_ORIGINS = os.getenv('ALLOWED_ORIGINS', '*')
RATE_LIMIT = os.getenv('RATE_LIMIT', '10 per minute')
RATE_LIMIT_STORAGE_URL = os.getenv('RATE_LIMIT_STORAGE_URL', 'memory://')

# ========== CREATING BLUEPRINT ==========
bp_bp = Blueprint('bp_display', __name__, url_prefix='/api/breakpoints')

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

class BPDisplayAPI(BaseDisplayAPI):
    """
    API for breakpoint information using database views.
    
    Uses v_breakpoint_details, v_parts_by_breakpoint, v_breakpoint_summary views
    for optimized queries.
    """

    def __init__(self, engine):
        """Initialize with database engine."""
        try:
            super().__init__(engine)
            logger.info("BPDisplayAPI initialized successfully")
        except ValueError as e:
            logger.error("ValueError initializing BPDisplayAPI: %s", e)
            raise
        except TypeError as e:
            logger.error("TypeError initializing BPDisplayAPI: %s", e)
            raise
        except AttributeError as e:
            logger.error("AttributeError initializing BPDisplayAPI: %s", e)
            raise
        except Exception as unexpected_error:
            logger.error(
                "Unexpected error initializing BPDisplayAPI: %s",
                unexpected_error,
                exc_info=True
            )
            raise RuntimeError(f"Unexpected error initializing BPDisplayAPI: {unexpected_error}") from unexpected_error

    def get_breakpoint_history(self, breakpoint_number: str) -> Dict[str, Any]:
        """
        Get full history for a specific breakpoint.
        
        Uses v_breakpoint_details and v_parts_by_breakpoint views.
        
        Args:
            breakpoint_number: Breakpoint number to get history for
            
        Returns:
            Dictionary with breakpoint history
        """
        if not breakpoint_number or not breakpoint_number.strip():
            logger.warning("get_breakpoint_history called with empty breakpoint_number")
            return {
                "success": False,
                "error": "Breakpoint number cannot be empty",
                "status": "invalid_parameter"
            }

        def query(session):
            try:
                # Get breakpoint details
                bp = session.query(BreakpointDetailsView).filter(
                    BreakpointDetailsView.breakpoint_number == breakpoint_number
                ).first()

                if not bp:
                    return {
                        "success": False,
                        "error": f"Breakpoint {breakpoint_number} not found",
                        "status": "not_found"
                    }

                # Get all parts affected by this breakpoint
                parts = session.query(PartsByBreakpointView).filter(
                    PartsByBreakpointView.breakpoint_number == breakpoint_number
                ).all()

                # Format result with normalization
                result = {
                    "success": True,
                    "breakpoint": {
                        "number": bp.breakpoint_number,
                        "date": bp.breakpoint_date.isoformat() if bp.breakpoint_date else None,
                        "status": bp.breakpoint_status,
                        "change_domain": bp.change_domain,
                        "change_nature": bp.change_nature,
                        "description": bp.description,
                        "solution": bp.solution,
                        "source": bp.source,
                        "batch_plan": bp.batch_plan,
                        "batch_fact": bp.batch_fact,
                        "input_date": bp.input_date.isoformat() if bp.input_date else None,
                    },
                    "summary": {
                        "new_parts_count": bp.new_parts_count,
                        "old_parts_count": bp.old_parts_count,
                        "models_affected_count": bp.models_affected_count,
                        "models_affected": bp.models_affected,
                        "add_count": bp.add_count,
                        "delete_count": bp.delete_count,
                        "update_count": bp.update_count,
                        "replace_count": bp.replace_count,
                    },
                    "parts_affected": [],
                    "total_parts_affected": len(parts)
                }

                # Format each part change
                for part in parts:
                    try:
                        change_entry = {
                            "action_type": part.action_type,
                            "model": {
                                "code": self.normalize_output("MODEL_CODE", part.model_code),
                                "name": self.normalize_output("MODEL_NAME", part.model_name),
                            } if part.model_code else None,
                        }

                        # Old part (if exists)
                        if part.old_part_id:
                            change_entry["old_part"] = {
                                "id": part.old_part_id,
                                "number": self.normalize_output("PART_NUMBER", part.old_part_number),
                                "name": self.normalize_output("PART_NAME", part.old_part_name),
                                "version": part.old_version_number,
                                "supplier": self.normalize_output("SUPPLIER_NAME", part.old_supplier_name),
                                "weight": float(part.old_part_weight_kg) if part.old_part_weight_kg else None,
                            }
                        else:
                            change_entry["old_part"] = None

                        # New part (if exists)
                        if part.new_part_id:
                            change_entry["new_part"] = {
                                "id": part.new_part_id,
                                "number": self.normalize_output("PART_NUMBER", part.new_part_number),
                                "name": self.normalize_output("PART_NAME", part.new_part_name),
                                "version": part.new_version_number,
                                "supplier": self.normalize_output("SUPPLIER_NAME", part.new_supplier_name),
                                "weight": float(part.new_part_weight_kg) if part.new_part_weight_kg else None,
                            }
                        else:
                            change_entry["new_part"] = None

                        result["parts_affected"].append(change_entry)

                    except (ValueError, TypeError, AttributeError) as e:
                        logger.warning(
                            "Error formatting part change for breakpoint %s: %s",
                            breakpoint_number, e
                        )
                        continue
                    except Exception as unexpected_error:
                        logger.warning(
                            "Unexpected error formatting part change for breakpoint %s: %s",
                            breakpoint_number, unexpected_error
                        )
                        continue

                return result

            except SQLAlchemyError as e:
                logger.error(
                    "SQLAlchemy error getting breakpoint history for %s: %s",
                    breakpoint_number, e
                )
                return {
                    "success": False,
                    "error": f"Database error: {str(e)}",
                    "status": "database_error"
                }
            except (ValueError, TypeError, AttributeError) as e:
                logger.error(
                    "Data error getting breakpoint history for %s: %s",
                    breakpoint_number, e
                )
                return {
                    "success": False,
                    "error": f"Data error: {str(e)}",
                    "status": "data_error"
                }
            except Exception as unexpected_error:
                logger.error(
                    "Unexpected error getting breakpoint history for %s: %s",
                    breakpoint_number,
                    unexpected_error,
                    exc_info=True
                )
                return {
                    "success": False,
                    "error": f"Unexpected error: {str(unexpected_error)}",
                    "status": "unexpected_error"
                }

        return self._safe_query(query)

    def search_breakpoints(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Search breakpoints with filters.
        
        Uses v_breakpoint_details view for optimized queries.
        
        Args:
            filters: Dictionary with filter parameters
            
        Returns:
            Dictionary with search results
        """
        def query(session):
            try:
                query = session.query(BreakpointDetailsView)

                conditions = []

                for key, value in filters.items():
                    if value is None or value == "":
                        continue

                    try:
                        str_value = str(value).lower() if isinstance(value, str) else value

                        if key == "breakpoint_number":
                            conditions.append(BreakpointDetailsView.breakpoint_number.ilike(f"%{str_value}%"))
                        elif key == "breakpoint_status":
                            conditions.append(BreakpointDetailsView.breakpoint_status.ilike(str_value))
                        elif key == "change_domain":
                            conditions.append(BreakpointDetailsView.change_domain.ilike(str_value))
                        elif key == "change_nature":
                            conditions.append(BreakpointDetailsView.change_nature.ilike(str_value))
                        elif key == "source":
                            conditions.append(BreakpointDetailsView.source.ilike(str_value))
                        elif key == "description":
                            conditions.append(BreakpointDetailsView.description.ilike(f"%{str_value}%"))
                        elif key == "date_from":
                            try:
                                conditions.append(BreakpointDetailsView.breakpoint_date >= datetime.fromisoformat(value))
                            except ValueError:
                                logger.debug("Invalid date format for date_from: %s", value)
                        elif key == "date_to":
                            try:
                                conditions.append(BreakpointDetailsView.breakpoint_date <= datetime.fromisoformat(value))
                            except ValueError:
                                logger.debug("Invalid date format for date_to: %s", value)

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

                # Order by date descending
                try:
                    results = query.order_by(
                        desc(BreakpointDetailsView.breakpoint_date)
                    ).all()
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
                        "message": "No breakpoints found matching the criteria",
                        "data": []
                    }

                # Format results
                result_data = []
                for row in results:
                    try:
                        entry = {
                            "breakpoint_number": row.breakpoint_number,
                            "breakpoint_date": row.breakpoint_date.isoformat() if row.breakpoint_date else None,
                            "breakpoint_status": row.breakpoint_status,
                            "change_domain": row.change_domain,
                            "change_nature": row.change_nature,
                            "source": row.source,
                            "description": row.description,
                            "new_parts_count": row.new_parts_count,
                            "old_parts_count": row.old_parts_count,
                            "models_affected_count": row.models_affected_count,
                            "models_affected": row.models_affected,
                            "add_count": row.add_count,
                            "delete_count": row.delete_count,
                            "update_count": row.update_count,
                            "replace_count": row.replace_count,
                            "total_parts_affected": (row.new_parts_count or 0) + (row.old_parts_count or 0)
                        }
                        result_data.append(entry)
                    except (ValueError, TypeError, AttributeError) as e:
                        logger.warning("Error formatting breakpoint entry: %s", e)
                        continue
                    except Exception as unexpected_error:
                        logger.warning(
                            "Unexpected error formatting breakpoint entry: %s",
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
                logger.error("SQLAlchemy error in search_breakpoints: %s", e)
                return {
                    "success": False,
                    "error": f"Database error: {str(e)}",
                    "status": "database_error"
                }
            except (ValueError, TypeError, AttributeError) as e:
                logger.error("Data error in search_breakpoints: %s", e)
                return {
                    "success": False,
                    "error": f"Data error: {str(e)}",
                    "status": "data_error"
                }
            except Exception as unexpected_error:
                logger.error(
                    "Unexpected error in search_breakpoints: %s",
                    unexpected_error,
                    exc_info=True
                )
                return {
                    "success": False,
                    "error": f"Unexpected error: {str(unexpected_error)}",
                    "status": "unexpected_error"
                }

        return self._safe_query(query)

    def get_breakpoint_statistics(self) -> Dict[str, Any]:
        """
        Get breakpoint statistics and summary.
        
        Uses v_breakpoint_summary and v_breakpoint_changes_summary views.
        
        Returns:
            Dictionary with breakpoint statistics
        """
        def query(session):
            try:
                # Get summary statistics
                summary = session.query(BreakpointSummaryView).all()

                # Get monthly statistics
                monthly = session.query(BreakpointChangesSummaryView).order_by(
                    desc(BreakpointChangesSummaryView.month)
                ).all()

                # Calculate totals
                total_breakpoints = len(summary)
                total_adds = sum(s.add_count or 0 for s in summary)
                total_deletes = sum(s.delete_count or 0 for s in summary)
                total_updates = sum(s.update_count or 0 for s in summary)
                total_replaces = sum(s.replace_count or 0 for s in summary)
                total_parts_affected = sum(
                    (s.new_parts_count or 0) + (s.old_parts_count or 0)
                    for s in summary
                )

                # Group by domain
                domain_stats = {}
                for s in summary:
                    domain = s.change_domain or 'unknown'
                    if domain not in domain_stats:
                        domain_stats[domain] = 0
                    domain_stats[domain] += 1

                # Group by nature
                nature_stats = {}
                for s in summary:
                    nature = s.change_nature or 'unknown'
                    if nature not in nature_stats:
                        nature_stats[nature] = 0
                    nature_stats[nature] += 1

                # Group by source
                source_stats = {}
                for s in summary:
                    source = s.source or 'unknown'
                    if source not in source_stats:
                        source_stats[source] = 0
                    source_stats[source] += 1

                return {
                    "success": True,
                    "statistics": {
                        "total_breakpoints": total_breakpoints,
                        "total_parts_affected": total_parts_affected,
                        "action_counts": {
                            "add": total_adds,
                            "delete": total_deletes,
                            "update": total_updates,
                            "replace": total_replaces,
                        },
                        "by_domain": domain_stats,
                        "by_nature": nature_stats,
                        "by_source": source_stats,
                    },
                    "monthly_trend": [
                        {
                            "month": m.month.isoformat() if m.month else None,
                            "breakpoint_count": m.breakpoint_count,
                            "parts_affected_count": m.parts_affected_count,
                            "models_affected_count": m.models_affected_count,
                            "change_domain": m.change_domain,
                            "change_nature": m.change_nature,
                            "source": m.source,
                        }
                        for m in monthly
                    ],
                    "breakpoints": [
                        {
                            "breakpoint_number": s.breakpoint_number,
                            "breakpoint_date": s.breakpoint_date.isoformat() if s.breakpoint_date else None,
                            "breakpoint_status": s.breakpoint_status,
                            "change_domain": s.change_domain,
                            "change_nature": s.change_nature,
                            "source": s.source,
                            "description": s.description,
                            "parts_affected": (s.new_parts_count or 0) + (s.old_parts_count or 0),
                        }
                        for s in summary
                    ]
                }

            except SQLAlchemyError as e:
                logger.error("SQLAlchemy error getting breakpoint statistics: %s", e)
                return {
                    "success": False,
                    "error": f"Database error: {str(e)}",
                    "status": "database_error"
                }
            except (ValueError, TypeError, AttributeError) as e:
                logger.error("Data error getting breakpoint statistics: %s", e)
                return {
                    "success": False,
                    "error": f"Data error: {str(e)}",
                    "status": "data_error"
                }
            except Exception as unexpected_error:
                logger.error(
                    "Unexpected error getting breakpoint statistics: %s",
                    unexpected_error,
                    exc_info=True
                )
                return {
                    "success": False,
                    "error": f"Unexpected error: {str(unexpected_error)}",
                    "status": "unexpected_error"
                }

        return self._safe_query(query)

    def get_breakpoint_details(self, breakpoint_number: str) -> Dict[str, Any]:
        """
        Get details for a specific breakpoint.
        
        Uses v_breakpoint_details view.
        
        Args:
            breakpoint_number: Breakpoint number to get details for
            
        Returns:
            Dictionary with breakpoint details
        """
        if not breakpoint_number or not breakpoint_number.strip():
            logger.warning("get_breakpoint_details called with empty breakpoint_number")
            return {
                "success": False,
                "error": "Breakpoint number cannot be empty",
                "status": "invalid_parameter"
            }

        def query(session):
            try:
                bp = session.query(BreakpointDetailsView).filter(
                    BreakpointDetailsView.breakpoint_number == breakpoint_number
                ).first()

                if not bp:
                    return {
                        "success": False,
                        "error": f"Breakpoint {breakpoint_number} not found",
                        "status": "not_found"
                    }

                return {
                    "success": True,
                    "data": {
                        "breakpoint_number": bp.breakpoint_number,
                        "breakpoint_date": bp.breakpoint_date.isoformat() if bp.breakpoint_date else None,
                        "breakpoint_status": bp.breakpoint_status,
                        "change_domain": bp.change_domain,
                        "change_nature": bp.change_nature,
                        "description": bp.description,
                        "solution": bp.solution,
                        "source": bp.source,
                        "batch_plan": bp.batch_plan,
                        "batch_fact": bp.batch_fact,
                        "input_date": bp.input_date.isoformat() if bp.input_date else None,
                        "new_parts_count": bp.new_parts_count,
                        "old_parts_count": bp.old_parts_count,
                        "models_affected_count": bp.models_affected_count,
                        "models_affected": bp.models_affected,
                        "add_count": bp.add_count,
                        "delete_count": bp.delete_count,
                        "update_count": bp.update_count,
                        "replace_count": bp.replace_count,
                    }
                }

            except SQLAlchemyError as e:
                logger.error(
                    "SQLAlchemy error getting breakpoint details for %s: %s",
                    breakpoint_number, e
                )
                return {
                    "success": False,
                    "error": f"Database error: {str(e)}",
                    "status": "database_error"
                }
            except (ValueError, TypeError, AttributeError) as e:
                logger.error(
                    "Data error getting breakpoint details for %s: %s",
                    breakpoint_number, e
                )
                return {
                    "success": False,
                    "error": f"Data error: {str(e)}",
                    "status": "data_error"
                }
            except Exception as unexpected_error:
                logger.error(
                    "Unexpected error getting breakpoint details for %s: %s",
                    breakpoint_number,
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

def get_bp_api():
    """Get BPDisplayAPI instance."""
    try:
        engine = initialize_database(create_tables=False)
        if engine:
            try:
                return BPDisplayAPI(engine)
            except (ValueError, TypeError, AttributeError) as e:
                logger.error("Error creating BPDisplayAPI instance: %s", e)
                return None
            except Exception as unexpected_error:
                logger.error(
                    "Unexpected error creating BPDisplayAPI instance: %s",
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

@bp_bp.route('/<string:breakpoint_number>/history', methods=['GET'])
@rate_limit()
@handle_api_response
def get_breakpoint_history_endpoint(breakpoint_number):
    """Get full history for a specific breakpoint."""
    try:
        api = get_bp_api()
        if not api:
            return jsonify({
                'error': 'Database connection not available',
                'success': False,
                'status': 'service_unavailable'
            }), 503

        if not breakpoint_number or not breakpoint_number.strip():
            return jsonify({
                'error': 'Breakpoint number cannot be empty',
                'success': False,
                'status': 'bad_request'
            }), 400

        return api.get_breakpoint_history(breakpoint_number)

    except SQLAlchemyError as e:
        logger.error("SQLAlchemy error in breakpoint history endpoint: %s", e)
        return jsonify({
            'error': 'Database error',
            'success': False,
            'status': 'database_error'
        }), 500
    except Exception as unexpected_error:
        logger.error(
            "Unexpected error in breakpoint history endpoint: %s",
            unexpected_error,
            exc_info=True
        )
        return jsonify({
            'error': 'Internal server error',
            'success': False,
            'status': 'error'
        }), 500


@bp_bp.route('/search', methods=['GET', 'POST'])
@rate_limit()
@handle_api_response
def search_breakpoints_endpoint():
    """Search breakpoints with filters."""
    try:
        api = get_bp_api()
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
            logger.error(
                "Unexpected error processing filters: %s",
                unexpected_error,
                exc_info=True
            )
            return jsonify({
                'error': 'Error processing filters',
                'success': False,
                'status': 'internal_error'
            }), 500

        logger.info("Breakpoint search with filters: %s", processed_filters)
        return api.search_breakpoints(processed_filters)

    except SQLAlchemyError as e:
        logger.error("SQLAlchemy error in search breakpoints endpoint: %s", e)
        return jsonify({
            'error': 'Database error',
            'success': False,
            'status': 'database_error'
        }), 500
    except Exception as unexpected_error:
        logger.error(
            "Unexpected error in search breakpoints endpoint: %s",
            unexpected_error,
            exc_info=True
        )
        return jsonify({
            'error': 'Internal server error',
            'success': False,
            'status': 'error'
        }), 500


@bp_bp.route('/stats', methods=['GET'])
@rate_limit()
@handle_api_response
def get_breakpoint_statistics_endpoint():
    """Get breakpoint statistics and summary."""
    try:
        api = get_bp_api()
        if not api:
            return jsonify({
                'error': 'Database connection not available',
                'success': False,
                'status': 'service_unavailable'
            }), 503

        return api.get_breakpoint_statistics()

    except SQLAlchemyError as e:
        logger.error("SQLAlchemy error in breakpoint statistics endpoint: %s", e)
        return jsonify({
            'error': 'Database error',
            'success': False,
            'status': 'database_error'
        }), 500
    except Exception as unexpected_error:
        logger.error(
            "Unexpected error in breakpoint statistics endpoint: %s",
            unexpected_error,
            exc_info=True
        )
        return jsonify({
            'error': 'Internal server error',
            'success': False,
            'status': 'error'
        }), 500


@bp_bp.route('/<string:breakpoint_number>/details', methods=['GET'])
@rate_limit()
@handle_api_response
def get_breakpoint_details_endpoint(breakpoint_number):
    """Get details for a specific breakpoint."""
    try:
        api = get_bp_api()
        if not api:
            return jsonify({
                'error': 'Database connection not available',
                'success': False,
                'status': 'service_unavailable'
            }), 503

        if not breakpoint_number or not breakpoint_number.strip():
            return jsonify({
                'error': 'Breakpoint number cannot be empty',
                'success': False,
                'status': 'bad_request'
            }), 400

        return api.get_breakpoint_details(breakpoint_number)

    except SQLAlchemyError as e:
        logger.error("SQLAlchemy error in breakpoint details endpoint: %s", e)
        return jsonify({
            'error': 'Database error',
            'success': False,
            'status': 'database_error'
        }), 500
    except Exception as unexpected_error:
        logger.error(
            "Unexpected error in breakpoint details endpoint: %s",
            unexpected_error,
            exc_info=True
        )
        return jsonify({
            'error': 'Internal server error',
            'success': False,
            'status': 'error'
        }), 500


@bp_bp.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    try:
        api = get_bp_api()
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
            'service': 'BP Display API',
            'timestamp': datetime.now().isoformat(),
            'environment': FLASK_ENV,
            'database_status': db_status,
            'features': {
                'breakpoint_history': True,
                'breakpoint_search': True,
                'breakpoint_statistics': True,
            }
        }), 200
    except Exception as unexpected_error:
        logger.error("Health check failed: %s", unexpected_error, exc_info=True)
        return jsonify({
            'status': 'degraded',
            'service': 'BP Display API',
            'error': str(unexpected_error)
        }), 200


@bp_bp.route('/', methods=['GET'])
def api_documentation():
    """API documentation."""
    try:
        return jsonify({
            'name': 'BP Display API',
            'version': '1.0.0',
            'description': 'Breakpoint display and history API for material flow database',
            'endpoints': {
                '/api/breakpoints/{breakpoint_number}/history': {
                    'methods': ['GET'],
                    'description': 'Get full history for a specific breakpoint'
                },
                '/api/breakpoints/search': {
                    'methods': ['GET', 'POST'],
                    'description': 'Search breakpoints with filters'
                },
                '/api/breakpoints/stats': {
                    'methods': ['GET'],
                    'description': 'Get breakpoint statistics and summary'
                },
                '/api/breakpoints/{breakpoint_number}/details': {
                    'methods': ['GET'],
                    'description': 'Get details for a specific breakpoint'
                },
                '/api/breakpoints/health': {
                    'methods': ['GET'],
                    'description': 'Health check'
                }
            },
            'filters': {
                'breakpoint_number': 'Partial match on breakpoint number',
                'breakpoint_status': 'Exact match on status',
                'change_domain': 'Exact match on change domain',
                'change_nature': 'Exact match on change nature',
                'source': 'Exact match on source (manual/automatic)',
                'description': 'Partial match on description',
                'date_from': 'Filter breakpoints from date (ISO format)',
                'date_to': 'Filter breakpoints to date (ISO format)'
            }
        })
    except Exception as unexpected_error:
        logger.error(
            "Unexpected error in API documentation: %s",
            unexpected_error,
            exc_info=True
        )
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
        CORS(app)

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
    app.register_blueprint(bp_bp)

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
    logger.info("Starting BP Display API on %s:%s", FLASK_HOST, FLASK_PORT)
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
        logger.error(
            "Failed to start application: %s",
            unexpected_error,
            exc_info=True
        )
        sys.exit(1)
