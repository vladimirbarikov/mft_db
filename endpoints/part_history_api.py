# pylint: disable=too-many-lines
# pylint: disable=wrong-import-position
"""
Part History API Module for Material Flow Table Database.

This module provides endpoints for displaying part change history.
Uses BaseDisplayAPI for common functionality and database views for performance.

ENDPOINTS:
    GET /api/parts/{part_number}/history - Get full part change history
    GET /api/parts/{part_number}/versions - Get all versions with summary
    GET /api/parts/{part_number}/timeline - Get timeline of changes
    GET /api/parts/{part_number}/changes - Get changes by breakpoint
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
from flask import Blueprint, Flask, jsonify
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from sqlalchemy import desc
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
    PartHistoryView,
    PartChangeSummaryView
)

# Logger setup
logger = get_logger("endpoints.part_history_api")

# ========== CONFIGURATION ==========

FLASK_SECRET_KEY = os.getenv('FLASK_SECRET_KEY')
if not FLASK_SECRET_KEY:
    raise RuntimeError("FLASK_SECRET_KEY must be set in .env file")

FLASK_HOST = os.getenv('FLASK_HOST', '0.0.0.0')
FLASK_PORT = int(os.getenv('PART_HISTORY_API_PORT', '5002'))
FLASK_DEBUG = os.getenv('FLASK_DEBUG', 'false').lower() == 'true'
FLASK_ENV = os.getenv('FLASK_ENV', 'development')
IS_PRODUCTION = FLASK_ENV == 'production'

ALLOWED_ORIGINS = os.getenv('ALLOWED_ORIGINS', '*')
RATE_LIMIT = os.getenv('RATE_LIMIT', '10 per minute')
RATE_LIMIT_STORAGE_URL = os.getenv('RATE_LIMIT_STORAGE_URL', 'memory://')

# ========== CREATING BLUEPRINT ==========
part_bp = Blueprint('part_history', __name__, url_prefix='/api/parts')

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

class PartHistoryAPI(BaseDisplayAPI):
    """
    API for part change history using database views.
    
    Uses v_part_history and v_part_change_summary views for optimized queries.
    Shows all versions (active + inactive) with change tracking.
    """

    def __init__(self, engine):
        """Initialize with database engine."""
        try:
            super().__init__(engine)
            logger.info("PartHistoryAPI initialized successfully")
        except ValueError as e:
            logger.error("ValueError initializing PartHistoryAPI: %s", e)
            raise
        except TypeError as e:
            logger.error("TypeError initializing PartHistoryAPI: %s", e)
            raise
        except AttributeError as e:
            logger.error("AttributeError initializing PartHistoryAPI: %s", e)
            raise
        except Exception as unexpected_error:
            logger.error(
                "Unexpected error initializing PartHistoryAPI: %s",
                unexpected_error,
                exc_info=True
            )
            raise RuntimeError(
                f"Unexpected error initializing PartHistoryAPI: {unexpected_error}"
            ) from unexpected_error

    def get_part_history(self, part_number: str) -> Dict[str, Any]:
        """
        Get full change history for a specific part.
        
        Uses v_part_history view for optimized queries.
        Shows all versions (active + inactive).
        
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
                ).order_by(
                    desc(PartHistoryView.version_number)
                ).all()

                if not history:
                    return {
                        "success": False,
                        "error": f"Part {part_number} not found",
                        "status": "not_found"
                    }

                # Get summary for this part
                summary = session.query(PartChangeSummaryView).filter(
                    PartChangeSummaryView.part_number == part_number
                ).first()

                result = {
                    "success": True,
                    "part_number": self.normalize_output("PART_NUMBER", part_number),
                    "total_versions": len(history),
                    "summary": {
                        "total_changes": summary.total_changes if summary else 0,
                        "manual_changes": summary.manual_changes if summary else 0,
                        "automatic_changes": summary.automatic_changes if summary else 0,
                        "latest_change_date": summary.latest_change_date.isoformat() if summary and summary.latest_change_date else None,
                        "latest_breakpoint_number": summary.latest_breakpoint_number if summary else None,
                        "domains_affected": summary.domains_affected.split(', ') if summary and summary.domains_affected else [],
                        "total_versions": summary.total_versions if summary else 0,
                    },
                    "history": []
                }

                # Format each version
                for h in history:
                    try:
                        entry = {
                            "version": h.version_number,
                            "part_name": self.normalize_output("PART_NAME", h.part_name),
                            "part_weight_kg": float(h.part_weight_kg) if h.part_weight_kg else None,
                            "supplier_name": self.normalize_output("SUPPLIER_NAME", h.supplier_name),
                            "localization": self.normalize_output("LOCALIZATION", h.localization),
                            "configuration": self.normalize_output("CONFIGURATION", h.configuration),
                            "transmission": h.transmission,
                            "is_active": h.is_active,
                            "deactivated_at": h.deactivated_at.isoformat() if h.deactivated_at else None,
                            "created_at": h.created_at.isoformat() if h.created_at else None,
                            "breakpoint": {
                                "number": h.breakpoint_number,
                                "date": h.breakpoint_date.isoformat() if h.breakpoint_date else None,
                                "status": h.breakpoint_status,
                                "description": h.description,
                                "solution": h.solution,
                                "batch_plan": h.batch_plan,
                                "batch_fact": h.batch_fact,
                            } if h.breakpoint_number else None,
                            "change": {
                                "source": h.change_source,
                                "domain": h.change_domain,
                                "nature": h.change_nature,
                                "action_type": h.change_action_type,
                            } if h.breakpoint_id else None,
                            "transition": {
                                "new_part_id": h.transition_new_part_id,
                                "old_part_id": h.transition_old_part_id,
                            } if h.breakpoint_id else None,
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
                logger.error(
                    "SQLAlchemy error getting part history for %s: %s",
                    part_number, e
                )
                return {
                    "success": False,
                    "error": f"Database error: {str(e)}",
                    "status": "database_error"
                }
            except (ValueError, TypeError, AttributeError) as e:
                logger.error(
                    "Data error getting part history for %s: %s",
                    part_number, e
                )
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

    def get_part_versions(self, part_number: str) -> Dict[str, Any]:
        """
        Get all versions with summary information.
        
        Args:
            part_number: Part number to get versions for
            
        Returns:
            Dictionary with part versions
        """
        if not part_number or not part_number.strip():
            logger.warning("get_part_versions called with empty part_number")
            return {
                "success": False,
                "error": "Part number cannot be empty",
                "status": "invalid_parameter"
            }

        def query(session):
            try:
                versions = session.query(PartHistoryView).filter(
                    PartHistoryView.part_number == part_number
                ).order_by(
                    desc(PartHistoryView.version_number)
                ).all()

                if not versions:
                    return {
                        "success": False,
                        "error": f"Part {part_number} not found",
                        "status": "not_found"
                    }

                result = {
                    "success": True,
                    "part_number": self.normalize_output("PART_NUMBER", part_number),
                    "total_versions": len(versions),
                    "versions": []
                }

                for v in versions:
                    try:
                        entry = {
                            "version": v.version_number,
                            "part_name": self.normalize_output("PART_NAME", v.part_name),
                            "supplier_name": self.normalize_output("SUPPLIER_NAME", v.supplier_name),
                            "configuration": self.normalize_output("CONFIGURATION", v.configuration),
                            "is_active": v.is_active,
                            "created_at": v.created_at.isoformat() if v.created_at else None,
                            "breakpoint_number": v.breakpoint_number,
                            "change_source": v.change_source,
                            "change_domain": v.change_domain,
                            "change_nature": v.change_nature,
                            "change_action_type": v.change_action_type,
                        }
                        result["versions"].append(entry)
                    except (ValueError, TypeError, AttributeError) as e:
                        logger.warning(
                            "Error formatting version entry for part %s version %s: %s",
                            part_number, v.version_number, e
                        )
                        continue
                    except Exception as unexpected_error:
                        logger.warning(
                            "Unexpected error formatting version entry for part %s version %s: %s",
                            part_number, v.version_number, unexpected_error
                        )
                        continue

                return result

            except SQLAlchemyError as e:
                logger.error(
                    "SQLAlchemy error getting part versions for %s: %s",
                    part_number, e
                )
                return {
                    "success": False,
                    "error": f"Database error: {str(e)}",
                    "status": "database_error"
                }
            except (ValueError, TypeError, AttributeError) as e:
                logger.error(
                    "Data error getting part versions for %s: %s",
                    part_number, e
                )
                return {
                    "success": False,
                    "error": f"Data error: {str(e)}",
                    "status": "data_error"
                }
            except Exception as unexpected_error:
                logger.error(
                    "Unexpected error getting part versions for %s: %s",
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

    def get_part_timeline(self, part_number: str) -> Dict[str, Any]:
        """
        Get timeline of changes for a part.
        
        Returns chronological list of changes with key events.
        
        Args:
            part_number: Part number to get timeline for
            
        Returns:
            Dictionary with part timeline
        """
        if not part_number or not part_number.strip():
            logger.warning("get_part_timeline called with empty part_number")
            return {
                "success": False,
                "error": "Part number cannot be empty",
                "status": "invalid_parameter"
            }

        def query(session):
            try:
                history = session.query(PartHistoryView).filter(
                    PartHistoryView.part_number == part_number
                ).order_by(
                    PartHistoryView.breakpoint_date.asc()
                ).all()

                if not history:
                    return {
                        "success": False,
                        "error": f"Part {part_number} not found",
                        "status": "not_found"
                    }

                timeline = []
                for h in history:
                    if h.breakpoint_date:
                        try:
                            entry = {
                                "date": h.breakpoint_date.isoformat(),
                                "event": f"Version {h.version_number}",
                                "breakpoint_number": h.breakpoint_number,
                                "change_source": h.change_source,
                                "change_domain": h.change_domain,
                                "change_nature": h.change_nature,
                                "change_action_type": h.change_action_type,
                                "supplier_name": self.normalize_output("SUPPLIER_NAME", h.supplier_name),
                                "configuration": self.normalize_output("CONFIGURATION", h.configuration),
                                "is_active": h.is_active,
                            }
                            timeline.append(entry)
                        except (ValueError, TypeError, AttributeError) as e:
                            logger.warning(
                                "Error formatting timeline entry for part %s: %s",
                                part_number, e
                            )
                            continue
                        except Exception as unexpected_error:
                            logger.warning(
                                "Unexpected error formatting timeline entry for part %s: %s",
                                part_number, unexpected_error
                            )
                            continue

                return {
                    "success": True,
                    "part_number": self.normalize_output("PART_NUMBER", part_number),
                    "total_events": len(timeline),
                    "timeline": timeline
                }

            except SQLAlchemyError as e:
                logger.error(
                    "SQLAlchemy error getting part timeline for %s: %s",
                    part_number, e
                )
                return {
                    "success": False,
                    "error": f"Database error: {str(e)}",
                    "status": "database_error"
                }
            except (ValueError, TypeError, AttributeError) as e:
                logger.error(
                    "Data error getting part timeline for %s: %s",
                    part_number, e
                )
                return {
                    "success": False,
                    "error": f"Data error: {str(e)}",
                    "status": "data_error"
                }
            except Exception as unexpected_error:
                logger.error(
                    "Unexpected error getting part timeline for %s: %s",
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

    def get_part_changes_by_breakpoint(self, part_number: str) -> Dict[str, Any]:
        """
        Get changes grouped by breakpoint.
        
        Args:
            part_number: Part number to get changes for
            
        Returns:
            Dictionary with changes grouped by breakpoint
        """
        if not part_number or not part_number.strip():
            logger.warning("get_part_changes_by_breakpoint called with empty part_number")
            return {
                "success": False,
                "error": "Part number cannot be empty",
                "status": "invalid_parameter"
            }

        def query(session):
            try:
                history = session.query(PartHistoryView).filter(
                    PartHistoryView.part_number == part_number
                ).order_by(
                    desc(PartHistoryView.breakpoint_date)
                ).all()

                if not history:
                    return {
                        "success": False,
                        "error": f"Part {part_number} not found",
                        "status": "not_found"
                    }

                # Group by breakpoint
                grouped = {}
                for h in history:
                    if h.breakpoint_number:
                        bp_key = h.breakpoint_number
                        if bp_key not in grouped:
                            grouped[bp_key] = {
                                "breakpoint_number": h.breakpoint_number,
                                "breakpoint_date": h.breakpoint_date.isoformat() if h.breakpoint_date else None,
                                "breakpoint_status": h.breakpoint_status,
                                "breakpoint_description": h.description,
                                "changes": []
                            }

                        try:
                            change_entry = {
                                "version": h.version_number,
                                "part_name": self.normalize_output("PART_NAME", h.part_name),
                                "supplier_name": self.normalize_output("SUPPLIER_NAME", h.supplier_name),
                                "configuration": self.normalize_output("CONFIGURATION", h.configuration),
                                "is_active": h.is_active,
                                "change_source": h.change_source,
                                "change_domain": h.change_domain,
                                "change_nature": h.change_nature,
                                "change_action_type": h.change_action_type,
                                "created_at": h.created_at.isoformat() if h.created_at else None,
                            }
                            grouped[bp_key]["changes"].append(change_entry)
                        except (ValueError, TypeError, AttributeError) as e:
                            logger.warning(
                                "Error formatting change entry for part %s: %s",
                                part_number, e
                            )
                            continue
                        except Exception as unexpected_error:
                            logger.warning(
                                "Unexpected error formatting change entry for part %s: %s",
                                part_number, unexpected_error
                            )
                            continue

                return {
                    "success": True,
                    "part_number": self.normalize_output("PART_NUMBER", part_number),
                    "total_breakpoints": len(grouped),
                    "breakpoints": list(grouped.values())
                }

            except SQLAlchemyError as e:
                logger.error(
                    "SQLAlchemy error getting part changes by breakpoint for %s: %s",
                    part_number, e
                )
                return {
                    "success": False,
                    "error": f"Database error: {str(e)}",
                    "status": "database_error"
                }
            except (ValueError, TypeError, AttributeError) as e:
                logger.error(
                    "Data error getting part changes by breakpoint for %s: %s",
                    part_number, e
                )
                return {
                    "success": False,
                    "error": f"Data error: {str(e)}",
                    "status": "data_error"
                }
            except Exception as unexpected_error:
                logger.error(
                    "Unexpected error getting part changes by breakpoint for %s: %s",
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

def get_part_history_api():
    """Get PartHistoryAPI instance."""
    try:
        engine = initialize_database(create_tables=False)
        if engine:
            try:
                return PartHistoryAPI(engine)
            except (ValueError, TypeError, AttributeError) as e:
                logger.error("Error creating PartHistoryAPI instance: %s", e)
                return None
            except Exception as unexpected_error:
                logger.error(
                    "Unexpected error creating PartHistoryAPI instance: %s",
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

@part_bp.route('/<string:part_number>/history', methods=['GET'])
@rate_limit()
@handle_api_response
def get_part_history_endpoint(part_number):
    """Get full change history for a specific part."""
    try:
        api = get_part_history_api()
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
        logger.error(
            "Unexpected error in part history endpoint: %s",
            unexpected_error,
            exc_info=True
        )
        return jsonify({
            'error': 'Internal server error',
            'success': False,
            'status': 'error'
        }), 500


@part_bp.route('/<string:part_number>/versions', methods=['GET'])
@rate_limit()
@handle_api_response
def get_part_versions_endpoint(part_number):
    """Get all versions with summary information."""
    try:
        api = get_part_history_api()
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

        return api.get_part_versions(part_number)

    except SQLAlchemyError as e:
        logger.error("SQLAlchemy error in part versions endpoint: %s", e)
        return jsonify({
            'error': 'Database error',
            'success': False,
            'status': 'database_error'
        }), 500
    except Exception as unexpected_error:
        logger.error(
            "Unexpected error in part versions endpoint: %s",
            unexpected_error,
            exc_info=True
        )
        return jsonify({
            'error': 'Internal server error',
            'success': False,
            'status': 'error'
        }), 500


@part_bp.route('/<string:part_number>/timeline', methods=['GET'])
@rate_limit()
@handle_api_response
def get_part_timeline_endpoint(part_number):
    """Get timeline of changes for a part."""
    try:
        api = get_part_history_api()
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

        return api.get_part_timeline(part_number)

    except SQLAlchemyError as e:
        logger.error("SQLAlchemy error in part timeline endpoint: %s", e)
        return jsonify({
            'error': 'Database error',
            'success': False,
            'status': 'database_error'
        }), 500
    except Exception as unexpected_error:
        logger.error(
            "Unexpected error in part timeline endpoint: %s",
            unexpected_error,
            exc_info=True
        )
        return jsonify({
            'error': 'Internal server error',
            'success': False,
            'status': 'error'
        }), 500


@part_bp.route('/<string:part_number>/changes', methods=['GET'])
@rate_limit()
@handle_api_response
def get_part_changes_by_breakpoint_endpoint(part_number):
    """Get changes grouped by breakpoint."""
    try:
        api = get_part_history_api()
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

        return api.get_part_changes_by_breakpoint(part_number)

    except SQLAlchemyError as e:
        logger.error("SQLAlchemy error in part changes endpoint: %s", e)
        return jsonify({
            'error': 'Database error',
            'success': False,
            'status': 'database_error'
        }), 500
    except Exception as unexpected_error:
        logger.error(
            "Unexpected error in part changes endpoint: %s",
            unexpected_error,
            exc_info=True
        )
        return jsonify({
            'error': 'Internal server error',
            'success': False,
            'status': 'error'
        }), 500


@part_bp.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    try:
        api = get_part_history_api()
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
            'service': 'Part History API',
            'timestamp': datetime.now().isoformat(),
            'environment': FLASK_ENV,
            'database_status': db_status,
            'features': {
                'full_history': True,
                'version_summary': True,
                'timeline': True,
                'changes_by_breakpoint': True,
            }
        }), 200
    except Exception as unexpected_error:
        logger.error("Health check failed: %s", unexpected_error, exc_info=True)
        return jsonify({
            'status': 'degraded',
            'service': 'Part History API',
            'error': str(unexpected_error)
        }), 200


@part_bp.route('/', methods=['GET'])
def api_documentation():
    """API documentation."""
    try:
        return jsonify({
            'name': 'Part History API',
            'version': '1.0.0',
            'description': 'Part change history and version tracking API',
            'endpoints': {
                '/api/parts/{part_number}/history': {
                    'methods': ['GET'],
                    'description': 'Get full change history for a specific part'
                },
                '/api/parts/{part_number}/versions': {
                    'methods': ['GET'],
                    'description': 'Get all versions with summary information'
                },
                '/api/parts/{part_number}/timeline': {
                    'methods': ['GET'],
                    'description': 'Get timeline of changes for a part'
                },
                '/api/parts/{part_number}/changes': {
                    'methods': ['GET'],
                    'description': 'Get changes grouped by breakpoint'
                },
                '/api/parts/health': {
                    'methods': ['GET'],
                    'description': 'Health check'
                }
            },
            'response_fields': {
                'history': {
                    'version': 'Version number',
                    'part_name': 'Part name',
                    'supplier_name': 'Supplier name',
                    'configuration': 'Configuration',
                    'is_active': 'Whether version is active',
                    'breakpoint_number': 'Breakpoint number',
                    'change_source': 'manual or automatic',
                    'change_domain': 'supplier, packaging, production, spec, config, multi',
                    'change_nature': 'business, technical, correction',
                    'change_action_type': 'ADD, DELETE, UPDATE, REPLACE'
                },
                'timeline': {
                    'date': 'Date of change',
                    'event': 'Description of event',
                    'breakpoint_number': 'Breakpoint number',
                    'change_source': 'Source of change',
                    'change_domain': 'Change domain',
                    'change_nature': 'Change nature'
                }
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
    app.register_blueprint(part_bp)

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
    logger.info("Starting Part History API on %s:%s", FLASK_HOST, FLASK_PORT)
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
