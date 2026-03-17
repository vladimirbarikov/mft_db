"""
Logging configuration module for MFT Database project.

This module provides comprehensive logging configuration with support for:
- Multiple log formats (simple, verbose, JSON, Airflow-specific)
- File rotation based on size (5-20 MB)
- Dedicated log directories for different components
- Custom JSON formatter for structured logging (ELK/Logstash compatible)
- Airflow task filtering with DAG/task context
- Hierarchical logger configuration based on project structure

Key components:
1. CustomJsonFormatter - JSON-structured log output with custom fields support
2. AirflowTaskFilter - Adds DAG/task context to Airflow-related logs
3. LOGGING_CONFIG - Comprehensive dictionary configuration for logging
4. get_logger() - Factory function for obtaining configured loggers

Directory structure:
    logs/
    ├── api_logs/          # API endpoints logs (endpoints.*)
    ├── airflow_logs/      # Airflow DAGs and tasks logs (dags.*, airflow.*)
    ├── app_logs/          # Application and config modules logs (config.*)
    ├── database_logs/     # Database operations logs (database.*)
    ├── json_logs/         # JSON-formatted logs for analysis (all sources)
    └── error_logs/        # Centralized ERROR and CRITICAL logs (all sources)

Usage:
    # In any module, use __name__ for automatic routing
    from config import get_logger
    
    logger = get_logger(__name__)
    logger.info("Module initialized")
    
    # With custom fields for JSON logging
    logger.info("Processing data", extra={
        'custom_fields': {
            'user_id': 123,
            'action': 'data_processing'
        }
    })

Log directories created:
    - logs/
        - app_logs/         : Main application logs (DEBUG+)
        - airflow_logs/     : Airflow-specific logs (INFO+/WARNING+)
        - api_logs/         : API endpoint logs (INFO+)
        - database_logs/    : Database-related operations (INFO+)
        - json_logs/        : JSON-formatted logs for analysis (INFO+)
        - error_logs/       : Centralized error logs - ALL ERROR and CRITICAL messages from ALL sources

Configuration features:
    - Console output: INFO+ to stdout, WARNING+ to stderr
    - File output for Airflow: WARNING+ between DAG runs, INFO+ during DAG execution
    - Centralized error log: ALL WARNING+ messages from ALL sources
    - File rotation: Automatic based on file size (5-20 MB)
    - Encoding: UTF-8 for all files
    - Thread/process safety: All handlers are thread-safe
    - Error handling: Graceful fallback if configuration fails
    - Moscow timezone (Europe/Moscow) for all timestamps
    - UTC time also included in JSON logs for compatibility

Version: 1.0.0
Compatibility: Python 3.12.3
Maintainer: PLD Engineering Center
Created: 2026-02-16
Last Modified: 2026-03-17
License: MIT
Status: Production
"""

# ====== STANDARD IMPORTS ======
from pathlib import Path
import os
import sys
import logging
import logging.config
import logging.handlers  # Required for RotatingFileHandler in dictConfig
import json
from typing import Any, Dict, Optional
from datetime import datetime, timezone
import zoneinfo

# ====== CONSTANTS AND SETTINGS ======
# Directory paths
PROJECT_ROOT = Path(__file__).parents[1]
LOG_DIR = PROJECT_ROOT / "logs"
AIRFLOW_LOG_DIR = LOG_DIR / "airflow_logs"
API_LOG_DIR = LOG_DIR / "api_logs"
APP_LOG_DIR = LOG_DIR / "app_logs"
DATABASE_LOG_DIR = LOG_DIR / "database_logs"
JSON_LOG_DIR = LOG_DIR / "json_logs"
ERROR_LOG_DIR = LOG_DIR / "error_logs"

# Time settings
MOSCOW_TZ = zoneinfo.ZoneInfo("Europe/Moscow")
LOG_ID = datetime.now(MOSCOW_TZ).strftime("%Y%m%d_%H%M%S")
CURRENT_TIME = datetime.now(MOSCOW_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')

# ====== HELPER FUNCTIONS ======
def ensure_log_dir(directory: Path) -> None:
    """Create log directory if it doesn't exist."""
    if not directory.exists():
        directory.mkdir(parents=True, exist_ok=True)
        # For Linux systems, set permissions 755 (rwxr-xr-x)
        if sys.platform.startswith('linux'):
            try:
                # Permissions: owner can do everything,
                # group and others can only read and execute
                os.chmod(directory, 0o755)
            except OSError as e:
                # Print error but don't interrupt execution
                print(f"Warning: Could not set permissions on {directory}: {e}", file=sys.stderr)


def get_log_file_path(directory: Path, filename: str) -> str:
    """Get full path to log file in specific directory."""
    ensure_log_dir(directory)
    return str(directory / filename)


# ====== FORMATTERS ======
class MoscowTimeFormatter(logging.Formatter):
    """Formatter that converts timestamps to Moscow timezone."""

    def formatTime(self, record, datefmt=None):
        """Convert record creation time to Moscow timezone."""
        # Convert to Moscow time
        moscow_time = datetime.fromtimestamp(record.created, MOSCOW_TZ)
        if datefmt:
            return moscow_time.strftime(datefmt)
        # Default format: YYYY-MM-DD HH:MM:SS
        return moscow_time.strftime('%Y-%m-%d %H:%M:%S')


class CustomJsonFormatter(logging.Formatter):
    """Custom formatter for JSON logs (ELK/Logstash compatible)."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON with timestamp in Moscow timezone."""
        # Convert timestamp to Moscow time
        moscow_time = datetime.fromtimestamp(record.created, MOSCOW_TZ)
        utc_time = datetime.fromtimestamp(record.created, timezone.utc)

        log_record: Dict[str, Any] = {
            "timestamp": moscow_time.isoformat(),
            "timestamp_utc": utc_time.isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "message": record.getMessage(),
            "thread": record.threadName,
            "process": record.processName,
        }

        # Add exceptions if present
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)

        # Add custom fields if provided
        try:
            custom_fields = getattr(record, 'custom_fields', None)
            if custom_fields:
                # Verify it's actually a dictionary
                if isinstance(custom_fields, dict):
                    # Check serializability
                    try:
                        json.dumps(custom_fields, default=str)
                        log_record.update(custom_fields)
                    except (TypeError, ValueError):
                        log_record["custom_fields_error"] = "Non-serializable data"
                else:
                    log_record["custom_data"] = str(custom_fields)
        except AttributeError:
            # Ignore if attribute doesn't exist
            pass

        return json.dumps(log_record, ensure_ascii=False, default=str)


# ====== FILTERS ======
class AirflowTaskFilter(logging.Filter):
    """Filter that adds DAG/task context to Airflow log records."""

    def filter(self, record):
        """Add dag_id, task_id and run_id attributes to record."""
        # Add DAG and task information if available
        if not hasattr(record, 'dag_id'):
            record.dag_id = 'unknown'
        if not hasattr(record, 'task_id'):
            record.task_id = 'unknown'
        if not hasattr(record, 'run_id'):
            record.run_id = 'unknown'
        return True


# ====== LOGGER CONFIGURATION ======
# Main logging configuration dictionary
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "filters": {
        "airflow_task_filter": {
            "()": AirflowTaskFilter,
        },
    },
    "formatters": {
        "verbose": {
            "()": MoscowTimeFormatter,
            "format": "%(asctime)s - %(name)s - %(levelname)s - [%(module)s:%(funcName)s:%(lineno)d] - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        },
        "simple": {
            "()": MoscowTimeFormatter,
            "format": "%(asctime)s - %(levelname)s - %(message)s",
            "datefmt": "%H:%M:%S"
        },
        "airflow": {
            "()": MoscowTimeFormatter,
            "format": "%(asctime)s [%(dag_id)s:%(task_id)s:%(run_id)s] - %(levelname)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        },
        "json": {
            "()": CustomJsonFormatter,
        }
    },
    "handlers": {
        # Console handlers
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "simple",
            "stream": "ext://sys.stdout",
        },
        "error_console": {
            "class": "logging.StreamHandler",
            "level": "WARNING",
            "formatter": "verbose",
            "stream": "ext://sys.stderr",
        },
        # App logs
        "app_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "DEBUG",
            "formatter": "verbose",
            "filename": get_log_file_path(APP_LOG_DIR, f"app_{LOG_ID}.log"),
            "maxBytes": 10 * 1024 * 1024,  # 10 MB
            "backupCount": 5,
            "encoding": "utf8",
            "delay": True,
        },
        # CENTRALIZED ERROR-ONLY logs (for all sources)
        "error_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "ERROR",  # ERROR and higher only (ERROR, CRITICAL)
            "formatter": "verbose",
            "filename": get_log_file_path(ERROR_LOG_DIR, f"errors_{LOG_ID}.log"),
            "maxBytes": 10 * 1024 * 1024,
            "backupCount": 5,
            "encoding": "utf8",
            "delay": True,
        },
        # JSON logs
        "json_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "json",
            "filename": get_log_file_path(JSON_LOG_DIR, f"json_{LOG_ID}.log"),
            "maxBytes": 10 * 1024 * 1024,
            "backupCount": 3,
            "encoding": "utf8",
            "delay": True,
        },
        # Airflow-specific logs (in the airflow_logs directory)
        "airflow_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "airflow",
            "filename": get_log_file_path(AIRFLOW_LOG_DIR, f"airflow_{LOG_ID}.log"),
            "maxBytes": 20 * 1024 * 1024,  # 20 MB
            "backupCount": 10,
            "encoding": "utf8",
            "delay": True,
            "filters": ["airflow_task_filter"]
        },
        # Database logs (in the database_logs directory)
        "database_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "verbose",
            "filename": get_log_file_path(DATABASE_LOG_DIR, f"database_{LOG_ID}.log"),
            "maxBytes": 5 * 1024 * 1024,
            "backupCount": 3,
            "encoding": "utf8",
            "delay": True,
        },
        # API logs (in the api_logs directory)
        "api_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "verbose",
            "filename": get_log_file_path(API_LOG_DIR, f"api_{LOG_ID}.log"),
            "maxBytes": 5 * 1024 * 1024,
            "backupCount": 3,
            "encoding": "utf8",
            "delay": True,
        },
    },
    "loggers": {
        # Root logger
        "": {
            "level": "DEBUG",
            "handlers": ["console", "app_file", "json_file", "error_file"],
            "propagate": False
        },
        # App module loggers
        "__main__": {
            "level": "INFO",
            "handlers": ["console", "app_file", "json_file", "error_file"],
            "propagate": False,
        },
        # API endpoints - all logs from endpoints/ go to api_logs/
        "endpoints": {
            "level": "INFO",
            "handlers": ["console", "api_file", "error_file"],
            "propagate": False,
        },
        # DAGs
        "dags": {
            "level": "INFO",
            "handlers": ["console", "airflow_file", "error_file"],
            "propagate": False,
        },
        "dags.bp_dag": {
            "level": "INFO",
            "handlers": ["console", "airflow_file", "error_file"],
            "propagate": False,
        },
        "dags.mft_dag": {
            "level": "INFO",
            "handlers": ["console", "airflow_file", "error_file"],
            "propagate": False,
        },
        # Airflow tasks
        "dags.tasks": {
            "level": "DEBUG",
            "handlers": ["console", "airflow_file", "error_file"],
            "propagate": False,
        },
        "dags.tasks.bp_loader": {
            "level": "INFO",
            "handlers": ["console", "app_file", "database_file", "airflow_file", "error_file"],
            "propagate": False,
        },
        "dags.tasks.bp_mapper": {
            "level": "INFO",
            "handlers": ["console", "app_file", "database_file", "airflow_file", "error_file"],
            "propagate": False,
        },
        "dags.tasks.connector": {
            "level": "DEBUG",
            "handlers": ["console", "app_file", "database_file", "airflow_file", "error_file"],
            "propagate": False,
        },
        "dags.tasks.extractor": {
            "level": "INFO",
            "handlers": ["console", "app_file", "database_file", "airflow_file", "error_file"],
            "propagate": False,
        },
        "dags.tasks.mft_loader": {
            "level": "INFO",
            "handlers": ["console", "app_file", "database_file", "airflow_file", "error_file"],
            "propagate": False,
        },
        "dags.tasks.mft_mapper": {
            "level": "INFO",
            "handlers": ["console", "app_file", "database_file", "airflow_file", "error_file"],
            "propagate": False,
        },
        "dags.tasks.serializer": {
            "level": "INFO",
            "handlers": ["console", "app_file", "database_file", "airflow_file", "error_file"],
            "propagate": False,
        },
        "dags.tasks.transformer": {
            "level": "INFO",
            "handlers": ["console", "app_file", "database_file", "airflow_file", "error_file"],
            "propagate": False,
        },
        # Database operations
        "database": {
            "level": "INFO",
            "handlers": ["console", "database_file", "error_file"],
            "propagate": False,
        },
        # Config modules
        "config": {
            "level": "INFO",
            "handlers": ["console", "app_file", "error_file"],
            "propagate": False,
        },
        # Airflow system logs
        "airflow": {
            "level": "WARNING",
            "handlers": ["airflow_file", "error_file"],
            "propagate": False,
        },
        "airflow.task": {
            "level": "INFO",
            "handlers": ["airflow_file", "error_file"],
            "propagate": False,
        },
        "airflow.processor": {
            "level": "WARNING",
            "handlers": ["airflow_file", "error_file"],
            "propagate": False,
        },
        "airflow.scheduler": {
            "level": "WARNING",
            "handlers": ["airflow_file", "error_file"],
            "propagate": False,
        },
        "airflow.models.dagbag": {
            "level": "WARNING",
            "handlers": ["airflow_file", "error_file"],
            "propagate": False,
        },
        "airflow.executors": {
            "level": "WARNING",
            "handlers": ["airflow_file", "error_file"],
            "propagate": False,
        },
        # Third-party libraries
        "sqlalchemy": {
            "level": "WARNING",
            "handlers": ["database_file", "error_file"],
            "propagate": False,
        },
        "sqlalchemy.engine": {
            "level": "INFO",
            "handlers": ["database_file", "error_file"],
            "propagate": False,
        },
        "requests": {
            "level": "WARNING",
            "handlers": ["app_file", "error_file"],
            "propagate": False,
        },
        # Special logger for all errors
        "errors": {
            "level": "WARNING",
            "handlers": ["error_file"],
            "propagate": False,
        },
    }
}


# ====== INITIALIZATION ======
# Configure logging on module import
try:
    # Apply main configuration
    logging.config.dictConfig(LOGGING_CONFIG)
except (ValueError, TypeError, KeyError) as e:
    # Specific exceptions that dictConfig can raise
    # Use print instead of logging as logging may not be configured
    print(f"Error configuring logging: {e}", file=sys.stderr)
    # Fallback to basic logging
    logging.basicConfig(level=logging.INFO)
else:
    # Only create logger if configuration succeeded
    init_logger = logging.getLogger(__name__)
    init_logger.debug("Logging initialized. Log ID: %s", LOG_ID)
    init_logger.debug("Project root: %s", PROJECT_ROOT)
    init_logger.debug("Main log directory: %s", LOG_DIR)
    init_logger.debug("Log directories created:")
    init_logger.debug("  - Airflow logs: %s", AIRFLOW_LOG_DIR)
    init_logger.debug("  - API logs: %s", API_LOG_DIR)
    init_logger.debug("  - App logs: %s", APP_LOG_DIR)
    init_logger.debug("  - Database logs: %s", DATABASE_LOG_DIR)
    init_logger.debug("  - JSON logs: %s", JSON_LOG_DIR)
    init_logger.debug("  - Error logs (CENTRALIZED): %s", ERROR_LOG_DIR)
    init_logger.debug("Central error log collects ALL ERROR and higher messages from ALL sources")


# ====== PUBLIC INTERFACE ======
def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Factory method for getting loggers.
    Use this method instead of calling logging.getLogger() directly.
    
    Args:
        name: Logger name (usually __name__). If None, uses current module name.

    Returns:
        Configured logger instance with appropriate handlers based on name.
    """
    if name is None:
        name = __name__

    logger_instance = logging.getLogger(name)

    # Add custom attributes for Airflow
    if 'airflow' in name.lower() or 'task' in name.lower():
        logger_instance.addFilter(AirflowTaskFilter())

    return logger_instance


# Create global logger for this module
logger = get_logger(__name__)


# Export only the public interface
__all__ = ['get_logger']
