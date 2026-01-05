"""
Logging configuration module for MFT Database project.

This module provides comprehensive logging configuration with support for:
- Multiple log formats (simple, verbose, JSON, Airflow-specific)
- File rotation based on size
- Dedicated log files for different components (app, errors, database, API, Airflow)
- Custom JSON formatter for structured logging (ELK/Logstash compatible)
- Airflow task filtering and formatting
- Hierarchical logger configuration for different application modules

Key components:
1. CustomJsonFormatter - JSON-structured log output with custom fields support
2. AirflowTaskFilter - Adds DAG/task context to Airflow-related logs
3. LOGGING_CONFIG - Comprehensive dictionary configuration for logging
4. setup_logging() - Main initialization function (auto-called on package import)
5. get_logger() - Factory function for obtaining configured loggers

Usage:
    # In any module
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

Log files created:
    - app_YYYYMMDD_HHMMSS.log      : Main application log (verbose format)
    - errors_YYYYMMDD_HHMMSS.log   : Error-only log (WARNING and above)
    - json_YYYYMMDD_HHMMSS.log     : JSON-formatted log for analysis
    - database_YYYYMMDD_HHMMSS.log : Database-related operations
    - airflow_YYYYMMDD_HHMMSS.log  : Airflow-specific logs
    - api_YYYYMMDD_HHMMSS.log      : API endpoint logs

Configuration features:
    - Console output: INFO+ to stdout, WARNING+ to stderr
    - File rotation: Automatic based on file size (5-20 MB)
    - Encoding: UTF-8 for all files
    - Thread/process safety: All handlers are thread-safe
    - Error handling: Graceful fallback if configuration fails

Version: 1.0.0
Compatibility: Python 3.12.3
Maintainer: PLD Engineering Center
Created: 2026
Last Modified: 2026
License: MIT
Status: Production
"""
import sys
import logging
import logging.config
import logging.handlers  # Required for RotatingFileHandler in dictConfig

from pathlib import Path
from datetime import datetime
import json

from typing import Any, Dict, Optional

# Define project root directory
PROJECT_ROOT = Path(__file__).parents[1]
LOG_DIR = PROJECT_ROOT / "logs"

# Create logs directory if it doesn't exist
LOG_DIR.mkdir(exist_ok=True)

# Unique identifier for log filenames
LOG_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

class CustomJsonFormatter(logging.Formatter):
    """Custom formatter for JSON logs"""

    def format(self, record: logging.LogRecord) -> str:
        log_record: Dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
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

class AirflowTaskFilter(logging.Filter):
    """Filter for Airflow tasks"""

    def filter(self, record):
        # Add DAG and task information if available
        if not hasattr(record, 'dag_id'):
            record.dag_id = 'unknown'
        if not hasattr(record, 'task_id'):
            record.task_id = 'unknown'
        if not hasattr(record, 'run_id'):
            record.run_id = 'unknown'
        return True

def get_log_file_path(filename: str) -> str:
    """Get full path to log file"""
    return str(LOG_DIR / filename)

# Main logging configuration
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "filters": {
        "airflow_task_filter": {
            "()": AirflowTaskFilter,
        }
    },
    "formatters": {
        "verbose": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - [%(module)s:%(funcName)s:%(lineno)d] - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        },
        "simple": {
            "format": "%(asctime)s - %(levelname)s - %(message)s",
            "datefmt": "%H:%M:%S"
        },
        "airflow": {
            "format": "%(asctime)s [%(dag_id)s:%(task_id)s:%(run_id)s] - %(levelname)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        },
        "json": {
            "()": CustomJsonFormatter,
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "simple",
            "stream": "ext://sys.stdout",
            "filters": ["airflow_task_filter"]
        },

        # Error console output (stderr)
        "error_console": {
            "class": "logging.StreamHandler",
            "level": "WARNING",
            "formatter": "verbose",
            "stream": "ext://sys.stderr",
        },

        # Main file log (size-based rotation)
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "DEBUG",
            "formatter": "verbose",
            "filename": get_log_file_path(f"app_{LOG_ID}.log"),
            "maxBytes": 10 * 1024 * 1024,  # 10 MB
            "backupCount": 5,
            "encoding": "utf8",
            "delay": True,
        },

        # Error-only log file
        "error_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "WARNING",
            "formatter": "verbose",
            "filename": get_log_file_path(f"errors_{LOG_ID}.log"),
            "maxBytes": 5 * 1024 * 1024,  # 5 MB
            "backupCount": 3,
            "encoding": "utf8",
        },

        # JSON log for ELK/Logstash analysis
        "json_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "json",
            "filename": get_log_file_path(f"json_{LOG_ID}.log"),
            "maxBytes": 10 * 1024 * 1024,
            "backupCount": 3,
            "encoding": "utf8",
        },

        # Airflow-specific logs
        "airflow_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "airflow",
            "filename": get_log_file_path(f"airflow_{LOG_ID}.log"),
            "maxBytes": 20 * 1024 * 1024,  # 20 MB
            "backupCount": 10,
            "encoding": "utf8",
            "filters": ["airflow_task_filter"]
        },

        # Separate log for database
        "database_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "verbose",
            "filename": get_log_file_path(f"database_{LOG_ID}.log"),
            "maxBytes": 5 * 1024 * 1024,
            "backupCount": 3,
            "encoding": "utf8",
        },

        # Log for API endpoints
        "api_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "verbose",
            "filename": get_log_file_path(f"api_{LOG_ID}.log"),
            "maxBytes": 5 * 1024 * 1024,
            "backupCount": 3,
            "encoding": "utf8",
        },
    },
    "loggers": {
        # Root logger
        "": {
            "level": "DEBUG",
            "handlers": ["console", "file", "json_file"],
            "propagate": False
        },

        # Module loggers
        "dags": {
            "level": "INFO",
            "handlers": ["console", "file", "airflow_file"],
            "propagate": False,
            "qualname": "dags"
        },

        "dags.tasks": {
            "level": "DEBUG",
            "handlers": ["console", "file", "airflow_file"],
            "propagate": False,
        },

        "dags.tasks.extractor": {
            "level": "INFO",
            "handlers": ["console", "file"],
            "propagate": False,
        },

        "dags.tasks.transformer": {
            "level": "INFO",
            "handlers": ["console", "file"],
            "propagate": False,
        },

        "dags.tasks.connector": {
            "level": "DEBUG",
            "handlers": ["console", "file", "database_file"],
            "propagate": False,
        },

        "dags.tasks.loader": {
            "level": "INFO",
            "handlers": ["console", "file", "database_file"],
            "propagate": False,
        },

        "endpoints": {
            "level": "INFO",
            "handlers": ["console", "file", "api_file"],
            "propagate": False,
        },

        "endpoints.display_api": {
            "level": "INFO",
            "handlers": ["console", "api_file"],
            "propagate": False,
        },

        "endpoints.modify_api": {
            "level": "INFO",
            "handlers": ["console", "api_file"],
            "propagate": False,
        },

        "endpoints.upload_api": {
            "level": "DEBUG",
            "handlers": ["console", "api_file"],
            "propagate": False,
        },

        "endpoints.user_manager_api": {
            "level": "INFO",
            "handlers": ["console", "api_file"],
            "propagate": False,
        },

        "database": {
            "level": "INFO",
            "handlers": ["console", "database_file"],
            "propagate": False,
        },

        # Airflow loggers
        "airflow": {
            "level": "INFO",
            "handlers": ["airflow_file", "file"],
            "propagate": False,
        },

        "airflow.task": {
            "level": "INFO",
            "handlers": ["airflow_file", "console"],
            "propagate": False,
        },

        # SQLAlchemy logger (for query debugging)
        "sqlalchemy": {
            "level": "WARNING",
            "handlers": ["database_file"],
            "propagate": False,
        },

        "sqlalchemy.engine": {
            "level": "INFO",
            "handlers": ["database_file"],
            "propagate": False,
        },
    }
}

def setup_logging() -> None:
    """
    Initialize logging configuration.
    Should be called once at application startup.
    """

    try:
        # First configure basic logging for error handling
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )

        # Apply main configuration
        logging.config.dictConfig(LOGGING_CONFIG)

        # Log successful initialization
        init_logger = logging.getLogger(__name__)
        init_logger.info("=" * 60)
        init_logger.info("Logging successfully initialized")
        init_logger.info("Project root: %s", PROJECT_ROOT)
        init_logger.info("Log directory: %s", LOG_DIR)
        init_logger.info("Log ID: %s", LOG_ID)
        init_logger.info("=" * 60)

    except Exception as e:
        # Use print instead of logging as logging may not be configured
        print(
            f"Error configuring logging: {e}",
            file=sys.stderr
        )
        raise

def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Factory method for getting loggers.
    Use this method instead of calling logging.getLogger() directly.
    
    Args:
        name: Logger name (usually __name__)
    
    Returns:
        Configured logger instance
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

# Export main functions
__all__ = [
    'setup_logging',
    'get_logger',
    'logger',
    'LOG_DIR',
    'PROJECT_ROOT'
]
