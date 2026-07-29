"""
Logging configuration module for MFT Database project.
Optimized for containerized deployment with Grafana Loki.

This module provides logging configuration for Docker container environments where:
- All logs are written to stdout (JSON format for Loki ingestion)
- No local log files are created on the host filesystem
- Airflow task context is preserved for structured logging
- Multiple log formats are supported (console, JSON for Loki)

Key components:
1. CustomJsonFormatter - JSON-structured log output compatible with Loki
2. AirflowTaskFilter - Adds DAG/task context to Airflow-related logs
3. LOGGING_CONFIG - Dictionary configuration with only console handlers
4. get_logger() - Factory function for obtaining configured loggers

Architecture decisions:
- All logs go to stdout/stderr (no file-based logging)
- JSON format for structured logging (Loki ingestion ready)
- Single source of truth: container logs collected by Promtail
- No log files on host system (security requirement)
- Logs stored in Docker volumes, not accessible to users

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

Version: 2.0.0
Compatibility: Python 3.12.3, Airflow 3.0+
Maintainer: PLD Engineering Center
Created: 2026-02-16
Last Modified: 2026-07-28
License: MIT
Status: Production
"""

# ====== STANDARD IMPORTS ======
import sys
import logging
import logging.config
import json
from typing import Any, Dict, Optional
from datetime import datetime, timezone
import zoneinfo

# ====== CONSTANTS AND SETTINGS ======
# Time settings
MOSCOW_TZ = zoneinfo.ZoneInfo("Europe/Moscow")


# ====== FORMATTERS ======
class MoscowTimeFormatter(logging.Formatter):
    """Formatter that converts timestamps to Moscow timezone."""

    def formatTime(self, record, datefmt=None):
        """
        Convert record creation time to Moscow timezone.

        Args:
            record: Log record containing creation timestamp
            datefmt: Optional date format string

        Returns:
            Formatted timestamp string in Moscow timezone
        """
        moscow_time = datetime.fromtimestamp(record.created, MOSCOW_TZ)
        if datefmt:
            return moscow_time.strftime(datefmt)
        return moscow_time.strftime('%Y-%m-%d %H:%M:%S')


class CustomJsonFormatter(logging.Formatter):
    """
    Custom formatter for JSON logs compatible with Loki.
    
    Produces structured JSON logs that Loki can parse and index.
    Includes Airflow-specific fields when available.
    """

    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as JSON with timestamp in Moscow timezone.

        Args:
            record: Log record to format

        Returns:
            JSON string representation of the log record
        """
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

        # Add Airflow-specific context if available (using getattr for type safety)
        dag_id = getattr(record, 'dag_id', None)
        if dag_id is not None:
            log_record["dag_id"] = dag_id

        task_id = getattr(record, 'task_id', None)
        if task_id is not None:
            log_record["task_id"] = task_id

        run_id = getattr(record, 'run_id', None)
        if run_id is not None:
            log_record["run_id"] = run_id

        # Add exceptions if present
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)

        # Add custom fields if provided
        try:
            custom_fields = getattr(record, 'custom_fields', None)
            if custom_fields:
                if isinstance(custom_fields, dict):
                    try:
                        json.dumps(custom_fields, default=str)
                        log_record.update(custom_fields)
                    except (TypeError, ValueError):
                        log_record["custom_fields_error"] = "Non-serializable data"
                else:
                    log_record["custom_data"] = str(custom_fields)
        except AttributeError:
            pass

        return json.dumps(log_record, ensure_ascii=False, default=str)


# ====== FILTERS ======
class AirflowTaskFilter(logging.Filter):
    """
    Filter that adds DAG/task context to Airflow log records.
    
    Ensures that all log records from Airflow components have
    dag_id, task_id, and run_id attributes for structured logging.
    """

    def filter(self, record) -> bool:
        """
        Add dag_id, task_id and run_id attributes to record.

        Args:
            record: Log record to enhance with Airflow context

        Returns:
            Always returns True to include the record
        """
        if not hasattr(record, 'dag_id'):
            record.dag_id = 'unknown'
        if not hasattr(record, 'task_id'):
            record.task_id = 'unknown'
        if not hasattr(record, 'run_id'):
            record.run_id = 'unknown'
        return True


# ====== LOGGER CONFIGURATION ======
# Main logging configuration dictionary
# All handlers are console-based (stdout/stderr) for container deployment
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
        # Primary handler - JSON format for Loki ingestion
        "console_json": {
            "class": "logging.StreamHandler",
            "level": "DEBUG",
            "formatter": "json",
            "stream": "ext://sys.stdout",
        },
        # Verbose console output for development/debugging
        "console_verbose": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "verbose",
            "stream": "ext://sys.stdout",
        },
        # Simple console output for quick glances
        "console_simple": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "simple",
            "stream": "ext://sys.stdout",
        },
        # Error console - WARNING+ to stderr
        "error_console": {
            "class": "logging.StreamHandler",
            "level": "WARNING",
            "formatter": "verbose",
            "stream": "ext://sys.stderr",
        },
        # Airflow-specific console with task context
        "console_airflow": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "airflow",
            "stream": "ext://sys.stdout",
            "filters": ["airflow_task_filter"],
        },
    },
        "loggers": {
        # Root logger
        "": {
            "level": "DEBUG",
            "handlers": ["console_json"],
            "propagate": False
        },
        # Main application logger
        "__main__": {
            "level": "INFO",
            "handlers": ["console_json"],
            "propagate": False,
        },
        # API endpoint loggers
        "endpoints": {
            "level": "INFO",
            "handlers": ["console_json"],
            "propagate": False,
        },
        "endpoints.mft_upload_api": { "level": "INFO", "propagate": True },
        "endpoints.mft_display_api": { "level": "INFO", "propagate": True },
        "endpoints.mft_modify_api": { "level": "INFO", "propagate": True },
        "endpoints.user_manager_api": { "level": "INFO", "propagate": True },

        # DAG and Task loggers
        "dags": {
            "level": "INFO",
            "handlers": ["console_json"], 
            "propagate": False,
        },
        "dags.bp_dag": { "level": "INFO", "propagate": True },
        "dags.mft_dag": { "level": "INFO", "propagate": True },
        "dags.tasks": {
            "level": "DEBUG",
            "handlers": ["console_json"], 
            "propagate": False,
        },
        "dags.tasks.bp_loader": { "level": "INFO", "propagate": True },
        "dags.tasks.bp_mapper": { "level": "INFO", "propagate": True },
        "dags.tasks.connector": { "level": "DEBUG", "propagate": True },
        "dags.tasks.extractor": { "level": "INFO", "propagate": True },
        "dags.tasks.mft_loader": { "level": "INFO", "propagate": True },
        "dags.tasks.mft_mapper": { "level": "INFO", "propagate": True },
        "dags.tasks.serializer": { "level": "INFO", "propagate": True },
        "dags.tasks.transformer": { "level": "INFO", "propagate": True },

        # Database operations
        "database": {
            "level": "INFO",
            "handlers": ["console_json"],
            "propagate": False,
        },
        # Config module
        "config": {
            "level": "INFO",
            "handlers": ["console_json"],
            "propagate": False,
        },
        # Airflow system loggers
        "airflow": {
            "level": "WARNING",
            "handlers": ["console_json", "error_console"],
            "propagate": False,
        },
        "airflow.task": {
            "level": "INFO",
            "handlers": ["console_json", "error_console"],
            "propagate": False,
        },
        "airflow.processor": {
            "level": "WARNING",
            "handlers": ["console_json", "error_console"],
            "propagate": False,
        },
        "airflow.scheduler": {
            "level": "WARNING",
            "handlers": ["console_json", "error_console"],
            "propagate": False,
        },
        "airflow.models.dagbag": {
            "level": "WARNING",
            "handlers": ["console_json", "error_console"],
            "propagate": False,
        },
        "airflow.executors": {
            "level": "WARNING",
            "handlers": ["console_json", "error_console"],
            "propagate": False,
        },
        # Third-party library loggers
        "sqlalchemy": {
            "level": "WARNING",
            "handlers": ["console_json", "error_console"],
            "propagate": False,
        },
        "sqlalchemy.engine": {
            "level": "INFO",
            "handlers": ["console_json", "error_console"],
            "propagate": False,
        },
        "requests": {
            "level": "WARNING",
            "handlers": ["console_json", "error_console"],
            "propagate": False,
        },
    }
}


# ====== INITIALIZATION ======
# Configure logging on module import
try:
    logging.config.dictConfig(LOGGING_CONFIG)
except (ValueError, TypeError, KeyError) as e:
    print(f"Error configuring logging: {e}", file=sys.stderr)
    logging.basicConfig(level=logging.INFO)
else:
    init_logger = logging.getLogger(__name__)
    init_logger.info("Logging initialized for container environment (stdout only)")
    init_logger.info("All logs are output to stdout/stderr in JSON format")
    init_logger.info("No local log files are created on host filesystem")
    init_logger.info("Logs are collected by Promtail and stored in Loki (Docker volume)")


# ====== PUBLIC INTERFACE ======
def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Factory method for getting loggers.

    Use this method instead of calling logging.getLogger() directly.
    Ensures consistent configuration across all modules.

    Args:
        name: Logger name (usually __name__). If None, uses current module name.

    Returns:
        Configured logger instance with appropriate handlers based on name.

    Example:
        >>> logger = get_logger(__name__)
        >>> logger.info("Module initialized")
    """
    if name is None:
        name = __name__

    logger_instance = logging.getLogger(name)

    # Add Airflow context filter if logger is Airflow-related
    if 'airflow' in name.lower() or 'task' in name.lower() or 'dag' in name.lower():
        logger_instance.addFilter(AirflowTaskFilter())

    return logger_instance


# Create global logger for this module
logger = get_logger(__name__)
