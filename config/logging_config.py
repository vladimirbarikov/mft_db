"""
Logging configuration module for MFT Database project.

This module provides comprehensive logging configuration with support for:
- Multiple log formats (simple, verbose, JSON, Airflow-specific)
- File rotation based on size
- Dedicated log directories for different components
- Custom JSON formatter for structured logging (ELK/Logstash compatible)
- Airflow task filtering and formatting
- Hierarchical logger configuration for different application modules

Key components:
1. CustomJsonFormatter - JSON-structured log output with custom fields support
2. AirflowTaskFilter - Adds DAG/task context to Airflow-related logs
3. TimeBasedFilter - Filters logs based on time of day (for scheduled DAGs)
4. LOGGING_CONFIG - Comprehensive dictionary configuration for logging
5. setup_logging() - Main initialization function (auto-called on package import)
6. get_logger() - Factory function for obtaining configured loggers

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

Log directories created:
    - logs/
        - app_logs/         : Main application logs (DEBUG+)
        - airflow_logs/     : Airflow-specific logs (INFO+/WARNING+)
        - api_logs/         : API endpoint logs (INFO+)
        - database_logs/    : Database-related operations (INFO+)
        - json_logs/        : JSON-formatted logs for analysis (INFO+)
        - error_logs/       : ALL ERROR-ONLY logs (ERROR and above from ALL sources)

Configuration features:
    - Console output: INFO+ to stdout, WARNING+ to stderr
    - File output for Airflow: WARNING+ between DAG runs, INFO+ during DAG execution
    - Centralized error log: ALL WARNING+ messages from ALL sources
    - File rotation: Automatic based on file size (5-20 MB)
    - Encoding: UTF-8 for all files
    - Thread/process safety: All handlers are thread-safe
    - Error handling: Graceful fallback if configuration fails

Version: 1.0.0
Compatibility: Python 3.12.3
Maintainer: PLD Engineering Center
Created: 2026-02-16
Last Modified: 2026-02-16
License: MIT
Status: Production
"""
# Standard library imports
from pathlib import Path
import sys
import logging
import logging.config
import logging.handlers  # Required for RotatingFileHandler in dictConfig
import json
from typing import Any, Dict, Optional
from datetime import datetime

# Third-party imports
import pytz

# Define project root directory
PROJECT_ROOT = Path(__file__).parents[1]

# Define main log directory and subdirectories
LOG_DIR = PROJECT_ROOT / "logs"
AIRFLOW_LOG_DIR = LOG_DIR / "airflow_logs"
API_LOG_DIR = LOG_DIR / "api_logs"
APP_LOG_DIR = LOG_DIR / "app_logs"
DATABASE_LOG_DIR = LOG_DIR / "database_logs"
JSON_LOG_DIR = LOG_DIR / "json_logs"
ERROR_LOG_DIR = LOG_DIR / "error_logs"

# Create all log directories if they don't exist
def create_log_directories():
    """Create all necessary log directories"""
    directories = [
        LOG_DIR,
        AIRFLOW_LOG_DIR,
        API_LOG_DIR,
        APP_LOG_DIR,
        DATABASE_LOG_DIR,
        JSON_LOG_DIR,
        ERROR_LOG_DIR
    ]

    for directory in directories:
        directory.mkdir(exist_ok=True)

    return directories

# Create directories
create_log_directories()

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

class TimeBasedFilter(logging.Filter):
    """Filter logs based on time of day for scheduled DAGs"""

    def __init__(self, name='', log_window_hours=2, dag_start_hour=3, timezone='Europe/Moscow'):
        """
        Args:
            name: Filter name
            log_window_hours: The length of the logging window in hours
            dag_start_hour: The DAG launch hour (in the specified time zone)
            timezone: Time zone (default is Europe/Moscow)
        """
        super().__init__(name)
        self.log_window_hours = log_window_hours
        self.dag_start_hour = dag_start_hour
        self.timezone = pytz.timezone(timezone)

    def filter(self, record):
        # Always skip WARNING and higher
        if record.levelno >= logging.WARNING:
            return True

        # Checking if the message is related to our DAG
        is_our_dag = False
        dag_id = getattr(record, 'dag_id', '')

        if 'mft_etl_pipeline' in str(dag_id):
            is_our_dag = True
        elif 'mft' in record.name.lower() or 'etl' in record.name.lower():
            is_our_dag = True

        if not is_our_dag:
            return True  # Skip all logs for other DAGs

        # Current time in the specified time zone
        current_time = datetime.now(self.timezone)
        current_hour = current_time.hour

        # DAG starts at 3:00 a.m., we give you a interval of 2 hours to complete
        log_end_hour = (self.dag_start_hour + self.log_window_hours) % 24

        # Check if it is the DAG execution interval
        if self.dag_start_hour <= current_hour < log_end_hour:
            return True  # Logging INFO can be created in the execution interval
        elif self.dag_start_hour > log_end_hour:  # The interval goes through midnight
            if current_hour >= self.dag_start_hour or current_hour < log_end_hour:
                return True

        # Outside the execution interval the INFO logs are prohibited
        return False

def get_log_file_path(directory: Path, filename: str) -> str:
    """Get full path to log file in specific directory"""
    return str(directory / filename)

# Main logging configuration
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "filters": {
        "airflow_task_filter": {
            "()": AirflowTaskFilter,
        },
        "time_based_filter": {
            "()": TimeBasedFilter,
            "log_window_hours": 2,  # 2 hours interval after DAG launch (3:00-5:00)
            "dag_start_hour": 3,  # 3:00 AM
            "timezone": "Europe/Moscow"  # Moscow time
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
        # Console handlers
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "simple",
            "stream": "ext://sys.stdout",
            "filters": ["airflow_task_filter"]
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
        },

        # Airflow-specific logs (in the airfow_logs directory)
        "airflow_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "INFO",
            "formatter": "airflow",
            "filename": get_log_file_path(AIRFLOW_LOG_DIR, f"airflow_{LOG_ID}.log"),
            "maxBytes": 20 * 1024 * 1024,  # 20 MB
            "backupCount": 10,
            "encoding": "utf8",
            "filters": ["time_based_filter", "airflow_task_filter"]
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

        # Module loggers - УБРАНЫ ссылки на task_file
        "dags": {
            "level": "INFO",
            "handlers": ["console", "app_file", "airflow_file", "error_file"],
            "propagate": False,
            "qualname": "dags"
        },

        "dags.tasks": {
            "level": "DEBUG",
            "handlers": ["console", "app_file", "airflow_file", "error_file"],
            "propagate": False,
        },

        "dags.tasks.extractor": {
            "level": "INFO",
            "handlers": ["console", "app_file", "airflow_file", "error_file"],
            "propagate": False,
        },

        "dags.tasks.transformer": {
            "level": "INFO",
            "handlers": ["console", "app_file", "airflow_file", "error_file"],
            "propagate": False,
        },

        "dags.tasks.connector": {
            "level": "DEBUG",
            "handlers": ["console", "app_file", "database_file", "airflow_file", "error_file"],
            "propagate": False,
        },

        "dags.tasks.loader": {
            "level": "INFO",
            "handlers": ["console", "app_file", "database_file", "airflow_file", "error_file"],
            "propagate": False,
        },

        # API loggers
        "endpoints": {
            "level": "INFO",
            "handlers": ["console", "api_file", "error_file"],
            "propagate": False,
        },

        "endpoints.display_api": {
            "level": "INFO",
            "handlers": ["console", "api_file", "error_file"],
            "propagate": False,
        },

        "endpoints.modify_api": {
            "level": "INFO",
            "handlers": ["console", "api_file", "error_file"],
            "propagate": False,
        },

        "endpoints.upload_api": {
            "level": "DEBUG",
            "handlers": ["console", "api_file", "error_file"],
            "propagate": False,
        },

        "endpoints.user_manager_api": {
            "level": "INFO",
            "handlers": ["console", "api_file", "error_file"],
            "propagate": False,
        },

        # Database loggers
        "database": {
            "level": "INFO",
            "handlers": ["console", "database_file", "error_file"],
            "propagate": False,
        },

        # Airflow loggers
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

        # SQLAlchemy loggers
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

        # Third-party loggers
        "urllib3": {
            "level": "WARNING",
            "handlers": ["app_file", "error_file"],
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

def setup_logging(airflow_log_level: str = "WARNING") -> None:
    """
    Initialize logging configuration.
    Should be called once at application startup.
    
    Args:
        airflow_log_level: Logging level for Airflow components between DAG runs
    """

    try:
        # Setting up the logging level for Airflow components
        LOGGING_CONFIG["loggers"]["airflow"]["level"] = airflow_log_level
        LOGGING_CONFIG["loggers"]["airflow.processor"]["level"] = airflow_log_level
        LOGGING_CONFIG["loggers"]["airflow.scheduler"]["level"] = airflow_log_level
        LOGGING_CONFIG["loggers"]["airflow.models.dagbag"]["level"] = airflow_log_level

        # First configure basic logging for error handling
        logging.basicConfig(
            level=logging.WARNING,
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
        init_logger.info("Main log directory: %s", LOG_DIR)
        init_logger.info("Log directories created:")
        init_logger.info("  - Airflow logs: %s", AIRFLOW_LOG_DIR)
        init_logger.info("  - API logs: %s", API_LOG_DIR)
        init_logger.info("  - App logs: %s", APP_LOG_DIR)
        init_logger.info("  - Database logs: %s", DATABASE_LOG_DIR)
        init_logger.info("  - JSON logs: %s", JSON_LOG_DIR)
        init_logger.info("  - Error logs (CENTRALIZED): %s", ERROR_LOG_DIR)
        init_logger.info("Log ID: %s", LOG_ID)
        init_logger.info("Airflow log level (between DAG runs): %s", airflow_log_level)
        init_logger.info("DAG execution window: 3:00-5:00 Moscow time")
        init_logger.info("Central error log collects ALL ERROR and higher messages from ALL sources")
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

# Helper function to get directory paths
def get_log_directories() -> Dict[str, Path]:
    """Get all log directory paths"""
    return {
        'main': LOG_DIR,
        'airflow': AIRFLOW_LOG_DIR,
        'api': API_LOG_DIR,
        'app': APP_LOG_DIR,
        'database': DATABASE_LOG_DIR,
        'json': JSON_LOG_DIR,
        'error': ERROR_LOG_DIR
    }

# Create global logger for this module
logger = get_logger(__name__)

# Export main functions and directories
__all__ = [
    'setup_logging',
    'get_logger',
    'get_log_directories',
    'logger',
    'LOG_DIR',
    'AIRFLOW_LOG_DIR',
    'API_LOG_DIR',
    'APP_LOG_DIR',
    'DATABASE_LOG_DIR',
    'JSON_LOG_DIR',
    'ERROR_LOG_DIR',
    'PROJECT_ROOT'
]
