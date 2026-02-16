"""
The logging configuration package for the Material Flow Table Database project.
"""

from .logging_config import (
    setup_logging,
    get_logger,
    logger,
    LOG_DIR,
    PROJECT_ROOT
)

# Automatic initialization of logging when importing a package
setup_logging()

__all__ = [
    'setup_logging',
    'get_logger',
    'logger',
    'LOG_DIR',
    'PROJECT_ROOT'
]

logger.info("The configuration package is loaded. Logs in: %s", LOG_DIR)
