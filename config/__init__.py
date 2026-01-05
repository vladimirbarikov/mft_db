# config/__init__.py
"""
Пакет конфигурации проекта MFT Database.
"""

from .logging_config import (
    setup_logging,
    get_logger,
    logger,
    LOG_DIR,
    PROJECT_ROOT
)

# Автоматическая инициализация логирования при импорте пакета
setup_logging()

__all__ = [
    'setup_logging',
    'get_logger',
    'logger',
    'LOG_DIR',
    'PROJECT_ROOT'
]

logger.info("Пакет конфигурации загружен. Логи в: %s", LOG_DIR)
