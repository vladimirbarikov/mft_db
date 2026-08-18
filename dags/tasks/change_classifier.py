# pylint: disable=too-many-lines
# pylint: disable=wrong-import-position
"""
Change Classifier Module.

Provides centralized logic for classifying changes by domain and nature.
Used by both BP pipeline (automatic changes) and API (manual changes).

CLASSIFICATION AXES:
    - Domain: WHAT changed (supplier, packaging, production, spec, config, multi)
    - Nature: WHY it changed (business, technical, correction)

Version: 1.0.0
Compatibility: Python 3.14.4+
Maintainer: PLD Engineering Center
Created: 2026-08-18
Last Modified: 2026-08-18
License: MIT
Status: Production
"""
from pathlib import Path
import sys
from typing import Dict, Any, Tuple

# The relative path to the root project directory
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Add the project path to sys.path
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Local imports
from config import get_logger
from config.columns_config import (
    CHANGE_DOMAINS,
    CHANGE_NATURES,
    FIELD_TO_DOMAIN,
)

# Logger setup
logger = get_logger(__name__)


class ChangeClassifier:
    """
    Centralized classifier for change domain and nature.
    
    Usage:
        domain, nature = ChangeClassifier.classify(changes, current_attrs)
    """

    @staticmethod
    def classify(
        changes: Dict[str, Any],
        current_attrs: Dict[str, Any]
    ) -> Tuple[str, str]:
        """
        Classify changes into domain and nature.
        
        Args:
            changes: Dict of changed fields with new values
            current_attrs: Dict of current attribute values
            
        Returns:
            Tuple[str, str]: (domain, nature)
        """
        try:
            if not changes:
                logger.debug("Empty changes dict, defaulting to MULTI/BUSINESS")
                return CHANGE_DOMAINS['MULTI'], CHANGE_NATURES['BUSINESS']

            if not current_attrs:
                logger.debug("Empty current_attrs dict, proceeding with empty attrs")
                current_attrs = {}

            domain = ChangeClassifier._determine_domain(changes)
            nature = ChangeClassifier._determine_nature(changes, current_attrs)
            return domain, nature

        except (ValueError, TypeError, AttributeError) as e:
            logger.error(
                "Expected error during classification: %s. "
                "Changes: %s, Current attrs: %s",
                e, changes, current_attrs
            )
            return CHANGE_DOMAINS['MULTI'], CHANGE_NATURES['BUSINESS']
        except Exception as unexpected_error:
            logger.error(
                "Unexpected error during classification: %s. "
                "Changes: %s, Current attrs: %s",
                unexpected_error, changes, current_attrs,
                exc_info=True
            )
            return CHANGE_DOMAINS['MULTI'], CHANGE_NATURES['BUSINESS']

    @staticmethod
    def _determine_domain(changes: Dict[str, Any]) -> str:
        """Determine change domain based on changed fields."""
        try:
            domains = set()

            for field in changes.keys():
                try:
                    if field in FIELD_TO_DOMAIN:
                        domains.add(FIELD_TO_DOMAIN[field])
                except (KeyError, ValueError, TypeError) as e:
                    logger.debug(
                        "Error processing field '%s' for domain: %s. Skipping.",
                        field, e
                    )
                    continue

            if len(domains) == 0:
                return CHANGE_DOMAINS['MULTI']
            elif len(domains) == 1:
                return domains.pop()
            else:
                return CHANGE_DOMAINS['MULTI']

        except (KeyError, ValueError, TypeError, AttributeError) as e:
            logger.error(
                "Expected error determining domain: %s. Changes: %s",
                e, changes
            )
            return CHANGE_DOMAINS['MULTI']
        except Exception as unexpected_error:
            logger.error(
                "Unexpected error determining domain: %s. Changes: %s",
                unexpected_error, changes,
                exc_info=True
            )
            return CHANGE_DOMAINS['MULTI']

    @staticmethod
    def _determine_nature(
        changes: Dict[str, Any],
        current_attrs: Dict[str, Any]
    ) -> str:
        """
        Determine change nature based on changes and current state.
        
        Simplified: only distinguishes corrections (typos) from business changes.
        Technical changes are not automatically detected.
        """
        try:
            # Check for corrections (typo fixes, formatting)
            try:
                if ChangeClassifier._is_correction(changes, current_attrs):
                    return CHANGE_NATURES['CORRECTION']
            except (ValueError, TypeError, AttributeError) as e:
                logger.debug(
                    "Error checking correction: %s. Continuing.",
                    e
                )
            except Exception as unexpected_error:
                logger.warning(
                    "Unexpected error checking correction: %s. Continuing.",
                    unexpected_error
                )

            # Default: business change
            return CHANGE_NATURES['BUSINESS']

        except (KeyError, ValueError, TypeError, AttributeError) as e:
            logger.error(
                "Expected error determining nature: %s. Changes: %s, Current attrs: %s",
                e, changes, current_attrs
            )
            return CHANGE_NATURES['BUSINESS']
        except Exception as unexpected_error:
            logger.error(
                "Unexpected error determining nature: %s. Changes: %s, Current attrs: %s",
                unexpected_error, changes, current_attrs,
                exc_info=True
            )
            return CHANGE_NATURES['BUSINESS']

    @staticmethod
    def _is_correction(
        changes: Dict[str, Any],
        current_attrs: Dict[str, Any]
    ) -> bool:
        """
        Check if changes look like correction (typo, formatting).
        
        Returns:
            bool: True if changes are corrections, False otherwise
        """
        try:
            for field, new_value in changes.items():
                try:
                    old_value = current_attrs.get(field)

                    # Only string fields, skip None
                    if old_value is None or new_value is None:
                        continue

                    if not isinstance(old_value, str) or not isinstance(new_value, str):
                        continue

                    # Case-only change → correction
                    try:
                        if old_value.lower() == new_value.lower():
                            logger.debug(
                                "Correction detected (case only): '%s' → '%s'",
                                old_value, new_value
                            )
                            return True
                    except (ValueError, TypeError, AttributeError) as e:
                        logger.debug(
                            "Error comparing lowercased values for field '%s': %s",
                            field, e
                        )
                        continue

                except (KeyError, ValueError, TypeError, AttributeError) as e:
                    logger.debug(
                        "Error processing field '%s' in correction check: %s",
                        field, e
                    )
                    continue
                except Exception as unexpected_error:
                    logger.warning(
                        "Unexpected error processing field '%s' in correction check: %s",
                        field, unexpected_error
                    )
                    continue

            return False

        except (KeyError, ValueError, TypeError, AttributeError) as e:
            logger.error(
                "Expected error in _is_correction: %s. Changes: %s, Current attrs: %s",
                e, changes, current_attrs
            )
            return False
        except Exception as unexpected_error:
            logger.error(
                "Unexpected error in _is_correction: %s. Changes: %s, Current attrs: %s",
                unexpected_error, changes, current_attrs,
                exc_info=True
            )
            return False

    @staticmethod
    def get_domain_from_fields(fields: list) -> str:
        """
        Get domain from list of field names.
        
        Args:
            fields: List of field names
            
        Returns:
            str: Domain name or 'multi'
        """
        try:
            if not fields:
                logger.debug("Empty fields list, defaulting to MULTI")
                return CHANGE_DOMAINS['MULTI']

            domains = set()
            for field in fields:
                try:
                    if field in FIELD_TO_DOMAIN:
                        domains.add(FIELD_TO_DOMAIN[field])
                except (KeyError, ValueError, TypeError) as e:
                    logger.debug(
                        "Error processing field '%s' in get_domain_from_fields: %s",
                        field, e
                    )
                    continue

            if len(domains) == 0:
                return CHANGE_DOMAINS['MULTI']
            elif len(domains) == 1:
                return domains.pop()
            else:
                return CHANGE_DOMAINS['MULTI']

        except (KeyError, ValueError, TypeError, AttributeError) as e:
            logger.error(
                "Expected error in get_domain_from_fields: %s. Fields: %s",
                e, fields
            )
            return CHANGE_DOMAINS['MULTI']
        except Exception as unexpected_error:
            logger.error(
                "Unexpected error in get_domain_from_fields: %s. Fields: %s",
                unexpected_error, fields,
                exc_info=True
            )
            return CHANGE_DOMAINS['MULTI']


# ============================================================================
# PUBLIC INTERFACE
# ============================================================================

__all__ = [
    'ChangeClassifier',
]
