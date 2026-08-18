"""
Change Classifier Module.

Provides centralized logic for classifying changes by domain and nature.
Used by both BP pipeline (automatic changes) and API (manual changes).

CLASSIFICATION AXES:
    - Domain: WHAT changed (supplier, packaging, production, spec, config, multi)
    - Nature: WHY it changed (business, technical, correction)

Version: 1.0.0
Created: 2026-08-18
Updated: 2026-08-18 - Import constants from columns_config.py
"""
from typing import Dict, Any, Tuple

# ============================================================================
# IMPORTS FROM COLUMNS_CONFIG (Single Source of Truth)
# ============================================================================

from config.columns_config import (
    CHANGE_DOMAINS,
    CHANGE_NATURES,
    FIELD_TO_DOMAIN,
)


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
        domain = ChangeClassifier._determine_domain(changes)
        nature = ChangeClassifier._determine_nature(changes, current_attrs)

        return domain, nature

    @staticmethod
    def _determine_domain(changes: Dict[str, Any]) -> str:
        """Determine change domain based on changed fields."""
        domains = set()

        for field in changes.keys():
            if field in FIELD_TO_DOMAIN:
                domains.add(FIELD_TO_DOMAIN[field])

        if len(domains) == 0:
            return CHANGE_DOMAINS['MULTI']
        elif len(domains) == 1:
            return domains.pop()
        else:
            return CHANGE_DOMAINS['MULTI']

    @staticmethod
    def _determine_nature(
        changes: Dict[str, Any],
        current_attrs: Dict[str, Any]
    ) -> str:
        """Determine change nature based on changes and current state."""

        # Check for corrections (typo fixes)
        if ChangeClassifier._is_correction(changes, current_attrs):
            return CHANGE_NATURES['CORRECTION']

        # Check for technical changes (minor adjustments)
        if ChangeClassifier._is_technical(changes, current_attrs):
            return CHANGE_NATURES['TECHNICAL']

        # Default: business change
        return CHANGE_NATURES['BUSINESS']

    @staticmethod
    def _is_correction(
        changes: Dict[str, Any],
        current_attrs: Dict[str, Any]
    ) -> bool:
        """Check if changes look like correction (typo, formatting)."""
        for field, new_value in changes.items():
            old_value = current_attrs.get(field)

            # Only string fields
            if not isinstance(old_value, str) or not isinstance(new_value, str):
                continue

            # Same length, small difference → likely typo
            if len(old_value) == len(new_value):
                diff_count = sum(1 for a, b in zip(old_value, new_value) if a != b)
                if diff_count <= 2:
                    return True

            # Case-only change → correction
            if old_value.lower() == new_value.lower():
                return True

        return False

    @staticmethod
    def _is_technical(
        changes: Dict[str, Any],
        current_attrs: Dict[str, Any]
    ) -> bool:
        """Check if changes are technical (minor adjustments)."""
        for field, new_value in changes.items():
            old_value = current_attrs.get(field)

            # Weight change < 0.1 kg → technical
            if field == 'part_weight_kg':
                try:
                    if abs(float(new_value) - float(old_value)) < 0.1:
                        return True
                except (ValueError, TypeError):
                    pass

            # Localization formatting → technical
            if field == 'localization':
                if str(old_value).lower() == str(new_value).lower():
                    return True

        return False

    @staticmethod
    def get_domain_from_fields(fields: list) -> str:
        """Get domain from list of field names."""
        domains = set()
        for field in fields:
            if field in FIELD_TO_DOMAIN:
                domains.add(FIELD_TO_DOMAIN[field])

        if len(domains) == 0:
            return CHANGE_DOMAINS['MULTI']
        elif len(domains) == 1:
            return domains.pop()
        else:
            return CHANGE_DOMAINS['MULTI']
