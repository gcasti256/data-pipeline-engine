"""Data validation primitives: schema checks, custom rules, and dead letter routing."""

from __future__ import annotations

from pipeline_engine.validation.dead_letter import DeadLetterEntry, DeadLetterQueue
from pipeline_engine.validation.rules import (
    CustomRule,
    NotNullRule,
    RangeRule,
    RegexRule,
    RuleSet,
    ValidationRule,
)
from pipeline_engine.validation.schema import SchemaValidator, ValidationResult

__all__ = [
    # Schema validation
    "SchemaValidator",
    "ValidationResult",
    # Custom rules
    "ValidationRule",
    "RangeRule",
    "NotNullRule",
    "RegexRule",
    "CustomRule",
    "RuleSet",
    # Dead letter queue
    "DeadLetterQueue",
    "DeadLetterEntry",
]
