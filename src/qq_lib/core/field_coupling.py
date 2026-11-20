# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab

"""
Utilities for defining and enforcing coupled fields in dataclasses.

This module provides `FieldCoupling` for specifying dominance-ordered
relationships among multiple fields, and the `@coupled_fields` decorator for
automatically enforcing these rules in a dataclass's `__post_init__`.
"""

from typing import Any, ClassVar


class FieldCoupling:
    """
    Represents a coupling among multiple fields, ordered by dominance.

    The earlier a field appears in `fields`, the more dominant it is.
    If multiple fields have values in an instance, only the most dominant
    one is preserved; all others are set to None.
    """

    def __init__(self, *fields: str):
        if len(fields) < 2:
            raise ValueError("FieldCoupling requires at least two fields")
        self.fields = list(fields)

    def contains(self, field_name: str) -> bool:
        """Return True if the field participates in this coupling."""
        return field_name in self.fields

    def get_fields(self) -> tuple[str, ...]:
        """Return all coupled fields as a tuple."""
        return tuple(self.fields)

    def has_value(self, instance: Any) -> bool:
        """Return True if any of the coupled fields has a non-None value."""
        return any(getattr(instance, field) is not None for field in self.fields)

    def get_most_dominant_set_field(self, instance: Any) -> str | None:
        """
        Return the name of the most dominant field that has a non-None value,
        or None if none of them do.
        """
        for field in self.fields:
            if getattr(instance, field) is not None:
                return field

        return None

    def enforce(self, instance: Any):
        """
        Enforce dominance rules: only the most dominant field that is set
        keeps its value; others are reset to None.
        """
        dominant_set_field = self.get_most_dominant_set_field(instance)
        if dominant_set_field is None:
            return

        for field in self.fields:
            if field != dominant_set_field:
                setattr(instance, field, None)


def _enforce_couplings(instance: HasCouplingMethods) -> None:
    """Enforce dominance rules for all couplings of the instance's class."""
    for coupling in instance._field_couplings:
        coupling.enforce(instance)


class HasCouplingMethods:
    """Mixin providing coupling enforcement for classes decorated with @coupled_fields."""

    _field_couplings: ClassVar[tuple[FieldCoupling, ...]] = ()

    @classmethod
    def get_coupling_for_field(cls, field_name: str) -> FieldCoupling | None:
        """Return the FieldCoupling that contains the given field name, or None."""
        for coupling in cls._field_couplings:
            if coupling.contains(field_name):
                return coupling

        return None

    def __post_init__(self) -> None:
        """Enforce dominance rules for all couplings of the class."""
        _enforce_couplings(self)


def coupled_fields(*couplings: FieldCoupling):
    """
    Class decorator that attaches multi-field coupling rules to a class.
    The class must inherit from `HasCouplingMethods`.

    If the class defines its own `__post_init__`, it is wrapped so that
    the coupling rules are enforced first.
    """

    def decorator(cls):
        cls._field_couplings = couplings

        own_post_init = cls.__dict__.get("__post_init__")
        if own_post_init is not None:

            def __post_init__(self: HasCouplingMethods) -> None:
                _enforce_couplings(self)
                own_post_init(self)

            cls.__post_init__ = __post_init__

        return cls

    return decorator
