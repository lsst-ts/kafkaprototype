from __future__ import annotations

__all__ = ["FieldInfo"]

import dataclasses
import typing
from xml.etree import ElementTree

import pydantic


def find_optional(element: ElementTree.Element, name: str, default: str) -> str:
    """Find an optional element in an XML element.

    Parameters
    ----------
    element : ElementTree.Element
        XML element
    name : str
        Field name.
    default : str
        Value to return if the field does not exist.
    """
    subelt = element.find(name)
    if subelt is None:
        return default
    return subelt.text


# Dict of SAL type: python type
PYTHON_TYPES = {
    "boolean": bool,
    "byte": int,
    "short": int,
    "int": int,
    "long": int,
    "long long": int,
    "unsigned short": int,
    "unsigned int": int,
    "unsigned long": int,
    "float": float,
    "double": float,
    "string": str,
}

# Dict of SAL type: Avro type
AVRO_TYPES = {
    "boolean": "boolean",
    "byte": "int",
    "short": "int",
    "int": "int",
    "long": "long",
    "long long": "long",
    "unsigned short": "int",
    "unsigned int": "int",
    "unsigned long": "long",
    "float": "float",
    "double": "double",
    "string": "string",
}


@dataclasses.dataclass
class FieldInfo:
    """Information about one field of a topic.

    Parameters
    ----------
    name : str
        Field name
    sal_type : str
        SAL data type.
    nelts : int
        For lists: the fixed list length.
    max_len : int
        For strings: the max length, or 0 for no limit.
    units : str
        Units, "unitless" if none.
    description : str
        Description (arbitrary text)

    Attributes
    ----------
    default_scalar_value : typing.Any
        For a scalar: the default value.
        For an array: the default value of one element.
    """

    name: str
    sal_type: str
    nelts: int = 1
    max_len: int = 1
    units: str = "unitless"
    description: str = ""
    default_scalar_value: typing.Any = dataclasses.field(init=False)

    def __post_init__(self):
        if self.sal_type == "string":
            if self.nelts > 1:
                raise ValueError(
                    f"nelts={self.nelts} > 1, but string fields cannot be arrays"
                )
        else:
            if self.max_len > 1:
                raise ValueError(f"max_len={self.max_len} can only be > 1 for strings")
        python_type = PYTHON_TYPES[self.sal_type]
        self.default_scalar_value = python_type()

    @classmethod
    def from_xml_element(
        cls, element: ElementTree.Element, is_indexed: bool
    ) -> FieldInfo:
        """Construct a FieldInfo from an XML element."""
        name = element.find("EFDB_Name").text
        description = find_optional(element, "Description", "")
        nelts = int(find_optional(element, "Count", "1"))
        units = find_optional(element, "Units", "unitless")
        sal_type = element.find("IDL_Type").text
        max_len = 0
        return FieldInfo(
            name=name,
            sal_type=sal_type,
            nelts=nelts,
            max_len=max_len,
            units=units,
            description=description,
        )

    def create_pydantic_arg(self) -> typing.Tuple[typing.Type, typing.Any]:
        """Return (dtype, pydantic.Field or scalar default).

        The result can be used as an argument value
        for a field in pydantic.create_model.

        The focus is on validation, not metadata, so the result omits
        sal_type, description, and units.
        """
        scalar_type = PYTHON_TYPES[self.sal_type]
        if self.nelts > 1:
            dtype = typing.List[scalar_type]
            field = pydantic.Field(
                default_factory=lambda: [self.default_scalar_value] * self.nelts,
                min_items=self.nelts,
                max_items=self.nelts,
            )
        else:
            dtype = scalar_type
            if self.max_len > 1:
                field = pydantic.Field(
                    self.default_scalar_value, max_length=self.max_len
                )
            else:
                field = self.default_scalar_value
        return (dtype, field)

    def create_dataclass_tuple(
        self,
    ) -> typing.Tuple[str, typing.Type, dataclasses.field]:
        """Create field data for dataclasses.make_dataclasses."""
        scalar_type = PYTHON_TYPES[self.sal_type]
        if self.nelts > 1:
            dtype = typing.List[scalar_type]
            field = dataclasses.field(
                default_factory=lambda: [self.default_scalar_value] * self.nelts
            )
        else:
            dtype = scalar_type
            field = dataclasses.field(default=self.default_scalar_value)
        return (self.name, dtype, field)

    def create_avro_schema(self) -> typing.Dict[str, typing.Any]:
        """Return an Avro schema for this field."""
        scalar_type = AVRO_TYPES[self.sal_type]
        if self.nelts > 1:
            return {
                "name": self.name,
                "type": {"type": "array", "items": scalar_type},
                "default": [self.default_scalar_value] * self.nelts,
            }
        else:
            return {
                "name": self.name,
                "type": scalar_type,
                "default": self.default_scalar_value,
            }
