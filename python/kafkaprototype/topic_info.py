from __future__ import annotations

__all__ = ["make_ackcmd_topic_info", "TopicInfo"]

import dataclasses
import typing
from xml.etree import ElementTree

import pydantic

from .field_info import FieldInfo, find_optional


_PRIVATE_FIELD_LIST = [
    FieldInfo(
        name="private_index",
        description="SAL index (only present for indexed SAL components)",
        nelts=1,
        sal_type="long",
        units="unitless",
    ),
    FieldInfo(
        name="private_sndStamp",
        description="Time of instance publication",
        nelts=1,
        sal_type="double",
        units="second",
    ),
    FieldInfo(
        name="private_rcvStamp",
        description="Time of instance reception",
        nelts=1,
        sal_type="double",
        units="second",
    ),
    FieldInfo(
        name="private_seqNum",
        description="Sequence number",
        nelts=1,
        sal_type="long",
        units="unitless",
    ),
    FieldInfo(
        name="private_identity",
        description="Identity of publisher: "
        "SAL component name for a CSC or user@host for a user",
        nelts=1,
        sal_type="string",
        units="unitless",
    ),
    FieldInfo(
        name="private_origin",
        description="Process ID of publisher",
        nelts=1,
        sal_type="long",
        units="unitless",
    ),
]
# Dict of field_name: FieldInfo for all private fields
# (including private_index, which is only present for indexed components).
PRIVATE_FIELDS = {info.name: info for info in _PRIVATE_FIELD_LIST}

_ACKCMD_FIELDS_LIST = [
    FieldInfo(
        name="ack",
        description="Acknowledgement code",
        nelts=1,
        sal_type="long",
        units="unitless",
    ),
    FieldInfo(
        name="error",
        description="An error code; only relevant if ack=FAILED",
        nelts=1,
        sal_type="long",
        units="unitless",
    ),
    FieldInfo(
        name="result",
        description="Message",
        nelts=1,
        max_len=256,
        sal_type="string",
        units="unitless",
    ),
    FieldInfo(
        name="identity",
        description="private_identity field of the command being acknowledged",
        nelts=1,
        sal_type="long",
        units="unitless",
    ),
    FieldInfo(
        name="origin",
        description="private_origin field of the command being acknowledged",
        nelts=1,
        sal_type="long",
        units="unitless",
    ),
    FieldInfo(
        name="cmdtype",
        description="Index of command in alphabetical list of commands, with 0 being the first",
        nelts=1,
        sal_type="long",
        units="unitless",
    ),
    FieldInfo(
        name="timeout",
        description="Estimated remaining duration of command; only relevant if ack=INPROGRESS",
        nelts=1,
        sal_type="double",
        units="second",
    ),
]
# Dict of field_name: FieldInfo for the public fields of the ackcmd topic
ACKCMD_FIELDS = {info.name: info for info in _ACKCMD_FIELDS_LIST}


def make_ackcmd_topic_info(component_name, indexed):
    """Make an ackcmd topic for a given component.

    Parameters
    ----------
    component_name : str
        SAL component name, e.g. MTMount
    indexed : str
        Is this component indexed?

    Returns
    -------
    topic_info : TopicInfo
        Info for the ackcmd topic.
    """
    fields = PRIVATE_FIELDS.copy()
    if not indexed:
        del fields["private_index"]
    fields.update(ACKCMD_FIELDS)
    return TopicInfo(
        component_name=component_name,
        sal_name="ackcmd",
        fields=fields,
        description="Command acknowledgement",
    )


class TopicInfo:
    """Information about one topic.

    Parameters
    ----------
    component_name : str
        SAL component name
    sal_name : str
        SAL topic name, e.g. logevent_summaryState
    fields : Dict[str, FieldInfo]
        Dict of field name: field info
    description : str, optional
        Description of topic.

    Attributes
    ----------
    attr_name : str
        Topic name used by salobj for attributes, e.g. evt_summaryState
    kafka_name : str
        Topic name used by Kafka
    avro_subject : str
        Subject name for Avro schema
    fields : Dict [str, FieldInfo]
        Dict of field name: field info
    array_fields : Dict [str, int]
        Dict of field name: array length for array fields
    str_fields : Dict[str, int]
        Dict of field name: str length for string fields
        with a specified max length (other than 1, meaning any length).
    """

    def __init__(
        self,
        component_name: str,
        sal_name: str,
        fields: typing.Dict[str, FieldInfo],
        description: str = "",
    ) -> None:
        self.component_name = component_name
        self.sal_name = sal_name
        self.fields = fields
        self.description = description

        if sal_name == "ackcmd":
            attr_name = "ackcmd"
        else:
            split_name = sal_name.split("_", 1)
            if len(split_name) == 1:
                attr_name = "tel_" + self.sal_name
            elif len(split_name) == 2:
                prefix, brief_name = split_name
                attr_prefix = {"logevent": "evt_", "command": "cmd_"}[prefix]
                attr_name = attr_prefix + brief_name
            else:
                raise RuntimeError(f"Cannot parse SAL topic name {self.sal_name!r}")
        self.attr_name = attr_name
        self.kafka_name = f"lsst.sal.{self.component_name}.{self.sal_name}"
        self.avro_subject = f"{self.kafka_name}-value"
        array_fields = dict()
        str_fields = dict()
        for field_info in self.fields.values():
            if field_info.nelts > 1:
                array_fields[field_info.name] = field_info.nelts
            elif field_info.max_len > 1:
                str_fields[field_info.name] = field_info.max_len
        self.array_fields = array_fields
        self.str_fields = str_fields

    @classmethod
    def from_xml_element(
        cls, element: ElementTree.Element, component_name: str, indexed: bool
    ) -> TopicInfo:
        """Construct a TopicInfo from a topic XML element.

        The result includes all info for all fields, including private ones.

        Parameters
        ----------
        element : ElementTree.Element
            XML topic element; an of SALCommand, SALEvent, or SALTelemetry.
        component_name : str
            SAL component name, e.g. MTMount
        indexed : str
            Is this component indexed?
        """
        full_name = element.find("EFDB_Topic").text
        sal_name = full_name.split("_", 1)[1]
        description = find_optional(element, "Description", "")

        fields = PRIVATE_FIELDS.copy()
        if not indexed:
            del fields["private_index"]
        for field_element in element.findall("item"):
            field_info = FieldInfo.from_xml_element(field_element, indexed=indexed)
            if field_info.name in fields:
                raise RuntimeError(f"field {field_info.name} already found.")
            fields[field_info.name] = field_info
        return cls(
            component_name=component_name,
            sal_name=sal_name,
            description=description,
            fields=fields,
        )

    def make_pydantic_model(self):
        """Create a pydantic Model.

        The name of the model is self.attr_name.

        The focus is on validation, not on metadata,
        so returned data is missing the topic description
        and all field descriptions and units.
        """
        kwargs = {
            field_info.name: field_info.make_pydantic_arg()
            for field_info in self.fields.values()
        }
        model = pydantic.create_model(self.attr_name, **kwargs)
        # Forbid extra keywords when creating a model
        model.__config__.extra = pydantic.Extra.forbid
        return model

    def make_dataclass(self):
        """Create a dataclass."""
        field_args = [
            field_info.make_dataclass_tuple() for field_info in self.fields.values()
        ]

        def validate(
            model,
            fields=self.fields.keys(),
            array_fields=self.array_fields,
            str_fields=self.str_fields,
        ):
            bad_arrays = [
                field_name
                for field_name in fields & array_fields
                if len(getattr(model, field_name)) != array_fields[field_name]
            ]
            if bad_arrays:
                raise ValueError(f"Array fields with incorrect length: {bad_arrays}")
            bad_strs = [
                field_name
                for field_name in fields & str_fields
                if len(getattr(model, field_name)) > str_fields[field_name]
            ]
            if bad_strs:
                raise ValueError(f"Str fields with incorrect length: {bad_strs}")

        return dataclasses.make_dataclass(
            self.attr_name, field_args, namespace={"__post_init__": validate}
        )

    def make_avro_schema(self):
        """Create an avro schema."""
        return {
            "type": "record",
            "name": self.sal_name,
            "namespace": f"lsst.sal.{self.component_name}",
            "fields": [
                field_info.make_avro_schema() for field_info in self.fields.values()
            ],
        }

    def validate_data(self, data_dict):
        """Perform preliminary validation of a data dict.

        Ignore incorrect data types, since Kafka will catch that.
        This is an attempt at faster validation than pydantic.
        """
        data_fields = data_dict.keys()
        extra_fields = data_fields - self.fields.keys()
        if extra_fields:
            raise ValueError(f"Unrecognized fields {extra_fields}")
        bad_arrays = [
            field_name
            for field_name in data_fields & self.array_fields
            if len(data_dict[field_name]) != self.array_fields[field_name]
        ]
        if bad_arrays:
            raise ValueError(f"Array fields with incorrect length: {bad_arrays}")
        bad_strs = [
            field_name
            for field_name in data_fields & self.str_fields
            if len(data_dict[field_name]) > self.str_fields[field_name]
        ]
        if bad_strs:
            raise ValueError(f"Str fields with incorrect length: {bad_strs}")
