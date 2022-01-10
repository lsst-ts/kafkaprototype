from __future__ import annotations

__all__ = ["ComponentInfo"]

import typing
from xml.etree import ElementTree

import lsst.ts.xml
from .topic_info import make_ackcmd_topic_info, TopicInfo

if typing.TYPE_CHECKING:
    import pydantic


class ComponentInfo:
    """Information about one SAL component.

    Parameters
    ----------
    name : `str`
        SAL component name.

    Attributes
    ----------
    name : str
        SAL component name
    description : str
        Description, from SALSubsystems.xml
    indexed : bool
        Is this component indexed (can there be different instances
        with different SAL indices)?
    added_generics : List [str]
        Added generic categories and/or SAL topic names
        (e.g. command_enterControl).
        This comes from the AddedGenerics field of SALSubsystems.xml
    topics : dict [str, TopicInfo], optional
        Dict of attr_name: TopicInfo.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self._set_basics()
        self.topics = self._make_topics()

    def make_avro_schema_dict(self) -> typing.Dict[str, typing.Dict[str, typing.Any]]:
        """Create a dict of topic attr name: Avro schema dict."""
        return {
            topic_info.attr_name: make_avro_schema()
            for topic_info in self.topics.values()
        }

    def make_pydantic_model_dict(self) -> typing.Dict[str, pydantic.Model]:
        """Create a dict of topic attr name: pydantic Model."""
        return {
            topic_info.attr_name: make_pydantic_model()
            for topic_info in self.topics.values()
        }

    def _get_topic_elements(self) -> typing.List[ElementTree.Element]:
        """Get component-specific topic elements.

        The ``name`` attribute must have been set.
        """
        interfaces_dir = lsst.ts.xml.get_sal_interfaces_dir()
        topic_elts: typing.List[ElementTree.Element] = []
        for topic_type in ("Command", "Event", "Telemetry"):
            file_topic_type = {
                "Command": "Commands",
                "Event": "Events",
                "Telemetry": "Telemetry",
            }.get(topic_type)
            filepath = interfaces_dir / self.name / f"{self.name}_{file_topic_type}.xml"
            if not filepath.is_file():
                continue
            topic_root = ElementTree.parse(filepath).getroot()
            topic_elts += topic_root.findall(f"SAL{topic_type}")
        return topic_elts

    def _make_topics(self) -> typing.Dict[str, TopicInfo]:
        """Set the ``topics`` attribute.

        The ``name``, ``indexed``, and ``added_generics`` fields must be
        set before calling this.
        """
        # Dict of topic_name: topic element
        # The topic name has no subsystem prefix,
        # e.g. logevent_logMessage
        generic_topic_element_dict, generic_category_dict = parse_sal_generics()
        component_topic_elements = self._get_topic_elements()

        # Dict of topic name: topic element
        topic_element_dict: typing.Dict[str, ElementTree.Element] = dict()

        def add_topic(elt):
            topic_name = elt.find("EFDB_Topic").text
            topic_name = topic_name.split("_", 1)[1]
            if topic_name in topic_element_dict:
                raise RuntimeError(f"topic {topic_name} already found")
            topic_element_dict[topic_name] = elt

        for generic_name in self.added_generics:
            if "_" in generic_name:
                # generic_name is a topic name
                topic_elt = generic_topic_element_dict.get(generic_name)
                if topic_elt is None:
                    raise ValueError(f"Unrecognized generic topic name {generic_name}")
                add_topic(topic_elt)
            else:
                # generic_name is a category name
                generic_elt_list = generic_category_dict.get(generic_name)
                if generic_elt_list is None:
                    raise ValueError(f"Unrecognized generic category {generic_name}")
                for elt in generic_elt_list:
                    add_topic(elt)

        for topic_elt in component_topic_elements:
            add_topic(topic_elt)

        topics_list: typing.List[TopicInfo] = [
            TopicInfo.from_xml_element(
                topic_element, component_name=self.name, indexed=self.indexed
            )
            for topic_element in topic_element_dict.values()
        ]
        topics_list.append(
            make_ackcmd_topic_info(component_name=self.name, indexed=self.indexed)
        )
        return {info.attr_name: info for info in topics_list}

    def _set_basics(self) -> None:
        """Parse SALSubystems.xml and set the added_generics, indexed,
        and description fields

        The ``name`` field must be set before calling this.
        """
        interfaces_dir = lsst.ts.xml.get_sal_interfaces_dir()
        subsystems_root = ElementTree.parse(
            interfaces_dir / "SALSubsystems.xml"
        ).getroot()
        elements = [
            elt for elt in subsystems_root if elt.find("Name").text == self.name
        ]
        if not elements:
            raise ValueError(f"No such component {self.name}")
        if len(elements) > 1:
            raise ValueError(f"Multiple components found with Name={self.name!r}")
        element = elements[0]

        self.description = element.find("Description").text
        self.added_generics = ["mandatory"] + [
            item.strip() for item in element.find("AddedGenerics").text.split(",")
        ]
        self.indexed = element.find("IndexEnumeration").text.strip() != "no"


def parse_sal_generics():
    """Parse SALGenerics.xml.

    Return two dicts:
    * topic_element_dict: topic name: XML topic element
        where topic name is missing the SALGeneric_ prefix
    * category_dict: category: list of XML topic elements
    """
    interfaces_dir = lsst.ts.xml.get_sal_interfaces_dir()
    generics = ElementTree.parse(interfaces_dir / "SALGenerics.xml").getroot()

    category_dict = dict()
    topic_element_dict = dict()
    for gen in generics.findall("*/"):
        topic_name = gen.find("EFDB_Topic").text
        brief_name = topic_name[len("SALGeneric_") :]
        topic_element_dict[brief_name] = gen
        category_elt = gen.find("Category")
        if category_elt is None:
            continue
        category = category_elt.text
        if category not in category_dict:
            category_dict[category] = []
        category_dict[category].append(gen)
    return topic_element_dict, category_dict
