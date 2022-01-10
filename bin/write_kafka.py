#!/usr/bin/env python

from __future__ import annotations

import argparse
import asyncio
import enum
import time

import aiohttp
from aiokafka import AIOKafkaProducer
from kafkit.registry.aiohttp import RegistryApi
from kafkit.registry import Serializer

import kafkaprototype


class ValidationType(enum.Enum):
    NONE = enum.auto()
    CUSTOM = enum.auto()
    DATACLASS = enum.auto()
    DATACLASS_AND_DECODE = enum.auto()
    PYDANTIC = enum.auto()
    PYDANTIC_AND_DECODE = enum.auto()


VALIDATION_DICT = {item.name.lower(): item for item in ValidationType}


async def main() -> None:
    parser = argparse.ArgumentParser(
        "Write messages for one topic of one SAL component to Kafka"
    )
    parser.add_argument("component", help="SAL component name")
    parser.add_argument("topic", help="Topic attribute name, e.g. evt_summaryState")
    parser.add_argument(
        "-n", "--number", type=int, default=1, help="Number of messages to write"
    )
    parser.add_argument(
        "--index",
        type=int,
        default=0,
        help="SAL index; ignored for non-indexed components",
    )
    parser.add_argument(
        "--nowait_ack", action="store_true", help="Wait for ack from Kafka (safer)?"
    )
    parser.add_argument(
        "--validation", choices=VALIDATION_DICT.keys(), default="dataclass"
    )
    args = parser.parse_args()
    validation = VALIDATION_DICT[args.validation]
    print(f"Parsing info for component {args.component}")
    component_info = kafkaprototype.ComponentInfo(args.component)
    print("Topics =", list(component_info.topics.keys()))
    print(f"Obtaining info for topic {args.topic}")
    topic_info = component_info.topics[args.topic]
    Model = topic_info.make_pydantic_model()
    DataClass = topic_info.make_dataclass()
    avro_schema = topic_info.make_avro_schema()
    print("avro_schema=", avro_schema)
    acks = 0 if args.nowait_ack else 1
    print("acks=", acks)

    # Create non-default data for all fields
    default_data_dict = Model().dict()
    data_dict = default_data_dict.copy()
    for name, value in data_dict.items():
        if isinstance(value, list):
            if isinstance(value[0], int):
                value = [1] * len(value)
            elif isinstance(value[0], float):
                value = [1.1] * len(value)
            else:
                raise RuntimeError(f"Unexpected array type for {name}: {value!r}")
        elif isinstance(value, int):
            value = 1
        elif isinstance(value, float):
            value = 1.1
        elif isinstance(value, str):
            value = "a short string"
        else:
            raise RuntimeError(f"Unexpected scalar type for {name}: {value!r}")
        data_dict[name] = value

    with aiohttp.TCPConnector(limit_per_host=20) as connector:
        http_session = aiohttp.ClientSession(connector=connector)
        print("Create RegistryApi")
        registry = RegistryApi(url="http://schema-registry:8081", session=http_session)
        print("Register the schema")
        schema_id = await registry.register_schema(
            schema=avro_schema, subject=topic_info.avro_subject
        )
        print(f"schema_id={schema_id}")
        print("Create a serializer")
        serializer = Serializer(schema=avro_schema, schema_id=schema_id)
        print("Create a producer")
        async with AIOKafkaProducer(
            bootstrap_servers="broker:29092",
            acks=acks,
            value_serializer=serializer,
        ) as producer:
            print("Publish data")
            t0 = time.time()
            for i in range(args.number):
                data_dict["private_seqNum"] = i + 1
                data_dict["private_sndStamp"] = time.time()
                if component_info.indexed:
                    data_dict["private_index"] = args.index
                send_data_dict = data_dict
                if validation == ValidationType.NONE:
                    pass
                elif validation == ValidationType.CUSTOM:
                    topic_info.validate_data(data_dict)
                elif validation == ValidationType.DATACLASS:
                    DataClass(**data_dict)
                elif validation == ValidationType.DATACLASS_AND_DECODE:
                    model = DataClass(**data_dict)
                    # Note: dataclasses.asdict is much slower than vars
                    # send_data_dict = dataclasses.asdict(model)
                    send_data_dict = vars(model)
                elif validation == ValidationType.PYDANTIC:
                    Model(**data_dict)
                elif validation == ValidationType.PYDANTIC_AND_DECODE:
                    model = Model(**data_dict)
                    send_data_dict = model.dict()
                else:
                    raise RuntimeError("Unsupported option")
                await producer.send_and_wait(
                    topic_info.kafka_name, value=send_data_dict
                )
            dt = time.time() - t0
            print(f"Wrote {args.number/dt:0.1f} messages/second: {args}")
    # Give time for the reader to finish,
    # to simplify copying timing from the terminal.
    await asyncio.sleep(1)


asyncio.run(main())
