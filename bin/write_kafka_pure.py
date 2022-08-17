#!/usr/bin/env python

# A pure-confluent_kafka version (no kafkit)

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import enum
import json
import time

from confluent_kafka import Producer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

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

    with concurrent.futures.ThreadPoolExecutor() as pool:
        loop = asyncio.get_running_loop()

        # Create the topic
        await loop.run_in_executor(
            pool,
            kafkaprototype.blocking_create_topics,
            [topic_info.kafka_name],
            "broker:29092",
        )

        # Register the schema and create a serializer
        registry = SchemaRegistryClient(dict(url="http://schema-registry:8081"))

        if True:
            print("Register the schema")
            schema = Schema(json.dumps(avro_schema), "AVRO")
            # Is explicitly registering the schema necessary?
            # Allegedly AvroSerializer does this (though it does
            # not seem to return the ID of the schema).
            schema_id = await loop.run_in_executor(
                pool, registry.register_schema, topic_info.avro_subject, schema
            )
            print(f"schema_id={schema_id}; subject={topic_info.avro_subject}")
        print("Create a serializer")
        # Run in the background because it might register the schema
        serializer = await loop.run_in_executor(
            pool, AvroSerializer, registry, json.dumps(avro_schema)
        )
        print("Create a producer")
        producer = Producer({"acks": acks, "bootstrap.servers": "broker:29092"})
        topic_name = topic_info.kafka_name
        serialization_context = SerializationContext(
            topic_info.kafka_name, MessageField.VALUE
        )

        async def write_1(pool, data_dict):
            """This is a bit ugly, but it works."""
            future = loop.create_future()
            raw_data = serializer(data_dict, serialization_context)

            def blocking_write_1(raw_data):
                def callback(err, _):
                    if err:
                        loop.call_soon_threadsafe(
                            future.set_exception, KafkaException(err)
                        )
                    else:
                        loop.call_soon_threadsafe(future.set_result, None)

                producer.produce(topic_name, raw_data, on_delivery=callback)
                producer.flush()

            await loop.run_in_executor(pool, blocking_write_1, raw_data)
            await future

        async def write_2(pool, data_dict):
            """This is cleaner, but creates concurrent.futures.Future()
            directly, which is explicitly not recommended.

            Also I see occasional high max latency -- a bit more often
            than using write_1.
            """

            def blocking_write_2(raw_data):
                cfuture = concurrent.futures.Future()

                def callback(err, _):
                    if err:
                        cfuture.set_exception(KafkaException(err))
                    else:
                        cfuture.set_result(None)

                producer.produce(topic_name, raw_data, on_delivery=callback)
                producer.flush()
                return asyncio.wrap_future(cfuture, loop=loop)

            raw_data = serializer(data_dict)
            await loop.run_in_executor(pool, blocking_write_2, raw_data)

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
            await write_1(pool, send_data_dict)
        dt = time.time() - t0
        print(f"Wrote {args.number/dt:0.1f} messages/second: {args}")

    # Give time for the reader to finish,
    # to simplify copying timing from the terminal.
    await asyncio.sleep(1)


asyncio.run(main())
