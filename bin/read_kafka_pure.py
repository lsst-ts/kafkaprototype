#!/usr/bin/env python

from __future__ import annotations

# A pure-confluent_kafka version (no kafkit)

import argparse
import asyncio
import base64
import concurrent.futures
import json

# import collections
import enum
import os
import time
import types

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

import numpy as np

import kafkaprototype


class PostProcess(enum.Enum):
    NONE = enum.auto()
    DATACLASS = enum.auto()
    PYDANTIC = enum.auto()
    SIMPLE_NAMESPACE = enum.auto()


POST_PROCESS_DICT = {item.name.lower(): item for item in PostProcess}


async def main() -> None:
    parser = argparse.ArgumentParser(
        "Read and print messages for one topic of one SAL component to Kafka."
    )
    parser.add_argument("component", help="SAL component name")
    parser.add_argument(
        "topic",
        nargs="+",
        help="Topic attribute names, e.g. evt_summaryState cmd_start",
    )
    parser.add_argument(
        "-n",
        "--number",
        type=int,
        default=10,
        help="Number of messages to read; 0 for no limit.",
    )
    parser.add_argument(
        "-t",
        "--time",
        action="store_true",
        help="Measure the elapsed time. This requires number > 1.",
    )
    parser.add_argument(
        "--max_history_read",
        type=int,
        default=1000,
        help="The max number of historical samples to read for indexed SAL components.",
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=1,
        help="The number of partitions per topic.",
    )
    parser.add_argument(
        "--postprocess",
        choices=POST_PROCESS_DICT.keys(),
        default="dataclass",
        help="How to handle the received data",
    )
    args = parser.parse_args()
    if args.time and args.number == 1:
        raise ValueError("You must specify --number > 1 with --time")
    print(f"Parsing info for component {args.component}")
    component_info = kafkaprototype.ComponentInfo(args.component)
    print(f"Obtaining info for topic {args.topic}")
    topic_infos = [component_info.topics[sal_name] for sal_name in args.topic]
    models = {
        topic_info.attr_name: topic_info.make_pydantic_model()
        for topic_info in topic_infos
    }
    data_classes = {
        topic_info.attr_name: topic_info.make_dataclass() for topic_info in topic_infos
    }
    post_process = POST_PROCESS_DICT[args.postprocess]
    delays = []
    with concurrent.futures.ThreadPoolExecutor() as pool:
        loop = asyncio.get_running_loop()

        print("Create Registry")
        registry = SchemaRegistryClient(dict(url="http://schema-registry:8081"))

        # Dict of schema_id: TopicInfo
        print("Register schemas and make deserializers")
        # Dict of Kafka topic name: deserializer, serialization context
        topic_deserializers_contexts = dict()
        for topic_info in topic_infos:
            avro_schema = topic_info.make_avro_schema()
            schema = Schema(json.dumps(avro_schema), "AVRO")
            schema_id = await loop.run_in_executor(
                pool, registry.register_schema, topic_info.avro_subject, schema
            )
            print(f"schema_id={schema_id} for subject={topic_info.avro_subject}")
            serialization_context = SerializationContext(
                topic_info.kafka_name, MessageField.VALUE
            )
            deserializer = AvroDeserializer(registry, json.dumps(avro_schema))

            topic_deserializers_contexts[topic_info.kafka_name] = (
                deserializer,
                serialization_context,
            )

        all_topics = [topic_info.kafka_name for topic_info in topic_infos]

        # Create all topics
        await loop.run_in_executor(
            pool, kafkaprototype.blocking_create_topics, all_topics, "broker:29092"
        )

        random_str = base64.urlsafe_b64encode(os.urandom(12)).decode().replace("=", "_")
        consumer = Consumer(
            {"group.id": random_str, "bootstrap.servers": "broker:29092"}
        )
        consumer.subscribe(all_topics)

        def blocking_read():
            while True:
                message = consumer.poll(timeout=0.1)
                if message is not None:
                    error = message.error()
                    if error is not None:
                        raise error
                    return message

        loop = asyncio.get_running_loop()
        with concurrent.futures.ThreadPoolExecutor() as pool:
            i = 0
            while True:
                message = await loop.run_in_executor(pool, blocking_read)
                deserializer, context = topic_deserializers_contexts[message.topic()]
                raw_data = message.value()
                i += 1
                data_dict = deserializer(raw_data, context)
                current_tai = time.time()
                data_dict["private_rcvStamp"] = current_tai
                delays.append(current_tai - data_dict["private_sndStamp"])
                if post_process == PostProcess.NONE:
                    pass
                elif post_process == PostProcess.DATACLASS:
                    DataClass = data_classes[topic_info.attr_name]
                    processed_data = DataClass(**data_dict)
                elif post_process == PostProcess.PYDANTIC:
                    Model = models[topic_info.attr_name]
                    processed_data = Model(**data_dict)
                elif post_process == PostProcess.SIMPLE_NAMESPACE:
                    processed_data = types.SimpleNamespace(**data_dict)
                else:
                    raise RuntimeError("Unsupported value of post_process")
                if not args.time:
                    print(f"read [{i}]: {processed_data!r}")
                if args.number > 0 and i >= args.number:
                    break
                # Don't start timing until the first message is processed,
                # to avoid delays in starting the producer
                # (and then to measure full read and process cycles).
                if i == 1:
                    t0 = time.time()

            dt = time.time() - t0
            if args.time:
                delays = np.array(delays)
                print(f"Read {(i-1)/dt:0.1f} messages/second: {args}")
                print(
                    f"Delay mean = {delays.mean():0.3f}, stdev = {delays.std():0.3f}, "
                    f"min = {delays.min():0.3f}, max = {delays.max():0.3f} seconds"
                )


asyncio.run(main())
