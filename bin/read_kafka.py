#!/usr/bin/env python

from __future__ import annotations

import argparse
import asyncio
import enum
import time
import types

import aiohttp
from aiokafka import AIOKafkaConsumer
from kafkit.registry.aiohttp import RegistryApi
from kafkit.registry import Deserializer
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
    parser.add_argument("topic", help="Topic attribute name, e.g. evt_summaryState")
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
        "--postprocess",
        choices=POST_PROCESS_DICT.keys(),
        default="pydantic",
        help="How to handle the received data",
    )
    args = parser.parse_args()
    if args.time and args.number == 1:
        raise ValueError("You must specify --number > 1 with --time")
    print(f"Parsing info for component {args.component}")
    component_info = kafkaprototype.ComponentInfo(args.component)
    print(f"Obtaining info for topic {args.topic}")
    topic_info = component_info.topics[args.topic]
    Model = topic_info.create_pydantic_model()
    DataClass = topic_info.create_dataclass()
    avro_schema = topic_info.create_avro_schema()
    print("avro_schema=", avro_schema)
    post_process = POST_PROCESS_DICT[args.postprocess]
    delays = []
    with aiohttp.TCPConnector(limit_per_host=20) as connector:
        http_session = aiohttp.ClientSession(connector=connector)
        print("Create RegistryApi")
        registry = RegistryApi(url="http://schema-registry:8081", session=http_session)
        print("Create a deserializer")
        deserializer = Deserializer(registry=registry)
        print("Create a consumer")
        async with AIOKafkaConsumer(
            topic_info.kafka_name,
            bootstrap_servers="broker:29092",
        ) as consumer:
            print("Try to read late-joiner date")
            try:
                partitions = consumer._subscription.assigned_partitions()
                partition = list(partitions)[0]
                position = await consumer.position(partition)
                consumer.seek(partition=partition, offset=position - 1)
                raw_data = await consumer.getone()
                full_data = await deserializer.deserialize(raw_data.value)
                data_dict = full_data["message"]
                print(
                    f"Read late-joiner data with private_seqNum={data_dict['private_seqNum']}, "
                    f"private_sndStamp={data_dict['private_sndStamp']}"
                )
            except Exception as e:
                print(f"Failed to read late-joiner data: {e!r}")
            print("Reading new data")
            i = 0
            async for raw_data in consumer:
                i += 1
                full_data = await deserializer.deserialize(raw_data.value)
                data_dict = full_data["message"]
                current_tai = time.time()
                data_dict["private_rcvStamp"] = current_tai
                delays.append(current_tai - data_dict["private_sndStamp"])
                if post_process == PostProcess.NONE:
                    pass
                elif post_process == PostProcess.DATACLASS:
                    processed_data = DataClass(**data_dict)
                elif post_process == PostProcess.PYDANTIC:
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
