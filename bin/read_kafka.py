#!/usr/bin/env python

from __future__ import annotations

import argparse
import asyncio
import collections
import enum
import time
import types
import typing

import aiohttp
from aiokafka import AIOKafkaConsumer, TopicPartition
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
        "--max_history_read",
        type=int,
        default=1000,
        help="The max number of historical samples to read for indexed SAL components.",
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
    with aiohttp.TCPConnector(limit_per_host=20) as connector:
        http_session = aiohttp.ClientSession(connector=connector)
        print("Create RegistryApi")
        registry = RegistryApi(url="http://schema-registry:8081", session=http_session)
        print("Create a deserializer")
        deserializer = Deserializer(registry=registry)
        print("Create a consumer")

        # Dict of schema_id: TopicInfo
        schema_id_read_topics: typing.Dict[int, kafkaprototype.TopicInfo] = dict()
        for topic_info in topic_infos:
            avro_schema = topic_info.make_avro_schema()
            schema_id = await registry.register_schema(
                schema=avro_schema, subject=topic_info.avro_subject
            )
            schema_id_read_topics[schema_id] = topic_info

        all_topics = [topic_info.kafka_name for topic_info in topic_infos]
        async with AIOKafkaConsumer(
            *all_topics, bootstrap_servers="broker:29092"
        ) as consumer:
            partition_lists = collections.defaultdict(list)
            for partition in consumer.assignment():
                partition_lists[partition.topic].append(partition)

            print("Try to read historical date")
            try:
                t0 = time.time()
                # Historical data is wanted for one or more topics.

                # List of historical data to print
                # (after timing how long extraction took).
                historical_data_list: typing.Dict[str, typing.Any] = []

                # Dict of topic attr_name: TopicPartition
                topic_partitions: typing.Dict[str, TopicPartition] = dict()
                for topic_info in topic_infos:
                    partition_ids = consumer.partitions_for_topic(topic_info.kafka_name)
                    # Handling multiple partitions is too much effort
                    if len(partition_ids) > 1:
                        print(
                            f"More than one partition for {topic_info.kafka_name}; "
                            "cannot get historical data"
                        )
                        continue
                    partition_id = list(partition_ids)[0]
                    topic_partitions[topic_info.attr_name] = TopicPartition(
                        topic_info.kafka_name, partition_id
                    )

                if not component_info.indexed:
                    print(
                        "Component is not indexed; read historical data the fast and easy way"
                    )
                    # Non-indexed SAL components are easy:
                    # just set the index back one for each
                    # topic for which we want historical data.
                    # There is no need to read the data here;
                    # let the main read loop handle it.
                    for attr_name, partition in topic_partitions.items():
                        position = await consumer.position(partition)
                        if position == 0:
                            print(f"No historical data available for {attr_name}")
                            continue

                        consumer.seek(partition=partition, offset=max(0, position - 1))

                        raw_data = await consumer.getone(partition)
                        full_data = await deserializer.deserialize(raw_data.value)
                        data_dict = full_data["message"]
                        historical_data_list.append(data_dict)
                else:
                    print(
                        "Component is indexed. Read historical data the slow and complicated way."
                    )
                    # Indexed SAL components are harder because we
                    # want the most recent value for each index
                    # (if found within max_history_read messages).
                    #
                    # For each topic:
                    # * Set the position way back
                    # * Read all messages, accumulating them
                    #   in a dict of [SAL index: data].
                    # * Feed the data to the ReadTopic

                    # Read max_history_read messages and save
                    # the most recent value seen for each index
                    for attr_name, partition in topic_partitions.items():
                        position = await consumer.position(partition)
                        if position == 0:
                            print(f"No historical data available for {attr_name}")
                            continue

                        # Dict of SAL index: historical data dict
                        historical_data_dicts: typing.Dict[
                            int, typing.Dict[str, typing.Any]
                        ] = dict()

                        consumer.seek(
                            partition=partition,
                            offset=max(0, position - args.max_history_read),
                        )

                        end_position = position - 1
                        while True:
                            raw_data = await consumer.getone(partition)
                            full_data = await deserializer.deserialize(raw_data.value)
                            data_dict = full_data["message"]
                            historical_data_dicts[
                                data_dict["private_index"]
                            ] = data_dict

                            if raw_data.offset == end_position:
                                # All done
                                break

                        historical_data_list += list(historical_data_dicts.values())

                dt = time.time() - t0

                for data_dict in historical_data_list:
                    if component_info.indexed:
                        print(
                            f"Read historical data for {attr_name}: "
                            f"private_index={data_dict['private_index']}, "
                            f"private_seqNum={data_dict['private_seqNum']}, "
                            f"private_sndStamp={data_dict['private_sndStamp']}, "
                            f"data age={t0 - data_dict['private_sndStamp']:0.2f}"
                        )
                    else:
                        print(
                            f"Read historical data for {attr_name}: "
                            f"private_seqNum={data_dict['private_seqNum']}, "
                            f"private_sndStamp={data_dict['private_sndStamp']}, "
                            f"data age={t0 - data_dict['private_sndStamp']:0.2f}"
                        )

                print(f"Reading historic data took {dt:0.2f} seconds")
            except Exception as e:
                print(f"Failed to read historical data: {e!r}")
            if args.number == 0:
                return
            print("Reading new data")

            i = 0
            async for raw_data in consumer:
                i += 1
                full_data = await deserializer.deserialize(raw_data.value)
                schema_id = full_data["id"]
                topic_info = schema_id_read_topics[schema_id]
                data_dict = full_data["message"]
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
