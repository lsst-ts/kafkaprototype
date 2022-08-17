__all__ = ["blocking_create_topics"]

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.error import KafkaError


def blocking_create_topics(topic_names: list[str], broker_addr: str) -> None:
    """Create missing Kafka topics.

    Parameters
    ----------
    topic_names : list[str]
        List of Kafka topic names
    broker_addr : str
        Kafka broker address, for example: "broker:29092"
    """
    # Dict of kafka topic name: topic_info
    new_topics_info = [
        NewTopic(
            topic_name,
            num_partitions=1,
            replication_factor=1,
        )
        for topic_name in topic_names
    ]
    broker_client = AdminClient({"bootstrap.servers": broker_addr})
    if new_topics_info:
        create_result = broker_client.create_topics(new_topics_info)
        for topic_name, future in create_result.items():
            exception = future.exception()
            if exception is None:
                continue
            elif (
                isinstance(exception.args[0], KafkaError)
                and exception.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS
            ):
                pass
            else:
                print(f"Failed to create topic {topic_name}: {exception!r}")
                raise exception
