from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic
from confluent_kafka.error import KafkaError


def create_topics(kakfa_topic_names, kafka_broker_addr):
    """Create Kafka topics.

    Warning: this is a blocking function.

    Parameters
    ----------
    kakfa_topic_names : `list` [`str`]
        List of kafka topic names
    kafka_broker_addr : `str`
        Kafka broker address, e.g. "broker:29092"

    Notes
    -----
    I would prefer to also configure the number of partitions for
    existing topics, but that information is not part of the
    configuration returned by AdminClient.describe_configs.
    Here is an example (where the topic has 2 partitions):

    {
    `compression.type': ConfigEntry(compression.type="producer"),
    'leader.replication.throttled.replicas':
        ConfigEntry(leader.replication.throttled.replicas=""),
    'message.downconversion.enable':
        ConfigEntry(message.downconversion.enable="true"),
    'min.insync.replicas': ConfigEntry(min.insync.replicas="1"),
    'segment.jitter.ms': ConfigEntry(segment.jitter.ms="0"),
    'cleanup.policy': ConfigEntry(cleanup.policy="delete"),
    'flush.ms': ConfigEntry(flush.ms="9223372036854775807"),
    'follower.replication.throttled.replicas':
        ConfigEntry(follower.replication.throttled.replicas=""),
    'segment.bytes': ConfigEntry(segment.bytes="1073741824"),
    'retention.ms': ConfigEntry(retention.ms="604800000"),
    'flush.messages': ConfigEntry(flush.messages="9223372036854775807"),
    'message.format.version': ConfigEntry(message.format.version="2.5-IV0"),
    'file.delete.delay.ms': ConfigEntry(file.delete.delay.ms="60000"),
    'max.compaction.lag.ms':
        ConfigEntry(max.compaction.lag.ms="9223372036854775807"),
    'max.message.bytes': ConfigEntry(max.message.bytes="1048588"),
    'min.compaction.lag.ms': ConfigEntry(min.compaction.lag.ms="0"),
    'message.timestamp.type': ConfigEntry(message.timestamp.type="CreateTime"),
    'preallocate': ConfigEntry(preallocate="false"),
    'min.cleanable.dirty.ratio': ConfigEntry(min.cleanable.dirty.ratio="0.5"),
    'index.interval.bytes': ConfigEntry(index.interval.bytes="4096"),
    'unclean.leader.election.enable':
        ConfigEntry(unclean.leader.election.enable="false"),
    'retention.bytes': ConfigEntry(retention.bytes="-1"),
    'delete.retention.ms': ConfigEntry(delete.retention.ms="86400000"),
    'segment.ms': ConfigEntry(segment.ms="604800000"),
    'message.timestamp.difference.max.ms':
        ConfigEntry(message.timestamp.difference.max.ms="9223372036854775807"),
    'segment.index.bytes': ConfigEntry(segment.index.bytes="10485760")
    }
    """
    # Create missing topics; I'd also suggest modifying the number
    # of partitions of the others, if needed, but that info isn't
    # part of the configuration
    print("Create an admin client")
    admin_client = AdminClient({"bootstrap.servers": kafka_broker_addr})
    resource_list = [
        ConfigResource(restype=ConfigResource.Type.TOPIC, name=name)
        for name in kakfa_topic_names
    ]
    config_future_dict = admin_client.describe_configs(resource_list)
    new_topics_metadata = []
    existing_topic_names = []
    for config, future in config_future_dict.items():
        topic_name = config.name
        exception = future.exception()
        if exception is None:
            existing_topic_names.append(topic_name)
        elif (
            isinstance(exception.args[0], KafkaError)
            and exception.args[0].code() == KafkaError.UNKNOWN_TOPIC_OR_PART
        ):
            new_topics_metadata.append(
                NewTopic(
                    topic_name,
                    num_partitions=1,
                    replication_factor=1,
                )
            )
        else:
            print(f"Unexpected issue with topic {topic_name}: {exception!r}")

    if new_topics_metadata:
        new_topic_names = [metadata.topic for metadata in new_topics_metadata]
        print(f"Create topics: {new_topic_names}")
        fs = admin_client.create_topics(new_topics_metadata)
        for topic_name, future in fs.items():
            try:
                future.result()  # The result itself is None
            except Exception as e:
                print(f"Failed to create topic {topic_name}: {e!r}")
                raise

    if existing_topic_names:
        print(f"These topics already exist: {existing_topic_names}")
