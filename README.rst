Prototype for reading and writing SAL topics using Kafka.

Installation
------------

Install the following required packages:

* aiohttp
* aiokafka
* kafkit
* ts_xml  (can be managed by ups)

If using ups then set up this package::

    setup -r .

User Guide
----------

To read and write SAL messages:

1) Git clone kafka-aggregator, cd into its root directory, and run the following:
docker-compose up -d zookeeper broker schema-registry kafdrop

This will download and run several docker images. The hostname and the port for each service is what you can use connect to, for example:
* Schema registry: http://schema-registry:8081 inside the docker environment or http://localhost:8081 in the schema registry Docker image.
* Broker: broker:29092  inside the docker environment or localhost:2902 in the broker Docker image. Note that 29092 is not the default port, which is 9092.

2) Run an lsst-dev Docker image with
  --network kafka-aggregator_default
  so it can see the kafka-aggregator images.

3) Run a reader in the background or in a separate login to lsst-dev.

Both command-line executables take these arguments:
* The SAL component name (e.g. Test or MTM1M3)
* The topic attribute name (e.g. evt_summaryState).
  The reader can read multiple topics, but the writer only writes one.
* The -n/--number option to specify the number of reads/writes.
* The -h/--help option to print help.

The writer also supports:
* --validation to test different kinds of validation.
  The "_and_decode" versions create the data class and then extract data as a dict from it.
* --nowait_ack: if specified the writer does not wait for an ack from the broker.
  This is considered unsafe, but it allows writing fast enough to stress the reader.
  (I also never saw a dropped message.)

The writer always sends a full data packet with non-default values,
to avoid Kafka saving bandwidth by omitting default values.

The reader also supports:
* -t/--time to time how long a set of reads takes (the writer always reports that).
  It is a separate option because the results are only be meaningful if you write
  at least that many messages at once.
  If you omit -t the reader will print each message it reads.
* --postprocess test the performance of different kinds of data classes to hold the result.


Here is an example:

read_kafka.py MTM1M3 tel_forceActuatorData -n 1000 -t --postprocess=simple_namespace &
# wait for it to start, then...
write_kafka.py MTM1M3 tel_forceActuatorData -n 1000 --validation=custom
