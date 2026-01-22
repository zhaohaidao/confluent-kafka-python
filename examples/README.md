The scripts in this directory provide code examples using Confluent's Python client:

* [adminapi.py](adminapi.py): collection of Kafka Admin API operations
* [avro-cli.py](avro-cli.py): produces Avro messages with Confluent Schema Registry and then reads them back again 
* [consumer.py](consumer.py): reads messages from a Kafka topic
* [producer.py](producer.py): reads lines from stdin and sends them to Kafka
* [red_consumer.py](red_consumer.py): reads messages from a Kafka topic using Red Kafka wrapper
* [red_producer.py](red_producer.py): produces messages to a Kafka topic using Red Kafka wrapper
* [red_roundtrip.py](red_roundtrip.py): produces and consumes a bounded set of messages using Red Kafka wrapper

EDS bootstrap usage:
Set `bootstrap.servers` to `eds://<service-name>` and export the required env vars:
`XHS_ENV`, `XHS_SERVICE`, `XHS_REGION`, `XHS_ZONE`, `EDS_HTTP_HOST`.

Additional examples for [Confluent Cloud](https://www.confluent.io/confluent-cloud/):

* [confluent_cloud.py](confluent_cloud.py): produces messages to Confluent Cloud and then reads them back again
* [confluentinc/examples](https://github.com/confluentinc/examples/tree/master/clients/cloud/python): integrates Confluent Cloud and Confluent Cloud Schema Registry
