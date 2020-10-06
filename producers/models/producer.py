"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
            "SCHEMA_REGISTRY_URL": "http://localhost:8081",
            "BROKER_URL": "PLAINTEXT://localhost:9092",
        }

        self.client = AdminClient(
            {"bootstrap.servers": self.broker_properties.get("BROKER_URL")}
        )

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        schema_registry = CachedSchemaRegistryClient(
            self.broker_properties.get("SCHEMA_REGISTRY_URL")
        )

        self.producer = AvroProducer(
            {"bootstrap.servers": self.broker_properties.get("BROKER_URL")},
            schema_registry=schema_registry,
            default_value_schema=self.value_schema,
            default_key_schema=self.key_schema,
        )

    def topic_exists(self):
        """Check if the given topic exists"""
        topics_metadata = self.client.list_topics()
        print(topics_metadata.topics.get(self.topic_name))
        return topics_metadata.topics.get(self.topic_name) is not None

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        if self.topic_exists():
            return

        futures = self.client.create_topics(
            [
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas,
                    config={"compression.type": "lz4"},
                )
            ]
        )

        for _topic, future in futures.items():
            try:
                future.result()
                print("topic " + self.topic_name + " created.")
            except Exception as e:
                print(f"failed to create topic {self.topic_name}: {e}")
                raise

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        topics_metadata = self.client.list_topics()
        for existing_topic in Producer.existing_topics:
            topic = topics_metadata.topics.get(existing_topic)
            topic.close()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
