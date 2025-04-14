import argparse
import time
import json
import os
import pandas as pd
from kafka.admin import NewTopic
from kafka import KafkaProducer, KafkaAdminClient
from schema_registry.client import SchemaRegistryClient, schema


OUTPUT_TOPICS = "tracking.raw_user_behavior"
BOOTSTRAP_SERVERS = "broker:9092"
SCHEMA_REGISTRY_SERVER = "http://schema-registry:8081"

parser = argparse.ArgumentParser()
parser.add_argument(
    "-m",
    "--mode",
    default="setup",
    choices=["setup", "teardown"],
    help="Whether to setup or teardown a Kafka topic with driver stats events. Setup will teardown before beginning emitting events.",
)
parser.add_argument(
    "-b",
    "--bootstrap_servers",
    default=BOOTSTRAP_SERVERS,
    help="Where the bootstrap server is",
)
parser.add_argument(
    "-s",
    "--schema_registry_server",
    default=SCHEMA_REGISTRY_SERVER,
    help="Where to host schema",
)
parser.add_argument(
    "-c",
    "--avro_schemas_path",
    default=os.path.join(os.path.dirname(__file__), "avro_schemas"),
    help="Folder containing all generated avro schemas",
)
args = parser.parse_args()


def create_topic(admin, topic_name) -> None:
    """
    Create a topic in Kafka.
    :param admin: Kafka Admin client
    :param topic_name: Name of the topic to create
    """
    try:
        topic = NewTopic(topic_name, num_partitions=12, replication_factor=1)
        admin.create_topics([topic])
        print(f"Topic {topic_name} created successfully.")
    except Exception:
        print(f"Topic {topic_name} already exists.")


def create_streams(servers, avro_schemas_path, schema_registry_client):
    producer = None
    admin = None

    # Retry logic for creating Kafka producer and admin client
    for _ in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=servers,
                value_serializer=str.encode,  # Simple string encoding
                batch_size=16384,  # Increase batch size (default 16384)
                buffer_memory=33554432,  # 32MB buffer memory
                compression_type="gzip",  # Enable compression
                linger_ms=50,  # Wait up to 50ms to batch messages
                acks=1,  # Only wait for leader acknowledgment
            )
            admin = KafkaAdminClient(bootstrap_servers=servers)
            print("SUCCESS: Kafka producer and admin client created.")
            break
        except Exception as e:
            print(f"Error creating Kafka producer: {e}")
            print("Retrying in 10 seconds...")
            time.sleep(10)

    # Retry logic for schema registry
    for _ in range(10):
        try:
            schema_registry_client.get_subjects()
            print("SUCCESS: Schema registry client created.")
            break
        except Exception as e:
            print(f"Error creating schema registry client: {e}")
            print("Retrying in 10 seconds...")
            time.sleep(10)
    else:
        print("ERROR: Failed to create schema registry client after 10 attempts.")

    # Load avro schema
    avro_schemas_path = f"{avro_schemas_path}/ecommerce_events.avsc"
    with open(avro_schemas_path, "r") as f:
        avro_schema = json.loads(f.read())

    # Load data and prepare for batch processing
    try:
        df = pd.read_parquet("data/sample.parquet")
        print(f"Loaded {len(df)} rows from parquet file.")
        # Pre-format all records for json serialization

        def format_record(row):
            index = row.name
            record = {
                "event_time": str(row["event_time"]),
                "event_type": str(row["event_type"]),
                "product_id": int(row["product_id"]),
                "category_id": int(row["category_id"]),
                "category_code": str(row["category_code"])
                if pd.notnull(row["category_code"])
                else None,
                "brand": str(row["brand"]) if pd.notnull(row["brand"]) else None,
                "price": float(row["price"]) if index % 10 != 0 else -100,
                "user_id": int(row["user_id"]),
                "user_session": str(row["user_session"]),
            }
            formatted_record = {
                "schema": {
                    "type": "struct",
                    "fields": avro_schema["fields"],
                },
                "payload": record,
            }
            return json.dumps(formatted_record)
    except Exception as e:
        print(f"Error loading parquet file: {e}")
        return
    # Process records in parallel using all available CPU cores
    print("Processing records in parallel...")
    records = df.apply(format_record, axis=1).tolist()
    print(f"Formatted {len(records)} records for Kafka.")

    # Get topic name and create it if needed
    topic_name = OUTPUT_TOPICS
    create_topic(admin, topic_name=topic_name)

    # Register schema if needed
    schema_version_info = schema_registry_client.check_version(
        f"{topic_name}-schema", schema.AvroSchema(avro_schema)
    )
    if schema_version_info is not None:
        schema_id = schema_version_info.schema_id
        print(f"Found existing schema ID: {schema_id}. Skipping creation!")
    else:
        schema_id = schema_registry_client.register(
            f"{topic_name}-schema", schema.AvroSchema(avro_schema)
        )
        print(f"Registered new schema with ID: {schema_id}")

    # Batch send records
    print("Starting to send records...")
    for i, record in enumerate(records):
        producer.send(topic_name, value=record)

        # Only print progress every 1000 records
        if i % 1000 == 0:
            print(f"Sent {i} records")
        time.sleep(0.05)

    # Make sure all messages are sent
    producer.flush()
    print(f"Finished sending {len(records)} records")


def teardown_streams(topic_name, servers):
    try:
        admin = KafkaAdminClient(bootstrap_servers=servers)
        admin.delete_topics([topic_name])
        print(f"Topic {topic_name} deleted successfully.")
    except Exception as e:
        print(f"Error deleting topic {topic_name}: {e}")


if __name__ == "__main__":
    parsed_args = vars(args)
    mode = parsed_args["mode"]
    servers = parsed_args["bootstrap_servers"]
    schema_registry_server = parsed_args["schema_registry_server"]

    # Teardown all previous streams
    print("Tearing down previous streams...")
    teardown_streams(OUTPUT_TOPICS, servers)

    if mode == "setup":
        print("Setting up streams...")
        schema_registry_client = SchemaRegistryClient(
            parsed_args["schema_registry_server"]
        )
        create_streams(
            servers=[servers],
            avro_schemas_path=parsed_args["avro_schemas_path"],
            schema_registry_client=schema_registry_client,
        )
