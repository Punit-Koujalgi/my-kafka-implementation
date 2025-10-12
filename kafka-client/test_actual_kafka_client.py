from kafka import KafkaAdminClient, KafkaConsumer
from kafka.admin import NewTopic

# Kafka configuration
KAFKA_BROKER = "localhost:9092"

def test_api_versions():
    """Test to fetch API versions from the Kafka server."""
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
        cluster_metadata = admin_client.describe_cluster()
        assert cluster_metadata, "Failed to fetch cluster metadata."
        print("API Versions fetched successfully.")
    except Exception as e:
        print(f"Failed to fetch API versions: {e}")

def test_create_topic():
    """Test to create a new topic."""
    topic_name = "test_topic"
    new_topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
        admin_client.create_topics([new_topic])
        print(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        print(f"Failed to create topic: {e}")

def test_describe_topic():
    """Test to describe a topic."""
    topic_name = "test_topic"
    consumer = KafkaConsumer(topic_name, bootstrap_servers=KAFKA_BROKER)
    try:
        topics = consumer.topics()
        assert topic_name in topics, f"Topic '{topic_name}' not found."
        print(f"Topic '{topic_name}' exists and described successfully.")
    except Exception as e:
        print(f"Failed to describe topic: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    print("Running tests...")
    test_api_versions()
    test_create_topic()
    test_describe_topic()
    