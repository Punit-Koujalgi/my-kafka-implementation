
from uuid import UUID, uuid4
from io import BytesIO


def convert_topic_name_to_uuid(topic_name: str) -> UUID:
    from kafka_server.metadata.cluster_metadata import ClusterMetadata
    cluster_metadata = ClusterMetadata()
    topic_id = cluster_metadata.get_topic_id(topic_name)

    if topic_id is None:
        return uuid4()
    return topic_id

def convert_uuid_to_topic_name(topic_id: UUID) -> str:
    from kafka_server.metadata.cluster_metadata import ClusterMetadata
    cluster_metadata = ClusterMetadata()
    topic_name = cluster_metadata.get_topic_name(topic_id)

    if topic_name is None:
        return "UNKNOWN_TOPIC"
    return topic_name

def get_all_topic_names() -> list[str]:
    from kafka_server.metadata.cluster_metadata import ClusterMetadata
    cluster_metadata = ClusterMetadata()
    return [topic[0] for topic in cluster_metadata.get_all_topics()]

