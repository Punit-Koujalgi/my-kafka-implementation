
from uuid import UUID, uuid4
from io import BytesIO

from kafka_client.request_builder import (
    create_api_versions_request_v1,
    create_create_topics_request_v1, 
    create_describe_topics_request_v1,
    create_metadata_request_v1,
    create_fetch_request_v1,
    create_produce_request_v1
)
from kafka_client.response_parser import (
    parse_api_versions_response_v1,
    parse_create_topics_response_v1,
    parse_describe_topics_response_v1, 
    parse_metadata_response_v1,
    parse_fetch_response_v1,
    parse_produce_response_v1
)
from kafka_client.client_utilities import ServerConnector

# New v1 API Functions using Kafka client

def connect_to_kafka_v1() -> tuple[bool, dict]:
    """Connect to Kafka broker using v1 API.
    
    Input format:
        None - No input parameters required
        
    Output format:
        tuple[bool, dict] - (success_status, response_data)
        - success_status: True if connection successful, False otherwise
        - response_data: On success, contains API versions info with keys:
            * 'correlation_id': int
            * 'error_code': int
            * 'throttle_time_ms': int
            * 'api_versions': list[dict] with keys:
                - 'api_key': int
                - 'min_version': int  
                - 'max_version': int
          On failure, contains {'error': str} with error message
    """
    try:
        connector = ServerConnector()
        request = create_api_versions_request_v1()
        response_data = connector.send(request)
        parsed_response = parse_api_versions_response_v1(response_data)
        return True, parsed_response
    except Exception as e:
        return False, {"error": str(e)}


def get_topics_overview_v1() -> tuple[dict, dict]:
    """Get overview of all topics using v1 API.
    
    Input format:
        None - No input parameters required
        
    Output format:
        tuple[dict, dict] - (topics_data, status_info)
        - topics_data: On success, contains:
            * 'total_topics': int
            * 'total_partitions': int  
            * 'total_records': int
            * 'topics': list[dict] with topic information
          On failure, contains default values with counts set to 0
        - status_info: On success, contains {'status': 'success'}
          On failure, contains {'error': str} with error message
    """
    try:
        connector = ServerConnector()
        request = create_metadata_request_v1()
        response_data = connector.send(request)
        parsed_response = parse_metadata_response_v1(response_data)
        return parsed_response, {"status": "success"}
    except Exception as e:
        return {
            "total_topics": 0,
            "total_partitions": 0,
            "total_records": 0,
            "topics": []
        }, {"error": str(e)}


def create_topics_v1(topics_data: list[dict]) -> tuple[list[dict], dict]:
    """Create topics using v1 API.
    
    Input format:
        topics_data: list[dict] - List of topic specifications, each dict containing:
            * 'topic_name': str - Name of the topic to create
            * 'partitions': int - Number of partitions for the topic
            * 'replication': int (optional) - Replication factor (default: 1)
            
    Output format:
        tuple[list[dict], dict] - (results, status_info)
        - results: list[dict] - List of creation results, each dict containing:
            * 'status': str - "✅" for success, "❌" for failure
            * 'topic_name': str - Name of the topic
            * 'topic_id': str - UUID of created topic (empty on failure)
            * 'partitions': int - Number of partitions
            * 'error': str (on failure) - Error message if creation failed
        - status_info: On success, contains {'status': 'success'}
          On failure, contains {'error': str} with error message
    """
    try:
        connector = ServerConnector()
        request = create_create_topics_request_v1(topics_data)
        response_data = connector.send(request)
        results = parse_create_topics_response_v1(response_data)
        return results, {"status": "success"}
    except Exception as e:
        return [{
            "status": "❌",
            "topic_name": topic["topic_name"],
            "topic_id": "",
            "partitions": topic["partitions"],
            "error": str(e)
        } for topic in topics_data], {"error": str(e)}


def describe_topics_v1(topic_requests: list[dict]) -> tuple[list[dict], dict]:
    """Describe topic partitions using v1 API.
    
    Input format:
        topic_requests: list[dict] - List of topic description requests, each dict containing:
            * 'topic_name': str - Name of the topic to describe
            
    Output format:
        tuple[list[dict], dict] - (results, status_info)
        - results: list[dict] - List of topic partition descriptions, each dict containing:
            * 'status': str - "✅" for success, "❌" for failure
            * 'topic_name': str - Name of the topic
            * 'partition': int - Partition number
            * 'error_code': int - Error code from response
            * 'leader_id': int - ID of partition leader broker
            * 'replicas': int - Number of replicas
        - status_info: On success, contains {'status': 'success'}
          On failure, contains {'error': str} with error message
    """
    try:
        connector = ServerConnector()
        request = create_describe_topics_request_v1(topic_requests)
        response_data = connector.send(request)
        results = parse_describe_topics_response_v1(response_data)
        return results, {"status": "success"}
    except Exception as e:
        return [{
            "status": "❌",
            "topic_name": req["topic_name"],
            "partition": 0,
            "error_code": -1,
            "leader_id": -1,
            "replicas": 0
        } for req in topic_requests], {"error": str(e)}


def produce_messages_v1(produce_requests: list[dict]) -> tuple[list[dict], dict]:
    """Produce messages to topics using v1 API.
    
    Input format:
        produce_requests: list[dict] - List of produce requests, each dict containing:
            * 'topic_name': str - Name of the topic to produce to
            * 'partition': int - Partition number to produce to
            * 'records': list[dict] - List of records to produce, each record dict containing:
                - 'key': str (optional) - Record key
                - 'value': str - Record value/message content
                
    Output format:
        tuple[list[dict], dict] - (results, status_info)
        - results: list[dict] - List of produce results, each dict containing:
            * 'topic': str - Name of the topic
            * 'partition': int - Partition number
            * 'records_added': int - Number of records successfully added
            * 'base_offset': int - Base offset of the first record (-1 on failure)
            * 'error': str (on failure) - Error message if production failed
        - status_info: On success, contains {'status': 'success'}
          On failure, contains {'error': str} with error message
    """
    try:
        connector = ServerConnector()
        request = create_produce_request_v1(produce_requests)
        response_data = connector.send(request)
        results = parse_produce_response_v1(response_data)
        return results, {"status": "success"}
    except Exception as e:
        return [{
            "topic": req["topic_name"],
            "partition": req["partition"],
            "records_added": 0,
            "base_offset": -1,
            "error": str(e)
        } for req in produce_requests], {"error": str(e)}


def consume_messages_v1(consume_requests: list[dict]) -> tuple[list[dict], dict]:
    """Consume messages from topics using v1 API.
    
    Input format:
        consume_requests: list[dict] - List of consume requests, each dict containing:
            * 'topic_name': str - Name of the topic to consume from
            * 'partition': int - Partition number to consume from
            * 'start_offset': int - Offset to start consuming from
            
    Output format:
        tuple[list[dict], dict] - (messages, status_info)
        - messages: list[dict] - List of consumed messages, each dict containing:
            * 'topic': str - Name of the topic
            * 'partition': int - Partition number
            * 'offset': int - Message offset
            * 'key': str - Message key (may be empty)
            * 'value': str - Message value/content
            * 'timestamp': int - Message timestamp
        - status_info: On success, contains {'status': 'success'}
          On failure, contains {'error': str} with error message
    """
    try:
        connector = ServerConnector()
        request = create_fetch_request_v1(consume_requests)
        response_data = connector.send(request)
        results = parse_fetch_response_v1(response_data)
        return results, {"status": "success"}
    except Exception as e:
        return [], {"error": str(e)}



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


def convert_topic_name_to_uuid(topic_name: str) -> UUID:
    from kafka_server.metadata.cluster_metadata import ClusterMetadata
    cluster_metadata = ClusterMetadata()
    topic_id = cluster_metadata.get_topic_id(topic_name)

    if topic_id is None:
        return UUID(int=0)
    return topic_id
