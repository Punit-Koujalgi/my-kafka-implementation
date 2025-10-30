

from uuid import UUID, uuid4
from io import BytesIO

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka_server.protocol.protocol import *

def create_api_versions_request_v1() -> bytes:
    """Create an API_VERSIONS request."""
    # Request header
    api_key = 18  # API_VERSIONS
    api_version = 4
    correlation_id = 1
    client_id = None
    
    header = b"".join([
        encode_int16(api_key),
        encode_int16(api_version), 
        encode_int32(correlation_id),
        encode_int16(0),
        encode_tagged_fields()
    ])
    
    # Request body (for ApiVersions v4)
    body = b"".join([
        encode_compact_string("test-client"),  # client_software_name
        encode_compact_string("1.0.0"),       # client_software_version
        encode_tagged_fields()
    ])
    
    request = header + body
    return encode_int32(len(request)) + request


def create_create_topics_request_v1(topics_data: list[dict]) -> bytes:
    """Create a CREATE_TOPICS request."""
    # Request header
    api_key = 19  # CREATE_TOPICS
    api_version = 7
    correlation_id = 2
    
    header = b"".join([
        encode_int16(api_key),
        encode_int16(api_version),
        encode_int32(correlation_id),
        encode_int16(0),
        encode_tagged_fields()
    ])
    
    # Build topics array
    topics_array = []
    for topic_data in topics_data:
        # Topic config (empty for now)
        configs = b"".join([
            encode_unsigned_varint(1),  # empty array (length 0 + 1)
        ])
        
        # Creatable topic
        topic = b"".join([
            encode_compact_string(topic_data["topic_name"]),     # name
            encode_int32(topic_data["partitions"]),              # num_partitions
            encode_int16(topic_data.get("replication", 1)),      # replication_factor
            encode_unsigned_varint(1),                           # assignments (empty array)
            configs,                                             # configs (empty array)
            encode_tagged_fields()
        ])
        topics_array.append(topic)
    
    # Request body
    body = b"".join([
        encode_unsigned_varint(len(topics_array) + 1),   # topics array length + 1
        *topics_array,
        encode_int32(30000),         # timeout_ms
        encode_int8(0),              # validate_only (false)
        encode_tagged_fields()
    ])
    
    request = header + body
    return encode_int32(len(request)) + request


def create_describe_topics_request_v1(topic_requests: list[dict]) -> bytes:
    """Create a DESCRIBE_TOPIC_PARTITIONS request."""
    # Request header
    api_key = 75  # DESCRIBE_TOPIC_PARTITIONS
    api_version = 0
    correlation_id = 3
    
    header = b"".join([
        encode_int16(api_key),
        encode_int16(api_version),
        encode_int32(correlation_id),
        encode_int16(0),
        encode_tagged_fields()
    ])
    
    # Build topics array
    topics_array = []
    for req in topic_requests:
        # Request topic
        request_topic = b"".join([
            encode_compact_string(req["topic_name"]),  # name
            encode_tagged_fields()
        ])
        topics_array.append(request_topic)
    
    # Request body
    body = b"".join([
        encode_unsigned_varint(len(topics_array) + 1),  # topics array length + 1
        *topics_array,
        encode_int32(100),  # response_partition_limit
        b"\xff",           # cursor (null - 0xff indicates null cursor)
        encode_tagged_fields()
    ])
    
    request = header + body
    return encode_int32(len(request)) + request


def create_metadata_request_v1(topic_names: list[str] | None = None) -> bytes:
    """Create a METADATA request."""
    # Request header
    api_key = 3  # METADATA
    api_version = 12
    correlation_id = 1001
    
    header = b"".join([
        encode_int16(api_key),
        encode_int16(api_version),
        encode_int32(correlation_id),
        encode_int16(0),  # client_id length (0 = null)
        encode_tagged_fields()
    ])
    
    # Request body
    if topic_names is None:
        # Request all topics
        topics_data = encode_unsigned_varint(0)  # null array
    else:
        # Request specific topics
        topics_array = []
        for topic_name in topic_names:
            topic_data = b"".join([
                encode_uuid(UUID(int=0)),  # topic_id (null UUID)
                encode_compact_nullable_string(topic_name),  # topic name
                encode_tagged_fields()
            ])
            topics_array.append(topic_data)
        
        topics_data = b"".join([
            encode_unsigned_varint(len(topics_array) + 1),  # array length + 1
            *topics_array
        ])
    
    body = b"".join([
        topics_data,
        encode_int8(0),  # allow_auto_topic_creation = false
        encode_int8(0),  # include_cluster_authorized_operations = false  
        encode_int8(0),  # include_topic_authorized_operations = false
        encode_tagged_fields()
    ])
    
    request = header + body
    return encode_int32(len(request)) + request



def create_fetch_request_v1(consume_requests: list[dict]) -> bytes:
    """Create a FETCH request."""
    # Request header
    api_key = 1  # FETCH
    api_version = 16  # Use the supported version
    correlation_id = 4

    header = b"".join([
        encode_int16(api_key),
        encode_int16(api_version),
        encode_int32(correlation_id),
        encode_int16(0),
        encode_tagged_fields()
    ])

    # Build topics array
    topics_array = []
    for req in consume_requests:
        # Convert topic_name to actual UUID
        from kafka_client.api import convert_topic_name_to_uuid
        topic_id = convert_topic_name_to_uuid(req["topic_name"])
        
        # Fetch partition
        fetch_partition = b"".join([
            encode_int32(req["partition"]),
            encode_int32(-1),  # current_leader_epoch
            encode_int64(req["start_offset"]),
            encode_int32(-1),  # last_fetched_epoch
            encode_int64(-1),  # log_start_offset
            encode_int32(1048576),  # partition_max_bytes
            encode_tagged_fields()
        ])

        # Fetch topic
        fetch_topic = b"".join([
            encode_uuid(topic_id),
            encode_unsigned_varint(2),  # partitions array length (1 + 1)
            fetch_partition,
            encode_tagged_fields()
        ])
        topics_array.append(fetch_topic)

    # Request body
    body = b"".join([
        encode_int32(500),  # max_wait_ms
        encode_int32(1),   # min_bytes
        encode_int32(1048576),  # max_bytes
        encode_int8(0),    # isolation_level
        encode_int32(0),   # session_id
        encode_int32(-1),  # session_epoch
        encode_unsigned_varint(len(topics_array) + 1),  # topics array length + 1
        *topics_array,
        encode_unsigned_varint(1),  # forgotten_topics_data (empty array)
        encode_compact_string(""),  # rack_id
        encode_tagged_fields()
    ])

    request = header + body
    return encode_int32(len(request)) + request


def create_produce_request_v1(produce_requests: list[dict]) -> bytes:
    """Create a PRODUCE request."""
    # Convert records to DefaultRecordBatch with key and value from dictionary
    from kafka_server.metadata.record_batch import DefaultRecordBatch
    from kafka_server.metadata.record import DefaultRecord

    # Request header
    api_key = 0  # PRODUCE
    api_version = 8
    correlation_id = 5

    header = b"".join([
        encode_int16(api_key),
        encode_int16(api_version),
        encode_int32(correlation_id),
        encode_int16(0),
        encode_tagged_fields()
    ])

    # Build topics array
    topics_array = []
    for req in produce_requests:
        # Create DefaultRecord objects from the records
        records = req.get("records", [])
        record_objects = [
            DefaultRecord(
                attributes=0,
                timestamp_delta=0,
                offset_delta=index,
                key=record.get("key", "").encode(),
                value=record.get("value", "").encode(),
                headers=[]
            )
            for index, record in enumerate(records)
        ]

        # Create a DefaultRecordBatch
        record_batch = DefaultRecordBatch(
            base_offset=0,
            partition_leader_epoch=0,
            magic=2,
            crc=0,
            attributes=0,
            last_offset_delta=len(record_objects) - 1,
            base_timestamp=0,
            max_timestamp=0,
            producer_id=-1,
            producer_epoch=-1,
            base_sequence=-1,
            records=record_objects # type: ignore
        )

        # Encode the record batch
        encoded_records = record_batch.encode()

        # Partition produce data
        partition_data = b"".join([
            encode_int32(req["partition"]),
            encode_compact_nullable_bytes(encoded_records),
            encode_tagged_fields()
        ])

        # Topic produce data
        topic_data = b"".join([
            encode_compact_string(req["topic_name"]),
            encode_unsigned_varint(2),  # partitions array length (1 + 1)
            partition_data,
            encode_tagged_fields()
        ])
        topics_array.append(topic_data)

    # Request body
    body = b"".join([
        encode_compact_nullable_string(None),  # transactional_id
        encode_int16(-1),  # acks
        encode_int32(5000),  # timeout_ms
        encode_unsigned_varint(len(topics_array) + 1),  # topics array length + 1
        *topics_array,
        encode_tagged_fields()
    ])

    request = header + body
    return encode_int32(len(request)) + request


# V2 Functions - Direct server integration for UI
from kafka_server.protocol.request import RequestHeader
from kafka_server.protocol.protocol import ApiKey
from kafka_server.apis.api_versions import ApiVersionsRequest
from kafka_server.apis.api_create_topics import CreateTopicsRequest, CreatableTopic
from kafka_server.apis.api_describe_topic_partitions import DescribeTopicPartitionsRequest, RequestTopic
from kafka_server.apis.api_metadata import MetadataRequest, MetadataRequestTopic
from kafka_server.apis.api_fetch import FetchRequest, FetchTopic, FetchPartition
from kafka_server.apis.api_produce import ProduceRequest, TopicProduceData, PartitionProduceData
from kafka_server.metadata.record_batch import DefaultRecordBatch
from kafka_server.metadata.record import DefaultRecord

def create_api_versions_request() -> ApiVersionsRequest:
    """Create an API_VERSIONS request for UI."""
    header = RequestHeader(
        request_api_key=ApiKey.API_VERSIONS,
        request_api_version=4,
        correlation_id=1,
        client_id=None
    )
    return ApiVersionsRequest(
        header=header,
        client_software_name="kafka-ui-client",
        client_software_version="1.0.0"
    )


def create_create_topics_request(topics_data: list[dict]) -> CreateTopicsRequest:
    """Create a CREATE_TOPICS request for UI."""
    header = RequestHeader(
        request_api_key=ApiKey.CREATE_TOPICS,
        request_api_version=7,
        correlation_id=2,
        client_id=None
    )
    
    topics = []
    for topic_data in topics_data:
        topic = CreatableTopic(
            name=topic_data["topic_name"],
            num_partitions=topic_data["partitions"],
            replication_factor=topic_data.get("replication", 1),
            assignments=[],
            configs=[]
        )
        topics.append(topic)
    
    return CreateTopicsRequest(
        header=header,
        topics=topics,
        timeout_ms=30000,
        validate_only=False
    )


def create_describe_topics_request(topic_requests: list[dict]) -> DescribeTopicPartitionsRequest:
    """Create a DESCRIBE_TOPIC_PARTITIONS request for UI."""
    header = RequestHeader(
        request_api_key=ApiKey.DESCRIBE_TOPIC_PARTITIONS,
        request_api_version=0,
        correlation_id=3,
        client_id=None
    )
    
    topics = []
    for req in topic_requests:
        topic = RequestTopic(name=req["topic_name"])
        topics.append(topic)
    
    return DescribeTopicPartitionsRequest(
        header=header,
        topics=topics,
        response_partition_limit=100,
        cursor=None
    )


def create_metadata_request(topic_names: list[str] | None = None) -> MetadataRequest:
    """Create a METADATA request for UI."""
    header = RequestHeader(
        request_api_key=ApiKey.METADATA,
        request_api_version=12,
        correlation_id=1001,
        client_id=None
    )
    
    topics = None
    if topic_names:
        topics = []
        for topic_name in topic_names:
            topic = MetadataRequestTopic(
                topic_id=UUID(int=0),  # null UUID
                name=topic_name
            )
            topics.append(topic)
    
    return MetadataRequest(
        header=header,
        topics=topics,
        allow_auto_topic_creation=False,
        include_cluster_authorized_operations=False,
        include_topic_authorized_operations=False
    )

def create_fetch_request(consume_requests: list[dict]) -> FetchRequest:
    """Create a FETCH request for UI."""

    from .api import convert_topic_name_to_uuid
    
    header = RequestHeader(
        request_api_key=ApiKey.FETCH,
        request_api_version=13,
        correlation_id=4,
        client_id=None
    )
    
    topics = []
    for req in consume_requests:
        topic_id = convert_topic_name_to_uuid(req["topic_name"])
        
        partition = FetchPartition(
            partition=req["partition"],
            current_leader_epoch=-1,
            fetch_offset=req["start_offset"],
            last_fetched_epoch=-1,
            log_start_offset=-1,
            partition_max_bytes=1048576
        )
        
        topic = FetchTopic(
            topic_id=topic_id,
            partitions=[partition]
        )
        topics.append(topic)
    
    return FetchRequest(
        header=header,
        max_wait_ms=5000,
        min_bytes=1,
        max_bytes=1048576,
        isolation_level=0,
        session_id=0,
        session_epoch=-1,
        topics=topics,
        forgotten_topics_data=[],
        rack_id=""
    )

def create_fetch_request_with_topic_Id(topic_id: UUID, partition: int) -> FetchRequest:
    """Create a FETCH request for a specific topic and partition."""
    header = RequestHeader(
        request_api_key=ApiKey.FETCH,
        request_api_version=13,
        correlation_id=4,
        client_id=None
    )

    partition_data = FetchPartition(
        partition=partition,
        current_leader_epoch=-1,
        fetch_offset=0,
        last_fetched_epoch=-1,
        log_start_offset=-1,
        partition_max_bytes=1048576
    )

    topic = FetchTopic(
        topic_id=topic_id,
        partitions=[partition_data]
    )

    return FetchRequest(
        header=header,
        max_wait_ms=5000,
        min_bytes=1,
        max_bytes=1048576,
        isolation_level=0,
        session_id=0,
        session_epoch=-1,
        topics=[topic],
        forgotten_topics_data=[],
        rack_id=""
    )

def create_produce_request(produce_requests: list[dict]) -> ProduceRequest:
    """Create a PRODUCE request for UI."""
    header = RequestHeader(
        request_api_key=ApiKey.PRODUCE,
        request_api_version=8,
        correlation_id=5,
        client_id=None
    )
    
    topics = []
    for req in produce_requests:
        # Create records from the request data
        record_objects = []
        for index, record in enumerate(req["records"]):
            record_obj = DefaultRecord(
                attributes=0,
                timestamp_delta=0,
                offset_delta=index,
                key=record["key"].encode() if record["key"] else b"",
                value=record["value"].encode() if record["value"] else b"",
                headers=[]
            )
            record_objects.append(record_obj)
        
        # Create record batch
        record_batch = DefaultRecordBatch(
            base_offset=0,
            partition_leader_epoch=0,
            magic=2,
            crc=0,
            attributes=0,
            last_offset_delta=len(record_objects) - 1,
            base_timestamp=0,
            max_timestamp=0,
            producer_id=-1,
            producer_epoch=-1,
            base_sequence=-1,
            records=record_objects # type: ignore
        )
        
        partition_data = PartitionProduceData(
            index=req["partition"],
            records=record_batch.encode()
        )
        
        topic_data = TopicProduceData(
            name=req["topic_name"],
            partition_data=[partition_data]
        )
        topics.append(topic_data)
    
    return ProduceRequest(
        header=header,
        transactional_id=None,
        acks=-1,
        timeout_ms=5000,
        topic_data=topics
    )



