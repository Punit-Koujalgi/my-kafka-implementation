

from uuid import UUID
from io import BytesIO

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka_server.protocol.protocol import *
from kafka_client.client_utilities import *

def create_api_versions_request() -> bytes:
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


def create_create_topics_request(topic_name: str, num_partitions: int = 1) -> bytes:
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
    
    # Topic config (empty for now)
    configs = b"".join([
        encode_unsigned_varint(1),  # empty array (length 0 + 1)
        #encode_tagged_fields()
    ])
    
    # Creatable topic
    topic = b"".join([
        encode_compact_string(topic_name),     # name
        encode_int32(num_partitions),          # num_partitions
        encode_int16(1),                       # replication_factor
        encode_unsigned_varint(1),             # assignments (empty array)
        configs,                               # configs (empty array)
        encode_tagged_fields()
    ])
    
    # Request body
    body = b"".join([
        encode_unsigned_varint(2),   # topics array length (1 + 1)
        topic,
        encode_int32(30000),         # timeout_ms
        encode_int8(0),              # validate_only (false)
        encode_tagged_fields()
    ])
    
    request = header + body
    return encode_int32(len(request)) + request


def create_describe_topics_request(topic_name: str) -> bytes:
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
    
    # Request topic
    request_topic = b"".join([
        encode_compact_string(topic_name),  # name
        encode_tagged_fields()
    ])
    
    # Request body
    body = b"".join([
        encode_unsigned_varint(2),   # topics array length (1 + 1)
        request_topic,
        encode_int32(100),           # response_partition_limit
        b"\xff",                     # cursor (null)
        encode_tagged_fields()
    ])
    
    request = header + body
    return encode_int32(len(request)) + request


def create_metadata_request(topic_names: list[str] | None = None) -> bytes:
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



def create_fetch_request(topic_name, partition: int, fetch_offset: int) -> bytes:

    # convert topic_name to UUID
    topic_id: UUID = convert_topic_name_to_uuid(topic_name)

    """Create a FETCH request."""
    # Request header
    api_key = 1  # FETCH
    api_version = 13
    correlation_id = 4

    header = b"".join([
        encode_int16(api_key),
        encode_int16(api_version),
        encode_int32(correlation_id),
        encode_int16(0),
        encode_tagged_fields()
    ])

    # Fetch partition
    fetch_partition = b"".join([
        encode_int32(partition),
        encode_int32(-1),  # current_leader_epoch
        encode_int64(fetch_offset),
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

    # Request body
    body = b"".join([
        encode_int32(5000),  # max_wait_ms
        encode_int32(1),    # min_bytes
        encode_int32(1048576),  # max_bytes
        encode_int8(0),     # isolation_level
        encode_int32(0),    # session_id
        encode_int32(-1),   # session_epoch
        encode_unsigned_varint(2),  # topics array length (1 + 1)
        fetch_topic,
        encode_unsigned_varint(1),  # forgotten_topics_data array length (0 + 1)
        encode_compact_string(""),  # rack_id
        encode_tagged_fields()
    ])

    request = header + body
    return encode_int32(len(request)) + request


def create_produce_request(topic_name: str, partition: int, records: list[tuple[str, str]]) -> bytes:
    """Create a PRODUCE request."""

    # Convert records to DefaultRecordBatch with key and value from dictionary
    from kafka_server.metadata.record_batch import DefaultRecordBatch
    from kafka_server.metadata.record import DefaultRecord

    # Create DefaultRecord objects from the records dictionary
    record_objects = [
        DefaultRecord(
            attributes=0,
            timestamp_delta=0,
            offset_delta=index,
            key=key.encode(),
            value=value.encode(),
            headers=[]
        )
        for index, (key, value) in enumerate(records)
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

    # Partition produce data
    partition_data = b"".join([
        encode_int32(partition),
        encode_compact_nullable_bytes(encoded_records),
        encode_tagged_fields()
    ])

    # Topic produce data
    topic_data = b"".join([
        encode_compact_string(topic_name),
        encode_unsigned_varint(2),  # partitions array length (1 + 1)
        partition_data,
        encode_tagged_fields()
    ])

    # Request body
    body = b"".join([
        encode_compact_nullable_string(None),  # transactional_id
        encode_int16(-1),  # acks
        encode_int32(5000),  # timeout_ms
        encode_unsigned_varint(2),  # topics array length (1 + 1)
        topic_data,
        encode_tagged_fields()
    ])

    request = header + body
    return encode_int32(len(request)) + request



