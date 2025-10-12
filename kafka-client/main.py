#!/usr/bin/env python3
"""
Simple Kafka client for testing CREATE_TOPICS functionality.
"""

import socket
import struct
from uuid import UUID
from io import BytesIO

# Import protocol functions from our Kafka app
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.protocol.protocol import *


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


def read_response(sock: socket.socket) -> bytes:
    """Read a response from the socket."""
    # Read message length
    length_bytes = sock.recv(4)
    if len(length_bytes) != 4:
        raise Exception("Failed to read message length")
    
    message_length = struct.unpack(">I", length_bytes)[0]
    
    # Read message
    message = b""
    while len(message) < message_length:
        chunk = sock.recv(message_length - len(message))
        if not chunk:
            raise Exception("Connection closed")
        message += chunk
    
    return message


def parse_api_versions_response(data: bytes) -> None:
    """Parse and print API_VERSIONS response."""
    readable = BytesIO(data)
    
    # Response header
    correlation_id = decode_int32(readable)
    print(f"API_VERSIONS Response - Correlation ID: {correlation_id}")
    
    # Response body
    error_code = decode_int16(readable)
    print(f"Error Code: {error_code}")
    
    # API keys array
    api_keys_count = decode_unsigned_varint(readable) - 1
    print(f"Supported APIs ({api_keys_count}):")
    
    for _ in range(api_keys_count):
        api_key = decode_int16(readable)
        min_version = decode_int16(readable)
        max_version = decode_int16(readable)
        decode_tagged_fields(readable)
        print(f"  API {api_key}: versions {min_version}-{max_version}")


def parse_create_topics_response(data: bytes) -> None:
    """Parse and print CREATE_TOPICS response."""
    readable = BytesIO(data)
    
    # Response header
    correlation_id = decode_int32(readable)
    print(f"CREATE_TOPICS Response - Correlation ID: {correlation_id}")
    
    # Response body
    throttle_time_ms = decode_int32(readable)
    print(f"Throttle Time: {throttle_time_ms}ms")
    
    # Topics array
    topics_count = decode_unsigned_varint(readable) - 1
    print(f"Topics ({topics_count}):")
    
    for _ in range(topics_count):
        name = decode_compact_string(readable)
        topic_id_bytes = readable.read(16)
        topic_id = UUID(bytes=topic_id_bytes)
        error_code = decode_int16(readable)
        error_message = decode_compact_nullable_string(readable)
        topic_config_error_code = decode_int16(readable)
        num_partitions = decode_int32(readable)
        replication_factor = decode_int16(readable)
        
        # Skip configs array
        configs_count = decode_unsigned_varint(readable) - 1
        for _ in range(configs_count):
            decode_compact_string(readable)  # name
            decode_compact_nullable_string(readable)  # value
            decode_int8(readable)  # read_only
            decode_int8(readable)  # config_source
            decode_int8(readable)  # is_sensitive
            decode_tagged_fields(readable)
        
        decode_tagged_fields(readable)
        
        print(f"  Topic: {name}")
        print(f"    ID: {topic_id}")
        print(f"    Error Code: {error_code}")
        if error_message:
            print(f"    Error Message: {error_message}")
        print(f"    Partitions: {num_partitions}")
        print(f"    Replication Factor: {replication_factor}")

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


def parse_metadata_response(data: bytes) -> None:
    """Parse and print METADATA response."""
    readable = BytesIO(data)
    
    # Response header
    correlation_id = decode_int32(readable)
    print(f"METADATA Response - Correlation ID: {correlation_id}")
    decode_tagged_fields(readable)  # header tagged fields
    
    # Response body
    throttle_time_ms = decode_int32(readable)
    print(f"Throttle Time: {throttle_time_ms}ms")
    
    # Brokers array
    brokers_count = decode_unsigned_varint(readable) - 1
    print(f"Brokers ({brokers_count}):")
    for _ in range(brokers_count):
        node_id = decode_int32(readable)
        host = decode_compact_string(readable)
        port = decode_int32(readable)
        rack = decode_compact_nullable_string(readable)
        decode_tagged_fields(readable)
        print(f"  Broker {node_id}: {host}:{port}" + (f" (rack: {rack})" if rack else ""))
    
    # Cluster info
    cluster_id = decode_compact_nullable_string(readable)
    controller_id = decode_int32(readable)
    print(f"Cluster ID: {cluster_id}")
    print(f"Controller ID: {controller_id}")
    
    # Topics array
    topics_count = decode_unsigned_varint(readable) - 1
    print(f"Topics ({topics_count}):")
    for _ in range(topics_count):
        error_code = decode_int16(readable)
        topic_name = decode_compact_nullable_string(readable)
        topic_id_bytes = readable.read(16)
        topic_id = UUID(bytes=topic_id_bytes)
        is_internal = decode_int8(readable) != 0
        
        # Partitions array
        partitions_count = decode_unsigned_varint(readable) - 1
        print(f"  Topic: {topic_name} (ID: {topic_id}, Internal: {is_internal})")
        print(f"    Error Code: {error_code}")
        print(f"    Partitions ({partitions_count}):")
        
        for _ in range(partitions_count):
            part_error_code = decode_int16(readable)
            partition_index = decode_int32(readable)
            leader_id = decode_int32(readable)
            leader_epoch = decode_int32(readable)
            
            # Replica nodes
            replica_count = decode_unsigned_varint(readable) - 1
            replica_nodes = []
            for _ in range(replica_count):
                replica_nodes.append(decode_int32(readable))
            
            # ISR nodes
            isr_count = decode_unsigned_varint(readable) - 1
            isr_nodes = []
            for _ in range(isr_count):
                isr_nodes.append(decode_int32(readable))
            
            # Offline replicas
            offline_count = decode_unsigned_varint(readable) - 1
            offline_replicas = []
            for _ in range(offline_count):
                offline_replicas.append(decode_int32(readable))
            
            decode_tagged_fields(readable)
            
            print(f"      Partition {partition_index}: Leader={leader_id}, "
                    f"Replicas={replica_nodes}, ISR={isr_nodes}, Error={part_error_code}")
        
        topic_auth_ops = decode_int32(readable)
        decode_tagged_fields(readable)
    
    cluster_auth_ops = decode_int32(readable)
    decode_tagged_fields(readable)
    print(f"Cluster Authorized Operations: {cluster_auth_ops}")

def create_fetch_request(topic_id: UUID, partition: int, fetch_offset: int) -> bytes:
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

def parse_fetch_response(data: bytes) -> None:
    """Parse and print FETCH response."""
    readable = BytesIO(data)

    # Response header
    correlation_id = decode_int32(readable)
    print(f"FETCH Response - Correlation ID: {correlation_id}")
    decode_tagged_fields(readable)  # header tagged fields

    # Response body
    throttle_time_ms = decode_int32(readable)
    print(f"Throttle Time: {throttle_time_ms}ms")

    error_code = decode_int16(readable)
    session_id = decode_int32(readable)
    print(f"Error Code: {error_code}, Session ID: {session_id}")

    # Responses array
    responses_count = decode_unsigned_varint(readable) - 1
    print(f"Responses ({responses_count}):")

    for _ in range(responses_count):
        topic_id = decode_uuid(readable)
        print(f"  Topic ID: {topic_id}")

        partitions_count = decode_unsigned_varint(readable) - 1
        print(f"    Partitions ({partitions_count}):")

        for _ in range(partitions_count):
            partition_index = decode_int32(readable)
            error_code = decode_int16(readable)
            high_watermark = decode_int64(readable)
            last_stable_offset = decode_int64(readable)
            log_start_offset = decode_int64(readable)

            print(f"      Partition {partition_index}: Error Code={error_code}, High Watermark={high_watermark}, Last Stable Offset={last_stable_offset}, Log Start Offset={log_start_offset}")

            # Skip aborted transactions array
            decode_compact_array(readable, lambda r: None)

            preferred_read_replica = decode_int32(readable)
            print(f"        Preferred Read Replica: {preferred_read_replica}")

            # Records
            records_length = decode_unsigned_varint(readable)
            records = BytesIO(readable.read(records_length))
            print(f"        Records Length: {records_length} bytes")
            if records:
                from app.metadata.record_batch import DefaultRecordBatch
                print(f"        Records Data: {DefaultRecordBatch.decode(records)}")

            decode_tagged_fields(readable)

    decode_tagged_fields(readable)

def create_produce_request(topic_name: str, partition: int, records: list[tuple[str, str]]) -> bytes:
    """Create a PRODUCE request."""

    # Convert records to DefaultRecordBatch with key and value from dictionary
    from app.metadata.record_batch import DefaultRecordBatch
    from app.metadata.record import DefaultRecord

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

def parse_produce_response(data: bytes) -> None:
    """Parse and print PRODUCE response."""
    readable = BytesIO(data)

    # Response header
    correlation_id = decode_int32(readable)
    print(f"PRODUCE Response - Correlation ID: {correlation_id}")
    decode_tagged_fields(readable)  # header tagged fields

    # Response body
    responses_count = decode_unsigned_varint(readable) - 1
    print(f"Responses ({responses_count}):")

    for _ in range(responses_count):
        topic_name = decode_compact_string(readable)
        print(f"  Topic: {topic_name}")

        partitions_count = decode_unsigned_varint(readable) - 1
        print(f"    Partitions ({partitions_count}):")

        for _ in range(partitions_count):
            partition_index = decode_int32(readable)
            error_code = decode_int16(readable)
            base_offset = decode_int64(readable)
            log_append_time_ms = decode_int64(readable)
            log_start_offset = decode_int64(readable)

            print(f"      Partition {partition_index}: Error Code={error_code}, Base Offset={base_offset}, Log Append Time={log_append_time_ms}, Log Start Offset={log_start_offset}")

            # Skip record errors array
            decode_compact_array(readable, lambda r: None)

            error_message = decode_compact_nullable_string(readable)
            if error_message:
                print(f"        Error Message: {error_message}")

            decode_tagged_fields(readable)

    decode_tagged_fields(readable)

def main():
    """Test Kafka functionality."""
    print("Testing Kafka functionality...")
    
    # Connect to Kafka
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect(("localhost", 9092))
        print("Connected to Kafka broker")
        
        # Test 1: API_VERSIONS request
        print("\n=== Testing API_VERSIONS ===")
        api_versions_request = create_api_versions_request()
        sock.send(api_versions_request)
        response = read_response(sock)
        parse_api_versions_response(response)
        '''
        # Test 2: CREATE_TOPICS request
        print("\n=== Testing CREATE_TOPICS ===")
        # topic_name = "test-topic-" + str(os.getpid())
        topic_name = "test-topic-1" 
        create_topics_request = create_create_topics_request(topic_name, 3)
        sock.send(create_topics_request)
        response = read_response(sock)
        parse_create_topics_response(response)
        
        # Test 3: DESCRIBE_TOPIC_PARTITIONS request
        print("\n=== Testing DESCRIBE_TOPIC_PARTITIONS ===")
        describe_request = create_describe_topics_request(topic_name)
        sock.send(describe_request)
        response = read_response(sock)
        print(f"DESCRIBE_TOPIC_PARTITIONS Response length: {len(response)} bytes")
        
        # Test 4: METADATA request
        print("\n=== Testing METADATA one topic ===")
        metadata_request = create_metadata_request(["test-topic-543772"])
        sock.send(metadata_request)
        response = read_response(sock)
        print(f"METADATA Response length: {len(response)} bytes")
        parse_metadata_response(response)

        # Test 5: METADATA request
        print("\n=== Testing METADATA all topics ===")
        metadata_request = create_metadata_request()
        sock.send(metadata_request)
        response = read_response(sock)
        print(f"METADATA Response length: {len(response)} bytes")
        parse_metadata_response(response)
        '''
        # Test 6: FETCH request
        print("\n=== Testing FETCH ===")
        topic_id = UUID("c7a3acab-c971-475f-86f4-be220ad3250d")  # Replace with actual topic ID
        fetch_request = create_fetch_request(topic_id, 0, 0)
        sock.send(fetch_request)
        response = read_response(sock)
        parse_fetch_response(response)

        # # Test 7: PRODUCE request
        # print("\n=== Testing PRODUCE ===")
        # records = [("key1", "test-record-data")]  # Replace with actual record data
        # produce_request = create_produce_request(topic_name, 0, records)
        # sock.send(produce_request)
        # response = read_response(sock)
        # parse_produce_response(response)

        print("\nTest completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        sock.close()


if __name__ == "__main__":
    main()
