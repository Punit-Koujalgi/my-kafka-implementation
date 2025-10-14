

from uuid import UUID
from io import BytesIO

import os
import sys
import socket
import struct

from kafka_server.protocol.protocol import *

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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



def parse_metadata_response(data: bytes):
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
                from kafka_server.metadata.record_batch import DefaultRecordBatch
                print(f"        Records Data: {DefaultRecordBatch.decode(records)}")

            decode_tagged_fields(readable)

    decode_tagged_fields(readable)

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


