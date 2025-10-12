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


def main():
    """Test the CREATE_TOPICS functionality."""
    print("Testing Kafka CREATE_TOPICS functionality...")
    
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
        
        # Test 2: CREATE_TOPICS request
        print("\n=== Testing CREATE_TOPICS ===")
        topic_name = "test-topic-" + str(os.getpid())
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
        
        print("\nTest completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        sock.close()


if __name__ == "__main__":
    main()
