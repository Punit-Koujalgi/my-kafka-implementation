#!/usr/bin/env python3
"""
Simple Kafka client for Testing Kafka-server functionality functionality.
"""

from .request_builder import *
from .response_parser import *
from .client_utilities import *

def main():
    """Test Kafka functionality."""
    print("Testing Kafka functionality...")
    
    try:
        sock = connect_to_server()

        # Test 1: API_VERSIONS request
        print("\n=== Testing API_VERSIONS ===")
        api_versions_request = create_api_versions_request()
        response = ServerConnector().send(api_versions_request)
        parse_api_versions_response(response)
        
        # Test 2: CREATE_TOPICS request
        print("\n=== Testing CREATE_TOPICS ===")
        topic_name = "test-topic-" + str(os.getpid())
        # topic_name = "test-topic-1" 
        create_topics_request = create_create_topics_request(topic_name, 3)
        response = ServerConnector().send(create_topics_request)
        parse_create_topics_response(response)
        
        # Test 3: DESCRIBE_TOPIC_PARTITIONS request
        print("\n=== Testing DESCRIBE_TOPIC_PARTITIONS ===")
        describe_request = create_describe_topics_request(topic_name)
        response = ServerConnector().send(describe_request)
        print(f"DESCRIBE_TOPIC_PARTITIONS Response length: {len(response)} bytes")
        
        # Test 4: METADATA request
        print("\n=== Testing METADATA one topic ===")
        metadata_request = create_metadata_request(["test-topic-543772"])
        response = ServerConnector().send(metadata_request)
        print(f"METADATA Response length: {len(response)} bytes")
        parse_metadata_response(response)

        # Test 5: METADATA request
        print("\n=== Testing METADATA all topics ===")
        metadata_request = create_metadata_request()
        response = ServerConnector().send(metadata_request)
        print(f"METADATA Response length: {len(response)} bytes")
        parse_metadata_response(response)
        
        # Test 6: FETCH request
        print("\n=== Testing FETCH ===")
        topic_id = UUID("c7a3acab-c971-475f-86f4-be220ad3250d")  # Replace with actual topic ID
        fetch_request = create_fetch_request(topic_id, 0, 0)
        response = ServerConnector().send(fetch_request)
        parse_fetch_response(response)

        # Test 7: PRODUCE request
        print("\n=== Testing PRODUCE ===")
        records = [("key1", "test-record-data")]  # Replace with actual record data
        produce_request = create_produce_request(topic_name, 0, records)
        response = ServerConnector().send(produce_request)
        parse_produce_response(response)

        # Test 8: FETCH request
        print("\n=== Testing FETCH ===")
        fetch_request = create_fetch_request(topic_name, 0, 0)
        response = ServerConnector().send(fetch_request)
        parse_fetch_response(response)


        print("\nTest completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
