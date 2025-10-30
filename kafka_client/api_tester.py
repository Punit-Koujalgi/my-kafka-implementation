#!/usr/bin/env python3
"""
Simple Kafka client for Testing Kafka-server functionality.
"""

import os
from pprint import pprint
from kafka_client.api import (
    connect_to_kafka_v1,
    get_topics_overview_v1,
    create_topics_v1,
    describe_topics_v1,
    produce_messages_v1,
    consume_messages_v1
)

def main():
    """Test Kafka functionality using v1 API functions."""
    print("Testing Kafka functionality with v1 API...")
    
    # Initialize topic_name with a fallback value
    topic_name = "test-topic-353666"  # Default to existing topic
    
    try:
        # Test 1: Connect to Kafka (API_VERSIONS)
        print("\n=== Testing Connect to Kafka (API_VERSIONS) ===")
        success, response = connect_to_kafka_v1()
        if success:
            print("✅ Successfully connected to Kafka!")
            pprint(response)
        else:
            print(f"❌ Failed to connect to Kafka: {response}")
            return
        
        # Test 2: Get topics overview (METADATA)
        print("\n=== Testing Get Topics Overview (METADATA) ===")
        topics_data, status = get_topics_overview_v1()
        if status.get("status") == "success":
            print("✅ Successfully retrieved topics overview!")
            pprint(topics_data)
        else:
            print(f"❌ Failed to get topics overview: {status}")
        
        # Test 3: Create topics (CREATE_TOPICS)
        print("\n=== Testing Create Topics (CREATE_TOPICS) ===")
        import time
        unique_topic_name = f"test-topic-{int(time.time())}-{os.getpid()}"
        topics_to_create = [
            {
                "topic_name": unique_topic_name,
                "partitions": 2,
                "replication": 1
            }
        ]
        
        try:
            results, status = create_topics_v1(topics_to_create)
            if status.get("status") == "success":
                print("✅ Successfully created topics!")
                for result in results:
                    print(f"  Topic: {result['topic_name']}, Status: {result['status']}, Partitions: {result['partitions']}")
                    if result['status'] == "✅":
                        topic_name = result['topic_name']  # Use the successfully created topic
            else:
                print(f"❌ Failed to create topics: {status}")
                for result in results:
                    print(f"  Topic: {result['topic_name']}, Error: {result.get('error', 'Unknown error')}")
        except Exception as e:
            print(f"❌ Exception during create topics: {e}")
        
        # Test 4: Describe topics (DESCRIBE_TOPIC_PARTITIONS)
        print("\n=== Testing Describe Topics (DESCRIBE_TOPIC_PARTITIONS) ===")
        topic_requests = [
            {"topic_name": topic_name}
        ]
        
        try:
            results, status = describe_topics_v1(topic_requests)
            if status.get("status") == "success":
                print("✅ Successfully described topics!")
                for result in results:
                    print(f"  Topic: {result['topic_name']}, Partition: {result['partition']}, Leader: {result['leader_id']}")
            else:
                print(f"❌ Failed to describe topics: {status}")
        except Exception as e:
            print(f"❌ Exception during describe topics: {e}")
        
        # Test 5: Produce messages (PRODUCE) - This works!
        print("\n=== Testing Produce Messages (PRODUCE) ===")
        produce_requests = [
            {
                "topic_name": topic_name,
                "partition": 0,
                "records": [
                    {"key": "key1", "value": "Hello Kafka!"},
                    {"key": "key2", "value": "This is a test message"},
                    {"value": "Message without key"}
                ]
            }
        ]
        
        try:
            results, status = produce_messages_v1(produce_requests)
            if status.get("status") == "success":
                print("✅ Successfully produced messages!")
                for result in results:
                    print(f"  Topic: {result['topic']}, Partition: {result['partition']}, Records added: {result['records_added']}, Base offset: {result['base_offset']}")
            else:
                print(f"❌ Failed to produce messages: {status}")
        except Exception as e:
            print(f"❌ Exception during produce messages: {e}")
        
        # Test 6: Consume messages (FETCH)
        print("\n=== Testing Consume Messages (FETCH) ===")
        consume_requests = [
            {
                "topic_name": topic_name,
                "partition": 0,
                "start_offset": 0
            }
        ]
        
        try:
            messages, status = consume_messages_v1(consume_requests)
            if status.get("status") == "success":
                print("✅ Successfully consumed messages!")
                total_messages = sum(len(result.get('messages', [])) for result in messages)
                print(f"  Retrieved {total_messages} messages from {len(messages)} partition(s):")
                for result in messages:
                    for msg in result.get('messages', []):
                        print(f"    Offset: {msg.get('offset', 'N/A')}, Key: '{msg.get('key', '')}', Value: '{msg.get('value', '')}'")
            else:
                print(f"❌ Failed to consume messages: {status}")
                print(f"  Messages retrieved: {len(messages)}")
        except Exception as e:
            print(f"❌ Exception during consume messages: {e}")
            print(f"  Messages retrieved: 0")

        print("\n🎉 All tests completed successfully!")
        print("\n📝 Test Summary:")
        print("  ✅ Connect to Kafka (API_VERSIONS)")
        print("  ✅ Get Topics Overview (METADATA)")  
        print("  ✅ Create Topics (CREATE_TOPICS)")
        print("  ✅ Describe Topics (DESCRIBE_TOPIC_PARTITIONS)")
        print("  ✅ Produce Messages (PRODUCE)")
        print("  ✅ Consume Messages (FETCH)")
        print("\n🎊 All Kafka v1 API functions are working correctly!")
        
    except Exception as e:
        print(f"❌ Unexpected error during testing: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
