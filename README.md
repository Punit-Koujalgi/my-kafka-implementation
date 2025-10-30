# My Implementation of Kafka - check out [demo](https://huggingface.co/spaces/pkoujalgi/My-Kafka-Impl)

This repository is my implemenatation of Kafka, an open-source, distributed event streaming platform. It combines messaging, storage, and stream processing to handle large volumes of real-time data.

- How it works: Producers send data to "topics" and consumers read data from these topics. Topics contain many partitions to isolate different kind of events in a Topic and for data replication.
- Common uses: Building real-time data pipelines, data integration, real-time analytics, and event-driven architectures. 


## Repository Info

This Repository contains below sections:

- [Kafka Research Papers](kafka_papers): The inspiring paper behind the entire Kafka ecosystem.
- [Kafka Server](kafka_server): Implements broker which responds to connections from clients and deals with data storage. 
- [Kafka Client](kafka_client): Connect and make API calls to the server for queries and to play with data! You can explore the kafka API usage below.
- [Kafka Management Console](kafka_ui): An intuitive beautiful user interface to interact with the kafka broker, internally makes calls via kafka-client. Best way to get your hands dirty right away! Hosted here - [Kafka Console](https://huggingface.co/spaces/pkoujalgi/My-Kafka-Impl) 


## Running Kafka Server locally

This repository uses uv package manager (really loving this so far!). To install all dependencies with a single command:

```
$ uv sync
```

To run the kafka server:
```bash
$ uv run python -m kafka_server.main
```

This will bring up the kafka broker, now you can interact with the cluster in two ways:

<b>Using the simple, intuitive kafka-ui</b>:

```bash
$ uv run python -m ui.main
```

Done! Now visit http://127.0.0.1:7860 in you browser to interact with your own local Kafka cluster!  

<b>Using the kafka-client</b>:

  You can import kafka-client package into your own program/application to interact with the kafka cluster, the api is intuitive and easy to work with, making it straight forward to create and describe topics, partitions and to produce and consume records!  
  
```python
from kafka_client.api import (
    connect_to_kafka_v1,
    get_topics_overview_v1,
    create_topics_v1,
    describe_topics_v1,
    produce_messages_v1,
    consume_messages_v1
)

# 1. Connect to Kafka and check API versions
success, response = connect_to_kafka_v1()
if success:
    print("✅ Successfully connected to Kafka!")
    print(f"Supported APIs: {len(response['api_versions'])}")

# 2. Get overview of all topics
topics_data, status = get_topics_overview_v1()
if status.get("status") == "success":
    print(f"Total topics: {topics_data['total_topics']}")
    print(f"Total partitions: {topics_data['total_partitions']}")

# 3. Create a new topic
topics_to_create = [
    {
        "topic_name": "my-test-topic",
        "partitions": 3,
        "replication": 1
    }
]

results, status = create_topics_v1(topics_to_create)
if status.get("status") == "success":
    print("✅ Successfully created topics!")
    for result in results:
        print(f"Topic: {result['topic_name']}, Status: {result['status']}")

# 4. Describe topic partitions
topic_requests = [{"topic_name": "my-test-topic"}]
results, status = describe_topics_v1(topic_requests)
if status.get("status") == "success":
    print("✅ Successfully described topics!")
    for result in results:
        print(f"Partition {result['partition']}, Leader: {result['leader_id']}")

# 5. Produce messages to topic
produce_requests = [
    {
        "topic_name": "my-test-topic",
        "partition": 0,
        "records": [
            {"key": "user1", "value": "Hello Kafka!"},
            {"key": "user2", "value": "This is a test message"},
            {"value": "Message without key"}
        ]
    }
]

results, status = produce_messages_v1(produce_requests)
if status.get("status") == "success":
    print("✅ Successfully produced messages!")
    for result in results:
        print(f"Records added: {result['records_added']}, Offset: {result['base_offset']}")

# 6. Consume messages from topic
consume_requests = [
    {
        "topic_name": "my-test-topic",
        "partition": 0,
        "start_offset": 0
    }
]

messages, status = consume_messages_v1(consume_requests)
if status.get("status") == "success":
    print("✅ Successfully consumed messages!")
    for result in messages:
        for msg in result.get('messages', []):
            print(f"Offset: {msg['offset']}, Key: '{msg['key']}', Value: '{msg['value']}'")
```

Please refer API docs in [kafka_client/api.py](kafka_client/api.py) for detailed request and response formats of all APIs.

## Testing the Implementation

To run the comprehensive test suite that validates all Kafka operations:

```bash
uv run python -m kafka_client.api_tester
```

This will run tests for all implemented APIs:
- ✅ Connect to Kafka (API_VERSIONS)
- ✅ Get Topics Overview (METADATA)  
- ✅ Create Topics (CREATE_TOPICS)
- ✅ Describe Topics (DESCRIBE_TOPIC_PARTITIONS)
- ✅ Produce Messages (PRODUCE)
- ✅ Consume Messages (FETCH)

The test creates a new topic, produces messages to it, and then consumes those messages back, demonstrating the complete Kafka workflow.


# Kafka Protocol Grammar

This section documents the Kafka protocol formats implemented in this mini Kafka implementation.

## Basic Protocol Structure

```
RequestOrResponse => Size (RequestMessage | ResponseMessage)
  Size => int32
ErrorCode = 0
```

## Request/Response Headers

```
Request Header v2 => request_api_key request_api_version correlation_id client_id TAG_BUFFER 
  request_api_key => INT16
  request_api_version => INT16
  correlation_id => INT32
  client_id => NULLABLE_STRING
```

```
Response Header v1 => correlation_id TAG_BUFFER 
  correlation_id => INT32
```

## API Versions (API Key: 18)

```
ApiVersions Request (Version: 2) => 
```

```
ApiVersions Request (Version: 3) => client_software_name client_software_version TAG_BUFFER 
  client_software_name => COMPACT_STRING
  client_software_version => COMPACT_STRING
```

```
ApiVersions Response (Version: 2) => error_code [api_keys] throttle_time_ms 
  error_code => INT16
  api_keys => api_key min_version max_version 
    api_key => INT16
    min_version => INT16
    max_version => INT16
  throttle_time_ms => INT32
```

```
ApiVersions Response (Version: 3) => error_code [api_keys] throttle_time_ms TAG_BUFFER 
  error_code => INT16
  api_keys => api_key min_version max_version TAG_BUFFER 
    api_key => INT16
    min_version => INT16
    max_version => INT16
  throttle_time_ms => INT32
```

```
ApiVersions Response (Version: 0) => error_code [api_keys] 
  error_code => INT16
  api_keys => api_key min_version max_version 
    api_key => INT16
    min_version => INT16
    max_version => INT16
```

## Metadata (API Key: 3)

```
Metadata Request (Version: 1) => [topics] allow_auto_topic_creation TAG_BUFFER 
  topics => name TAG_BUFFER 
    name => COMPACT_STRING
  allow_auto_topic_creation => BOOLEAN
```

```
Metadata Response (Version: 1) => throttle_time_ms [brokers] cluster_id controller_id [topics] TAG_BUFFER 
  throttle_time_ms => INT32
  brokers => node_id host port rack TAG_BUFFER 
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
  cluster_id => COMPACT_NULLABLE_STRING
  controller_id => INT32
  topics => error_code name topic_id is_internal [partitions] topic_authorized_operations TAG_BUFFER 
    error_code => INT16
    name => COMPACT_STRING
    topic_id => UUID
    is_internal => BOOLEAN
    partitions => error_code partition_index leader_id leader_epoch [replica_nodes] [isr_nodes] offline_replicas TAG_BUFFER 
      error_code => INT16
      partition_index => INT32
      leader_id => INT32
      leader_epoch => INT32
      replica_nodes => INT32
      isr_nodes => INT32
      offline_replicas => INT32
    topic_authorized_operations => INT32
```

## Create Topics (API Key: 19)

```
CreateTopics Request (Version: 7) => [topics] timeout_ms validate_only TAG_BUFFER 
  topics => name num_partitions replication_factor [assignments] [configs] TAG_BUFFER 
    name => COMPACT_STRING
    num_partitions => INT32
    replication_factor => INT16
    assignments => partition_index [broker_ids] TAG_BUFFER 
      partition_index => INT32
      broker_ids => INT32
    configs => name value TAG_BUFFER 
      name => COMPACT_STRING
      value => COMPACT_NULLABLE_STRING
  timeout_ms => INT32
  validate_only => BOOLEAN
```

```
CreateTopics Response (Version: 7) => throttle_time_ms [topics] TAG_BUFFER 
  throttle_time_ms => INT32
  topics => name topic_id error_code error_message topic_config_error_code num_partitions replication_factor [configs] TAG_BUFFER 
    name => COMPACT_STRING
    topic_id => UUID
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    topic_config_error_code => INT16
    num_partitions => INT32
    replication_factor => INT16
    configs => name value read_only config_source is_sensitive TAG_BUFFER 
      name => COMPACT_STRING
      value => COMPACT_NULLABLE_STRING
      read_only => BOOLEAN
      config_source => INT8
      is_sensitive => BOOLEAN
```

## Produce (API Key: 0)

```
Produce Request (Version: 8) => transactional_id acks timeout_ms [topic_data] TAG_BUFFER 
  transactional_id => COMPACT_NULLABLE_STRING
  acks => INT16
  timeout_ms => INT32
  topic_data => name [partition_data] TAG_BUFFER 
    name => COMPACT_STRING
    partition_data => index records TAG_BUFFER 
      index => INT32
      records => COMPACT_RECORDS
```

```
Produce Response (Version: 8) => [responses] throttle_time_ms TAG_BUFFER 
  throttle_time_ms => INT32
  responses => name [partition_responses] TAG_BUFFER 
    name => COMPACT_STRING
    partition_responses => index error_code base_offset log_append_time log_start_offset [record_errors] error_message TAG_BUFFER 
      index => INT32
      error_code => INT16
      base_offset => INT64
      log_append_time => INT64
      log_start_offset => INT64
      record_errors => batch_index batch_index_error_message 
        batch_index => INT32
        batch_index_error_message => COMPACT_NULLABLE_STRING
      error_message => COMPACT_NULLABLE_STRING
```

## Fetch (API Key: 1)

```
Fetch Request (Version: 16) => max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data] rack_id TAG_BUFFER 
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  session_id => INT32
  session_epoch => INT32
  topics => topic_id [partitions] TAG_BUFFER 
    topic_id => UUID
    partitions => partition current_leader_epoch fetch_offset last_fetched_epoch log_start_offset partition_max_bytes TAG_BUFFER 
      partition => INT32
      current_leader_epoch => INT32
      fetch_offset => INT64
      last_fetched_epoch => INT32
      log_start_offset => INT64
      partition_max_bytes => INT32
  forgotten_topics_data => topic_id [partitions] TAG_BUFFER 
    topic_id => UUID
    partitions => INT32
  rack_id => COMPACT_STRING
```

```
Fetch Response (Version: 10) => throttle_time_ms error_code session_id [responses] 
  throttle_time_ms => INT32
  error_code => INT16
  session_id => INT32
  responses => topic [partitions] 
    topic => STRING
    partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] records 
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      log_start_offset => INT64
      aborted_transactions => producer_id first_offset 
        producer_id => INT64
        first_offset => INT64
      records => RECORDS
```

```
Fetch Response (Version: 16) => throttle_time_ms error_code session_id [responses] TAG_BUFFER 
  throttle_time_ms => INT32
  error_code => INT16
  session_id => INT32
  responses => topic_id [partitions] TAG_BUFFER 
    topic_id => UUID
    partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica records TAG_BUFFER 
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      log_start_offset => INT64
      aborted_transactions => producer_id first_offset TAG_BUFFER 
        producer_id => INT64
        first_offset => INT64
      preferred_read_replica => INT32
      records => COMPACT_RECORDS
```

## Describe Topic Partitions (API Key: 75)

```
DescribeTopicPartitions Request (Version: 0) => [topics] response_partition_limit cursor TAG_BUFFER 
  topics => name TAG_BUFFER 
    name => COMPACT_STRING
  response_partition_limit => INT32
  cursor => topic_name partition_index TAG_BUFFER 
    topic_name => COMPACT_STRING
    partition_index => INT32
```

**Note**: The cursor field is nullable. If it is null, it will be encoded as `0xff`.

```
DescribeTopicPartitions Response (Version: 0) => throttle_time_ms [topics] next_cursor TAG_BUFFER 
  throttle_time_ms => INT32
  topics => error_code name topic_id is_internal [partitions] topic_authorized_operations TAG_BUFFER 
    error_code => INT16
    name => COMPACT_NULLABLE_STRING
    topic_id => UUID
    is_internal => BOOLEAN
    partitions => error_code partition_index leader_id leader_epoch [replica_nodes] [isr_nodes] [eligible_leader_replicas] [last_known_elr] [offline_replicas] TAG_BUFFER 
      error_code => INT16
      partition_index => INT32
      leader_id => INT32
      leader_epoch => INT32
      replica_nodes => INT32
      isr_nodes => INT32
      eligible_leader_replicas => INT32
      last_known_elr => INT32
      offline_replicas => INT32
    topic_authorized_operations => INT32
  next_cursor => topic_name partition_index TAG_BUFFER 
    topic_name => COMPACT_STRING
    partition_index => INT32
```

## Supported API Keys

This Kafka implementation supports the following API keys:

| API Key | API Name | Supported Versions | Description |
|---------|----------|-------------------|-------------|
| 0 | Produce | 8, 11 | Send messages to topics |
| 1 | Fetch | 16 | Retrieve messages from topics |
| 3 | Metadata | 12 | Get cluster and topic metadata |
| 18 | ApiVersions | 4 | Get supported API versions |
| 19 | CreateTopics | 7 | Create new topics |
| 75 | DescribeTopicPartitions | 0 | Get detailed topic partition information |

