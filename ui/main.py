import gradio as gr
import pandas as pd
from typing import List, Dict, Any
import time
import uuid

from kafka_client.client_utilities import *
from kafka_client.response_parser import *
from kafka_client.client_utilities import *


def connect_to_kafka():
    """Connecting to Kafka broker"""

    time.sleep(2)
    return True

def get_topics_overview():
    """Get overview of all topics"""
    


    return {
        "total_topics": 5,
        "total_partitions": 15,
        "total_records": 1250,
        "topics": [
            {"topic_name": "user-events", "topic_id": "topic-001", "partition": 0, "records": 350},
            {"topic_name": "user-events", "topic_id": "topic-001", "partition": 1, "records": 280},
            {"topic_name": "user-events", "topic_id": "topic-001", "partition": 2, "records": 120},
            {"topic_name": "order-processing", "topic_id": "topic-002", "partition": 0, "records": 200},
            {"topic_name": "order-processing", "topic_id": "topic-002", "partition": 1, "records": 150},
            {"topic_name": "system-logs", "topic_id": "topic-003", "partition": 0, "records": 150},
        ]
    }

def create_topics_api(topics_data):
    """Create topics via API"""
    # Mock response
    results = []
    for topic in topics_data:
        results.append({
            "status": "✅" if topic["topic_name"] != "error-topic" else "❌",
            "topic_name": topic["topic_name"],
            "topic_id": f"topic-{uuid.uuid4().hex[:6]}",
            "partitions": topic["partitions"],
            "error": "" if topic["topic_name"] != "error-topic" else "Topic already exists"
        })
    return results

def describe_topic_api(topic_requests):
    """Describe topic partitions"""
    # Mock response
    results = []
    for req in topic_requests:
        for partition in req["partitions"]:
            results.append({
                "status": "✅",
                "topic_name": req["topic_name"],
                "partition": partition,
                "error_code": 0,
                "leader_id": 1,
                "replicas": 3
            })
    return results

def produce_messages_api(produce_requests):
    """Produce messages to topics"""
    # Mock response
    results = []
    for req in produce_requests:
        results.append({
            "topic": req["topic_name"],
            "partition": req["partition"],
            "records_added": req["record_count"]
        })
    return results

def consume_messages_api(consume_requests):
    """Consume messages from topics"""
    # Mock response
    results = []
    for req in consume_requests:
        for i in range(min(req["max_messages"], 3)):  # Return max 3 mock messages
            results.append({
                "topic": req["topic_name"],
                "partition": req["partition"],
                "offset": req["start_offset"] + i,
                "key": f"key-{i+1}",
                "value": f"Sample message {i+1} from {req['topic_name']}"
            })
    return results

# Global variables to store data
pending_topics = []
pending_describe_requests = []
pending_produce_requests = []
pending_consume_requests = []
produce_records = []
api_logs = []

def add_api_log(action, details):
    """Add API call to logs"""
    global api_logs
    timestamp = time.strftime("%H:%M:%S")
    api_logs.insert(0, f"[{timestamp}] {action}: {details}")
    return api_logs[:10]  # Keep only last 10 logs

def update_connection_status():
    """Update connection status with loading animation"""
    if connect_to_kafka():
        return "##🟢 **Connected to Kafka Broker**"
    else:
        return "##🔴 **Failed to connect to Kafka Broker**"

# Tab functions
def load_overview():
    """Load overview data"""
    data = get_topics_overview()
    add_api_log("METADATA", f"Retrieved {data['total_topics']} topics")
    
    # Create DataFrame for topics table
    topics_df = pd.DataFrame(data["topics"])
    
    return (
        f"📊 **Total Topics:** {data['total_topics']}",
        f"🗂️ **Total Partitions:** {data['total_partitions']}",
        f"📝 **Total Records:** {data['total_records']}",
        topics_df
    )

def add_topic_to_pending(topic_name, partitions, replication):
    """Add topic to pending creation list"""
    global pending_topics
    if topic_name:
        pending_topics.append({
            "topic_name": topic_name,
            "partitions": partitions,
            "replication": replication
        })
        df = pd.DataFrame(pending_topics)
        return df, "", 1, 1  # Clear inputs, reset sliders

def remove_topic_from_pending(evt: gr.SelectData):
    """Remove topic from pending list"""
    global pending_topics
    if 0 <= evt.index[0] < len(pending_topics):
        pending_topics.pop(evt.index[0])
        df = pd.DataFrame(pending_topics) if pending_topics else pd.DataFrame(columns=["topic_name", "partitions", "replication"])
        return df

def create_all_topics():
    """Create all pending topics"""
    global pending_topics
    if not pending_topics:
        return pd.DataFrame(columns=["status", "topic_name", "topic_id", "partitions", "error"])
    
    results = create_topics_api(pending_topics)
    add_api_log("CREATE_TOPICS", f"Created {len(results)} topics")
    pending_topics = []  # Clear pending topics
    
    return pd.DataFrame(results), pd.DataFrame(columns=["topic_name", "partitions", "replication"])

def add_describe_request(topic_name, partitions):
    """Add describe request to pending list"""
    global pending_describe_requests
    if topic_name and partitions:
        pending_describe_requests.append({
            "topic_name": topic_name,
            "partitions": partitions
        })
        df = pd.DataFrame([{
            "topic_name": req["topic_name"],
            "partitions": ", ".join(map(str, req["partitions"]))
        } for req in pending_describe_requests])
        return df, ""  # Clear topic name input

def describe_topics():
    """Describe all pending topic requests"""
    global pending_describe_requests
    if not pending_describe_requests:
        return pd.DataFrame(columns=["status", "topic_name", "partition", "error_code", "leader_id", "replicas"])
    
    results = describe_topic_api(pending_describe_requests)
    add_api_log("DESCRIBE_TOPICS", f"Described {len(pending_describe_requests)} topic requests")
    pending_describe_requests = []
    
    return pd.DataFrame(results), pd.DataFrame(columns=["topic_name", "partitions"])

def add_produce_record(key, value):
    """Add record to produce records list"""
    global produce_records
    if key or value:
        produce_records.append({"key": key, "value": value})
        df = pd.DataFrame(produce_records)
        return df, "", ""  # Clear inputs

def add_produce_request(topic_name, partition):
    """Add produce request to pending list"""
    global pending_produce_requests, produce_records
    if topic_name and produce_records:
        pending_produce_requests.append({
            "topic_name": topic_name,
            "partition": partition,
            "record_count": len(produce_records)
        })
        produce_records = []  # Clear records after adding to request
        
        df_requests = pd.DataFrame(pending_produce_requests)
        df_records = pd.DataFrame(columns=["key", "value"])
        return df_requests, df_records, ""  # Clear topic name

def produce_messages():
    """Produce all pending messages"""
    global pending_produce_requests
    if not pending_produce_requests:
        return pd.DataFrame(columns=["topic", "partition", "records_added"])
    
    results = produce_messages_api(pending_produce_requests)
    add_api_log("PRODUCE", f"Produced messages to {len(pending_produce_requests)} topic-partitions")
    pending_produce_requests = []
    
    return pd.DataFrame(results), pd.DataFrame(columns=["topic_name", "partition", "record_count"])

def add_consume_request(topic_name, partition, start_offset, max_messages):
    """Add consume request to pending list"""
    global pending_consume_requests
    if topic_name:
        pending_consume_requests.append({
            "topic_name": topic_name,
            "partition": partition,
            "start_offset": start_offset,
            "max_messages": max_messages
        })
        df = pd.DataFrame(pending_consume_requests)
        return df, ""  # Clear topic name

def consume_messages():
    """Consume messages from all pending requests"""
    global pending_consume_requests
    if not pending_consume_requests:
        return pd.DataFrame(columns=["topic", "partition", "offset", "key", "value"])
    
    results = consume_messages_api(pending_consume_requests)
    add_api_log("CONSUME", f"Consumed messages from {len(pending_consume_requests)} requests")
    pending_consume_requests = []
    
    return pd.DataFrame(results), pd.DataFrame(columns=["topic_name", "partition", "start_offset", "max_messages"])

def update_api_logs():
    """Update API logs dropdown"""
    return gr.Dropdown(choices=api_logs, value=api_logs[0] if api_logs else None)

def get_log_details(selected_log):
    """Get details of selected log"""
    if selected_log:
        return f"**Selected Log:** {selected_log}\n\n📋 **Details:** This shows the API request/response details for the selected operation."
    return "Select a log entry to view details"

# Create the Gradio interface
with gr.Blocks(title="🚀 Kafka Broker Console",
            #    theme=gr.themes.Soft(), # type: ignore
               fill_width=True) as demo:
    # Header section
    with gr.Row():
        with gr.Column(scale=8):
            gr.Markdown("# 🚀 **Kafka Broker Console**")
        with gr.Column():
            connection_status = gr.Markdown("🔄 **Connecting to Kafka Broker...**")
    
    # Update connection status on load
    demo.load(update_connection_status, outputs=[connection_status])
    
    # Main content area
    with gr.Row():
        # First column (scale 7)
        with gr.Column(scale=7):
            with gr.Tabs():
                # Overview Tab
                with gr.TabItem("📊 Overview"):
                    with gr.Row():
                        total_topics = gr.Markdown("📊 **Total Topics:** Loading...")
                        total_partitions = gr.Markdown("🗂️ **Total Partitions:** Loading...")
                        total_records = gr.Markdown("📝 **Total Records:** Loading...")
                    
                    refresh_overview = gr.Button("🔄 Refresh Overview", variant="primary")
                    overview_table = gr.Dataframe(
                        headers=["Topic Name", "Topic ID", "Partition", "Records"],
                        label="📋 Topics Overview"
                    )
                    
                    refresh_overview.click(
                        load_overview,
                        outputs=[total_topics, total_partitions, total_records, overview_table]
                    )
                
                # Create Topics Tab
                with gr.TabItem("➕ Create Topics"):
                    with gr.Row():
                        topic_name_input = gr.Textbox(label="📝 Topic Name", placeholder="Enter topic name")
                        partitions_slider = gr.Slider(1, 10, value=1, step=1, label="🗂️ Number of Partitions")
                        replication_slider = gr.Slider(1, 5, value=1, step=1, label="🔄 Replication Factor")
                    
                    add_topic_btn = gr.Button("➕ Add Topic", variant="secondary")
                    pending_topics_table = gr.Dataframe(
                        headers=["Topic Name", "Partitions", "Replication", "🗑️"],
                        label="📋 Pending Topics"
                    )
                    
                    create_topics_btn = gr.Button("🚀 Create All Topics", variant="primary")
                    creation_results_table = gr.Dataframe(
                        headers=["Status", "Topic Name", "Topic ID", "Partitions", "Error"],
                        label="📊 Creation Results"
                    )
                    
                    add_topic_btn.click(
                        add_topic_to_pending,
                        inputs=[topic_name_input, partitions_slider, replication_slider],
                        outputs=[pending_topics_table, topic_name_input, partitions_slider, replication_slider]
                    )
                    
                    pending_topics_table.select(
                        remove_topic_from_pending,
                        outputs=[pending_topics_table]
                    )
                    
                    create_topics_btn.click(
                        create_all_topics,
                        outputs=[creation_results_table, pending_topics_table]
                    )
                
                # Describe Topic Tab
                with gr.TabItem("🔍 Describe Topics"):
                    with gr.Row():
                        describe_topic_input = gr.Textbox(label="📝 Topic Name", placeholder="Enter topic name")
                        partitions_multiselect = gr.CheckboxGroup(
                            choices=[0, 1, 2, 3, 4], 
                            label="🗂️ Select Partitions",
                            value=[0]
                        )
                    
                    add_describe_btn = gr.Button("➕ Add Describe Request", variant="secondary")
                    pending_describe_table = gr.Dataframe(
                        headers=["Topic Name", "Partitions"],
                        label="📋 Pending Describe Requests"
                    )
                    
                    describe_btn = gr.Button("🔍 Describe Topics", variant="primary")
                    describe_results_table = gr.Dataframe(
                        headers=["Status", "Topic Name", "Partition", "Error Code", "Leader ID", "Replicas"],
                        label="📊 Describe Results"
                    )
                    
                    add_describe_btn.click(
                        add_describe_request,
                        inputs=[describe_topic_input, partitions_multiselect],
                        outputs=[pending_describe_table, describe_topic_input]
                    )
                    
                    describe_btn.click(
                        describe_topics,
                        outputs=[describe_results_table, pending_describe_table]
                    )
                
                # Produce Tab
                with gr.TabItem("📤 Produce"):
                    gr.Markdown("### 📝 Add Records")
                    with gr.Row():
                        record_key = gr.Textbox(label="🔑 Key", placeholder="Enter key")
                        record_value = gr.Textbox(label="💬 Value", placeholder="Enter value")
                    
                    add_record_btn = gr.Button("➕ Add Record", variant="secondary")
                    records_table = gr.Dataframe(
                        headers=["Key", "Value"],
                        label="📋 Records to Produce"
                    )
                    
                    gr.Markdown("### 🎯 Topic Configuration")
                    with gr.Row():
                        produce_topic_input = gr.Textbox(label="📝 Topic Name", placeholder="Enter topic name")
                        produce_partition_input = gr.Number(label="🗂️ Partition", value=0, minimum=0)
                    
                    add_produce_request_btn = gr.Button("➕ Add to Produce Queue", variant="secondary")
                    produce_requests_table = gr.Dataframe(
                        headers=["Topic Name", "Partition", "Record Count"],
                        label="📋 Produce Requests"
                    )
                    
                    produce_btn = gr.Button("🚀 Produce Messages", variant="primary")
                    produce_results_table = gr.Dataframe(
                        headers=["Topic", "Partition", "Records Added"],
                        label="📊 Produce Results"
                    )
                    
                    add_record_btn.click(
                        add_produce_record,
                        inputs=[record_key, record_value],
                        outputs=[records_table, record_key, record_value]
                    )
                    
                    add_produce_request_btn.click(
                        add_produce_request,
                        inputs=[produce_topic_input, produce_partition_input],
                        outputs=[produce_requests_table, records_table, produce_topic_input]
                    )
                    
                    produce_btn.click(
                        produce_messages,
                        outputs=[produce_results_table, produce_requests_table]
                    )
                
                # Consume Tab
                with gr.TabItem("📥 Consume"):
                    with gr.Row():
                        consume_topic_input = gr.Textbox(label="📝 Topic Name", placeholder="Enter topic name")
                        consume_partition_input = gr.Number(label="🗂️ Partition", value=0, minimum=0)
                    
                    with gr.Row():
                        start_offset_input = gr.Number(label="📍 Start Offset", value=0, minimum=0)
                        max_messages_slider = gr.Slider(1, 100, value=10, step=1, label="📊 Max Messages")
                    
                    add_consume_request_btn = gr.Button("➕ Add Consume Request", variant="secondary")
                    consume_requests_table = gr.Dataframe(
                        headers=["Topic Name", "Partition", "Start Offset", "Max Messages"],
                        label="📋 Consume Requests"
                    )
                    
                    consume_btn = gr.Button("📥 Consume Messages", variant="primary")
                    consume_results_table = gr.Dataframe(
                        headers=["Topic", "Partition", "Offset", "Key", "Value"],
                        label="📊 Consumed Messages"
                    )
                    
                    add_consume_request_btn.click(
                        add_consume_request,
                        inputs=[consume_topic_input, consume_partition_input, start_offset_input, max_messages_slider],
                        outputs=[consume_requests_table, consume_topic_input]
                    )
                    
                    consume_btn.click(
                        consume_messages,
                        outputs=[consume_results_table, consume_requests_table]
                    )
        
        # Second column (scale 3)
        with gr.Column(scale=3):
            gr.Markdown("### 📡 **API Activity Logs**")
            api_logs_dropdown = gr.Dropdown(
                choices=api_logs,
                label="Recent API Calls",
                interactive=True
            )
            
            log_details = gr.Markdown("Select a log entry to view details")
            
            # Update logs dropdown when API calls are made
            api_logs_dropdown.change(
                get_log_details,
                inputs=[api_logs_dropdown],
                outputs=[log_details]
            )

if __name__ == "__main__":
    demo.launch()