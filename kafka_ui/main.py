import gradio as gr
import pandas as pd
from typing import List, Dict, Any
from dataclasses import dataclass, asdict, is_dataclass
from pprint import pformat
import time
import threading
import os
import glob
import sys

# Add parent directory to path to import sibling modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka_client.client_utilities import *
from kafka_client.request_builder import *
from kafka_client.response_parser import *
from kafka_client.api import get_all_topic_names
from kafka_ui.constants import *

# Global variables for real-time streaming
streaming_active = False
streaming_thread = None
streaming_lock = threading.Lock()
streaming_offsets = {}  # Track offsets per topic-partition


def cleanup_kafka_data():
    """Clean up Kafka data files in /tmp/kraft-combined-logs/"""
    import shutil
    
    kafka_data_dir = "/tmp/kraft-combined-logs"
    
    try:
        if os.path.exists(kafka_data_dir):
            # Remove all contents but keep the directory
            for item in os.listdir(kafka_data_dir):
                item_path = os.path.join(kafka_data_dir, item)
                if os.path.isdir(item_path):
                    shutil.rmtree(item_path)
                else:
                    os.remove(item_path)
            
            return "✅ Kafka data cleaned successfully! All files in /tmp/kraft-combined-logs/ have been removed."
        else:
            return "⚠️ Kafka data directory /tmp/kraft-combined-logs/ does not exist."
    except Exception as e:
        return f"❌ Error cleaning Kafka data: {str(e)}"


def connect_to_kafka():
    """Connecting to Kafka broker"""
    time.sleep(4) # Wait for kafka to be ready
    try:
        request = create_api_versions_request()
        add_api_log("ApiVersionsRequest", request)

        response_data, dropDown = parse_api_versions_response(request, add_api_log)
        return True, dropDown
    except Exception as e:
        add_api_log("CONNECT", f"Failed to connect: {str(e)}")
        return False, api_logs_dropdown

def get_topics_overview_api():
    """Get overview of all topics"""
    try:
        request = create_metadata_request()
        add_api_log("MetadataRequest", request)

        response_data, dropDown = parse_metadata_response(request, add_api_log)
        return response_data, dropDown
    except Exception as e:
        print(f"Error occurred while fetching metadata: {e}")
        add_api_log("METADATA ERROR", f"Error: {str(e)}")
        # Return fallback data
        return {
            "total_topics": 0,
            "total_partitions": 0,
            "total_records": 0,
            "topics": []
        }, api_logs_dropdown

def create_topics_api(topics_data):
    """Create topics via API"""
    try:
        request = create_create_topics_request(topics_data)
        add_api_log("CreateTopicsRequest", request)

        results, dropDown = parse_create_topics_response(request, add_api_log)
        return results, dropDown
    except Exception as e:
        add_api_log("CREATE_TOPICS", f"Error: {str(e)}")
        # Return error results
        return [{
            "status": "❌",
            "topic_name": topic["topic_name"],
            "topic_id": "",
            "partitions": topic["partitions"],
            "error": str(e)
        } for topic in topics_data], api_logs_dropdown

def describe_topic_api(topic_requests):
    """Describe topic partitions"""
    try:
        request = create_describe_topics_request(topic_requests)
        add_api_log("DescribeTopicsRequest", request)

        results, dropDown = parse_describe_topics_response(request, add_api_log, topic_requests) # topic_requests for filtering
        return results, dropDown
    except Exception as e:
        print(f"Error: {str(e)}")
        add_api_log("DESCRIBE_TOPICS", f"Error: {str(e)}")

        return [{
            "status": "❌",
            "topic_name": req["topic_name"],
            "partition": 0,
            "error_code": -1,
            "leader_id": -1,
            "replicas": 0
        } for req in topic_requests], api_logs_dropdown

def produce_messages_api(produce_requests):
    """Produce messages to topics"""
    try:
        request = create_produce_request(produce_requests)
        add_api_log("ProduceRequest", request)

        results, dropDown = parse_produce_response(request, add_api_log, produce_requests)
        return results, dropDown
    except Exception as e:
        add_api_log("PRODUCE ERROR", f"Error: {str(e)}")
        # Return error results
        return [{
            "topic": req["topic_name"],
            "partition": req["partition"],
            "records_added": 0,
            "base_offset": -1,
        } for req in produce_requests], api_logs_dropdown

def consume_messages_api(consume_requests):
    """Consume messages from topics"""
    try:
        request = create_fetch_request(consume_requests)
        add_api_log("FetchRequest", request)

        results, dropDown = parse_fetch_response(request, add_api_log)
        return results, dropDown
    except Exception as e:
        add_api_log("CONSUME ERROR", f"Error: {str(e)}")

        # Return empty results
        return [], api_logs_dropdown

# Global variables to store data
pending_topics = []
pending_describe_requests = []
pending_produce_requests = []
pending_consume_requests = []
produce_records = []
api_logs = []
api_request_tracker = {}

def refresh_topic_dropdowns():
    """Refresh all topic dropdowns"""
    try:
        topics = get_all_topic_names()
        return (
            gr.update(choices=topics, value=topics[0] if topics else None),  # describe
            gr.update(choices=topics, value=topics[0] if topics else None),  # produce  
            gr.update(choices=topics, value=topics[0] if topics else None)   # consume
        )
    except Exception as e:
        print(f"Error refreshing topic dropdowns: {e}")
        return (
            gr.update(choices=[], value=None),
            gr.update(choices=[], value=None), 
            gr.update(choices=[], value=None)
        )

def start_real_time_streaming():
    """Start real-time message streaming"""
    global streaming_active, streaming_thread

    if not pending_consume_requests:
        return "▶️ Start Real-time Streaming"

    with streaming_lock:
        if streaming_active:
            return "⚠️ Streaming already active!"
        
        streaming_active = True
        for pending in pending_consume_requests:
            topic_name = pending["topic_name"]
            partition = pending["partition"]
            start_offset = pending["start_offset"]
            streaming_offsets[(topic_name, partition)] = start_offset
        
        def streaming_worker():
            global streaming_active
            
            while streaming_active:
                try:
                    
                    for pending in pending_consume_requests:
                        topic_name = pending["topic_name"]
                        partition = pending["partition"]
                        start_offset = streaming_offsets.get((topic_name, partition), pending["start_offset"])
                        pending["start_offset"] = start_offset  # Update to current offset
                    
                    request = create_fetch_request(pending_consume_requests)
                    add_api_log("FetchRequest", request)

                    time.sleep(2)  # Poll every 2 seconds
                    
                except Exception as e:
                    print(f"Streaming error: {e}")
                    time.sleep(2)
        
        streaming_thread = threading.Thread(target=streaming_worker, daemon=True)
        streaming_thread.start()
        
        return "🔴 Stop Streaming"

def stop_real_time_streaming():
    """Stop real-time message streaming"""
    global streaming_active, streaming_thread
    
    with streaming_lock:
        streaming_active = False
        if streaming_thread:
            streaming_thread.join(timeout=1)
        streaming_thread = None
        streaming_offsets.clear()
        
        return "▶️ Start Real-time Streaming"

def toggle_real_time_streaming(current_state):
    """Toggle real-time streaming on/off"""

    if current_state == "▶️ Start Real-time Streaming":

        global pending_consume_requests
        if not pending_consume_requests:
            gr.Warning("⚠️ No consume requests added for streaming!")
        else:
            gr.Info("""
                🌊 Real-time Streaming Active!\n
                
                💡Tip: Open this app in another browser tab to produce messages while viewing live updates here!
            """
            )
            return "🔴 Stop Streaming", gr.update(interactive=False), gr.update(active=True)
        
    return "▶️ Start Real-time Streaming", gr.update(interactive=True), gr.update(active=False)

def update_connection_status():
    """Update connection status with loading animation"""
    status, dropDown =  connect_to_kafka()
    if status:
        return "🟢 **Connected to Kafka Broker**", dropDown
    else:
        return "🔴 **Failed to connect to Kafka Broker**", dropDown

def get_kafka_files():
    """Get list of Kafka data files"""
    try:
        files = glob.glob("/tmp/kraft-combined-logs/*")
        if not files:
            return []
        return [(os.path.basename(f), f) for f in files]
    except Exception as e:
        print(f"Error getting Kafka files: {e}")
        return []

# Tab functions
def load_overview():
    """Load overview data"""
    time.sleep(3)  # Wait for kafka to be ready
    data, dropDown = get_topics_overview_api()
    
    # DataFrame for topics table
    topics_df = pd.DataFrame(data["topics"])
    
    return (
        f"📊 **Total Topics:** {data['total_topics']}",
        f"🗂️ **Total Partitions:** {data['total_partitions']}",
        f"📝 **Total Records:** {data['total_records']}",
        topics_df,
        dropDown
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
    
    results, dropDown = create_topics_api(pending_topics)
    pending_topics = []  # Clear pending topics
    
    return pd.DataFrame(results), pd.DataFrame(columns=["topic_name", "partitions", "replication"]), dropDown

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
        return df

def describe_topics():
    """Describe all pending topic requests"""
    global pending_describe_requests
    if not pending_describe_requests:
        return pd.DataFrame(columns=["status", "topic_name", "partition", "error_code", "leader_id", "replicas"]), \
            pd.DataFrame(columns=["topic_name", "partitions"]), api_logs_dropdown
    
    results, dropDown = describe_topic_api(pending_describe_requests)
    pending_describe_requests = []

    return pd.DataFrame(results), pd.DataFrame(columns=["topic_name", "partitions"]), dropDown

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
            "record_count": len(produce_records),
            "records": produce_records.copy()  # Store a copy of current records
        })
        produce_records = []  # Clear records after adding to request
        
        df_requests = pd.DataFrame([{
            "topic_name": req["topic_name"],
            "partition": req["partition"],
            "record_count": req["record_count"]
        } for req in pending_produce_requests])
        df_records = pd.DataFrame(columns=["key", "value"])
        return df_requests, df_records
    return pd.DataFrame(columns=["topic_name", "partition", "record_count"]), pd.DataFrame(columns=["key", "value"])

def produce_messages():
    """Produce all pending messages"""
    global pending_produce_requests
    if not pending_produce_requests:
        return pd.DataFrame(columns=["topic", "partition", "records_added", "base_offset"]), pd.DataFrame(columns=["key", "value"]), api_logs_dropdown

    results, dropDown = produce_messages_api(pending_produce_requests)
    pending_produce_requests = []

    return pd.DataFrame(results), pd.DataFrame(columns=["topic_name", "partition", "record_count", "base_offset"]), dropDown

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
        return df 

def consume_messages(streaming_toggle_btn):
    """Consume messages from all pending requests"""
    global pending_consume_requests
    if not pending_consume_requests:
        return pd.DataFrame(columns=["topic", "partition", "offset", "key", "value"]), \
            pd.DataFrame(columns=["topic_name", "partition", "start_offset", "max_messages"]), api_logs_dropdown
    
    results, dropDown = consume_messages_api(pending_consume_requests)
    # print(streaming_toggle_btn)
    if streaming_toggle_btn == "▶️ Start Real-time Streaming":
        pending_consume_requests = []

    return pd.DataFrame(results), pd.DataFrame(columns=["topic_name", "partition", "start_offset", "max_messages"]), dropDown

def get_log_details(selected_log):
    """Get details of selected log"""

    api_log_output = "Select a log entry to view details"
    if selected_log and selected_log != "No requests yet!":
        api_log_output = f"{selected_log}\n{api_request_tracker[selected_log]}".replace("```", "")

    return api_log_output

# Create the Gradio interface
with gr.Blocks(title="🚀 Kafka Broker Management UI",
            #    css=CUSTOM_CSS,
               head=CUSTOM_JS,
               fill_width=True) as demo:
    
    # Header section
    with gr.Row():
        with gr.Column(scale=8):
            gr.Markdown("# 🚀 **Kafka Broker Management UI**")
        # with gr.Column():
            connection_status = gr.Markdown("🔄 **Connecting to Kafka Broker...**")
    
    # Main content area
    with gr.Row():
        with gr.Column(scale=6):
            with gr.Tabs() as tabs:
                # Overview Tab
                with gr.TabItem("📊 Overview", elem_id="overview-tab"):
                    with gr.Row():
                        total_topics = gr.Markdown("📊 **Total Topics:** Loading...")
                        total_partitions = gr.Markdown("🗂️ **Total Partitions:** Loading...")
                        total_records = gr.Markdown("📝 **Total Records:** Loading...")
                    
                    with gr.Row():
                        refresh_overview = gr.Button("🔄 Refresh Overview", variant="primary", elem_id="refresh-button")
                        download_data_btn = gr.Button("📥 Download Kafka Data", variant="secondary")
                        cleanup_data_btn = gr.Button("🗑️ Cleanup Kafka Data", variant="stop")
                    
                    overview_table = gr.Dataframe(
                        headers=["Topic Name", "Topic ID", "Partition", "Records"],
                        label="📋 Topics Overview",
                        show_row_numbers=True,
                        row_count=14,
                        show_fullscreen_button=True,
                        show_search="filter",
                        wrap=True
                    )
                
                # Create Topics Tab
                with gr.TabItem("➕ Create Topics"):
                    with gr.Row():
                        topic_name_input = gr.Textbox(label="📝 Topic Name", placeholder="Enter topic name")
                        partitions_slider = gr.Slider(1, 5, value=1, step=1, label="🗂️ Number of Partitions")
                        replication_slider = gr.Slider(1, 3, value=1, step=1, label="🔄 Replication Factor")
                    
                    add_topic_btn = gr.Button("➕ Add Topic", variant="secondary")
                    pending_topics_table = gr.Dataframe(
                        headers=["Topic Name", "Partitions", "Replication", "🗑️"],
                        label="📋 Pending Topics",
                        
                    )
                    
                    create_topics_btn = gr.Button("🚀 Create All Topics", variant="primary")
                    creation_results_table = gr.Dataframe(
                        headers=["Status", "Topic Name", "Topic ID", "Partitions", "Error"],
                        label="📊 Creation Results",
                        show_row_numbers=True,
                        row_count=14,
                        show_fullscreen_button=True,
                        show_search="filter",
                        wrap=True
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
                
                # Describe Topic Tab
                with gr.TabItem("🔍 Describe Topics"):
                    with gr.Row():
                        describe_topic_input = gr.Dropdown(
                            label="📝 Topic Name", 
                            choices=[], 
                            value=None,
                            allow_custom_value=True
                        )
                        
                        partitions_multiselect = gr.CheckboxGroup(
                            choices=[0, 1, 2, 3, 4], 
                            label="🗂️ Select Partitions",
                            value=[0]
                        )
                    
                    add_describe_btn = gr.Button("➕ Add Describe Request", variant="secondary")
                    pending_describe_table = gr.Dataframe(
                        headers=["Topic Name", "Partitions"],
                        label="📋 Pending Describe Requests",
                        show_row_numbers=True,
                        row_count=14,
                        show_fullscreen_button=True,
                        show_search="filter",
                        wrap=True
                    )
                    
                    describe_btn = gr.Button("🔍 Describe Topics", variant="primary")
                    describe_results_table = gr.Dataframe(
                        headers=["Status", "Topic Name", "Partition", "Error Code", "Leader ID", "Replicas"],
                        label="📊 Describe Results"
                    )
                    
                    add_describe_btn.click(
                        add_describe_request,
                        inputs=[describe_topic_input, partitions_multiselect],
                        outputs=[pending_describe_table]
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
                        label="📋 Records to Produce",
                        show_row_numbers=True,
                        row_count=14,
                        show_fullscreen_button=True,
                        show_search="filter",
                        wrap=True
                    )
                    
                    gr.Markdown("### 🎯 Topic Configuration")
                    with gr.Row():
                        produce_topic_input = gr.Dropdown(
                            label="📝 Topic Name", 
                            choices=[], 
                            value=None,
                            allow_custom_value=True
                        )

                        produce_partition_input = gr.Number(label="🗂️ Partition", value=0, minimum=0)
                    
                    add_produce_request_btn = gr.Button("➕ Add to Produce Queue", variant="secondary")
                    produce_requests_table = gr.Dataframe(
                        headers=["Topic Name", "Partition", "Record Count"],
                        label="📋 Produce Requests",
                        show_row_numbers=True,
                        row_count=14,
                        show_fullscreen_button=True,
                        show_search="filter",
                        wrap=True
                    )
                    
                    produce_btn = gr.Button("🚀 Produce Messages", variant="primary")
                    produce_results_table = gr.Dataframe(
                        headers=["Topic", "Partition", "Records Added", "Base Offset"],
                        label="📊 Produce Results",
                        show_row_numbers=True,
                        row_count=14,
                        show_fullscreen_button=True,
                        show_search="filter",
                        wrap=True
                    )
                    
                    add_record_btn.click(
                        add_produce_record,
                        inputs=[record_key, record_value],
                        outputs=[records_table, record_key, record_value]
                    )
                    
                    add_produce_request_btn.click(
                        add_produce_request,
                        inputs=[produce_topic_input, produce_partition_input],
                        outputs=[produce_requests_table, records_table]
                    )
                    
                
                # Consume Tab
                with gr.TabItem("📥 Consume"):
                    with gr.Row():
                        consume_topic_input = gr.Dropdown(
                            label="📝 Topic Name", 
                            choices=[], 
                            value=None,
                            allow_custom_value=True
                        )

                        consume_partition_input = gr.Number(label="🗂️ Partition", value=0, minimum=0)
                    
                    with gr.Row():
                        start_offset_input = gr.Number(label="📍 Start Offset", value=0, minimum=0)
                        max_messages_slider = gr.Slider(1, 100, value=10, step=1, label="📊 Max Messages")
                    
                    add_consume_request_btn = gr.Button("➕ Add Consume Request", variant="secondary")
                    consume_requests_table = gr.Dataframe(
                        headers=["Topic Name", "Partition", "Start Offset", "Max Messages"],
                        label="📋 Consume Requests",
                        show_row_numbers=True,
                        row_count=14,
                        show_fullscreen_button=True,
                        show_search="filter",
                        wrap=True
                    )
                    
                    # Real-time streaming and consume buttons
                    with gr.Row():
                        streaming_toggle_btn = gr.Button("▶️ Start Real-time Streaming", variant="primary")
                        consume_btn = gr.Button("📥 Consume Messages", variant="primary")
                    
                    consume_results_table = gr.Dataframe(
                        headers=["Topic", "Partition", "Batch Offset", "Key", "Value"],
                        column_widths=["20%", "10%", "10%", "30%", "30%"],
                        label="📊 Consumed Messages",
                        show_row_numbers=True,
                        row_count=14,
                        show_fullscreen_button=True,
                        show_search="filter",
                        wrap=True
                    )

                    add_consume_request_btn.click(
                        add_consume_request,
                        inputs=[consume_topic_input, consume_partition_input, start_offset_input, max_messages_slider],
                        outputs=[consume_requests_table]
                    )
                
                # Downloads Tab
                with gr.TabItem("📁 Downloads", elem_id="downloads-tab") as download_tab:
                    gr.Markdown("### 📥 Download Kafka Data")
                    download_dropdown = gr.Dropdown(
                        label="From the dropdown, select <TopicName>-<PartitionNumber> pair to download...",
                        choices=get_kafka_files(),
                        value=None
                    )
                    download_file = gr.File(label="Selected file", interactive=False, type="binary")
                    gr.Textbox(
                        value="Downloaded log file will be in binary and user would need to decode via Kafka protocol to make sense of it. Happy exploring Kafka!",
                        label="ℹ️ Info",
                        interactive=False,
                        lines=3
                    )
        
        # Second column (scale 3)
        with gr.Column(scale=3):
            gr.Markdown("### 📡 **API Activity Logs ([Kafka Wire Protocol](https://kafka.apache.org/protocol.html))**")
            api_logs_dropdown = gr.Dropdown(
                choices=api_logs,
                value=api_logs[0] if api_logs else "No requests yet!",
                label="Recent API Calls",
                interactive=True,
                allow_custom_value=True
            )
            
            log_details = gr.Textbox("Select a log entry to view details", lines=20, label="Log Details", max_lines=20, autoscroll=False)
            # log_details = gr.HTML("Select a log entry to view details", elem_id="log-details", label="Log Details", min_height=400, max_height=400)
    
    # Cleanup warning section
    with gr.Column(visible=False) as cleanup_warning_modal:

        gr.Markdown(WARNING_MESSAGE)
        
        with gr.Row():
            cleanup_confirm_btn = gr.Button("🗑️ Yes, Delete All Data", variant="stop")
            cleanup_cancel_btn = gr.Button("❌ Cancel", variant="secondary")
            
    def add_api_log(action, details):
        """Add API call to logs"""

        if is_dataclass(details):
            details = asdict(details) # type: ignore
            
        global api_logs
        timestamp = time.strftime("%H:%M:%S")
        api_logs.insert(0, f"[{timestamp}] {action}")
        api_request_tracker[f"[{timestamp}] {action}"] = f"```\n{pformat(details, indent=2, width=80)}\n```"

        print(action)
        return gr.update(choices=api_logs[0:15], value=api_logs[0] if len(api_logs) > 0 else "")  # Update dropdown with latest log

    # Update logs dropdown when API calls are made
    api_logs_dropdown.change(
        get_log_details,
        inputs=[api_logs_dropdown],
        outputs=[log_details]
    )

    refresh_overview.click(
        update_connection_status,
        outputs=[connection_status, api_logs_dropdown]
    ).then(
        load_overview,
        outputs=[total_topics, total_partitions, total_records, overview_table, api_logs_dropdown]
    ).then(
        refresh_topic_dropdowns,
        outputs=[describe_topic_input, produce_topic_input, consume_topic_input]
    )

    download_dropdown.change(
        lambda x: x + "/00000000000000000000.log",
        inputs=[download_dropdown],
        outputs=[download_file]
    )

    download_data_btn.click(
        lambda: gr.Info("To download Kafka data files, go to Downloads Tab (maybe in overflow menu) select a topic from the dropdown and click the 'Download' button below it."),
    )

    cleanup_data_btn.click(
        lambda: gr.update(visible=True),
        outputs=[cleanup_warning_modal]
    ).then(
        lambda: gr.Info("Read Warning message at the end of the page and select 'Yes' to proceed!")
	)

    def handle_cleanup_confirm():
        result = cleanup_kafka_data()
        gr.Info(result)
        return gr.update(visible=False)

    cleanup_confirm_btn.click(
        handle_cleanup_confirm,
        outputs=[cleanup_warning_modal]
    ).then(
        load_overview,
        outputs=[total_topics, total_partitions, total_records, overview_table, api_logs_dropdown]
    ).then(
        refresh_topic_dropdowns,
        outputs=[describe_topic_input, produce_topic_input, consume_topic_input]
    )

    cleanup_cancel_btn.click(
        lambda: gr.update(visible=False),
        outputs=[cleanup_warning_modal]
    )

    create_topics_btn.click(
        create_all_topics,
        outputs=[creation_results_table, pending_topics_table, api_logs_dropdown]
    ).then(
        refresh_topic_dropdowns,
        outputs=[describe_topic_input, produce_topic_input, consume_topic_input]
    )

    describe_btn.click(
        describe_topics,
        outputs=[describe_results_table, pending_describe_table, api_logs_dropdown]
    )

    produce_btn.click(
        produce_messages,
        outputs=[produce_results_table, produce_requests_table, api_logs_dropdown]
    )

    consume_btn.click(
        consume_messages,
        inputs=[streaming_toggle_btn],
        outputs=[consume_results_table, consume_requests_table, api_logs_dropdown]
    )
    
    timer = gr.Timer(2, active=False)
    timer.tick(
        consume_messages,
        inputs=[streaming_toggle_btn],
        outputs=[consume_results_table, consume_requests_table, api_logs_dropdown]
    )

    # Streaming toggle handler
    streaming_toggle_btn.click(
        toggle_real_time_streaming,
        inputs=[streaming_toggle_btn],
        outputs=[streaming_toggle_btn, consume_btn, timer]
    )


if __name__ == "__main__":
    demo.launch()

