# Kafka Gradio UI — Initial Layout
# ---------------------------------------------------------------
# This is an incremental starting point for a Kafka control panel
# built with Gradio (Blocks). All API calls are stubbed with
# placeholder functions and dummy data so you can see the full
# layout and interactions. Icons and bold text are sprinkled in
# for a friendlier, more visual feel.
#
# How to run:
#   pip install gradio pandas
#   python app.py

import time
import random
import string
import datetime as dt
from typing import List, Dict, Tuple

import pandas as pd
import gradio as gr

# ---------------------------------------------------------------
# Dummy API functions (stubs)
# ---------------------------------------------------------------

def api_connect_broker() -> Dict:
    """Pretend to connect to a Kafka broker and return a connection summary."""
    time.sleep(1.5)  # simulate latency
    return {
        "ok": True,
        "broker": "localhost:9092",
        "cluster_id": "test-cluster",
        "version": "3.6.0",
    }


def _rand_topic_id() -> str:
    return "t_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=6))


def api_get_topics_overview() -> Dict:
    """Return overview with totals and a flat list of topic/partition stats."""
    topics = [
        {"topic_name": "orders", "topic_id": _rand_topic_id(), "partitions": 3},
        {"topic_name": "payments", "topic_id": _rand_topic_id(), "partitions": 2},
        {"topic_name": "shipments", "topic_id": _rand_topic_id(), "partitions": 4},
    ]
    rows = []
    for t in topics:
        for p in range(t["partitions"]):
            rows.append(
                {
                    "topic_name": t["topic_name"],
                    "topic_id": t["topic_id"],
                    "partition": p,
                    "records": random.randint(100, 2000),
                }
            )
    df = pd.DataFrame(rows, columns=["topic_name", "topic_id", "partition", "records"])
    totals = {
        "total_topics": df["topic_name"].nunique(),
        "total_partitions": df.shape[0],
        "total_records": int(df["records"].sum()),
    }
    return {"totals": totals, "df": df}


def api_create_topics(requests: List[Dict]) -> pd.DataFrame:
    """Pretend to create topics, randomly succeed/fail, and return results."""
    results = []
    for req in requests:
        ok = random.random() > 0.15
        topic_id = _rand_topic_id() if ok else None
        err = "" if ok else random.choice(["INVALID_CONFIG", "BROKER_NOT_AVAILABLE", "TOPIC_ALREADY_EXISTS"]) 
        results.append(
            {
                "status": "✅" if ok else "❌",
                "topic_name": req["topic_name"],
                "topic_id": topic_id or "-",
                "partitions": req["partitions"],
                "replication": req["replication"],
                "error": err,
            }
        )
    return pd.DataFrame(results, columns=["status", "topic_name", "topic_id", "partitions", "replication", "error"])


def api_describe_topic(topic_name: str, partitions: List[int]) -> pd.DataFrame:
    rows = []
    for p in partitions:
        valid = random.random() > 0.1
        rows.append(
            {
                "status": "✅" if valid else "⚠️",
                "topic_name": topic_name,
                "partition": p,
                "error_code": 0 if valid else random.choice([3, 5, 6]),
                "leader_id": random.randint(1, 5) if valid else -1,
                "replicas": random.randint(1, 3) if valid else 0,
            }
        )
    return pd.DataFrame(rows, columns=["status", "topic_name", "partition", "error_code", "leader_id", "replicas"])


def api_produce(topic_name: str, partition: int, records: List[Tuple[str, str]]) -> Dict:
    ok = random.random() > 0.05
    return {
        "ok": ok,
        "topic": topic_name,
        "partition": partition,
        "added": len(records) if ok else 0,
        "error": "" if ok else "NOT_LEADER_FOR_PARTITION",
    }


def api_consume(topic_name: str, partition: int, start_offset: int, max_msgs: int) -> pd.DataFrame:
    count = random.randint(0, max_msgs)
    rows = []
    for i in range(count):
        rows.append(
            {
                "topic": topic_name,
                "partition": partition,
                "offset": start_offset + i,
                "key": f"key-{random.randint(100,999)}",
                "value": f"value-{random.randint(1000,9999)}",
            }
        )
    return pd.DataFrame(rows, columns=["topic", "partition", "offset", "key", "value"])

# ---------------------------------------------------------------
# Logging helpers (right-side column)
# ---------------------------------------------------------------

def now_str():
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def push_log(log: List[Dict], title: str, details_md: str):
    """Prepend a log entry and return: new_log, dropdown.update, top_content."""
    entry = {"title": f"{now_str()} — {title}", "details": details_md}
    new_log = [entry] + (log or [])
    choices = [e["title"] for e in new_log]
    value = choices[0] if choices else None
    content = new_log[0]["details"] if new_log else ""
    return new_log, gr.Dropdown(choices=choices, value=value), content


def log_select_content(log: List[Dict], selection: str) -> str:
    for e in log or []:
        if e["title"] == selection:
            return e["details"]
    return ""

# ---------------------------------------------------------------
# UI event handlers
# ---------------------------------------------------------------

# Header: connect to broker on app load

def on_connect(log):
    res = api_connect_broker()
    if res.get("ok"):
        md = f"✅ **Connected to Kafka Broker** — `{res['broker']}`  \\\n**Cluster:** `{res['cluster_id']}`  |  **Version:** `{res['version']}`"
        log, dd_upd, content = push_log(
            log,
            "Connected to broker",
            f"**Success** connecting to `{res['broker']}`.\n\n``""\n{res}\n``""",
        )
    else:
        md = "❌ **Failed to connect to Kafka Broker**"
        log, dd_upd, content = push_log(log, "Connection failed", "Could not connect.")
    return md, log, dd_upd, content

# Overview

def on_overview_refresh(log):
    data = api_get_topics_overview()
    df = data["df"]
    totals = data["totals"]
    t_md = f"**{totals['total_topics']}**"
    p_md = f"**{totals['total_partitions']}**"
    r_md = f"**{totals['total_records']}**"
    log, dd_upd, content = push_log(
        log,
        "Fetched topics overview",
        f"**Totals** → Topics: **{totals['total_topics']}**, Partitions: **{totals['total_partitions']}**, Records: **{totals['total_records']}**",
    )
    return df, t_md, p_md, r_md, log, dd_upd, content

# Create Topics

def on_create_add(topic_name, partitions, replication, pending, log):
    pending = pending or []
    if not topic_name:
        # No-op but keep UI smooth
        return pd.DataFrame(pending), pending, log, gr.Dropdown(), ""
    pending.append(
        {
            "Topic Name": topic_name,
            "Partitions": int(partitions),
            "Replication": int(replication),
            "🗑️ Cancel": "✖️",
        }
    )
    df = pd.DataFrame(pending, columns=["Topic Name", "Partitions", "Replication", "🗑️ Cancel"])
    log, dd_upd, content = push_log(
        log,
        "Queued topic for creation",
        f"Added **{topic_name}** with **{partitions}** partition(s), replication **{replication}**.",
    )
    return df, pending, log, dd_upd, content


def on_create_table_select(evt: gr.SelectData, df: pd.DataFrame, pending, log):
    # Remove row if the clicked cell is in the cancel column
    if df is None or df.empty:
        return df, pending, log, gr.Dropdown(), ""
    row_idx, col_idx = evt.index
    col_name = df.columns[col_idx]
    if col_name != "🗑️ Cancel":
        return df, pending, log, gr.Dropdown(), ""
    # Drop the selected row
    if 0 <= row_idx < len(df):
        removed = df.iloc[row_idx].to_dict()
        df = df.drop(index=row_idx).reset_index(drop=True)
        # also remove from pending (mirror by index)
        pending = [r for i, r in enumerate(pending or []) if i != row_idx]
        log, dd_upd, content = push_log(
            log,
            "Removed pending topic",
            f"Canceled **{removed.get('Topic Name','?')}**.",
        )
        return df, pending, log, dd_upd, content
    return df, pending, log, gr.Dropdown(), ""


def on_create_submit(pending, log):
    pending = pending or []
    reqs = [
        {
            "topic_name": r["Topic Name"],
            "partitions": int(r["Partitions"]),
            "replication": int(r["Replication"]),
        }
        for r in pending
    ]
    if not reqs:
        return pd.DataFrame(columns=["status", "topic_name", "topic_id", "partitions", "replication", "error"]), log, gr.Dropdown(), ""

    log, dd_upd1, _ = push_log(log, "Sending create-topics request", f"Creating **{len(reqs)}** topic(s)...")
    res_df = api_create_topics(reqs)
    success_cnt = int((res_df["status"] == "✅").sum())
    fail_cnt = int((res_df["status"] != "✅").sum())
    log, dd_upd2, content = push_log(
        log,
        "Create-topics response",
        f"Created: **{success_cnt}**, Failed: **{fail_cnt}**.",
    )
    # prefer latest dropdown update
    return res_df, log, dd_upd2, content

# Describe Topic

def on_describe_add(topic_name, parts, existing_df, log):
    parts = parts or []
    if not topic_name or not parts:
        return (existing_df if isinstance(existing_df, pd.DataFrame) else pd.DataFrame(columns=["status","topic_name","partition","error_code","leader_id","replicas"]),
                log, gr.Dropdown(), "")
    df_new = api_describe_topic(topic_name, [int(p) for p in parts])
    if isinstance(existing_df, pd.DataFrame) and not existing_df.empty:
        out_df = pd.concat([existing_df, df_new], ignore_index=True)
    else:
        out_df = df_new
    log, dd_upd, content = push_log(
        log,
        "Described topic partitions",
        f"**{topic_name}** → partitions {', '.join(map(str, parts))}",
    )
    return out_df, log, dd_upd, content

# Produce

def on_record_add(k, v, records_df, log):
    records_df = records_df if isinstance(records_df, pd.DataFrame) else pd.DataFrame(columns=["key", "value"])
    new_row = pd.DataFrame([[k or "", v or ""]], columns=["key", "value"])
    out_df = pd.concat([records_df, new_row], ignore_index=True)
    log, dd_upd, content = push_log(log, "Added record", f"Key: `{k or ''}` | Value length: **{len((v or '').strip())}**")
    return out_df, "", "", log, dd_upd, content


def on_produce_bundle(topic_name, partition, staged_df, pending_df, bundles_state, log):
    if not topic_name or staged_df is None or staged_df.empty:
        return pending_df, bundles_state, log, gr.Dropdown(), ""
    num = len(staged_df)
    # store bundle
    bundles = bundles_state or []
    bundles.append({
        "topic": topic_name,
        "partition": int(partition),
        "records": staged_df[["key", "value"]].to_dict(orient="records"),
    })
    # show summary in pending table
    pending_df = pending_df if isinstance(pending_df, pd.DataFrame) else pd.DataFrame(columns=["topic", "partition", "records"])
    pending_df = pd.concat([
        pending_df,
        pd.DataFrame([[topic_name, int(partition), num]], columns=["topic", "partition", "records"]),
    ], ignore_index=True)
    log, dd_upd, content = push_log(log, "Queued produce request", f"**{topic_name}** [p={partition}] — **{num}** record(s)")
    return pending_df, bundles, log, dd_upd, content


def on_produce_send(bundles_state, log):
    bundles = bundles_state or []
    if not bundles:
        return pd.DataFrame(columns=["status", "topic", "partition", "records", "error"]) , log, gr.Dropdown(), ""
    rows = []
    total_ok = 0
    total_err = 0
    log, dd_upd1, _ = push_log(log, "Sending produce requests", f"Bundles: **{len(bundles)}**")
    for b in bundles:
        res = api_produce(b["topic"], b["partition"], [(r["key"], r["value"]) for r in b["records"]])
        ok = res["ok"]
        rows.append({
            "status": "✅" if ok else "❌",
            "topic": res["topic"],
            "partition": res["partition"],
            "records": res["added"],
            "error": res["error"],
        })
        if ok:
            total_ok += 1
        else:
            total_err += 1
    out_df = pd.DataFrame(rows, columns=["status", "topic", "partition", "records", "error"])
    log, dd_upd2, content = push_log(log, "Produce responses", f"Success: **{total_ok}**, Failed: **{total_err}**")
    return out_df, log, dd_upd2, content

# Consume

def on_consume_add(topic_name, partition, start_offset, max_msgs, pending_df, log):
    if not topic_name:
        return pending_df, log, gr.Dropdown(), ""
    pending_df = pending_df if isinstance(pending_df, pd.DataFrame) else pd.DataFrame(columns=["topic", "partition", "start_offset", "max_messages"])
    row = pd.DataFrame([[topic_name, int(partition), int(start_offset), int(max_msgs)]], columns=["topic", "partition", "start_offset", "max_messages"]) 
    pending_df = pd.concat([pending_df, row], ignore_index=True)
    log, dd_upd, content = push_log(log, "Queued consume request", f"**{topic_name}** [p={partition}] offset **{start_offset}** max **{max_msgs}**")
    return pending_df, log, dd_upd, content


def on_consume_send(pending_df, log):
    pending_df = pending_df if isinstance(pending_df, pd.DataFrame) else pd.DataFrame(columns=["topic", "partition", "start_offset", "max_messages"])
    if pending_df.empty:
        return pd.DataFrame(columns=["topic", "partition", "offset", "key", "value"]), log, gr.Dropdown(), ""
    all_rows = []
    log, dd_upd1, _ = push_log(log, "Sending consume requests", f"Batches: **{len(pending_df)}**")
    for _, r in pending_df.iterrows():
        df = api_consume(r["topic"], int(r["partition"]), int(r["start_offset"]), int(r["max_messages"]))
        all_rows.append(df)
    out = pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame(columns=["topic", "partition", "offset", "key", "value"])
    log, dd_upd2, content = push_log(log, "Consume responses", f"Returned **{len(out)}** message(s)")
    return out, log, dd_upd2, content

# Right column: when user changes log selection

def on_log_select(log, selection):
    return log_select_content(log, selection)

# ---------------------------------------------------------------
# Build UI
# ---------------------------------------------------------------
with gr.Blocks(title="Kafka Control Center", fill_width=True, theme=gr.themes.Default()) as demo:
    # Global state
    log_state = gr.State([])
    create_pending_state = gr.State([])  # list of dicts queued for creation
    produce_bundles_state = gr.State([])  # list of bundles to send

    # Header / Connection status
    with gr.Row():
        gr.Markdown("""
        # 🧭 **Kafka Control Center**
        Manage topics, produce/consume messages, and inspect metadata — all from one place.
        """)
        header_status = gr.Markdown("⏳ **Connecting to Kafka Broker...**")

    # Two columns (7 / remainder)
    with gr.Row():
        with gr.Column(scale=7):
            with gr.Tabs():
                # ------------------ Overview ------------------
                with gr.Tab("Overview"):
                    with gr.Row():
                        over_topics = gr.Markdown("**0**\n\n**Total Topics**")
                        over_parts = gr.Markdown("**0**\n\n**Total Partitions**")
                        over_records = gr.Markdown("**0**\n\n**Total Records**")
                    over_df = gr.Dataframe(
                        headers=["topic_name", "topic_id", "partition", "records"],
                        datatype=["str", "str", "number", "number"],
                        row_count=(5, "dynamic"),
                        interactive=False,
                        label="📋 Topic / Partition Stats",
                    )
                    over_btn = gr.Button("🔄 Refresh Overview", variant="primary")

                # ------------------ Create topics ------------------
                with gr.Tab("Create topics"):
                    with gr.Row():
                        c_name = gr.Textbox(label="Topic name", placeholder="e.g. invoices")
                        c_part = gr.Slider(label="Partitions", minimum=1, maximum=20, step=1, value=3)
                        c_rep = gr.Slider(label="Replication", minimum=1, maximum=3, step=1, value=1)
                    c_add = gr.Button("➕ Add topic to list", variant="secondary")
                    c_pending = gr.Dataframe(
                        headers=["Topic Name", "Partitions", "Replication", "🗑️ Cancel"],
                        datatype=["str", "number", "number", "str"],
                        interactive=False,
                        row_count=(4, "dynamic"),
                        label="📝 Topics to be created (click 🗑️ to remove)",
                    )
                    c_submit = gr.Button("🚀 Create topics", variant="primary")
                    c_result = gr.Dataframe(
                        headers=["status", "topic_name", "topic_id", "partitions", "replication", "error"],
                        datatype=["str", "str", "str", "number", "number", "str"],
                        interactive=False,
                        row_count=(4, "dynamic"),
                        label="📦 Create results",
                    )

                # ------------------ Describe topic ------------------
                with gr.Tab("Describe topic"):
                    with gr.Row():
                        d_name = gr.Textbox(label="Topic name", placeholder="e.g. orders")
                        d_parts = gr.Dropdown(
                            label="Partitions",
                            choices=[str(i) for i in range(10)],
                            multiselect=True,
                            value=["0"],
                        )
                        d_add = gr.Button("🔍 Add & Describe")
                    d_df = gr.Dataframe(
                        headers=["status", "topic_name", "partition", "error_code", "leader_id", "replicas"],
                        datatype=["str", "str", "number", "number", "number", "number"],
                        interactive=False,
                        row_count=(5, "dynamic"),
                        label="🧾 Describe results",
                    )

                # ------------------ Produce ------------------
                with gr.Tab("Produce"):
                    with gr.Row():
                        p_name = gr.Textbox(label="Topic name", placeholder="e.g. orders")
                        p_part = gr.Slider(label="Partition", minimum=0, maximum=9, step=1, value=0)
                    with gr.Row():
                        r_key = gr.Textbox(label="Record key", placeholder="optional")
                        r_val = gr.Textbox(label="Record value", placeholder="value text")
                    r_add = gr.Button("🧩 Add record")
                    r_table = gr.Dataframe(
                        headers=["key", "value"],
                        datatype=["str", "str"],
                        interactive=False,
                        row_count=(4, "dynamic"),
                        label="🧺 Staged records",
                    )
                    p_bundle = gr.Button("📦 Add topic (bundle staged records)")
                    p_pending = gr.Dataframe(
                        headers=["topic", "partition", "records"],
                        datatype=["str", "number", "number"],
                        interactive=False,
                        row_count=(4, "dynamic"),
                        label="⏳ Pending produce requests",
                    )
                    p_send = gr.Button("📤 Produce")
                    p_results = gr.Dataframe(
                        headers=["status", "topic", "partition", "records", "error"],
                        datatype=["str", "str", "number", "number", "str"],
                        interactive=False,
                        row_count=(4, "dynamic"),
                        label="📈 Produce results",
                    )

                # ------------------ Consume ------------------
                with gr.Tab("Consume"):
                    with gr.Row():
                        c2_name = gr.Textbox(label="Topic name", placeholder="e.g. orders")
                        c2_part = gr.Slider(label="Partition", minimum=0, maximum=9, step=1, value=0)
                        c2_off = gr.Number(label="Start offset", value=0, precision=0)
                        c2_max = gr.Slider(label="Max messages", minimum=1, maximum=100, step=1, value=10)
                    c2_add = gr.Button("📝 Add request")
                    c2_pending = gr.Dataframe(
                        headers=["topic", "partition", "start_offset", "max_messages"],
                        datatype=["str", "number", "number", "number"],
                        interactive=False,
                        row_count=(4, "dynamic"),
                        label="⏳ Pending consume requests",
                    )
                    c2_send = gr.Button("📥 Consume")
                    c2_results = gr.Dataframe(
                        headers=["topic", "partition", "offset", "key", "value"],
                        datatype=["str", "number", "number", "str", "str"],
                        interactive=False,
                        row_count=(6, "dynamic"),
                        label="🗃️ Consumed messages",
                    )

        # Right column: Kafka activity log
        with gr.Column():
            log_dd = gr.Dropdown(label="📜 Kafka requests & responses (latest first)", choices=[], value=None)
            log_view = gr.Markdown("Select a log entry to view details.")

    # -----------------------------------------------------------
    # Wire up events
    # -----------------------------------------------------------
    demo.load(on_connect, inputs=[log_state], outputs=[header_status, log_state, log_dd, log_view])

    # Overview
    over_btn.click(on_overview_refresh, inputs=[log_state], outputs=[over_df, over_topics, over_parts, over_records, log_state, log_dd, log_view])

    # Create topics
    c_add.click(on_create_add, inputs=[c_name, c_part, c_rep, create_pending_state, log_state], outputs=[c_pending, create_pending_state, log_state, log_dd, log_view])
    c_pending.select(on_create_table_select, inputs=[c_pending, create_pending_state, log_state], outputs=[c_pending, create_pending_state, log_state, log_dd, log_view])
    c_submit.click(on_create_submit, inputs=[create_pending_state, log_state], outputs=[c_result, log_state, log_dd, log_view])

    # Describe topic
    d_add.click(on_describe_add, inputs=[d_name, d_parts, d_df, log_state], outputs=[d_df, log_state, log_dd, log_view])

    # Produce
    r_add.click(on_record_add, inputs=[r_key, r_val, r_table, log_state], outputs=[r_table, r_key, r_val, log_state, log_dd, log_view])
    p_bundle.click(on_produce_bundle, inputs=[p_name, p_part, r_table, p_pending, produce_bundles_state, log_state], outputs=[p_pending, produce_bundles_state, log_state, log_dd, log_view])
    p_send.click(on_produce_send, inputs=[produce_bundles_state, log_state], outputs=[p_results, log_state, log_dd, log_view])

    # Consume
    c2_add.click(on_consume_add, inputs=[c2_name, c2_part, c2_off, c2_max, c2_pending, log_state], outputs=[c2_pending, log_state, log_dd, log_view])
    c2_send.click(on_consume_send, inputs=[c2_pending, log_state], outputs=[c2_results, log_state, log_dd, log_view])

    # Log selection
    log_dd.change(on_log_select, inputs=[log_state, log_dd], outputs=[log_view])

    # Seed some dummy data so layout looks alive at first render
    def _seed_on_start():
        data = api_get_topics_overview()
        return (
            data["df"],
            f"**{data['totals']['total_topics']}**",
            f"**{data['totals']['total_partitions']}**",
            f"**{data['totals']['total_records']}**",
        )

    # Populate Overview immediately (without logging)
    over_df.value, over_topics.value, over_parts.value, over_records.value = _seed_on_start()

if __name__ == "__main__":
    demo.launch()
