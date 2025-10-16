#!/usr/bin/env python3
"""
Test script to launch the Kafka UI
"""

if __name__ == "__main__":
    from ui.main import demo
    demo.launch(
        server_name="0.0.0.0",
        server_port=7860,
        share=False,
        debug=True
    )
