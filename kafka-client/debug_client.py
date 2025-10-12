#!/usr/bin/env python3
"""
Simple test to debug the API_VERSIONS request/response issue.
"""

import socket
import struct
import sys
import os
from io import BytesIO

# Add the app directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.protocol.protocol import *


def test_simple_connection():
    """Test basic connection and API_VERSIONS request."""
    print("Testing simple connection...")
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect(("localhost", 9092))
        print("✓ Connected to Kafka broker")
        
        # Create a simple API_VERSIONS request (version 4)
        # Header: api_key(2) + api_version(2) + correlation_id(4) + client_id + tagged_fields
        # Body: client_software_name + client_software_version + tagged_fields
        
        request_data = b"".join([
            # Header
            struct.pack(">H", 18),  # api_key = API_VERSIONS (18)
            struct.pack(">H", 4),   # api_version = 4
            struct.pack(">I", 1),   # correlation_id = 1
            encode_compact_nullable_string("test-client"),  # client_id
            encode_tagged_fields(),  # tagged_fields
            
            # Body
            encode_compact_nullable_string("test-client"),  # client_software_name
            encode_compact_nullable_string("1.0.0"),       # client_software_version  
            encode_tagged_fields()  # tagged_fields
        ])
        
        # Send request with length prefix
        message = struct.pack(">I", len(request_data)) + request_data
        print(f"Sending request: {len(message)} bytes total ({len(request_data)} body)")
        print(f"Request hex: {message.hex()}")
        
        sock.send(message)
        print("✓ Request sent")
        
        # Try to read response length
        try:
            length_bytes = sock.recv(4)
            if len(length_bytes) != 4:
                print(f"✗ Expected 4 bytes for length, got {len(length_bytes)}")
                return
            
            response_length = struct.unpack(">I", length_bytes)[0]
            print(f"✓ Response length: {response_length} bytes")
            
            # Read response body
            response_body = b""
            while len(response_body) < response_length:
                chunk = sock.recv(response_length - len(response_body))
                if not chunk:
                    print("✗ Connection closed while reading response")
                    return
                response_body += chunk
            
            print(f"✓ Response received: {len(response_body)} bytes")
            print(f"Response hex: {response_body.hex()}")
            
            # Parse response header
            readable = BytesIO(response_body)
            correlation_id = decode_int32(readable)
            print(f"✓ Correlation ID: {correlation_id}")
            
            if correlation_id == 1:
                print("✓ Correlation ID matches!")
            else:
                print(f"✗ Correlation ID mismatch: expected 1, got {correlation_id}")
                
        except Exception as e:
            print(f"✗ Error reading response: {e}")
            
    except Exception as e:
        print(f"✗ Connection error: {e}")
    finally:
        sock.close()


if __name__ == "__main__":
    test_simple_connection()
