#!/usr/bin/env python3
"""
BifroMQ Regression Test: Persistent Session & Inbox Offline Messages
Tests QoS1 offline message delivery accuracy with clean_session=False.
"""

import paho.mqtt.client as mqtt
import time
import threading
import sys

BROKER_HOST = "192.168.139.2"
BROKER_PORT = 30784
TOPIC = "test/persistent/qos1"
NUM_MESSAGES = 10

received_offline_msgs = []
lock = threading.Lock()
connected_event = threading.Event()
msg_received_event = threading.Event()


def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        connected_event.set()
        print(f"[{client._client_id.decode()}] Connected")
    else:
        print(f"[{client._client_id.decode()}] Connection failed: {rc}")


def on_disconnect(client, userdata, disconnect_flags, rc, properties=None):
    print(f"[{client._client_id.decode()}] Disconnected: {rc}")
    connected_event.clear()


def on_message(client, userdata, msg):
    with lock:
        received_offline_msgs.append({
            "topic": msg.topic,
            "payload": msg.payload.decode(),
            "qos": msg.qos,
            "mid": msg.mid
        })
    print(f"[{client._client_id.decode()}] Received: {msg.payload.decode()} (qos={msg.qos})")
    if len(received_offline_msgs) >= NUM_MESSAGES:
        msg_received_event.set()


def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    print(f"[{client._client_id.decode()}] Subscribed: {granted_qos}")


def create_client(client_id, clean_session=True):
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
        clean_session=clean_session
    )
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.on_subscribe = on_subscribe
    return client


def test_persistent_session_offline_messages():
    print("=" * 60)
    print("TEST: Persistent Session + QoS1 Offline Message Accuracy")
    print("=" * 60)

    # Step 1: Client A connects with clean_session=False and subscribes
    print("\n[Step 1] Client A connects (clean_session=False) and subscribes")
    client_a = create_client("client-a-persistent", clean_session=False)
    connected_event.clear()
    client_a.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
    client_a.loop_start()
    if not connected_event.wait(timeout=10):
        print("ERROR: Client A failed to connect")
        sys.exit(1)

    client_a.subscribe(TOPIC, qos=1)
    time.sleep(1)

    # Step 2: Client A disconnects
    print("\n[Step 2] Client A disconnects")
    connected_event.clear()
    client_a.disconnect()
    client_a.loop_stop()
    time.sleep(2)

    # Step 3: Client B connects and publishes QoS1 messages
    print(f"\n[Step 3] Client B publishes {NUM_MESSAGES} QoS1 messages")
    client_b = create_client("client-b-publisher", clean_session=True)
    connected_event.clear()
    client_b.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
    client_b.loop_start()
    if not connected_event.wait(timeout=10):
        print("ERROR: Client B failed to connect")
        sys.exit(1)

    expected_payloads = []
    for i in range(NUM_MESSAGES):
        payload = f"offline-msg-{i:03d}"
        expected_payloads.append(payload)
        result = client_b.publish(TOPIC, payload, qos=1)
        print(f"  Published: {payload} (mid={result.mid})")
        time.sleep(0.2)

    time.sleep(3)  # Wait for messages to be stored in inbox
    client_b.disconnect()
    client_b.loop_stop()

    # Step 4: Client A reconnects with clean_session=False
    print("\n[Step 4] Client A reconnects (clean_session=False)")
    received_offline_msgs.clear()
    msg_received_event.clear()

    client_a = create_client("client-a-persistent", clean_session=False)
    connected_event.clear()
    client_a.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
    client_a.loop_start()
    if not connected_event.wait(timeout=10):
        print("ERROR: Client A failed to reconnect")
        sys.exit(1)

    # Wait for offline messages
    print("  Waiting for offline messages...")
    msg_received_event.wait(timeout=30)
    time.sleep(2)

    # Step 5: Validate results
    print("\n[Step 5] Validation")
    with lock:
        actual_payloads = [m["payload"] for m in received_offline_msgs]

    print(f"  Expected messages: {NUM_MESSAGES}")
    print(f"  Received messages: {len(received_offline_msgs)}")

    success = True
    if len(received_offline_msgs) != NUM_MESSAGES:
        print(f"  FAIL: Message count mismatch!")
        success = False
    else:
        print(f"  PASS: Message count correct")

    # Check order
    if actual_payloads == expected_payloads:
        print(f"  PASS: Message order preserved")
    else:
        print(f"  FAIL: Message order mismatch!")
        print(f"    Expected: {expected_payloads}")
        print(f"    Actual:   {actual_payloads}")
        success = False

    # Check QoS
    qos_mismatch = [m for m in received_offline_msgs if m["qos"] != 1]
    if not qos_mismatch:
        print(f"  PASS: All messages delivered with QoS1")
    else:
        print(f"  FAIL: Some messages have wrong QoS: {qos_mismatch}")
        success = False

    client_a.disconnect()
    client_a.loop_stop()

    # Step 6: Test session expiry (MQTT 5.0 style)
    print("\n[Step 6] Session Expiry Test (MQTT 5.0)")
    props = mqtt.Properties(mqtt.PacketTypes.CONNECT)
    props.SessionExpiryInterval = 3600  # 1 hour session expiry

    client_c = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="client-c-expiry",
        protocol=mqtt.MQTTv5
    )
    client_c.on_connect = on_connect
    client_c.on_message = on_message
    connected_event.clear()
    client_c.connect(BROKER_HOST, BROKER_PORT, keepalive=60, properties=props, clean_start=False)
    client_c.loop_start()
    if not connected_event.wait(timeout=10):
        print("ERROR: Client C failed to connect")
        sys.exit(1)
    client_c.subscribe(TOPIC, qos=1)
    time.sleep(1)
    connected_event.clear()
    client_c.disconnect()
    client_c.loop_stop()
    print("  Client C disconnected with persistent session (expiry=3600s)")

    # Publish message while client C is offline
    client_d = create_client("client-d-pub", clean_session=True)
    connected_event.clear()
    client_d.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
    client_d.loop_start()
    connected_event.wait(timeout=10)
    client_d.publish(TOPIC, "expiry-test-msg", qos=1)
    time.sleep(2)
    client_d.disconnect()
    client_d.loop_stop()

    # Reconnect client C with same session expiry (reuse client object)
    received_offline_msgs.clear()
    msg_received_event.clear()
    connected_event.clear()
    client_c.connect(BROKER_HOST, BROKER_PORT, keepalive=60, properties=props, clean_start=False)
    client_c.loop_start()
    connected_event.wait(timeout=10)
    msg_received_event.wait(timeout=15)
    time.sleep(2)

    expiry_found = any(m["payload"] == "expiry-test-msg" for m in received_offline_msgs)
    if expiry_found:
        print("  PASS: Session preserved offline message after reconnect")
    else:
        print("  FAIL: Session did not preserve offline message!")
        success = False

    client_c.disconnect()
    client_c.loop_stop()

    print("\n" + "=" * 60)
    if success:
        print("ALL TESTS PASSED")
    else:
        print("SOME TESTS FAILED")
    print("=" * 60)
    return success


if __name__ == "__main__":
    try:
        result = test_persistent_session_offline_messages()
        sys.exit(0 if result else 1)
    except Exception as e:
        print(f"\nEXCEPTION: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
