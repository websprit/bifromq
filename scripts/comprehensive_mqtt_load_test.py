#!/usr/bin/env python3
"""
Comprehensive MQTT Load Test for BifroMQ
Simultaneously tests: connections, subscriptions, publishing, and consuming
"""
import multiprocessing
import random
import string
import time
import paho.mqtt.client as mqtt
import sys

BROKER_HOST = "192.168.139.2"
BROKER_PORT = 30715
NUM_CONNECTIONS = 500
NUM_TOPICS_PER_CLIENT = 5
PUBLISH_RATE_PER_CLIENT = 2  # msgs/sec
RUN_DURATION_SECONDS = 300
CLIENT_PREFIX = "load_test_"


def random_topic(prefix="load/topic"):
    return f"{prefix}/{''.join(random.choices(string.ascii_lowercase + string.digits, k=6))}"


def client_worker(client_id, topics_to_subscribe, topics_to_publish, stop_event):
    client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv5)
    received_count = [0]
    connected = [False]

    def on_connect(client, userdata, flags, rc, properties=None):
        connected[0] = True
        for t in topics_to_subscribe:
            client.subscribe(t, qos=1)

    def on_message(client, userdata, msg):
        received_count[0] += 1

    def on_disconnect(client, userdata, rc, properties=None):
        connected[0] = False

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    try:
        client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
        client.loop_start()

        # Wait for connection
        wait_start = time.time()
        while not connected[0] and time.time() - wait_start < 10:
            time.sleep(0.1)

        if not connected[0]:
            print(f"[{client_id}] Failed to connect")
            client.loop_stop()
            return

        print(f"[{client_id}] Connected, subscribed to {len(topics_to_subscribe)} topics")

        publish_interval = 1.0 / PUBLISH_RATE_PER_CLIENT
        last_publish = time.time()
        start_time = time.time()

        while not stop_event.is_set() and time.time() - start_time < RUN_DURATION_SECONDS:
            now = time.time()
            if now - last_publish >= publish_interval:
                topic = random.choice(topics_to_publish)
                payload = f"msg_{client_id}_{int(now * 1000)}"
                client.publish(topic, payload, qos=1)
                last_publish = now
            time.sleep(0.01)

        elapsed = time.time() - start_time
        print(f"[{client_id}] Done. Published ~{int(elapsed * PUBLISH_RATE_PER_CLIENT)} msgs, Received {received_count[0]} msgs")

    except Exception as e:
        print(f"[{client_id}] Error: {e}")
    finally:
        client.loop_stop()
        client.disconnect()


def main():
    print(f"Starting comprehensive MQTT load test...")
    print(f"Broker: {BROKER_HOST}:{BROKER_PORT}")
    print(f"Connections: {NUM_CONNECTIONS}")
    print(f"Topics per client: {NUM_TOPICS_PER_CLIENT}")
    print(f"Publish rate per client: {PUBLISH_RATE_PER_CLIENT} msg/s")
    print(f"Duration: {RUN_DURATION_SECONDS}s")
    print(f"Total expected publish rate: {NUM_CONNECTIONS * PUBLISH_RATE_PER_CLIENT} msg/s")
    print("=" * 60)

    # Pre-generate topic pools
    all_topics = [random_topic() for _ in range(NUM_CONNECTIONS * NUM_TOPICS_PER_CLIENT * 2)]

    stop_event = multiprocessing.Event()
    processes = []

    for i in range(NUM_CONNECTIONS):
        client_id = f"{CLIENT_PREFIX}{i}"
        topic_offset = i * NUM_TOPICS_PER_CLIENT * 2
        topics_to_sub = all_topics[topic_offset:topic_offset + NUM_TOPICS_PER_CLIENT]
        topics_to_pub = all_topics[topic_offset + NUM_TOPICS_PER_CLIENT:topic_offset + NUM_TOPICS_PER_CLIENT * 2]
        p = multiprocessing.Process(target=client_worker, args=(client_id, topics_to_sub, topics_to_pub, stop_event))
        p.start()
        processes.append(p)
        time.sleep(0.02)  # Stagger connections

    print(f"All {NUM_CONNECTIONS} client processes started")
    print(f"Test running for {RUN_DURATION_SECONDS} seconds...")
    print("Open Grafana dashboard to observe metrics: http://192.168.139.2:30353/d/bifromq-metrics/bifromq-metrics-dashboard")

    try:
        time.sleep(RUN_DURATION_SECONDS)
    except KeyboardInterrupt:
        print("\nInterrupted by user")

    print("\nStopping test...")
    stop_event.set()

    for p in processes:
        p.join(timeout=5)
        if p.is_alive():
            p.terminate()

    print("Load test completed!")


if __name__ == "__main__":
    main()
