#!/usr/bin/env python3
import random
import string
import time
import threading
import os
import sys
from paho.mqtt.client import Client

BROKER = "bifromq-headless.default.svc.cluster.local"
PORT = 1883
CONN_PER_POD = 2000
PUB_TOPICS = 20
SUB_TOPICS = 20
MSG_PER_SEC = 20
PAYLOAD = b"x" * 128
DURATION = 60
PREFIX = "perf/t2"

def rand_t():
    return f"{PREFIX}/{''.join(random.choices(string.ascii_lowercase + string.digits, k=12))}"

clients = []
pub_lists = []

pod_idx = os.environ.get("JOB_COMPLETION_INDEX", "?")
print(f"[Pod {pod_idx}] Starting {CONN_PER_POD} connections...")

for i in range(CONN_PER_POD):
    cid = f"p{pod_idx}-c{i}"
    c = Client(client_id=cid, reconnect_on_failure=False)
    c.connect(BROKER, PORT, keepalive=60)
    c.loop_start()
    clients.append(c)
    pubs = [rand_t() for _ in range(PUB_TOPICS)]
    subs = [rand_t() for _ in range(SUB_TOPICS)]
    pub_lists.append(pubs)
    for t in subs:
        c.subscribe(t, qos=0)

print(f"[Pod {pod_idx}] All {len(clients)} connections ready")

stop = threading.Event()
def publish_loop():
    while not stop.is_set():
        for idx, c in enumerate(clients):
            if not c.is_connected():
                continue
            for t in pub_lists[idx]:
                c.publish(t, PAYLOAD, qos=0)
        stop.wait(1.0)

t = threading.Thread(target=publish_loop, daemon=True)
t.start()

for i in range(DURATION):
    time.sleep(1)
    if i % 10 == 0:
        print(f"[Pod {pod_idx}] running {i}s/{DURATION}s")

stop.set()
t.join()
for c in clients:
    c.loop_stop()
    c.disconnect()
print(f"[Pod {pod_idx}] Done")
