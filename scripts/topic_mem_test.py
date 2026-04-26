#!/usr/bin/env python3
"""
大量 topic 内存压测脚本
逐步增加 topic 数量，每批次结束后记录 pod 内存占用。
"""

import subprocess
import time
import random
import string
import threading
import sys

from paho.mqtt.client import Client

# 连接配置
BROKER_HOST = "127.0.0.1"
BROKER_PORT = 32695  # NodePort for mqtt-tcp
BATCH_SIZES = [100, 1000, 5000, 10000, 20000, 50000, 100000]
TOPIC_PREFIX = "perf/topic"
QOS = 0


def random_topic():
    """生成随机 topic 名称避免碰撞。"""
    suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=16))
    return f"{TOPIC_PREFIX}/{suffix}"


def get_pod_memory():
    """获取 bifromq pod 内存使用量 (MiB)。"""
    try:
        out = subprocess.check_output(
            [
                "kubectl", "exec", "bifromq-0", "-n", "default", "--",
                "bash", "-c",
                "cat /sys/fs/cgroup/memory.current 2>/dev/null || cat /sys/fs/cgroup/memory/memory.usage_in_bytes 2>/dev/null",
            ],
            stderr=subprocess.DEVNULL,
            text=True,
        )
        return int(int(out.strip()) / 1024 / 1024)
    except Exception as e:
        return f"error({e})"


def publish_batch(num_topics, client_id_prefix="pub"):
    """发布一批 retained 消息到不同 topic。"""
    client = Client(client_id=f"{client_id_prefix}-{threading.current_thread().ident}")
    client.connect(BROKER_HOST, BROKER_PORT, 60)
    for i in range(num_topics):
        topic = random_topic()
        payload = b"x" * 128  # 128 bytes payload
        client.publish(topic, payload, qos=QOS, retain=True)
    client.disconnect()


def run_test():
    print(f"{'Batch':>10} {'Topics':>10} {'Memory(MiB)':>14} {'Time(s)':>10}")
    print("-" * 50)

    total_topics = 0
    for batch in BATCH_SIZES:
        start = time.time()
        publish_batch(batch)
        elapsed = time.time() - start
        total_topics += batch
        # 等待内存稳定
        time.sleep(3)
        mem = get_pod_memory()
        print(f"{batch:>10} {total_topics:>10} {mem:>14} {elapsed:>10.2f}")
        sys.stdout.flush()

    print("\nDone. Retained messages may keep memory elevated.")


if __name__ == "__main__":
    print("Waiting for BifroMQ to be fully ready...")
    time.sleep(2)
    print(f"Target broker: {BROKER_HOST}:{BROKER_PORT}")
    run_test()
