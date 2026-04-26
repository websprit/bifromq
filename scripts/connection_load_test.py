#!/usr/bin/env python3
"""
高并发场景压测：20000 个连接，每连接发布 10 topic + 订阅 10 topic，每秒发 10 条消息。
使用 multiprocessing + threading 绕过 Python GIL。
"""

import multiprocessing
import random
import string
import subprocess
import sys
import threading
import time

from paho.mqtt.client import Client

# ===================== 配置 =====================
BROKER_HOST = "127.0.0.1"
BROKER_PORT = 32695          # NodePort for mqtt-tcp
TOTAL_CONN = 20_000          # 总连接数
CONN_PER_PROC = 1_000        # 每个进程管理的连接数
PUB_TOPICS_PER_CONN = 10     # 每个连接发布的 topic 数
SUB_TOPICS_PER_CONN = 10     # 每个连接订阅的 topic 数
MSG_PER_SEC = 10             # 每个连接每秒发送的消息数
PAYLOAD_SIZE = 128           # 消息 payload 字节数
RUN_DURATION_SEC = 60        # 持续运行秒数
TOPIC_PREFIX = "perf/load"


def random_topic():
    suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=12))
    return f"{TOPIC_PREFIX}/{suffix}"


def get_pod_memory():
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
    except Exception:
        return -1


class ConnWorker:
    """单个工作进程内的连接管理器。"""

    def __init__(self, worker_id, num_conn):
        self.worker_id = worker_id
        self.num_conn = num_conn
        self.clients = []
        self.pub_topics = []
        self.sub_topics = []
        self._stop = threading.Event()

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            # 订阅本连接对应的 10 个 topic
            for t in userdata["sub_topics"]:
                client.subscribe(t, qos=0)

    def _on_message(self, client, userdata, msg):
        pass  # 收到消息不做处理

    def start(self):
        print(f"[Worker {self.worker_id}] 建立 {self.num_conn} 个连接...")
        for i in range(self.num_conn):
            cid = f"w{self.worker_id}-c{i}-{int(time.time()*1000)%10000}"
            c = Client(client_id=cid)

            pub_list = [random_topic() for _ in range(PUB_TOPICS_PER_CONN)]
            sub_list = [random_topic() for _ in range(SUB_TOPICS_PER_CONN)]
            self.pub_topics.append(pub_list)
            self.sub_topics.append(sub_list)

            c.user_data_set({"pub_topics": pub_list, "sub_topics": sub_list})
            c.on_connect = self._on_connect
            c.on_message = self._on_message
            c.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
            c.loop_start()
            self.clients.append(c)
        print(f"[Worker {self.worker_id}] 连接建立完成")

    def publish_loop(self):
        """在独立线程中持续发布消息。"""
        interval = 1.0 / MSG_PER_SEC
        payload = b"x" * PAYLOAD_SIZE
        while not self._stop.is_set():
            t0 = time.time()
            for idx, c in enumerate(self.clients):
                if not c.is_connected():
                    continue
                topics = self.pub_topics[idx]
                for t in topics:
                    c.publish(t, payload, qos=0)
            elapsed = time.time() - t0
            sleep_time = max(0, 1.0 - elapsed)
            self._stop.wait(sleep_time)

    def run(self):
        self.start()
        pub_thread = threading.Thread(target=self.publish_loop, daemon=True)
        pub_thread.start()
        # 主线程等待 stop 信号
        while not self._stop.is_set():
            time.sleep(0.5)
        print(f"[Worker {self.worker_id}] 停止中...")
        for c in self.clients:
            c.loop_stop()
            c.disconnect()
        print(f"[Worker {self.worker_id}] 已停止")

    def stop(self):
        self._stop.set()


def worker_process(worker_id, num_conn, ready_queue, stop_event):
    worker = ConnWorker(worker_id, num_conn)
    worker.start()
    ready_queue.put(worker_id)
    # 启动发布循环
    pub_thread = threading.Thread(target=worker.publish_loop, daemon=True)
    pub_thread.start()
    stop_event.wait()
    print(f"[Worker {worker_id}] 收到停止信号")
    worker.stop()


def main():
    num_procs = TOTAL_CONN // CONN_PER_PROC
    print(f"压测配置: {TOTAL_CONN} 连接, {num_procs} 进程, "
          f"每进程 {CONN_PER_PROC} 连接, 运行 {RUN_DURATION_SEC}s")
    print(f"发布: {PUB_TOPICS_PER_CONN} topics/conn, "
          f"订阅: {SUB_TOPICS_PER_CONN} topics/conn, "
          f"发送: {MSG_PER_SEC} msg/s/conn")
    print(f"总消息速率 ≈ {TOTAL_CONN * MSG_PER_SEC}/s")
    print("=" * 60)

    ready_queue = multiprocessing.Queue()
    stop_event = multiprocessing.Event()

    procs = []
    for w in range(num_procs):
        p = multiprocessing.Process(
            target=worker_process,
            args=(w, CONN_PER_PROC, ready_queue, stop_event),
        )
        p.start()
        procs.append(p)

    # 等待所有 worker 就绪
    ready = set()
    print("等待所有 worker 就绪...")
    while len(ready) < num_procs:
        wid = ready_queue.get()
        ready.add(wid)
        print(f"  Worker {wid} 就绪 ({len(ready)}/{num_procs})")

    print("\n所有连接建立完成，开始监控内存...")
    print(f"{'Elapsed(s)':>12} {'Memory(MiB)':>14}")
    print("-" * 30)

    start_time = time.time()
    while time.time() - start_time < RUN_DURATION_SEC:
        elapsed = int(time.time() - start_time)
        mem = get_pod_memory()
        print(f"{elapsed:>12} {mem:>14}")
        sys.stdout.flush()
        time.sleep(5)

    print("\n测试结束，发送停止信号...")
    stop_event.set()
    for p in procs:
        p.join(timeout=30)
        if p.is_alive():
            print(f"Worker {p.pid} 未能在 30s 内退出，强制终止")
            p.terminate()

    final_mem = get_pod_memory()
    print(f"\n最终内存: {final_mem} MiB")
    print("Done.")


if __name__ == "__main__":
    # macOS 默认 fork 方式可能导致 paho-mqtt 问题，强制 spawn
    multiprocessing.set_start_method("spawn", force=True)
    main()
