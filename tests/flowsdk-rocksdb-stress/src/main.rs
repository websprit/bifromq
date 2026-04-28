// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use async_trait::async_trait;
use flowsdk::mqtt_client::client::ConnectionResult;
use flowsdk::mqtt_client::{
    MqttClientError, MqttClientOptions, MqttMessage, TokioAsyncClientConfig, TokioAsyncMqttClient,
    TokioMqttEventHandler,
};
use std::env;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::process::Command;
use tokio::time::{sleep, Duration, Instant};

type DynError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone)]
struct Config {
    image: String,
    container: String,
    docker_cmd: Vec<String>,
    peer: String,
    username: String,
    start_container: bool,
    clients: usize,
    publishers: usize,
    messages_per_client: usize,
    payload_bytes: usize,
    mqtt_version: u8,
    connect_timeout_ms: u64,
    op_timeout_ms: u64,
    drain_timeout_secs: u64,
    ready_grace_secs: u64,
    run_id: String,
}

impl Config {
    fn from_env() -> Self {
        Self {
            image: env_or(
                "BIFROMQ_IMAGE",
                "ghcr.io/websprit/bifromq:feature-perf-optimization",
            ),
            container: env_or("BIFROMQ_CONTAINER", "bifromq-flowsdk-rocksdb-stress"),
            docker_cmd: env_or("BIFROMQ_DOCKER_CMD", "docker")
                .split_whitespace()
                .map(ToString::to_string)
                .collect(),
            peer: env_or("BIFROMQ_PEER", "localhost:11883"),
            username: env_or("BIFROMQ_USERNAME", ""),
            start_container: env_bool("BIFROMQ_START_CONTAINER", true),
            clients: env_usize("BIFROMQ_STRESS_CLIENTS", 80),
            publishers: env_usize("BIFROMQ_STRESS_PUBLISHERS", 8),
            messages_per_client: env_usize("BIFROMQ_STRESS_MESSAGES_PER_CLIENT", 60),
            payload_bytes: env_usize("BIFROMQ_STRESS_PAYLOAD_BYTES", 512),
            mqtt_version: env_u8("BIFROMQ_STRESS_MQTT_VERSION", 5),
            connect_timeout_ms: env_u64("BIFROMQ_STRESS_CONNECT_TIMEOUT_MS", 30_000),
            op_timeout_ms: env_u64("BIFROMQ_STRESS_OP_TIMEOUT_MS", 15_000),
            drain_timeout_secs: env_u64("BIFROMQ_STRESS_DRAIN_TIMEOUT_SECS", 90),
            ready_grace_secs: env_u64("BIFROMQ_STRESS_READY_GRACE_SECS", 15),
            run_id: env::var("BIFROMQ_STRESS_RUN_ID").unwrap_or_else(|_| {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("system clock before unix epoch")
                    .as_secs()
                    .to_string()
            }),
        }
    }
}

#[derive(Clone)]
struct Counters {
    connected: Arc<AtomicU64>,
    messages: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
}

impl Counters {
    fn new() -> Self {
        Self {
            connected: Arc::new(AtomicU64::new(0)),
            messages: Arc::new(AtomicU64::new(0)),
            errors: Arc::new(AtomicU64::new(0)),
        }
    }
}

struct Handler {
    counters: Counters,
}

#[async_trait]
impl TokioMqttEventHandler for Handler {
    async fn on_connected(&mut self, result: &ConnectionResult) {
        if result.is_success() {
            self.counters.connected.fetch_add(1, Ordering::Relaxed);
        } else {
            self.counters.errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    async fn on_message_received(&mut self, _publish: &MqttMessage) {
        self.counters.messages.fetch_add(1, Ordering::Relaxed);
    }

    async fn on_error(&mut self, _error: &MqttClientError) {
        self.counters.errors.fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), DynError> {
    let cfg = Config::from_env();
    let counters = Counters::new();
    let expected_messages = cfg.clients * cfg.messages_per_client;
    println!(
        "FlowSDK RocksDB stress: peer={}, mqtt=v{}, clients={}, publishers={}, messages={}, payload={}B, run_id={}",
        cfg.peer, cfg.mqtt_version, cfg.clients, cfg.publishers, expected_messages, cfg.payload_bytes, cfg.run_id
    );

    if cfg.start_container {
        start_container(&cfg).await?;
    }
    wait_for_broker(&cfg).await?;

    println!("Phase 1: create online subscribers and subscriptions");
    let subscribers = create_subscribers(&cfg, counters.clone()).await?;

    println!("Phase 2: publish QoS1 retained messages to exercise retain RocksDB writes");
    let publish_start = Instant::now();
    let publish_ok = publish_retained_load(&cfg, counters.clone()).await?;
    let publish_elapsed = publish_start.elapsed();
    println!(
        "Published {} QoS1 retained messages in {:.2}s ({:.1} msg/s)",
        publish_ok,
        publish_elapsed.as_secs_f64(),
        publish_ok as f64 / publish_elapsed.as_secs_f64().max(0.001)
    );

    sleep(Duration::from_secs(2)).await;
    let online_received = counters.messages.load(Ordering::Relaxed);
    println!(
        "Online subscribers observed {} deliveries before retained read-back",
        online_received
    );
    for client in &subscribers {
        client.disconnect().await?;
    }
    drop(subscribers);

    println!("Phase 3: resubscribe and verify retained messages can be read back");
    let retained_baseline = counters.messages.load(Ordering::Relaxed);
    let _retained_subscribers = create_subscribers(&cfg, counters.clone()).await?;
    wait_for_messages(
        &cfg,
        retained_baseline + cfg.clients as u64,
        counters.clone(),
    )
    .await?;

    let received = counters.messages.load(Ordering::Relaxed);
    let errors = counters.errors.load(Ordering::Relaxed);
    println!(
        "RESULT ok: published={}, received={}, retained_verified={}, errors={}, connected_events={}",
        publish_ok,
        received,
        cfg.clients,
        errors,
        counters.connected.load(Ordering::Relaxed)
    );
    let expected_total = retained_baseline + cfg.clients as u64;
    if received < expected_total {
        return Err(format!(
            "retained read-back incomplete: expected at least {expected_total}, got {received}"
        )
        .into());
    }
    if errors > 0 {
        return Err(format!("FlowSDK observed {} async client errors", errors).into());
    }
    Ok(())
}

async fn start_container(cfg: &Config) -> io::Result<()> {
    let _ = docker_command(cfg)
        .args(["rm", "-f", &cfg.container])
        .status()
        .await?;
    let status = docker_command(cfg)
        .args([
            "run",
            "-d",
            "--name",
            &cfg.container,
            "-e",
            "JVM_HEAP_OPTS=-Xms1024m -Xmx1024m -XX:MaxDirectMemorySize=512m",
            "-e",
            "JVM_GC_OPTS=-Xlog:gc*:file=/bifromq/logs/gc-%t.log:time,tid,tags:filecount=5,filesize=50m",
            "-p",
            "11883:1883",
            "-p",
            "11884:1884",
            "-p",
            "18080:8080",
            "-p",
            "21456:14567/udp",
            &cfg.image,
        ])
        .status()
        .await?;
    if !status.success() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("docker run failed with status {status}"),
        ));
    }
    Ok(())
}

fn docker_command(cfg: &Config) -> Command {
    let mut command = Command::new(&cfg.docker_cmd[0]);
    if cfg.docker_cmd.len() > 1 {
        command.args(&cfg.docker_cmd[1..]);
    }
    command
}

async fn wait_for_broker(cfg: &Config) -> Result<(), DynError> {
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut attempt = 0;
    loop {
        attempt += 1;
        match connect_client(cfg, "probe", Counters::new(), true).await {
            Ok(client) => {
                let _ = client.disconnect().await;
                let _ = client.shutdown().await;
                println!(
                    "Broker accepted MQTT connection after {} attempt(s)",
                    attempt
                );
                if cfg.ready_grace_secs > 0 {
                    println!(
                        "Waiting {}s for KV ranges to finish bootstrap",
                        cfg.ready_grace_secs
                    );
                    sleep(Duration::from_secs(cfg.ready_grace_secs)).await;
                }
                return Ok(());
            }
            Err(e) if Instant::now() < deadline => {
                println!("Waiting for broker: {e}");
                sleep(Duration::from_secs(2)).await;
            }
            Err(e) => return Err(format!("broker did not become ready: {e}").into()),
        }
    }
}

async fn create_subscribers(
    cfg: &Config,
    counters: Counters,
) -> Result<Vec<TokioAsyncMqttClient>, DynError> {
    let clients = create_connected_clients(cfg, counters, true).await?;
    for (idx, client) in clients.iter().enumerate() {
        let topic = topic_for(cfg, idx);
        client
            .subscribe_sync_with_timeout(&topic, 1, cfg.op_timeout_ms)
            .await?;
    }
    Ok(clients)
}

async fn create_connected_clients(
    cfg: &Config,
    counters: Counters,
    clean_start: bool,
) -> Result<Vec<TokioAsyncMqttClient>, DynError> {
    let mut clients = Vec::with_capacity(cfg.clients);
    for idx in 0..cfg.clients {
        let client_id = format!("flowsdk-rocksdb-{}-{idx}", cfg.run_id);
        let client = connect_client(cfg, &client_id, counters.clone(), clean_start).await?;
        clients.push(client);
    }
    Ok(clients)
}

async fn connect_client(
    cfg: &Config,
    client_id: &str,
    counters: Counters,
    clean_start: bool,
) -> Result<TokioAsyncMqttClient, DynError> {
    let mut options = MqttClientOptions::builder()
        .peer(&cfg.peer)
        .client_id(client_id)
        .mqtt_version(cfg.mqtt_version)
        .clean_start(clean_start)
        .session_expiry_interval(3600)
        .keep_alive(30)
        .reconnect(false)
        .auto_ack(true);
    if !cfg.username.is_empty() {
        options = options.username(cfg.username.as_str());
    }
    let options = options.build();
    let client = TokioAsyncMqttClient::new(
        options,
        Box::new(Handler { counters }),
        TokioAsyncClientConfig {
            auto_reconnect: false,
            command_queue_size: 4096,
            max_buffer_size: 4096,
            connect_timeout_ms: Some(cfg.connect_timeout_ms),
            subscribe_timeout_ms: Some(cfg.op_timeout_ms),
            publish_ack_timeout_ms: Some(cfg.op_timeout_ms),
            ..TokioAsyncClientConfig::default()
        },
    )
    .await?;
    let result = client
        .connect_sync_with_timeout(cfg.connect_timeout_ms)
        .await?;
    if !result.is_success() {
        return Err(format!(
            "connect failed for {client_id}: code=0x{:02x} {}",
            result.reason_code,
            result.reason_description()
        )
        .into());
    }
    Ok(client)
}

async fn publish_retained_load(cfg: &Config, counters: Counters) -> Result<usize, DynError> {
    let mut handles = Vec::with_capacity(cfg.publishers);
    for publisher_idx in 0..cfg.publishers {
        let cfg = cfg.clone();
        let counters = counters.clone();
        handles.push(tokio::spawn(async move {
            let client_id = format!("flowsdk-rocksdb-pub-{}-{publisher_idx}", cfg.run_id);
            let client = connect_client(&cfg, &client_id, counters, true).await?;
            let payload = vec![b'x'; cfg.payload_bytes];
            let mut ok = 0usize;
            for seq in
                (publisher_idx..cfg.clients * cfg.messages_per_client).step_by(cfg.publishers)
            {
                let topic = topic_for(&cfg, seq % cfg.clients);
                let mut message = payload.clone();
                message.extend_from_slice(format!(":{publisher_idx}:{seq}").as_bytes());
                let result = client
                    .publish_sync_with_timeout(&topic, &message, 1, true, cfg.op_timeout_ms)
                    .await?;
                if !is_publish_success(&result) {
                    return Err(format!("publish failed: {:?}", result.reason_code).into());
                }
                ok += 1;
            }
            let _ = client.disconnect().await;
            let _ = client.shutdown().await;
            Ok::<usize, DynError>(ok)
        }));
    }

    let mut total = 0usize;
    for handle in handles {
        total += handle.await??;
    }
    Ok(total)
}

async fn wait_for_messages(
    cfg: &Config,
    expected: u64,
    counters: Counters,
) -> Result<(), DynError> {
    let deadline = Instant::now() + Duration::from_secs(cfg.drain_timeout_secs);
    loop {
        let got = counters.messages.load(Ordering::Relaxed);
        if got >= expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(
                format!("timeout waiting for offline messages: got {got}/{expected}").into(),
            );
        }
        println!("Waiting for retained read-back: {got}/{expected}");
        sleep(Duration::from_secs(2)).await;
    }
}

fn topic_for(cfg: &Config, idx: usize) -> String {
    format!("bifromq/flowsdk/rocksdb/{}/client/{idx}", cfg.run_id)
}

fn is_publish_success(result: &flowsdk::mqtt_client::PublishResult) -> bool {
    result
        .reason_code
        .is_none_or(|code| code == 0 || code == 0x10)
}

fn env_or(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

fn env_bool(name: &str, default: bool) -> bool {
    env::var(name)
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u8(name: &str, default: u8) -> u8 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
