/*
 * MQTT over QUIC Consumer Client using NanoSDK
 *
 * Connects to BifroMQ MQTT over QUIC broker, subscribes to
 * topic filters, and prints received messages.
 *
 * Usage: ./quic_consumer <broker_url> [multi_stream]
 *   broker_url:    mqtt-quic://host:port
 *   multi_stream:  1 to enable multi-stream, 0 (default) for single-stream
 *
 * Example:
 *   ./quic_consumer mqtt-quic://host.docker.internal:14567 0
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <nng/nng.h>
#include <nng/mqtt/mqtt_quic.h>
#include <nng/mqtt/mqtt_client.h>

static volatile int running = 1;
static volatile int msg_count = 0;

static void signal_handler(int sig) {
    (void)sig;
    running = 0;
}

/* ----- Callback functions ----- */

static int connect_cb(void *rmsg, void *arg) {
    (void)rmsg;
    printf("[Consumer] ✅ Connected to broker (%s)\n", (char *)arg);
    return 0;
}

static int disconnect_cb(void *rmsg, void *arg) {
    (void)rmsg;
    printf("[Consumer] ❌ Disconnected from broker (%s)\n", (char *)arg);
    running = 0;
    return 0;
}

static int msg_send_cb(void *rmsg, void *arg) {
    (void)rmsg;
    (void)arg;
    return 0;
}

static int msg_recv_cb(void *rmsg, void *arg) {
    (void)arg;
    nng_msg *msg = rmsg;
    uint32_t topicsz, payloadsz;

    char *topic   = (char *)nng_mqtt_msg_get_publish_topic(msg, &topicsz);
    char *payload = (char *)nng_mqtt_msg_get_publish_payload(msg, &payloadsz);

    msg_count++;
    printf("[Consumer] 📥 #%d Received: topic=%.*s payload=%.*s\n",
           msg_count, topicsz, topic, payloadsz, payload);

    return 0;
}

/* ----- Message composing ----- */

static nng_msg *compose_connect(void) {
    nng_msg *msg;
    nng_mqtt_msg_alloc(&msg, 0);
    nng_mqtt_msg_set_packet_type(msg, NNG_MQTT_CONNECT);
    nng_mqtt_msg_set_connect_proto_version(msg, 4);
    nng_mqtt_msg_set_connect_keep_alive(msg, 60);
    nng_mqtt_msg_set_connect_clean_session(msg, true);
    nng_mqtt_msg_set_connect_client_id(msg, "nanosdk-consumer-1");
    return msg;
}

static nng_msg *compose_subscribe(const char **topics, int *qos_list, int count) {
    nng_msg *msg;
    nng_mqtt_msg_alloc(&msg, 0);
    nng_mqtt_msg_set_packet_type(msg, NNG_MQTT_SUBSCRIBE);

    nng_mqtt_topic_qos *subscriptions = calloc(count, sizeof(nng_mqtt_topic_qos));
    for (int i = 0; i < count; i++) {
        subscriptions[i].qos          = qos_list[i];
        subscriptions[i].topic.buf    = (uint8_t *)topics[i];
        subscriptions[i].topic.length = strlen(topics[i]);
    }
    nng_mqtt_msg_set_subscribe_topics(msg, subscriptions, count);
    free(subscriptions);
    return msg;
}

int main(int argc, char **argv) {
    setvbuf(stdout, NULL, _IONBF, 0);
    setvbuf(stderr, NULL, _IONBF, 0);

    if (argc < 2) {
        fprintf(stderr, "Usage: %s <broker_url> [multi_stream]\n", argv[0]);
        fprintf(stderr, "  e.g. %s mqtt-quic://host.docker.internal:14567 0\n", argv[0]);
        return 1;
    }

    const char *url = argv[1];
    int multi_stream = (argc > 2) ? atoi(argv[2]) : 0;

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    printf("╔══════════════════════════════════════════════════╗\n");
    printf("║  NanoSDK MQTT over QUIC Consumer                ║\n");
    printf("║  Broker: %-40s║\n", url);
    printf("║  Multi-stream: %-34s║\n", multi_stream ? "ON" : "OFF");
    printf("╚══════════════════════════════════════════════════╝\n");

    /* QUIC configuration */
    conf_quic config = {
        .tls = {
            .enable      = false,
            .cafile      = "",
            .certfile    = "",
            .keyfile     = "",
            .key_password = "",
            .verify_peer = false,
            .set_fail    = false,
        },
        .multi_stream    = multi_stream ? true : false,
        .qos_first       = false,
        .qkeepalive      = 30,
        .qconnect_timeout = 60,
        .qdiscon_timeout  = 30,
        .qidle_timeout    = 60,
    };

    nng_socket sock;
    int rv;
    const char *cb_arg = "consumer";

    printf("[Consumer] Opening QUIC socket to %s...\n", url);
    /* Open QUIC MQTT connection */
    if ((rv = nng_mqtt_quic_client_open_conf(&sock, url, &config)) != 0) {
        fprintf(stderr, "[Consumer] Failed to open QUIC client: %s (rv=%d)\n", nng_strerror(rv), rv);
        return 1;
    }
    printf("[Consumer] QUIC socket opened OK\n");

    /* Register callbacks */
    nng_mqtt_quic_set_connect_cb(&sock, connect_cb, (void *)cb_arg);
    nng_mqtt_quic_set_disconnect_cb(&sock, disconnect_cb, (void *)cb_arg);
    nng_mqtt_quic_set_msg_send_cb(&sock, msg_send_cb, (void *)cb_arg);
    nng_mqtt_quic_set_msg_recv_cb(&sock, msg_recv_cb, (void *)cb_arg);

    /* Send CONNECT */
    nng_msg *conn_msg = compose_connect();
    nng_sendmsg(sock, conn_msg, NNG_FLAG_ALLOC);
    printf("[Consumer] CONNECT sent, waiting for CONNACK...\n");

    /* Wait for connection to establish */
    nng_msleep(2000);

    /* Subscribe to topic filters */
    const char *topics[] = {
        "sensor/#",
        "device/#",
    };
    int qos_list[] = {0, 0};
    int topic_count = sizeof(topics) / sizeof(topics[0]);

    nng_msg *sub_msg = compose_subscribe(topics, qos_list, topic_count);
    rv = nng_sendmsg(sock, sub_msg, NNG_FLAG_ALLOC);
    if (rv == 0) {
        printf("[Consumer] SUBSCRIBE sent for %d topic filters:\n", topic_count);
        for (int i = 0; i < topic_count; i++) {
            printf("  - %s (QoS %d)\n", topics[i], qos_list[i]);
        }
    } else {
        fprintf(stderr, "[Consumer] Subscribe failed: %s\n", nng_strerror(rv));
    }

    /* Wait for messages */
    printf("[Consumer] Waiting for messages (Ctrl-C to exit)...\n");
    int timeout_secs = 30; /* Stop after 30 seconds of waiting */
    int elapsed = 0;

    while (running && elapsed < timeout_secs) {
        nng_msleep(1000);
        elapsed++;
        if (elapsed % 5 == 0) {
            printf("[Consumer] ... %d seconds elapsed, %d messages received\n",
                   elapsed, msg_count);
        }
    }

    printf("\n╔══════════════════════════════════════════════════╗\n");
    printf("║  Consumer Summary                               ║\n");
    printf("║  Total messages received: %-24d║\n", msg_count);
    printf("║  Duration: %d seconds                            ║\n", elapsed);
    printf("╚══════════════════════════════════════════════════╝\n");

    nng_close(sock);
    printf("[Consumer] Done.\n");
    return 0;
}
