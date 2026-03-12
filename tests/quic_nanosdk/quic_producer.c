/*
 * MQTT over QUIC Producer Client using NanoSDK
 * 
 * Connects to BifroMQ MQTT over QUIC broker and publishes 
 * messages to multiple topics to test multi-stream routing.
 *
 * Usage: ./quic_producer <broker_url> [multi_stream]
 *   broker_url:    mqtt-quic://host:port
 *   multi_stream:  1 to enable multi-stream, 0 (default) for single-stream
 *
 * Example:
 *   ./quic_producer mqtt-quic://host.docker.internal:14567 0
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <nng/nng.h>
#include <nng/mqtt/mqtt_quic.h>
#include <nng/mqtt/mqtt_client.h>

static volatile int running = 1;

static void signal_handler(int sig) {
    (void)sig;
    running = 0;
}

/* ----- Callback functions ----- */

static int connect_cb(void *rmsg, void *arg) {
    (void)rmsg;
    printf("[Producer] ✅ Connected to broker (%s)\n", (char *)arg);
    return 0;
}

static int disconnect_cb(void *rmsg, void *arg) {
    (void)rmsg;
    printf("[Producer] ❌ Disconnected from broker (%s)\n", (char *)arg);
    running = 0;
    return 0;
}

static int msg_send_cb(void *rmsg, void *arg) {
    (void)rmsg;
    (void)arg;
    return 0;
}

static int msg_recv_cb(void *rmsg, void *arg) {
    (void)rmsg;
    (void)arg;
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
    nng_mqtt_msg_set_connect_client_id(msg, "nanosdk-producer-1");
    return msg;
}

static nng_msg *compose_publish(const char *topic, const char *payload, int qos) {
    nng_msg *msg;
    nng_mqtt_msg_alloc(&msg, 0);
    nng_mqtt_msg_set_packet_type(msg, NNG_MQTT_PUBLISH);
    nng_mqtt_msg_set_publish_dup(msg, 0);
    nng_mqtt_msg_set_publish_qos(msg, qos);
    nng_mqtt_msg_set_publish_retain(msg, 0);
    nng_mqtt_msg_set_publish_topic(msg, topic);
    nng_mqtt_msg_set_publish_payload(msg, (uint8_t *)payload, strlen(payload));
    return msg;
}

/* ----- Test data ----- */

typedef struct {
    const char *topic;
    const char *payload;
} test_message_t;

static test_message_t test_messages[] = {
    {"sensor/temp",     "{\"value\": 23.5, \"unit\": \"C\",  \"ts\": 0}"},
    {"sensor/humidity", "{\"value\": 65,   \"unit\": \"%\",  \"ts\": 0}"},
    {"device/status",   "{\"online\": true, \"battery\": 85, \"ts\": 0}"},
    {"sensor/pressure", "{\"value\": 1013, \"unit\": \"hPa\",\"ts\": 0}"},
    {"device/alarm",    "{\"type\": \"motion\", \"zone\": 3, \"ts\": 0}"},
};

#define NUM_MESSAGES (sizeof(test_messages) / sizeof(test_messages[0]))

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
    printf("║  NanoSDK MQTT over QUIC Producer                ║\n");
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
    const char *cb_arg = "producer";

    /* Open QUIC MQTT connection */
    if ((rv = nng_mqtt_quic_client_open_conf(&sock, url, &config)) != 0) {
        fprintf(stderr, "[Producer] Failed to open QUIC client: %s\n", nng_strerror(rv));
        return 1;
    }

    /* Register callbacks */
    nng_mqtt_quic_set_connect_cb(&sock, connect_cb, (void *)cb_arg);
    nng_mqtt_quic_set_disconnect_cb(&sock, disconnect_cb, (void *)cb_arg);
    nng_mqtt_quic_set_msg_send_cb(&sock, msg_send_cb, (void *)cb_arg);
    nng_mqtt_quic_set_msg_recv_cb(&sock, msg_recv_cb, (void *)cb_arg);

    /* Send CONNECT */
    nng_msg *conn_msg = compose_connect();
    nng_sendmsg(sock, conn_msg, NNG_FLAG_ALLOC);
    printf("[Producer] CONNECT sent, waiting for CONNACK...\n");

    /* Wait for connection to establish */
    nng_msleep(2000);

    /* Publish messages in a loop */
    int msg_count = 0;
    char payload_buf[256];

    printf("[Producer] Starting to publish messages...\n");
    while (running && msg_count < 15) {
        int idx = msg_count % NUM_MESSAGES;
        
        /* Add sequence number to payload */
        snprintf(payload_buf, sizeof(payload_buf),
                 "{\"topic\": \"%s\", \"seq\": %d, \"data\": %s}",
                 test_messages[idx].topic, msg_count, test_messages[idx].payload);

        nng_msg *pub_msg = compose_publish(test_messages[idx].topic, payload_buf, 0);
        rv = nng_sendmsg(sock, pub_msg, NNG_FLAG_NONBLOCK);
        
        if (rv == 0) {
            printf("[Producer] 📤 Published #%d: topic=%s\n", msg_count, test_messages[idx].topic);
            msg_count++;
        } else {
            fprintf(stderr, "[Producer] Send failed: %s\n", nng_strerror(rv));
        }

        nng_msleep(500);  /* 500ms interval */
    }

    printf("[Producer] Published %d messages total\n", msg_count);
    
    /* Wait a bit for delivery */
    nng_msleep(2000);

    nng_close(sock);
    printf("[Producer] Done.\n");
    return 0;
}
