// Author: wangha <wangwei at emqx dot io>
//
// This software is supplied under the terms of the MIT License, a
// copy of which should be located in the distribution where this
// file was obtained (LICENSE.txt).  A copy of the license may also be
// found online at https://opensource.org/licenses/MIT.
//

//
// This is just a simple MQTT client demonstration application.
//
// The application has three sub-commands: `conn` `pub` and `sub`.
// The `conn` sub-command connects to the server.
// The `pub` sub-command publishes a given message to the server and then
// exits. The `sub` sub-command subscribes to the given topic filter and blocks
// waiting for incoming messages.
//
// # Example:
//
// Connect to the specific server:
// ```
// $ ./quic_client conn 'mqtt-quic://127.0.0.1:14567'
// ```
//
// Subscribe to `topic` and waiting for messages:
// ```
// $ ./quic_client sub 'mqtt-tcp://127.0.0.1:14567' topic
// ```
//
// Publish 'hello' to `topic`:
// ```
// $ ./quic_client pub 'mqtt-tcp://127.0.0.1:14567' topic hello
// ```
//

#include <nng/mqtt/mqtt_client.h>
#include <nng/mqtt/mqtt_quic.h>
#include <nng/nng.h>
#include <nng/supplemental/util/platform.h>

#include "msquic.h"

#include <stdio.h>
#include <stdlib.h>

#define CONN 1
#define SUB 2
#define PUB 3

static nng_socket *g_sock;
static int g_pending_type;
static int g_pending_qos;
static const char *g_pending_topic;
static const char *g_pending_data;

conf_quic config_user = {
	.tls = {
		.enable = false,
		.cafile = "",
		.certfile = "",
		.keyfile  = "",
		.key_password = "",
		.verify_peer = true,
		.set_fail = true,
	},
	.multi_stream = false,
	.qos_first  = false,
	.qkeepalive = 10,
	.qconnect_timeout = 60,
	.qdiscon_timeout = 30,
	.qidle_timeout = 30,
};

static nng_msg *
compose_connect()
{
	nng_msg *msg;
	nng_mqtt_msg_alloc(&msg, 0);
	nng_mqtt_msg_set_packet_type(msg, NNG_MQTT_CONNECT);
	property *p  = mqtt_property_alloc();
	property *p1 = mqtt_property_set_value_u32(SESSION_EXPIRY_INTERVAL, 120);
	property *p2 = mqtt_property_set_value_u16(TOPIC_ALIAS_MAXIMUM, 10);
	mqtt_property_append(p, p1);
	mqtt_property_append(p, p2);
	nng_mqtt_msg_set_connect_property(msg, p);

	nng_mqtt_msg_set_connect_proto_version(msg, MQTT_PROTOCOL_VERSION_v5);
	nng_mqtt_msg_set_connect_keep_alive(msg, 10);
	nng_mqtt_msg_set_connect_clean_session(msg, true);

	return msg;
}

static nng_msg *
compose_subscribe(int qos, char *topic)
{
	nng_msg *msg;
	nng_mqtt_msg_alloc(&msg, 0);
	nng_mqtt_msg_set_packet_type(msg, NNG_MQTT_SUBSCRIBE);


	nng_mqtt_topic_qos subscriptions[] = {
		{ .qos     = qos,
		    .topic = { .buf = (uint8_t *) topic,
		        .length     = strlen(topic) } },
	};

	int count = sizeof(subscriptions) / sizeof(nng_mqtt_topic_qos);

	nng_mqtt_msg_set_subscribe_topics(msg, subscriptions, count);
	return msg;
}

static nng_msg *
compose_publish(int qos, char *topic, char *payload)
{
	nng_msg *msg;
	nng_mqtt_msg_alloc(&msg, 0);
	nng_mqtt_msg_set_packet_type(msg, NNG_MQTT_PUBLISH);
	nng_mqtt_msg_set_publish_dup(msg, 0);
	nng_mqtt_msg_set_publish_qos(msg, qos);
	nng_mqtt_msg_set_publish_retain(msg, 0);
	nng_mqtt_msg_set_publish_topic(msg, topic);
	nng_mqtt_msg_set_publish_payload(
	    msg, (uint8_t *) payload, strlen(payload));

	return msg;
}

static int
connect_cb(void *rmsg, void *arg)
{
	printf("[Connected][%s]...\n", (char *) arg);
	if (g_sock != NULL) {
		nng_msg *msg = NULL;
		switch (g_pending_type) {
		case SUB:
			msg = compose_subscribe(g_pending_qos, (char *) g_pending_topic);
			break;
		case PUB:
			msg = compose_publish(g_pending_qos, (char *) g_pending_topic, (char *) g_pending_data);
			break;
		default:
			break;
		}
		if (msg != NULL) {
			nng_sendmsg(*g_sock, msg, NNG_FLAG_ALLOC);
		}
	}
	return 0;
}

static int
disconnect_cb(void *rmsg, void *arg)
{
	printf("[Disconnected][%s]...\n", (char *) arg);
	return 0;
}

static int
msg_send_cb(void *rmsg, void *arg)
{
	printf("[Msg Sent][%s]...\n", (char *) arg);
	return 0;
}

static int
msg_recv_cb(void *rmsg, void *arg)
{
	printf("[Msg Arrived][%s]...\n", (char *) arg);
	nng_msg *msg = rmsg;
	uint32_t topicsz, payloadsz;

	char *topic = (char *) nng_mqtt_msg_get_publish_topic(msg, &topicsz);
	char *payload =
	    (char *) nng_mqtt_msg_get_publish_payload(msg, &payloadsz);

	printf("topic   => %.*s\n"
	       "payload => %.*s\n",
	    topicsz, topic, payloadsz, payload);
	return 0;
}

int
client(int type, const char *url, const char *qos, const char *topic,
    const char *data)
{
	nng_socket  sock;
	int         rv, q;
	nng_msg    *msg;
	const char *arg = "CLIENT FOR QUIC";

	if ((rv = nng_mqttv5_quic_client_open_conf(
	         &sock, url, &config_user)) != 0) {
		fprintf(stderr, "error in quic client open: %s\n", nng_strerror(rv));
		return rv;
	}

	if (0 !=
	        nng_mqtt_quic_set_connect_cb(
	            &sock, connect_cb, (void *) arg) ||
	    0 !=
	        nng_mqtt_quic_set_disconnect_cb(
	            &sock, disconnect_cb, (void *) arg) ||
	    0 !=
	        nng_mqtt_quic_set_msg_recv_cb(
	            &sock, msg_recv_cb, (void *) arg) ||
	    0 !=
	        nng_mqtt_quic_set_msg_send_cb(
	            &sock, msg_send_cb, (void *) arg)) {
		fprintf(stderr, "error in quic client cb set.\n");
		nng_close(sock);
		return 1;
	}
	g_sock = &sock;

	// MQTT Connect...
	msg = compose_connect();
	nng_sendmsg(sock, msg, NNG_FLAG_ALLOC);

	if (qos) {
		q = atoi(qos);
		if (q < 0 || q > 2) {
			printf("Qos should be in range(0~2).\n");
			q = 0;
		}
	}

	switch (type) {
	case CONN:
		break;
	case SUB:
		g_pending_type = type;
		g_pending_qos = qos ? q : 0;
		g_pending_topic = topic;
		g_pending_data = data;
		break;
	case PUB:
		g_pending_type = type;
		g_pending_qos = qos ? q : 0;
		g_pending_topic = topic;
		g_pending_data = data;
		break;
	default:
		printf("Unknown command.\n");
	}

	for (;;)
		nng_msleep(1000);

	return 0;
}

static void
printf_helper(char *exec)
{
	fprintf(stderr,
	    "Usage: %s conn <url>\n"
	    "       %s sub  <url> <qos> <topic>\n"
	    "       %s pub  <url> <qos> <topic> <data> or <stdin-line>\n",
	    exec, exec, exec);
	exit(EXIT_FAILURE);
}

int
main(int argc, char **argv)
{
	if (argc < 3) {
		goto error;
	}
	if (0 == strncmp(argv[1], "conn", 4) && argc == 3) {
		client(CONN, argv[2], NULL, NULL, NULL);
	} else if (0 == strncmp(argv[1], "sub", 3) && argc == 5) {
		client(SUB, argv[2], argv[3], argv[4], NULL);
	} else if (0 == strncmp(argv[1], "pub", 3) && argc == 6) {
		client(PUB, argv[2], argv[3], argv[4], argv[5]);
	} else {
		goto error;
	}

	return 0;

error:

	printf_helper(argv[0]);
	return 0;
}