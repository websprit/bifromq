import argparse
import asyncio
import ssl

from aioquic.asyncio.client import connect
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import ConnectionTerminated, HandshakeCompleted, ProtocolNegotiated, StreamDataReceived


def mqtt_connect_packet(client_id: str) -> bytes:
    protocol_name = b"\x00\x04MQTT"
    variable_header = protocol_name + bytes([0x04, 0x02, 0x00, 0x3C])
    client_id_bytes = client_id.encode("utf-8")
    payload = len(client_id_bytes).to_bytes(2, "big") + client_id_bytes
    remaining_length = len(variable_header) + len(payload)
    if remaining_length >= 128:
        raise ValueError("probe only supports single-byte MQTT remaining length")
    return bytes([0x10, remaining_length]) + variable_header + payload


def hex_prefix(data: bytes, size: int = 32) -> str:
    return " ".join(f"{byte:02x}" for byte in data[:size])


class ProbeProtocol(QuicConnectionProtocol):
    def quic_event_received(self, event) -> None:
        if isinstance(event, ProtocolNegotiated):
            print(f"AIOQUIC: protocolNegotiated alpn={event.alpn_protocol}")
        elif isinstance(event, HandshakeCompleted):
            print(
                "AIOQUIC: handshakeCompleted "
                f"alpn={event.alpn_protocol} earlyDataAccepted={event.early_data_accepted}"
            )
        elif isinstance(event, StreamDataReceived):
            print(
                "AIOQUIC: streamDataReceived "
                f"streamId={event.stream_id} endStream={event.end_stream} bytes={len(event.data)} "
                f"hex={hex_prefix(event.data)}"
            )
        elif isinstance(event, ConnectionTerminated):
            print(
                "AIOQUIC: connectionTerminated "
                f"errorCode={event.error_code} frameType={event.frame_type} reason={event.reason_phrase!r}"
            )
        super().quic_event_received(event)


async def run_probe(host: str, port: int, client_id: str, timeout: float) -> int:
    packet = mqtt_connect_packet(client_id)
    configuration = QuicConfiguration(is_client=True, alpn_protocols=["mqtt"])
    configuration.verify_mode = ssl.CERT_NONE
    configuration.server_name = host

    print(f"AIOQUIC: endpoint={host}:{port}")
    print(f"AIOQUIC: connectPacketBytes={len(packet)} hex={hex_prefix(packet)}")

    async with connect(
        host,
        port,
        configuration=configuration,
        create_protocol=ProbeProtocol,
        wait_connected=True,
    ) as protocol:
        print("AIOQUIC: connected")
        reader, writer = await protocol.create_stream(is_unidirectional=False)
        stream_id = writer.get_extra_info("stream_id")
        print(f"AIOQUIC: openedStream streamId={stream_id}")

        writer.write(packet)
        await writer.drain()
        print("AIOQUIC: sent CONNECT over bidi stream")

        try:
            data = await asyncio.wait_for(reader.read(32), timeout=timeout)
        except TimeoutError:
            print(f"AIOQUIC: readTimeout seconds={timeout}")
            return 2

        print(f"AIOQUIC: received bytes={len(data)} hex={hex_prefix(data)}")
        return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Explicit QUIC bidi-stream probe for BifroMQ")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=14567)
    parser.add_argument("--client-id", default="aioquic-probe")
    parser.add_argument("--timeout", type=float, default=8.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        return asyncio.run(run_probe(args.host, args.port, args.client_id, args.timeout))
    except Exception as error:
        print(f"AIOQUIC: topLevelError={error!r}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())