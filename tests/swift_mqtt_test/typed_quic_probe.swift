import Foundation
import Network

@available(macOS 26.0, *)
private struct ProbeTimeout: Error, CustomStringConvertible {
    let operation: String
    let seconds: Double
    var description: String { "ProbeTimeout(operation: \(operation), seconds: \(seconds))" }
}

@available(macOS 26.0, *)
private func withTimeout<T: Sendable>(_ seconds: Double, operation: String, _ body: @escaping @Sendable () async throws -> T) async throws -> T {
    try await withThrowingTaskGroup(of: T.self) { group in
        group.addTask {
            try await body()
        }
        group.addTask {
            try await Task.sleep(for: .seconds(seconds))
            throw ProbeTimeout(operation: operation, seconds: seconds)
        }
        let result = try await group.next()!
        group.cancelAll()
        return result
    }
}

@available(macOS 26.0, *)
private func mqttConnectPacket(clientID: String) -> Data {
    let protocolName = Data([0x00, 0x04]) + Data("MQTT".utf8)
    let variableHeader = protocolName + Data([0x04, 0x02, 0x00, 0x3c])
    let clientIDData = Data(clientID.utf8)
    let payload = Data([UInt8((clientIDData.count >> 8) & 0xff), UInt8(clientIDData.count & 0xff)]) + clientIDData
    let remainingLength = variableHeader.count + payload.count
    precondition(remainingLength < 128, "Probe CONNECT packet only supports single-byte remaining length")
    return Data([0x10, UInt8(remainingLength)]) + variableHeader + payload
}

@available(macOS 26.0, *)
private func hexPrefix(_ data: Data, maxBytes: Int = 32) -> String {
    data.prefix(maxBytes).map { String(format: "%02x", $0) }.joined(separator: " ")
}

@available(macOS 26.0, *)
private func describe(_ value: Any) -> String {
    String(describing: value)
}

@available(macOS 26.0, *)
@main
struct TypedQUICProbe {
    static func main() async {
        let endpoint = NWEndpoint.hostPort(host: .ipv4(.loopback), port: 14567)
        let connectPacket = mqttConnectPacket(clientID: "typed-quic-probe")

        print("PROBE: endpoint=\(endpoint)")
        print("PROBE: connectPacketBytes=\(connectPacket.count) hex=\(hexPrefix(connectPacket))")

        do {
            try await withThrowingTaskGroup(of: Void.self) { group in
                try await withNetworkConnection(to: endpoint) {
                    QUIC(alpn: ["mqtt"])
                        .idleTimeout(30)
                        .initialMaxData(10_000_000)
                        .initialMaxBidirectionalStreams(100)
                        .initialMaxUnidirectionalStreams(10)
                        .tls.peerAuthentication(.required)
                        .tls.certificateValidator { metadata, trust in
                            print("PROBE: certificateValidator metadata=\(metadata) trust=\(trust)")
                            return true
                        }
                } _: { connection in
                    let negotiatedALPN = connection.negotiatedALPN ?? "nil"
                    print("PROBE: connected remoteMaxBidi=\(connection.remoteMaxStreamsBidirectional) remoteMaxUni=\(connection.remoteMaxStreamsUnidirectional) usableDatagramFrameSize=\(connection.usableDatagramFrameSize)")
                    print("PROBE: negotiatedALPN=\(negotiatedALPN) remoteIdleTimeout=\(connection.remoteIdleTimeout)")
                    try await Task.sleep(for: .seconds(1))
                    let negotiatedALPNAfter1s = connection.negotiatedALPN ?? "nil"
                    print("PROBE: after1s remoteMaxBidi=\(connection.remoteMaxStreamsBidirectional) remoteMaxUni=\(connection.remoteMaxStreamsUnidirectional) usableDatagramFrameSize=\(connection.usableDatagramFrameSize)")
                    print("PROBE: after1s negotiatedALPN=\(negotiatedALPNAfter1s) remoteIdleTimeout=\(connection.remoteIdleTimeout)")

                    group.addTask {
                        do {
                            try await connection.inboundStreams { stream in
                                print("PROBE: inboundStream streamID=\(stream.streamID) directionality=\(stream.directionality) initiator=\(stream.initiator)")
                                let received = try await stream.receive(atLeast: 1, atMost: 32)
                                print("PROBE: inboundStream receive=\(describe(received))")
                            }
                        } catch {
                            print("PROBE: inboundStreams error=\(error)")
                        }
                    }

                    print("PROBE: aboutToOpenStream")
                    let stream = try await withTimeout(8, operation: "openStream") {
                        try await connection.openStream(directionality: .bidirectional)
                    }
                    print("PROBE: openedStream streamID=\(stream.streamID) directionality=\(stream.directionality) initiator=\(stream.initiator)")
                    try await stream.send(connectPacket, endOfStream: false)
                    print("PROBE: sent CONNECT over typed bidi stream")
                    let first = try await withTimeout(8, operation: "stream.receive") {
                        try await stream.receive(atLeast: 1, atMost: 32)
                    }
                    print("PROBE: streamReceive=\(describe(first))")
                    group.cancelAll()
                }
            }
        } catch {
            print("PROBE: topLevelError=\(error)")
            exit(1)
        }

        exit(0)
    }
}
