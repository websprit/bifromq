/*
 * swift-mqtt MQTT over QUIC / TCP 测试客户端
 * 用法:
 *   swift run MQTTTest both mqtt-quic://127.0.0.1:14567   # QUIC
 *   swift run MQTTTest both mqtt://127.0.0.1:1883         # TCP
 *   swift run MQTTTest consumer mqtt-quic://127.0.0.1:14567
 *   swift run MQTTTest producer mqtt-quic://127.0.0.1:14567
 */
import MQTT
import Foundation

// MARK: - URL 解析

func makeEndpoint(from urlStr: String) -> (Endpoint, Bool) {
    var str = urlStr
    var isQUIC = false
    if str.hasPrefix("mqtt-quic://") {
        isQUIC = true
        str = str.replacingOccurrences(of: "mqtt-quic://", with: "")
    } else if str.hasPrefix("mqtt://") {
        str = str.replacingOccurrences(of: "mqtt://", with: "")
    } else if str.hasPrefix("mqtts://") {
        str = str.replacingOccurrences(of: "mqtts://", with: "")
    }
    let parts = str.split(separator: ":")
    let host = String(parts[0])
    let port = UInt16(parts.count > 1 ? parts[1] : "1883") ?? 1883

    let endpoint: Endpoint
    if isQUIC {
        var tls = TLSOptions()
        tls.trust = .trustAll   // 信任自签名证书
        endpoint = .quic(host: host, port: port, tls: tls)
    } else {
        endpoint = .tcp(host: host, port: port)
    }
    return (endpoint, isQUIC)
}

// MARK: - Consumer

final class MQTTConsumer: MQTTClient.V3, @unchecked Sendable {
    let url: String
    var messageCount = 0

    init(url: String) {
        self.url = url
        let (endpoint, _) = makeEndpoint(from: url)
        super.init(endpoint)
        self.config.keepAlive = 60
        self.config.pingEnabled = true
        self.delegateQueue = .main
        self.delegate = self
    }
}

extension MQTTConsumer: MQTTDelegate {
    func mqtt(_ mqtt: MQTTClient, didUpdate status: Status, prev: Status) {
        print("[Consumer] Status: \(prev) → \(status)")
        guard status == .opened else {
            if case .closed(let reason) = status {
                print("[Consumer] ❌ Closed: \(reason?.description ?? "no reason")")
            }
            return
        }
        print("[Consumer] ✅ Connected to \(url)")
        self.subscribe(to: "sensor/#")
        self.subscribe(to: "device/#")
        print("[Consumer] 📋 Subscribed: sensor/# device/#  — waiting for messages...")
    }

    func mqtt(_ mqtt: MQTTClient, didReceive error: any Error) {
        print("[Consumer] ⚠️  Error: \(error)")
    }

    func mqtt(_ mqtt: MQTTClient, didReceive message: Message) {
        messageCount += 1
        let text = String(data: message.payload, encoding: .utf8) ?? "(binary)"
        print("[Consumer] 📥 #\(messageCount)  topic=\(message.topic)  payload=\(text)")
    }
}

// MARK: - Producer

final class MQTTProducer: MQTTClient.V3, @unchecked Sendable {
    let url: String
    var publishCount = 0
    var timer: Timer?

    static let messages: [(topic: String, payload: String)] = [
        ("sensor/temp",     #"{"value":23.5,"unit":"C"}"#),
        ("sensor/humidity", #"{"value":65.0,"unit":"%"}"#),
        ("device/status",   #"{"online":true,"battery":85}"#),
        ("sensor/pressure", #"{"value":1013,"unit":"hPa"}"#),
        ("device/alarm",    #"{"type":"motion","zone":3}"#),
    ]

    init(url: String) {
        self.url = url
        let (endpoint, _) = makeEndpoint(from: url)
        super.init(endpoint)
        self.config.keepAlive = 60
        self.config.pingEnabled = true
        self.delegateQueue = .main
        self.delegate = self
    }

    func startPublishing() {
        timer = Timer.scheduledTimer(withTimeInterval: 0.8, repeats: true) { [weak self] _ in
            guard let self = self else { return }
            if self.publishCount >= 10 {
                self.timer?.invalidate()
                print("[Producer] ✅ Done — published \(self.publishCount) messages")
                return
            }
            let m = Self.messages[self.publishCount % Self.messages.count]
            let text = #"{"seq":\#(self.publishCount),"data":\#(m.payload)}"#
            self.publish(to: m.topic, payload: text, qos: .atMostOnce)
            print("[Producer] 📤 #\(self.publishCount)  topic=\(m.topic)")
            self.publishCount += 1
        }
    }
}

extension MQTTProducer: MQTTDelegate {
    func mqtt(_ mqtt: MQTTClient, didUpdate status: Status, prev: Status) {
        print("[Producer] Status: \(prev) → \(status)")
        guard status == .opened else {
            if case .closed(let reason) = status {
                timer?.invalidate()
                print("[Producer] ❌ Closed: \(reason?.description ?? "no reason")")
            }
            return
        }
        print("[Producer] ✅ Connected to \(url)")
        DispatchQueue.main.asyncAfter(deadline: .now() + 1.0) { self.startPublishing() }
    }

    func mqtt(_ mqtt: MQTTClient, didReceive error: any Error) {
        print("[Producer] ⚠️  Error: \(error)")
    }

    func mqtt(_ mqtt: MQTTClient, didReceive message: Message) {}
}

// MARK: - Main

let mode    = CommandLine.arguments.count > 1 ? CommandLine.arguments[1] : "both"
let urlStr  = CommandLine.arguments.count > 2 ? CommandLine.arguments[2] : "mqtt-quic://127.0.0.1:14567"
let (_, isQUIC) = makeEndpoint(from: urlStr)

print("")
print("╔════════════════════════════════════════════════════════════╗")
print("║  swift-mqtt MQTT over \(isQUIC ? "QUIC" : "TCP ") Test                        ║")
print("║  Broker : \(urlStr)")
print("║  Mode   : \(mode)")
print("╚════════════════════════════════════════════════════════════╝")
print("")

MQTT.Logger.level = .debug

var consumer: MQTTConsumer?
var producer: MQTTProducer?

if mode == "consumer" || mode == "both" {
    consumer = MQTTConsumer(url: urlStr)
    consumer?.open(Identity(UUID().uuidString, username: "DevOnly/consumer"))
}

if mode == "producer" || mode == "both" {
    let delay: Double = (mode == "both") ? 3.0 : 0.0
    DispatchQueue.main.asyncAfter(deadline: .now() + delay) {
        producer = MQTTProducer(url: urlStr)
        producer?.open(Identity(UUID().uuidString, username: "DevOnly/producer"))
    }
}

// 运行 45 秒后打印摘要并退出
DispatchQueue.main.asyncAfter(deadline: .now() + 45.0) {
    let rx = consumer?.messageCount ?? 0
    let tx = producer?.publishCount ?? 0
    print("")
    print("╔════════════════════════════════════════════════════════════╗")
    print("║  Test Summary                                             ║")
    print("║  Consumer received: \(rx) messages")
    print("║  Producer sent:     \(tx) messages")
    if rx == tx && tx > 0 {
        print("║  ✅ PASS — all messages delivered!")
    } else if tx == 0 {
        print("║  ❌ FAIL — no messages were sent (connection issue)")
    } else {
        print("║  ⚠️  Partial — \(rx)/\(tx) messages received")
    }
    print("╚════════════════════════════════════════════════════════════╝")

    consumer?.close()
    producer?.close()
    DispatchQueue.main.asyncAfter(deadline: .now() + 1.0) { exit(rx > 0 ? 0 : 1) }
}

RunLoop.main.run()
