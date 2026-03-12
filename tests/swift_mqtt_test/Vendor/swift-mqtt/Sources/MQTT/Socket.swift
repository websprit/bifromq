//
//  Socket.swift
//  swift-mqtt
//
//  Created by supertext on 2025/3/7.
//

import Foundation
import Network

protocol SocketDelegate:AnyObject{
    func socket(_ socket:Socket,didReceive error:Error)
    func socket(_ socket:Socket,didReceive packet:Packet)
}
private struct PendingSend {
    let data:Data
    let promise:Promise<Void>
}
final class Socket:@unchecked Sendable{
    private static let quicGroupReadyTimeout:TimeInterval = 3.0
    private let queue = DispatchQueue(label: "mqtt.socket.queue")
    private let config:Config
    private let socketID = UUID().uuidString.prefix(8)
    private var header:UInt8 = 0
    private var length:Int = 0
    private var multiply = 1
    private let endpoint:Endpoint
    @Safely private var conn:NWConnection?
    @Safely private var groupRef:AnyObject?
    @Safely private var awaitingQUICStream = false
    @Safely private var quicConnectionReady = false
    @Safely private var receiveLoopStarted = false
    @Safely private var pendingSends:[PendingSend] = []
    @Safely private var attempt = 0
    @Safely private var quicReadyTimeoutWorkItem:DispatchWorkItem?
    weak var delegate:SocketDelegate?
    init(endpoint:Endpoint,config:Config){
        self.config = config
        self.endpoint = endpoint
    }
    deinit{
        stop()
    }
    func stop(){
        Logger.debug("SOCKET STOP: socket=\(socketID) endpoint=\(endpoint.debugSummary) conn=\(conn.debugID) group=\(groupRef.debugID) awaitingQUICStream=\(awaitingQUICStream) pendingSends=\(pendingSends.count) attempt=\(attempt)")
        self.$conn.write { conn in
            conn?.cancel()
            conn = nil
        }
        self.$groupRef.write { groupRef in
            if #available(macOS 11.0, iOS 14.0, watchOS 7.0, tvOS 14.0, *), let group = groupRef as? NWConnectionGroup {
                group.cancel()
            }
            groupRef = nil
        }
        self.$awaitingQUICStream.write { $0 = false }
        self.$quicConnectionReady.write { $0 = false }
        self.$receiveLoopStarted.write { $0 = false }
        self.$quicReadyTimeoutWorkItem.write { workItem in
            workItem?.cancel()
            workItem = nil
        }
        self.$pendingSends.write { sends in
            sends.removeAll()
        }
    }
    func start(){
        let started = self.$conn.write { conn in
            if conn != nil {
                Logger.debug("SOCKET START SKIP: socket=\(self.socketID) endpoint=\(self.endpoint.debugSummary) connAlreadyExists=\(conn.debugID) attempt=\(self.attempt)")
                return false
            }
            let attempt = self.$attempt.write { value in
                value += 1
                return value
            }
            let params = endpoint.params(config: config)
            Logger.debug("SOCKET START: socket=\(self.socketID) endpoint=\(endpoint.debugSummary) mode=direct attempt=\(attempt) params=\(params.1.debugSummary)")
            let directConn = NWConnection.init(to: params.0, using: params.1)
            self.bind(connection: directConn, source: "direct", attempt: attempt)
            directConn.start(queue: self.queue)
            conn = directConn
            return true
        }
        if started, !isQUIC {
            beginReceiveLoop()
        }
    }
    func send(data:Data)->Promise<Void>{
        if let conn {
            if isQUIC, !quicConnectionReady {
                let promise = Promise<Void>()
                Logger.debug("SOCKET SEND QUEUED: endpoint=\(endpoint.debugSummary) bytes=\(data.count) waitingForQUICConnectionReady=true")
                self.$pendingSends.write { sends in
                    sends.append(PendingSend(data: data, promise: promise))
                }
                return promise
            }
            return sendNow(data: data, via: conn)
        }
        if isQUIC, awaitingQUICStream {
            let promise = Promise<Void>()
            Logger.debug("SOCKET SEND QUEUED: endpoint=\(endpoint.debugSummary) bytes=\(data.count) waitingForExplicitQUICStream=true")
            self.$pendingSends.write { sends in
                sends.append(PendingSend(data: data, promise: promise))
            }
            return promise
        }
        return Promise(MQTTError.unconnected)
    }

    private func sendNow(data:Data, via conn:NWConnection)->Promise<Void>{
        let promise = Promise<Void>()
        Logger.debug("SOCKET SEND: endpoint=\(endpoint.debugSummary) bytes=\(data.count) context=\(NWConnection.ContentContext.mqtt(isws).debugSummary)")
        conn.send(content: data,contentContext: .mqtt(isws), completion: .contentProcessed({ error in
            if let error{
                Logger.error("SOCKET SEND: \(data.count) bytes failed. error:\(error)")
                promise.done(error)
            }else{
                Logger.debug("SOCKET SEND: endpoint=\(self.endpoint.debugSummary) bytes=\(data.count) completed")
                promise.done(())
            }
        }))
        return promise
    }
    
    private func handle(state:NWConnection.State, source:String, attempt:Int, connection:NWConnection){
        Logger.debug("SOCKET STATE: socket=\(socketID) endpoint=\(endpoint.debugSummary) source=\(source) attempt=\(attempt) connection=\(connection.debugID) currentConn=\(conn.debugID) state=\(state.debugSummary)")
        switch state{
        case .cancelled:
            // This is the network telling us we're closed. We don't need to actually do anything here
            break
        case .failed(let error):
            // The connection has failed for some reason.
            self.$quicConnectionReady.write { $0 = false }
            self.delegate?.socket(self, didReceive: error)
        case .ready:
            // Ok connection is ready. But we don't need to do anything at all.
            if isQUIC {
                self.$quicConnectionReady.write { $0 = true }
                if self.$receiveLoopStarted.write({ started in
                    if started {
                        return false
                    }
                    started = true
                    return true
                }) {
                    Logger.debug("SOCKET QUIC READY: socket=\(socketID) endpoint=\(endpoint.debugSummary) source=\(source) attempt=\(attempt) connection=\(connection.debugID) startingReceiveLoop=true")
                    beginReceiveLoop()
                }
                flushPendingSends(via: connection)
            }
            break
        case .preparing:
            // This just means connections are being actively established. We have no specific action here.
            break
        case .setup:
            //
            break
        case .waiting(let error):
            // Perhaps nothing will happen, but here we can safely treat this as an error and there is no harm in doing so.
            if shouldIgnoreTransientQUICConnectionError(error) {
                Logger.debug("SOCKET QUIC WAITING: socket=\(socketID) endpoint=\(endpoint.debugSummary) source=\(source) attempt=\(attempt) ignoredTransientError=\(error)")
                return
            }
            self.delegate?.socket(self, didReceive: error)
        default:
            // Never happen
            break
        }
    }

    @available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *)
    private func startQUICStreamConnection(attempt:Int) -> Bool {
        let params = endpoint.params(config: config)
        Logger.debug("SOCKET QUIC GROUP START: socket=\(socketID) endpoint=\(endpoint.debugSummary) attempt=\(attempt) params=\(params.1.debugSummary)")

        let group = NWConnectionGroup(with: NWMultiplexGroup(to: params.0), using: params.1)
        group.stateUpdateHandler = { [weak self] state in
            guard let self else { return }
            let currentGroupID = (self.groupRef as? NWConnectionGroup).debugID
            let matchesCurrent = ((self.groupRef as? NWConnectionGroup) === group)
            Logger.debug("SOCKET QUIC GROUP STATE: socket=\(self.socketID) endpoint=\(self.endpoint.debugSummary) attempt=\(attempt) group=\(group.debugID) currentGroup=\(currentGroupID) matchesCurrent=\(matchesCurrent) awaitingQUICStream=\(self.awaitingQUICStream) currentConn=\(self.conn.debugID) state=\(state.debugSummary)")
            switch state {
            case .ready:
                self.cancelQUICReadyTimeout(group: group)
                self.activateQUICStreamConnection(group: group, endpoint: params.0, attempt: attempt)
            case .waiting(let error):
                Logger.debug("SOCKET QUIC GROUP ERROR: socket=\(self.socketID) endpoint=\(self.endpoint.debugSummary) attempt=\(attempt) group=\(group.debugID) deliveringError=\(error)")
                if !matchesCurrent {
                    Logger.debug("SOCKET QUIC GROUP WAITING: socket=\(self.socketID) endpoint=\(self.endpoint.debugSummary) attempt=\(attempt) group=\(group.debugID) ignored because group is stale")
                    return
                }
                if self.shouldDelayQUICWaitingError(error) {
                    self.scheduleQUICReadyTimeout(group: group, attempt: attempt)
                    return
                }
                self.cancelQUICReadyTimeout(group: group)
                self.delegate?.socket(self, didReceive: error)
            case .failed(let error):
                Logger.debug("SOCKET QUIC GROUP FAILED: socket=\(self.socketID) endpoint=\(self.endpoint.debugSummary) attempt=\(attempt) group=\(group.debugID) deliveringError=\(error)")
                self.cancelQUICReadyTimeout(group: group)
                if matchesCurrent {
                    self.delegate?.socket(self, didReceive: error)
                }
            case .cancelled:
                self.cancelQUICReadyTimeout(group: group)
                Logger.debug("SOCKET QUIC GROUP CANCELLED: socket=\(self.socketID) endpoint=\(self.endpoint.debugSummary) attempt=\(attempt) group=\(group.debugID) awaitingQUICStream=\(self.awaitingQUICStream)")
                if self.awaitingQUICStream {
                    self.delegate?.socket(self, didReceive: MQTTError.unconnected)
                }
            case .setup:
                break
            @unknown default:
                break
            }
        }
        group.newConnectionHandler = { [weak self] newConn in
            guard let self else { return }
            Logger.debug("SOCKET QUIC GROUP NEW CONNECTION: socket=\(self.socketID) endpoint=\(self.endpoint.debugSummary) attempt=\(attempt) group=\(group.debugID) connection=\(newConn.debugID) incomingQueueSet=\(newConn.queue != nil)")
        }
        group.start(queue: queue)
        self.$groupRef.write { current in
            current = group
        }
        self.$awaitingQUICStream.write { $0 = true }
        self.$quicConnectionReady.write { $0 = false }
        self.scheduleQUICReadyTimeout(group: group, attempt: attempt)
        return true
    }

    @available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *)
    private func activateQUICStreamConnection(group:NWConnectionGroup, endpoint:NWEndpoint, attempt:Int) {
        let alreadyConnected = self.$conn.write { conn in
            conn != nil
        }
        let currentGroup = self.groupRef as? NWConnectionGroup
        let matchesCurrent = currentGroup === group
        Logger.debug("SOCKET QUIC GROUP ACTIVATE: socket=\(socketID) endpoint=\(self.endpoint.debugSummary) attempt=\(attempt) group=\(group.debugID) currentGroup=\(currentGroup.debugID) matchesCurrent=\(matchesCurrent) awaitingQUICStream=\(awaitingQUICStream) alreadyConnected=\(alreadyConnected)")
        if alreadyConnected || !matchesCurrent {
            return
        }

        let streamOptions = NWProtocolQUIC.Options()
        streamOptions.direction = .bidirectional
        let streamConn = NWConnection(from: group, to: nil, using: streamOptions)
            ?? group.extract(connectionTo: nil, using: streamOptions)
        guard let streamConn else {
            Logger.warning("SOCKET QUIC GROUP EXTRACT: socket=\(socketID) endpoint=\(self.endpoint.debugSummary) attempt=\(attempt) group=\(group.debugID) currentGroup=\(currentGroup.debugID) matchesCurrent=\(matchesCurrent) awaitingQUICStream=\(awaitingQUICStream) returned nil after group ready")
            self.$awaitingQUICStream.write { $0 = false }
            self.delegate?.socket(self, didReceive: MQTTError.unconnected)
            return
        }
        Logger.debug("SOCKET QUIC GROUP EXTRACT: socket=\(socketID) endpoint=\(self.endpoint.debugSummary) attempt=\(attempt) group=\(group.debugID) connection=\(streamConn.debugID) direction=bidirectional afterReady=true")
        bind(connection: streamConn, source: "quic-stream", attempt: attempt)
        self.$conn.write { conn in
            conn = streamConn
        }
        self.$awaitingQUICStream.write { $0 = false }
        self.$quicConnectionReady.write { $0 = false }
        streamConn.start(queue: queue)
    }

    private func shouldDelayQUICWaitingError(_ error:Error) -> Bool {
        guard awaitingQUICStream else {
            return false
        }
        guard case let NWError.posix(posix) = error else {
            return false
        }
        switch posix {
        case .ENETDOWN, .ENETUNREACH:
            Logger.debug("SOCKET QUIC GROUP WAITING: socket=\(socketID) endpoint=\(endpoint.debugSummary) delaying transient error=\(posix) while awaiting explicit stream")
            return true
        default:
            return false
        }
    }

    private func shouldIgnoreTransientQUICConnectionError(_ error:Error) -> Bool {
        guard isQUIC, !quicConnectionReady else {
            return false
        }
        guard case let NWError.posix(posix) = error else {
            return false
        }
        switch posix {
        case .ENETDOWN, .ENETUNREACH:
            return true
        default:
            return false
        }
    }

    @available(macOS 11.0, iOS 14.0, watchOS 7.0, tvOS 14.0, *)
    private func scheduleQUICReadyTimeout(group:NWConnectionGroup, attempt:Int) {
        let workItem = DispatchWorkItem { [weak self, weak group] in
            guard let self, let group else { return }
            let currentGroup = self.groupRef as? NWConnectionGroup
            let matchesCurrent = currentGroup === group
            Logger.debug("SOCKET QUIC GROUP TIMEOUT: socket=\(self.socketID) endpoint=\(self.endpoint.debugSummary) attempt=\(attempt) group=\(group.debugID) currentGroup=\(currentGroup.debugID) matchesCurrent=\(matchesCurrent) awaitingQUICStream=\(self.awaitingQUICStream)")
            guard matchesCurrent, self.awaitingQUICStream, self.conn == nil else {
                return
            }
            self.$awaitingQUICStream.write { $0 = false }
            self.delegate?.socket(self, didReceive: MQTTError.unconnected)
        }
        self.$quicReadyTimeoutWorkItem.write { current in
            current?.cancel()
            current = workItem
        }
        queue.asyncAfter(deadline: .now() + Self.quicGroupReadyTimeout, execute: workItem)
    }

    @available(macOS 11.0, iOS 14.0, watchOS 7.0, tvOS 14.0, *)
    private func cancelQUICReadyTimeout(group:NWConnectionGroup) {
        let matchesCurrent = (self.groupRef as? NWConnectionGroup) === group
        guard matchesCurrent || self.groupRef == nil else {
            return
        }
        self.$quicReadyTimeoutWorkItem.write { workItem in
            workItem?.cancel()
            workItem = nil
        }
    }

    private func flushPendingSends(via conn:NWConnection) {
        let sends = self.$pendingSends.write { sends in
            let pending = sends
            sends.removeAll()
            return pending
        }
        if !sends.isEmpty {
            Logger.debug("SOCKET SEND FLUSH: socket=\(socketID) endpoint=\(endpoint.debugSummary) count=\(sends.count) connection=\(conn.debugID)")
        }
        for pending in sends {
            sendNow(data: pending.data, via: conn).then { _ in
                pending.promise.done(())
            }.catch { error in
                pending.promise.done(error)
            }
        }
    }

    private func beginReceiveLoop() {
        if isws{
            readMessage()
        }else{
            readHeader()
        }
    }

    private func bind(connection: NWConnection, source:String, attempt:Int) {
        Logger.debug("SOCKET BIND: socket=\(socketID) endpoint=\(endpoint.debugSummary) source=\(source) attempt=\(attempt) connection=\(connection.debugID)")
        connection.stateUpdateHandler = {[weak self] state in
            self?.handle(state: state, source: source, attempt: attempt, connection: connection)
        }
        connection.viabilityUpdateHandler = { [weak self] isViable in
            guard let self else { return }
            Logger.debug("SOCKET VIABILITY: socket=\(self.socketID) endpoint=\(self.endpoint.debugSummary) source=\(source) attempt=\(attempt) connection=\(connection.debugID) viable=\(isViable)")
        }
        if #available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *) {
            if let metadata = connection.metadata(definition: NWProtocolQUIC.definition) as? NWProtocolQUIC.Metadata {
                Logger.debug("SOCKET QUIC METADATA: socket=\(socketID) endpoint=\(endpoint.debugSummary) source=\(source) attempt=\(attempt) connection=\(connection.debugID) streamId=\(metadata.streamIdentifier) localMaxBidi=\(metadata.localMaxStreamsBidirectional) remoteMaxBidi=\(metadata.remoteMaxStreamsBidirectional)")
            } else {
                Logger.debug("SOCKET QUIC METADATA: socket=\(socketID) endpoint=\(endpoint.debugSummary) source=\(source) attempt=\(attempt) connection=\(connection.debugID) unavailable")
            }
        }
    }
    
    private func readHeader(){
        self.reset()
        self.readData(1) {[weak self] result in
            if let self{
                self.header = result[0]
                self.readLength()
            }
        }
    }
    private func readLength(){
        self.readData(1) {[weak self] result in
            guard let self else{ return }
            let byte = result[0]
            self.length += Int(byte & 0x7f) * self.multiply
            if byte & 0x80 != 0{
                let result = self.multiply.multipliedReportingOverflow(by: 0x80)
                if result.overflow {
                    self.delegate?.socket(self, didReceive: MQTTError.decodeError(.varintOverflow))
                    return
                }
                self.multiply = result.partialValue
                return self.readLength()
            }
            if self.length > 0{
                return self.readPayload()
            }
            self.dispath(data: Data())
        }
    }
    
    private func readPayload(){
        self.readData(length) {[weak self] data in
            self?.dispath(data: data)
        }
    }
    private func dispath(data:Data){
        guard let type = PacketType(rawValue: header) ?? PacketType(rawValue: header & 0xF0) else {
            self.delegate?.socket(self, didReceive: MQTTError.decodeError(.unrecognisedPacketType))
            return
        }
        let incoming:IncomingPacket = .init(type: type, flags: self.header & 0xF, remainingData: .init(data: data))
        do {
            self.delegate?.socket(self, didReceive: try incoming.packet(with: self.config.version))
        } catch {
            self.delegate?.socket(self, didReceive: error)
        }
        self.readHeader()
    }
    private func readData(_ length:Int,finish:(@Sendable (Data)->Void)?){
        guard let conn else { return }
        Logger.debug("SOCKET READ: endpoint=\(endpoint.debugSummary) mode=stream requested=\(length)")
        conn.receive(minimumIncompleteLength: length, maximumLength: length, completion: {[weak self] content, contentContext, isComplete, error in
            guard let self else{
                return
            }
            Logger.debug("SOCKET READ: endpoint=\(self.endpoint.debugSummary) mode=stream requested=\(length) received=\(content?.count ?? 0) complete=\(isComplete) context=\(contentContext?.debugSummary ?? "nil") error=\(String(describing: error))")
            if let error{
                self.delegate?.socket(self, didReceive: error)
                return
            }
            guard let data = content,data.count == length else{
                let code:MQTTError.Decode = isComplete ? .streamIsComplete : .unexpectedDataLength
                self.delegate?.socket(self, didReceive: MQTTError.decodeError(code))
                return
            }
            // if we get correct data,we dispatch it! even if the stream has already been completed.
            finish?(data)
        })
    }
    private func readMessage(){
        guard let conn else { return }
        Logger.debug("SOCKET READ: endpoint=\(endpoint.debugSummary) mode=message requested=next")
        conn.receiveMessage {[weak self] content, contentContext, isComplete, error in
            guard let self else{ return }
            Logger.debug("SOCKET READ: endpoint=\(self.endpoint.debugSummary) mode=message received=\(content?.count ?? 0) complete=\(isComplete) context=\(contentContext?.debugSummary ?? "nil") error=\(String(describing: error))")
            if let error{
                self.delegate?.socket(self, didReceive: error)
                return
            }
            guard let data = content else{
                self.delegate?.socket(self, didReceive: MQTTError.decodeError(.unexpectedDataLength))
                return
            }
            do {
                var buffer = DataBuffer(data: data)
                let incoming = try IncomingPacket.read(from: &buffer)
                let message = try incoming.packet(with: self.config.version)
                self.delegate?.socket(self, didReceive: message)
            }catch{
                self.delegate?.socket(self, didReceive: error)
            }
            self.readMessage()
        }
    }
    private func reset(){
        header = 0
        length = 0
        multiply = 1
    }
    private var isws:Bool{
        switch endpoint.type{
        case.ws,.wss:
            return true
        default:
            return false
        }
    }
    private var isQUIC:Bool {
        if case .quic = endpoint.type {
            return true
        }
        return false
    }
}

private extension Endpoint{
    var debugSummary:String{
        switch nw {
        case .hostPort(let host, let port):
            return "\(type.debugSummary)://\(host):\(port)"
        case .url(let url):
            return "\(type.debugSummary)://\(url.absoluteString)"
        default:
            return "\(type.debugSummary)://\(nw)"
        }
    }
}

private extension Prototype{
    var debugSummary:String{
        switch self {
        case .ws:
            return "ws"
        case .tcp:
            return "tcp"
        case .tls:
            return "tls"
        case .wss:
            return "wss"
        case .quic:
            return "quic"
        }
    }
}

private extension NWConnection.State{
    var debugSummary:String{
        switch self {
        case .setup:
            return "setup"
        case .waiting(let error):
            return "waiting(error=\(error))"
        case .preparing:
            return "preparing"
        case .ready:
            return "ready"
        case .failed(let error):
            return "failed(error=\(error))"
        case .cancelled:
            return "cancelled"
        @unknown default:
            return "unknown"
        }
    }
}

@available(macOS 11.0, iOS 14.0, watchOS 7.0, tvOS 14.0, *)
private extension NWConnectionGroup.State {
    var debugSummary:String {
        switch self {
        case .setup:
            return "setup"
        case .waiting(let error):
            return "waiting(error=\(error))"
        case .ready:
            return "ready"
        case .failed(let error):
            return "failed(error=\(error))"
        case .cancelled:
            return "cancelled"
        @unknown default:
            return "unknown"
        }
    }
}

private extension NWConnection.ContentContext{
    var debugSummary:String{
        let metadataNames = protocolMetadata.map { metadata in
            String(describing: type(of: metadata))
        }.joined(separator: ",")
        return "id=\(identifier) final=\(isFinal) metadata=[\(metadataNames)]"
    }
}

private extension NWParameters{
    var debugSummary:String{
        let transport = defaultProtocolStack.transportProtocol.map { String(describing: type(of: $0)) } ?? "nil"
        let internet = defaultProtocolStack.internetProtocol.map { String(describing: type(of: $0)) } ?? "nil"
        let applications = defaultProtocolStack.applicationProtocols.map { String(describing: type(of: $0)) }.joined(separator: ",")
        return "transport=\(transport) internet=\(internet) app=[\(applications)] fastOpen=\(allowFastOpen) includePeerToPeer=\(includePeerToPeer)"
    }
}

private extension NWEndpoint {
    var debugDescription:String {
        switch self {
        case .hostPort(let host, let port):
            return "\(host):\(port)"
        case .opaque(let opaque):
            return "opaque:\(opaque)"
        case .service(let name, let type, let domain, let interface):
            return "service(name=\(name),type=\(type),domain=\(domain),interface=\(String(describing: interface)))"
        case .unix(let path):
            return "unix:\(path)"
        case .url(let url):
            return url.absoluteString
        @unknown default:
            return String(describing: self)
        }
    }
}

private extension Optional where Wrapped: AnyObject {
    var debugID:String {
        switch self {
        case .none:
            return "nil"
        case .some(let value):
            return String(UInt(bitPattern: ObjectIdentifier(value)), radix: 16)
        }
    }
}

private extension NWConnection {
    var debugID:String {
        String(UInt(bitPattern: ObjectIdentifier(self)), radix: 16)
    }
}

@available(macOS 11.0, iOS 14.0, watchOS 7.0, tvOS 14.0, *)
private extension NWConnectionGroup {
    var debugID:String {
        String(UInt(bitPattern: ObjectIdentifier(self)), radix: 16)
    }
}
extension NWConnection.ContentContext{
    static func mqtt(_ isws:Bool)->NWConnection.ContentContext{
        if isws{
            return .wsMQTTContext
        }
        return .mqttContext
    }
    private static var mqttContext:NWConnection.ContentContext = {
        return .init(identifier: "swift-mqtt")
    }()
    private static var wsMQTTContext:NWConnection.ContentContext = {
        return .init(identifier: "swift-mqtt",metadata: [NWProtocolWebSocket.Metadata(opcode: .binary)])
    }()
}
