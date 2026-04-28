#!/usr/bin/env python3
"""Generate BifroMQ Architecture SVG"""
import xml.etree.ElementTree as ET

def escape_xml(text):
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")

def create_svg():
    W, H = 1400, 1100
    ns = "http://www.w3.org/2000/svg"
    svg = ET.Element("svg", {
        "xmlns": ns,
        "width": str(W),
        "height": str(H),
        "viewBox": f"0 0 {W} {H}"
    })

    defs = ET.SubElement(svg, "defs")
    # arrow marker
    marker = ET.SubElement(defs, "marker", {
        "id": "arrow", "markerWidth": "10", "markerHeight": "7",
        "refX": "9", "refY": "3.5", "orient": "auto"
    })
    ET.SubElement(marker, "polygon", {"points": "0 0, 10 3.5, 0 7", "fill": "#64748B"})

    marker2 = ET.SubElement(defs, "marker", {
        "id": "arrow-dashed", "markerWidth": "10", "markerHeight": "7",
        "refX": "9", "refY": "3.5", "orient": "auto"
    })
    ET.SubElement(marker2, "polygon", {"points": "0 0, 10 3.5, 0 7", "fill": "#94A3B8"})

    marker3 = ET.SubElement(defs, "marker", {
        "id": "arrow-bidi", "markerWidth": "10", "markerHeight": "7",
        "refX": "5", "refY": "3.5", "orient": "auto"
    })
    ET.SubElement(marker3, "polygon", {"points": "0 0, 10 3.5, 0 7", "fill": "#64748B"})

    # Background
    rect = ET.SubElement(svg, "rect", {"width": str(W), "height": str(H), "fill": "#F8FAFC"})

    # Title
    title = ET.SubElement(svg, "text", {
        "x": str(W // 2), "y": "30", "text-anchor": "middle",
        "font-family": "system-ui, sans-serif", "font-size": "22", "font-weight": "bold", "fill": "#1E293B"
    })
    title.text = "Apache BifroMQ Architecture"

    subtitle = ET.SubElement(svg, "text", {
        "x": str(W // 2), "y": "52", "text-anchor": "middle",
        "font-family": "system-ui, sans-serif", "font-size": "12", "fill": "#64748B"
    })
    subtitle.text = "Distributed MQTT Broker with Native Multi-Tenancy | JDK 17+ | Netty | gRPC | RocksDB | Custom Raft | QUIC"

    # Layer definitions: (y, height, label, color)
    layers = [
        (70, 90,  "External Clients",          "#FEF3C7"),   # 0
        (170, 80, "Assembly Layer (Guice DI)", "#E0E7FF"),   # 1
        (260, 110, "API & Plugin Layer (PF4J)","#DBEAFE"),   # 2
        (380, 110, "Protocol Layer (MQTT)",    "#D1FAE5"),   # 3
        (500, 130, "Business Services Layer",   "#CFFAFE"),   # 4
        (640, 130, "Storage Layer (base-kv)",  "#F3E8FF"),   # 5
        (780, 110, "Cluster & RPC Layer",       "#FCE7F3"),   # 6
        (900, 110, "Foundation Layer",            "#F3F4F6"),   # 7
    ]

    # Draw layer backgrounds and labels
    for y, h, label, color in layers:
        r = ET.SubElement(svg, "rect", {
            "x": "40", "y": str(y), "width": str(W - 80), "height": str(h),
            "rx": "8", "ry": "8", "fill": color, "stroke": "#CBD5E1", "stroke-width": "1"
        })
        t = ET.SubElement(svg, "text", {
            "x": "55", "y": str(y + 18), "font-family": "system-ui, sans-serif",
            "font-size": "11", "font-weight": "bold", "fill": "#475569"
        })
        t.text = label

    # Helper for boxes
    boxes = []
    def box(x, y, w, h, label, fill, stroke="#94A3B8", fontsize="10"):
        r = ET.SubElement(svg, "rect", {
            "x": str(x), "y": str(y), "width": str(w), "height": str(h),
            "rx": "6", "ry": "6", "fill": fill, "stroke": stroke, "stroke-width": "1"
        })
        # multi-line text support
        lines = label.split("\\n")
        start_y = y + h // 2 - (len(lines) - 1) * 7
        for i, line in enumerate(lines):
            t = ET.SubElement(svg, "text", {
                "x": str(x + w // 2), "y": str(start_y + i * 14),
                "text-anchor": "middle", "font-family": "system-ui, sans-serif",
                "font-size": fontsize, "fill": "#1E293B"
            })
            t.text = line
        boxes.append((x + w // 2, y + h // 2, w, h))
        return (x + w // 2, y + h // 2)

    # External Clients row 0
    bc = 70 + 90 // 2
    b1 = box(70,  bc - 25, 120, 50, "MQTT 3.1/3.1.1\nTCP/TLS", "#FEF3C7")
    b2 = box(220, bc - 25, 120, 50, "MQTT 5.0\nTCP/TLS", "#FEF3C7")
    b3 = box(370, bc - 25, 140, 50, "MQTT over WS/WSS", "#FEF3C7")
    b4 = box(540, bc - 25, 140, 50, "MQTT over QUIC", "#FEF3C7")
    b5 = box(710, bc - 25, 150, 50, "HTTP/gRPC Admin", "#FEF3C7")

    # Assembly row 1
    bc = 170 + 80 // 2
    st = box(70, bc - 25, 220, 50, "StandaloneStarter\nGuice DI Container", "#E0E7FF", "#6366F1")
    pd = box(320, bc - 25, 140, 50, "build-plugin-demo", "#E0E7FF")

    # API & Plugin row 2
    bc = 260 + 110 // 2
    api = box(70, bc - 30, 180, 60, "bifromq-apiserver\nHTTP / gRPC Admin API", "#DBEAFE", "#3B82F6")
    pm = box(280, bc - 30, 150, 60, "plugin-manager\nPF4J Lifecycle", "#DBEAFE")
    pa = box(460, bc - 20, 120, 40, "auth-provider", "#DBEAFE")
    pb = box(600, bc - 20, 120, 40, "client-balancer", "#DBEAFE")
    pe = box(740, bc - 20, 120, 40, "event-collector", "#DBEAFE")
    pt = box(880, bc - 20, 130, 40, "resource-throttler", "#DBEAFE")
    ps = box(1030, bc - 20, 120, 40, "setting-provider", "#DBEAFE")
    psb = box(1170, bc - 20, 100, 40, "sub-broker", "#DBEAFE")

    # Protocol row 3
    bc = 380 + 110 // 2
    ms = box(70, bc - 35, 280, 70, "bifromq-mqtt-server\nNetty Handlers: v3 | v5 | WS | QUIC", "#D1FAE5", "#10B981", "11")
    mbc = box(380, bc - 20, 140, 40, "mqtt-broker-client", "#D1FAE5")
    mbr = box(550, bc - 20, 160, 40, "broker-rpc-definition", "#D1FAE5")
    msp = box(740, bc - 20, 120, 40, "mqtt-server-spi", "#D1FAE5")

    # Business row 4
    bc = 500 + 130 // 2
    dist = box(70, bc - 45, 260, 90, "bifromq-dist\nTopic Matching · Subscription Routing\nclient · rpc · coproc · worker · server", "#CFFAFE", "#06B6D4", "10")
    inbox = box(360, bc - 45, 250, 90, "bifromq-inbox\nPer-Tenant/Client Message Queueing\nclient · rpc · coproc · store · server", "#CFFAFE", "#06B6D4", "10")
    retain = box(640, bc - 45, 250, 90, "bifromq-retain\nRetained Message Storage\nclient · rpc · coproc · store · gc · server", "#CFFAFE", "#06B6D4", "10")
    sd = box(920, bc - 35, 180, 70, "bifromq-session-dict\nMQTT Session Registry\nclient · rpc · server", "#CFFAFE", "#06B6D4", "10")
    deli = box(1130, bc - 25, 140, 50, "bifromq-deliverer\nBatching & Pipelining", "#CFFAFE", "#06B6D4")

    # Storage row 5
    bc = 640 + 130 // 2
    kv_srv = box(70, bc - 45, 200, 90, "base-kv-store-server\nCoprocessor Host", "#F3E8FF", "#8B5CF6", "10")
    kv_cli = box(300, bc - 20, 130, 40, "kv-store-client", "#F3E8FF")
    kv_raft = box(460, bc - 35, 160, 70, "base-kv-raft\nCustom Raft Consensus", "#F3E8FF", "#8B5CF6", "10")
    kv_rdb = box(650, bc - 35, 200, 70, "kv-local-engine-rocksdb\nGroupCommitWriteQueue", "#F3E8FF", "#8B5CF6", "10")
    kv_mem = box(880, bc - 20, 140, 40, "kv-local-engine-memory", "#F3E8FF")
    kv_coproc = box(1050, bc - 20, 140, 40, "kv-store-coproc-api", "#F3E8FF")
    kv_meta = box(1210, bc - 20, 100, 40, "kv-meta-service", "#F3E8FF")

    # Cluster row 6
    bc = 780 + 110 // 2
    cl = box(70, bc - 35, 200, 70, "base-cluster\nMembership · Failure Detection\nUDP/TCP Transport", "#FCE7F3", "#EC4899", "10")
    rpc_s = box(300, bc - 20, 110, 40, "rpc-server", "#FCE7F3")
    rpc_c = box(440, bc - 20, 110, 40, "rpc-client", "#FCE7F3")
    rpc_i = box(580, bc - 20, 110, 40, "grpc-inproc", "#FCE7F3")
    rpc_t = box(720, bc - 20, 130, 40, "traffic-governor", "#FCE7F3")
    crdt = box(880, bc - 35, 160, 70, "base-crdt\nORMap · CCounter · Gossip", "#FCE7F3", "#EC4899", "10")
    sched = box(1070, bc - 20, 120, 40, "base-scheduler", "#FCE7F3")

    # Foundation row 7
    bc = 900 + 110 // 2
    env = box(70, bc - 20, 160, 40, "base-env / util / logger", "#F3F4F6")
    hlc = box(260, bc - 20, 140, 40, "base-hlc (HLC)", "#F3F4F6")
    hl = box(430, bc - 20, 120, 40, "base-hookloader", "#F3F4F6")
    ct = box(580, bc - 20, 140, 40, "bifromq-common-type", "#F3F4F6")
    met = box(750, bc - 20, 120, 40, "bifromq-metrics", "#F3F4F6")
    sp = box(900, bc - 20, 120, 40, "bifromq-sysprops", "#F3F4F6")
    nb = box(1050, bc - 25, 180, 50, "bifromq-native-binding\nRust / JNI", "#F3F4F6")

    def line(x1, y1, x2, y2, dashed="false", label="", color="#64748B"):
        style = f"stroke:{color};stroke-width:1.5;fill:none"
        if dashed == "true":
            style += ";stroke-dasharray:5,5"
        el = ET.SubElement(svg, "line", {
            "x1": str(x1), "y1": str(y1), "x2": str(x2), "y2": str(y2),
            "style": style,
            "marker-end": "url(#arrow)" if y2 > y1 or x2 > x1 else "none"
        })
        if label:
            mid_x = (x1 + x2) // 2
            mid_y = (y1 + y2) // 2
            # background for label
            lb = ET.SubElement(svg, "rect", {
                "x": str(mid_x - 20), "y": str(mid_y - 8),
                "width": "40", "height": "16", "rx": "3",
                "fill": "#F8FAFC", "stroke": "none"
            })
            lt = ET.SubElement(svg, "text", {
                "x": str(mid_x), "y": str(mid_y + 4),
                "text-anchor": "middle", "font-family": "system-ui, sans-serif",
                "font-size": "8", "fill": "#64748B"
            })
            lt.text = label

    # Draw key connections (simplified to avoid total spaghetti)
    # External -> Protocol
    for bx in [b1, b2, b3, b4]:
        line(bx[0], bx[1] + 25, bx[0], ms[1] - 35)
    line(b5[0], b5[1] + 25, api[0], api[1] - 30)

    # Protocol -> Business
    line(ms[0], ms[1] + 35, dist[0], dist[1] - 45, label="Pub/Sub")
    line(ms[0], ms[1] + 35, inbox[0], inbox[1] - 45, label="Queue")
    line(ms[0], ms[1] + 35, sd[0], sd[1] - 35, label="Session")
    line(ms[0], ms[1] + 35, retain[0], retain[1] - 45, label="Retain")
    line(deli[0], deli[1] + 25, ms[0], ms[1] + 35, label="Deliver")

    # Business internal
    line(dist[0], dist[1] + 45, deli[0], deli[1] - 25, label="Route")
    line(dist[0], dist[1] + 45, inbox[0], inbox[1] + 45)
    line(dist[0], dist[1] + 45, retain[0], retain[1] + 45)
    line(inbox[0], inbox[1] + 45, sd[0], sd[1] + 35, label="LWT")

    # Business -> Storage (CoProc dashed)
    for bx in [dist, inbox, retain]:
        line(bx[0], bx[1] + 45, kv_srv[0], kv_srv[1] - 45, dashed="true", label="CoProc")
    line(sd[0], sd[1] + 35, kv_srv[0], kv_srv[1] - 45)

    # Storage internal
    line(kv_srv[0], kv_srv[1] + 45, kv_raft[0], kv_raft[1] - 35, label="Consensus")
    line(kv_raft[0], kv_raft[1] + 35, kv_rdb[0], kv_rdb[1] - 35, label="WAL")
    line(kv_raft[0], kv_raft[1] + 35, kv_mem[0], kv_mem[1] - 20, dashed="true")

    # Cluster internal
    line(cl[0], cl[1] + 35, rpc_s[0], rpc_s[1] - 20)
    line(rpc_c[0], rpc_c[1] - 20, rpc_s[0], rpc_s[1] - 20, label="gRPC")
    line(crdt[0], crdt[1] + 35, cl[0], cl[1] - 35, label="Gossip")

    # Storage -> Cluster
    line(kv_srv[0], kv_srv[1] + 45, rpc_s[0], rpc_s[1] + 20)

    # Business -> Cluster
    for bx in [dist, inbox, retain, sd, deli, api]:
        line(bx[0], bx[1] + 45, rpc_c[0], rpc_c[1] - 20)

    # Plugin
    line(pm[0], pm[1] + 30, pa[0], pa[1] - 20, dashed="true")
    line(ms[0], ms[1] + 35, pa[0], pa[1] - 20, label="Auth")
    line(ms[0], ms[1] + 35, pb[0], pb[1] - 20, label="Balance")
    line(ms[0], ms[1] + 35, pt[0], pt[1] - 20, label="Throttle")
    line(dist[0], dist[1] + 45, psb[0], psb[1] - 20, label="Delegate")
    line(pe[0], pe[1] + 20, api[0], api[1] + 30, label="Events")

    # Assembly to core services
    for target in [api, ms, dist, inbox, retain, sd, cl, pm]:
        line(st[0], st[1] + 25, target[0], target[1] - 30, dashed="true", label="Guice", color="#6366F1")

    # Foundation to upper
    line(ct[0], ct[1] - 20, cl[0], cl[1] - 35)
    line(hlc[0], hlc[1] - 20, kv_raft[0], kv_raft[1] - 35)
    line(env[0], env[1] - 20, cl[0], cl[1] - 35)
    line(nb[0], nb[1] - 25, dist[0], dist[1] + 45, label="TopicMatch")

    # Legend
    lx, ly = 1000, 1030
    ET.SubElement(svg, "text", {
        "x": str(lx), "y": str(ly), "font-family": "system-ui, sans-serif",
        "font-size": "11", "font-weight": "bold", "fill": "#475569"
    }).text = "Legend:"
    ET.SubElement(svg, "line", {
        "x1": str(lx + 50), "y1": str(ly - 3), "x2": str(lx + 100), "y2": str(ly - 3),
        "style": "stroke:#64748B;stroke-width:1.5", "marker-end": "url(#arrow)"
    })
    ET.SubElement(svg, "text", {
        "x": str(lx + 110), "y": str(ly), "font-family": "system-ui, sans-serif",
        "font-size": "10", "fill": "#64748B"
    }).text = "Dependency / RPC"

    ET.SubElement(svg, "line", {
        "x1": str(lx + 220), "y1": str(ly - 3), "x2": str(lx + 270), "y2": str(ly - 3),
        "style": "stroke:#94A3B8;stroke-width:1.5;stroke-dasharray:5,5", "marker-end": "url(#arrow-dashed)"
    })
    ET.SubElement(svg, "text", {
        "x": str(lx + 280), "y": str(ly), "font-family": "system-ui, sans-serif",
        "font-size": "10", "fill": "#64748B"
    }).text = "CoProc / Optional"

    return svg

svg = create_svg()
tree = ET.ElementTree(svg)
tree.write("/Users/ugreen/IdeaProjects/bifromq/bifromq-architecture.svg", encoding="utf-8", xml_declaration=True)
print("SVG generated at: /Users/ugreen/IdeaProjects/bifromq/bifromq-architecture.svg")
