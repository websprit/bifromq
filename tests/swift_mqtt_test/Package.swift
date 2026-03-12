// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "MQTTTest",
    platforms: [
        .macOS(.v13)
    ],
    dependencies: [
        .package(url: "https://github.com/emqx/swift-mqtt.git", from: "1.5.0"),
    ],
    targets: [
        .executableTarget(
            name: "MQTTTest",
            dependencies: [
                .product(name: "MQTT", package: "swift-mqtt"),
            ]
        ),
    ]
)
