// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "MQTTTest",
    platforms: [
        .macOS(.v13)
    ],
    dependencies: [
        .package(path: "Vendor/swift-mqtt"),
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
