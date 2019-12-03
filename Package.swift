// swift-tools-version:5.0
import PackageDescription

let package = Package(
    name: "swift-tds",
    products: [
        .library(name: "TDS", targets: ["TDS"]),
        .executable(name: "TDSVersionCheck", targets: ["TDSVersionCheck"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.0.0"),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", from: "2.0.0"),
        .package(url: "https://github.com/apple/swift-metrics.git", from: "1.0.0-a"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.0.0"),
    ],
    targets: [
        .target(name: "TDS", dependencies: ["Logging", "Metrics", "NIO", "NIOSSL"]),
        .target(name: "TDSVersionCheck", dependencies: ["TDS"]),
        .testTarget(name: "TDSTests", dependencies: ["TDS"]),
    ]
)
