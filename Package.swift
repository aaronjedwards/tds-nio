// swift-tools-version: 6.1
// Copyright 2026 TDSNIO project authors
// SPDX-License-Identifier: Apache-2.0

import PackageDescription

let swiftSettings: [SwiftSetting] = [
    .enableUpcomingFeature("MemberImportVisibility"),
    .enableUpcomingFeature("InternalImportsByDefault"),
]

let package = Package(
    name: "tds-nio",
    platforms: [.macOS(.v13), .iOS(.v16), .tvOS(.v16), .watchOS(.v9), .visionOS(.v1)],
    products: [
        .library(
            name: "TDSNIO",
            targets: ["TDSNIO"]
        )
    ],
    traits: [
        .trait(name: "_IOTracing"),
        .trait(name: "DistributedTracingSupport"),
        .default(enabledTraits: ["DistributedTracingSupport"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-log.git", from: "1.5.4"),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.81.0"),
        .package(url: "https://github.com/apple/swift-nio-transport-services.git", from: "1.23.0"),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", from: "2.29.0"),
        .package(url: "https://github.com/apple/swift-crypto.git", "3.9.0"..<"5.0.0"),
        .package(url: "https://github.com/apple/swift-distributed-tracing.git", from: "1.3.0"),
        .package(url: "https://github.com/swift-server/swift-service-lifecycle.git", from: "2.6.0"),
        .package(url: "https://github.com/vapor/postgres-nio.git", from: "1.21.4"),
    ],
    targets: [
        .target(
            name: "TDSNIO",
            dependencies: [
                .product(name: "Logging", package: "swift-log"),
                .product(name: "NIO", package: "swift-nio"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOTransportServices", package: "swift-nio-transport-services"),
                .product(name: "NIOSSL", package: "swift-nio-ssl"),
                .product(name: "NIOTLS", package: "swift-nio"),
                .product(name: "Crypto", package: "swift-crypto"),
                .product(name: "_CryptoExtras", package: "swift-crypto"),
                .product(
                    name: "Tracing",
                    package: "swift-distributed-tracing",
                    condition: .when(traits: ["DistributedTracingSupport"])
                ),
                .product(name: "ServiceLifecycle", package: "swift-service-lifecycle"),
                .product(name: "_ConnectionPoolModule", package: "postgres-nio"),
            ],
            swiftSettings: swiftSettings
        ),
        .testTarget(
            name: "TDSNIOTests",
            dependencies: [
                "TDSNIO",
                .product(
                    name: "InMemoryTracing",
                    package: "swift-distributed-tracing",
                    condition: .when(traits: ["DistributedTracingSupport"])
                ),
                .product(name: "NIOTestUtils", package: "swift-nio"),
            ]
        ),
        .testTarget(
            name: "IntegrationTests",
            dependencies: [
                "TDSNIO",
                .product(name: "Logging", package: "swift-log"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOSSL", package: "swift-nio-ssl"),
            ]
        ),
    ]
)
