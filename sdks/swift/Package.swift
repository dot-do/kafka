// swift-tools-version:5.9
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "KafkaDo",
    platforms: [
        .macOS(.v14),
        .iOS(.v17),
        .tvOS(.v17),
        .watchOS(.v10)
    ],
    products: [
        .library(
            name: "KafkaDo",
            targets: ["KafkaDo"]
        )
    ],
    dependencies: [],
    targets: [
        .target(
            name: "KafkaDo",
            dependencies: [],
            swiftSettings: [
                .enableExperimentalFeature("StrictConcurrency")
            ]
        ),
        .testTarget(
            name: "KafkaDoTests",
            dependencies: ["KafkaDo"]
        )
    ]
)
