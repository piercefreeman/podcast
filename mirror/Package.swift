// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "MirrorApp",
    platforms: [
        .macOS(.v14)
    ],
    products: [
        .executable(
            name: "MirrorApp",
            targets: ["MirrorApp"])
    ],
    targets: [
        .executableTarget(
            name: "MirrorApp",
            dependencies: [],
            path: "Sources",
            exclude: ["Info.plist"],
            swiftSettings: [
                .unsafeFlags(["-parse-as-library"])
            ])
    ]
)
