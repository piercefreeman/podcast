import SwiftUI
import ScreenCaptureKit
import Combine

struct WindowInfo: Identifiable {
    let id: UInt32
    let window: SCWindow
    let title: String
    let appName: String
    var thumbnail: CGImage?
}

@MainActor
class ScreenCaptureManager: NSObject, ObservableObject {
    @Published var capturedFrame: CGImage?

    private var stream: SCStream?
    private var streamOutput: StreamOutput?

    func fetchAvailableWindowsWithThumbnails() async -> [WindowInfo] {
        do {
            let content = try await SCShareableContent.excludingDesktopWindows(
                false,
                onScreenWindowsOnly: true
            )

            // Filter out windows that are too small or system windows
            let windows = content.windows.filter { window in
                guard let app = window.owningApplication else { return false }
                guard window.frame.width > 100 && window.frame.height > 100 else { return false }

                // Exclude this app from the list
                return app.bundleIdentifier != Bundle.main.bundleIdentifier
            }

            // Create window info with thumbnails
            var windowInfos: [WindowInfo] = []
            for window in windows {
                let appName = window.owningApplication?.applicationName ?? "Unknown"
                let title = window.title ?? ""
                let thumbnail = await captureThumbnail(for: window)

                windowInfos.append(WindowInfo(
                    id: window.windowID,
                    window: window,
                    title: title,
                    appName: appName,
                    thumbnail: thumbnail
                ))
            }

            return windowInfos
        } catch {
            print("Error fetching windows: \(error)")
            return []
        }
    }

    private func captureThumbnail(for window: SCWindow) async -> CGImage? {
        do {
            let filter = SCContentFilter(desktopIndependentWindow: window)

            let config = SCStreamConfiguration()
            config.width = 400
            config.height = 300
            config.pixelFormat = kCVPixelFormatType_32BGRA
            config.showsCursor = false

            let image = try await SCScreenshotManager.captureImage(
                contentFilter: filter,
                configuration: config
            )

            return image
        } catch {
            print("Error capturing thumbnail: \(error)")
            return nil
        }
    }

    func startCapture(for window: SCWindow) async {
        do {
            let filter = SCContentFilter(desktopIndependentWindow: window)

            let config = SCStreamConfiguration()
            config.width = Int(window.frame.width) * 2
            config.height = Int(window.frame.height) * 2
            config.minimumFrameInterval = CMTime(value: 1, timescale: 60)
            config.queueDepth = 5
            config.pixelFormat = kCVPixelFormatType_32BGRA
            config.showsCursor = false

            streamOutput = StreamOutput(captureManager: self)
            stream = SCStream(filter: filter, configuration: config, delegate: nil)

            try stream?.addStreamOutput(
                streamOutput!,
                type: .screen,
                sampleHandlerQueue: .global(qos: .userInteractive)
            )

            try await stream?.startCapture()
        } catch {
            print("Error starting capture: \(error)")
        }
    }

    func stopCapture() {
        Task {
            do {
                try await stream?.stopCapture()
                stream = nil
                streamOutput = nil
                capturedFrame = nil
            } catch {
                print("Error stopping capture: \(error)")
            }
        }
    }

    func updateFrame(_ frame: CGImage) {
        capturedFrame = frame
    }
}

class StreamOutput: NSObject, SCStreamOutput {
    weak var captureManager: ScreenCaptureManager?

    init(captureManager: ScreenCaptureManager) {
        self.captureManager = captureManager
    }

    func stream(_ stream: SCStream, didOutputSampleBuffer sampleBuffer: CMSampleBuffer, of type: SCStreamOutputType) {
        guard type == .screen,
              let imageBuffer = sampleBuffer.imageBuffer else {
            return
        }

        let ciImage = CIImage(cvPixelBuffer: imageBuffer)
        let context = CIContext()

        guard let cgImage = context.createCGImage(ciImage, from: ciImage.extent) else {
            return
        }

        Task { @MainActor in
            captureManager?.updateFrame(cgImage)
        }
    }
}
