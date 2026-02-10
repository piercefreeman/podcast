import SwiftUI
import AppKit

@MainActor
class MirrorWindowManager: ObservableObject {
    private var mirrorWindow: NSWindow?

    func openMirrorWindow(
        captureManager: ScreenCaptureManager,
        displayID: UInt32?,
        horizontalFlip: Binding<Bool>,
        verticalFlip: Binding<Bool>
    ) {
        // Close existing window if any
        closeMirrorWindow()

        // Create the mirror view
        let mirrorView = MirrorWindowView(
            captureManager: captureManager,
            horizontalFlip: horizontalFlip,
            verticalFlip: verticalFlip,
            showsControls: false
        )
        let hostingController = NSHostingController(rootView: mirrorView)

        // Create new window
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 800, height: 600),
            styleMask: [.borderless],
            backing: .buffered,
            defer: false
        )

        window.title = "Mirror"
        window.contentViewController = hostingController
        window.backgroundColor = .black
        window.hasShadow = false
        window.isMovable = false
        positionWindow(window, on: displayID)
        window.makeKeyAndOrderFront(nil)
        window.level = .floating

        // Store reference
        mirrorWindow = window

        // Handle window closing
        NotificationCenter.default.addObserver(
            forName: NSWindow.willCloseNotification,
            object: window,
            queue: .main
        ) { [weak self] _ in
            Task { @MainActor in
                captureManager.stopCapture()
                self?.mirrorWindow = nil
            }
        }
    }

    func closeMirrorWindow() {
        mirrorWindow?.close()
        mirrorWindow = nil
    }

    private func positionWindow(_ window: NSWindow, on displayID: UInt32?) {
        guard let screen = NSScreen.screen(for: displayID) else {
            window.center()
            return
        }

        window.setFrame(screen.frame, display: true)
    }
}

extension NSScreen {
    var displayID: UInt32? {
        (deviceDescription[NSDeviceDescriptionKey("NSScreenNumber")] as? NSNumber)?.uint32Value
    }

    static func screen(for displayID: UInt32?) -> NSScreen? {
        guard let displayID else { return NSScreen.main }
        return NSScreen.screens.first { $0.displayID == displayID } ?? NSScreen.main
    }
}
