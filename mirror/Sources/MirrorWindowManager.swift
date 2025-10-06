import SwiftUI
import AppKit

@MainActor
class MirrorWindowManager: ObservableObject {
    private var mirrorWindow: NSWindow?

    func openMirrorWindow(captureManager: ScreenCaptureManager) {
        // Close existing window if any
        closeMirrorWindow()

        // Create the mirror view
        let mirrorView = MirrorWindowView(captureManager: captureManager)
        let hostingController = NSHostingController(rootView: mirrorView)

        // Create new window
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 800, height: 600),
            styleMask: [.titled, .closable, .resizable, .miniaturizable],
            backing: .buffered,
            defer: false
        )

        window.title = "Mirror"
        window.contentViewController = hostingController
        window.center()
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
}
