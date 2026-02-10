import SwiftUI
import ScreenCaptureKit
import AppKit

struct DisplayInfo: Identifiable {
    let id: UInt32
    let name: String
    let isMain: Bool
}

struct ContentView: View {
    @StateObject private var captureManager = ScreenCaptureManager()
    @StateObject private var windowManager = MirrorWindowManager()
    @State private var availableWindows: [WindowInfo] = []
    @State private var availableDisplays: [DisplayInfo] = []
    @State private var selectedWindowID: UInt32?
    @State private var selectedDisplayID: UInt32?
    @State private var horizontalFlip = true
    @State private var verticalFlip = false
    @State private var isLoading = true
    @State private var isMirroring = false

    let columns = [
        GridItem(.adaptive(minimum: 200, maximum: 250), spacing: 16)
    ]

    var body: some View {
        VStack(spacing: 0) {
            // Header
            VStack(spacing: 8) {
                Text("Select Window to Mirror")
                    .font(.system(size: 24, weight: .semibold))
                Text("Choose a window to display on your teleprompter")
                    .font(.system(size: 13))
                    .foregroundColor(.secondary)
            }
            .padding(.top, 24)
            .padding(.bottom, 20)

            if !availableDisplays.isEmpty {
                VStack(spacing: 12) {
                    HStack {
                        Text("Mirror display")
                            .font(.system(size: 13, weight: .medium))
                            .foregroundColor(.secondary)

                        Spacer()

                        Picker("Mirror display", selection: $selectedDisplayID) {
                            ForEach(availableDisplays) { display in
                                Text(display.isMain ? "\(display.name) (Main)" : display.name)
                                    .tag(Optional(display.id))
                            }
                        }
                        .pickerStyle(.menu)
                        .frame(maxWidth: 260, alignment: .trailing)
                    }

                    HStack(spacing: 10) {
                        Text("Mirror controls")
                            .font(.system(size: 13, weight: .medium))
                            .foregroundColor(.secondary)

                        Spacer()

                        FlipButton(
                            icon: "arrow.left.and.right",
                            label: "Horizontal",
                            isActive: horizontalFlip,
                            action: { horizontalFlip.toggle() }
                        )

                        FlipButton(
                            icon: "arrow.up.and.down",
                            label: "Vertical",
                            isActive: verticalFlip,
                            action: { verticalFlip.toggle() }
                        )
                    }
                }
                .padding(.horizontal, 20)
                .padding(.bottom, 12)
            }

            Divider()

            if isLoading {
                Spacer()
                VStack(spacing: 16) {
                    ProgressView()
                        .scaleEffect(1.2)
                    Text("Scanning windows...")
                        .foregroundColor(.secondary)
                }
                Spacer()
            } else if availableWindows.isEmpty {
                Spacer()
                VStack(spacing: 12) {
                    Image(systemName: "macwindow.badge.plus")
                        .font(.system(size: 48))
                        .foregroundColor(.secondary)
                    Text("No windows available")
                        .font(.headline)
                    Text("Open some windows and try again")
                        .font(.subheadline)
                        .foregroundColor(.secondary)
                }
                Spacer()
            } else {
                ScrollView {
                    LazyVGrid(columns: columns, spacing: 16) {
                        ForEach(availableWindows) { windowInfo in
                            WindowPreviewCard(
                                windowInfo: windowInfo,
                                isSelected: selectedWindowID == windowInfo.id,
                                onSelect: {
                                    selectedWindowID = windowInfo.id
                                }
                            )
                        }
                    }
                    .padding(20)
                }
            }

            // Bottom action bar
            if !isLoading && !availableWindows.isEmpty {
                Divider()

                HStack(spacing: 12) {
                    Button(action: {
                        Task {
                            isLoading = true
                            availableWindows = await captureManager.fetchAvailableWindowsWithThumbnails()
                            isLoading = false
                        }
                    }, label: {
                        HStack(spacing: 6) {
                            Image(systemName: "arrow.clockwise")
                            Text("Refresh")
                        }
                        .frame(maxWidth: .infinity)
                        .padding(.vertical, 12)
                        .background(Color.secondary.opacity(0.1))
                        .foregroundColor(.primary)
                        .cornerRadius(8)
                    })
                    .buttonStyle(.plain)

                    if isMirroring {
                        Button(action: {
                            captureManager.stopCapture()
                            windowManager.closeMirrorWindow()
                            isMirroring = false
                            selectedWindowID = nil
                        }, label: {
                            HStack(spacing: 6) {
                                Image(systemName: "stop.fill")
                                Text("Stop Mirror")
                            }
                            .frame(maxWidth: .infinity)
                            .padding(.vertical, 12)
                            .background(Color.red)
                            .foregroundColor(.white)
                            .cornerRadius(8)
                        })
                        .buttonStyle(.plain)
                    } else {
                        Button(action: {
                            if let windowInfo = availableWindows.first(where: { $0.id == selectedWindowID }) {
                                Task {
                                    await captureManager.startCapture(for: windowInfo.window)
                                    windowManager.openMirrorWindow(
                                        captureManager: captureManager,
                                        displayID: selectedDisplayID,
                                        horizontalFlip: $horizontalFlip,
                                        verticalFlip: $verticalFlip
                                    )
                                    isMirroring = true
                                }
                            }
                        }, label: {
                            HStack(spacing: 6) {
                                Image(systemName: "play.fill")
                                Text("Start Mirror")
                            }
                            .frame(maxWidth: .infinity)
                            .padding(.vertical, 12)
                            .background(selectedWindowID != nil ? Color.accentColor : Color.gray)
                            .foregroundColor(.white)
                            .cornerRadius(8)
                        })
                        .buttonStyle(.plain)
                        .disabled(selectedWindowID == nil)
                    }
                }
                .padding(20)
            }
        }
        .frame(width: 700, height: 600)
        .onAppear {
            refreshDisplays()
            Task {
                availableWindows = await captureManager.fetchAvailableWindowsWithThumbnails()
                isLoading = false
            }
        }
        .onReceive(NotificationCenter.default.publisher(
            for: NSApplication.didChangeScreenParametersNotification
        )) { _ in
            refreshDisplays()
        }
    }

    private func refreshDisplays() {
        let screens = NSScreen.screens
        availableDisplays = screens.enumerated().compactMap { index, screen in
            guard let displayID = screen.displayID else { return nil }
            let resolvedName = screen.localizedName.isEmpty ? "Display \(index + 1)" : screen.localizedName
            return DisplayInfo(
                id: displayID,
                name: resolvedName,
                isMain: screen == NSScreen.main
            )
        }

        if let selectedDisplayID,
           availableDisplays.contains(where: { $0.id == selectedDisplayID }) {
            return
        }

        if let nonMainDisplay = availableDisplays.first(where: { !$0.isMain }) {
            selectedDisplayID = nonMainDisplay.id
        } else if let mainDisplay = availableDisplays.first(where: { $0.isMain }) {
            selectedDisplayID = mainDisplay.id
        } else {
            selectedDisplayID = availableDisplays.first?.id
        }
    }
}

struct WindowPreviewCard: View {
    let windowInfo: WindowInfo
    let isSelected: Bool
    let onSelect: () -> Void

    var body: some View {
        Button(action: onSelect) {
            VStack(alignment: .leading, spacing: 8) {
                // Thumbnail
                ZStack {
                    RoundedRectangle(cornerRadius: 8)
                        .fill(Color.black.opacity(0.05))

                    if let thumbnail = windowInfo.thumbnail {
                        Image(decorative: thumbnail, scale: 1.0)
                            .resizable()
                            .aspectRatio(contentMode: .fit)
                            .cornerRadius(8)
                    } else {
                        Image(systemName: "macwindow")
                            .font(.system(size: 40))
                            .foregroundColor(.secondary)
                    }

                    if isSelected {
                        RoundedRectangle(cornerRadius: 8)
                            .strokeBorder(Color.accentColor, lineWidth: 3)
                    }
                }
                .frame(height: 140)

                // Info
                VStack(alignment: .leading, spacing: 4) {
                    Text(windowInfo.appName)
                        .font(.system(size: 13, weight: .semibold))
                        .lineLimit(1)

                    if !windowInfo.title.isEmpty {
                        Text(windowInfo.title)
                            .font(.system(size: 11))
                            .foregroundColor(.secondary)
                            .lineLimit(1)
                    }
                }
            }
            .padding(12)
            .background(isSelected ? Color.accentColor.opacity(0.1) : Color.clear)
            .cornerRadius(10)
            .overlay(
                RoundedRectangle(cornerRadius: 10)
                    .strokeBorder(Color.secondary.opacity(0.2), lineWidth: 1)
            )
        }
        .buttonStyle(.plain)
    }
}
