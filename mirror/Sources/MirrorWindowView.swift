import SwiftUI

struct MirrorWindowView: View {
    @ObservedObject var captureManager: ScreenCaptureManager
    @State private var horizontalFlip = true
    @State private var verticalFlip = false
    @State private var isHovering = false

    var body: some View {
        ZStack {
            // Mirror content
            if let frame = captureManager.capturedFrame {
                Image(decorative: frame, scale: 1.0)
                    .resizable()
                    .aspectRatio(contentMode: .fit)
                    .rotation3DEffect(
                        .degrees(horizontalFlip ? 180 : 0),
                        axis: (x: 0, y: 1, z: 0)
                    )
                    .rotation3DEffect(
                        .degrees(verticalFlip ? 180 : 0),
                        axis: (x: 1, y: 0, z: 0)
                    )
            } else {
                VStack {
                    ProgressView()
                        .scaleEffect(1.5)
                    Text("Loading mirror...")
                        .padding()
                }
            }

            // Hover overlay controls
            VStack {
                HStack(spacing: 10) {
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
                .padding(.horizontal, 14)
                .padding(.vertical, 12)
                .background(
                    RoundedRectangle(cornerRadius: 12)
                        .fill(.ultraThinMaterial)
                        .shadow(color: .black.opacity(0.3), radius: 10, x: 0, y: 4)
                )
                .opacity(isHovering ? 1 : 0)
                .animation(.easeInOut(duration: 0.25), value: isHovering)

                Spacer()
            }
            .padding(20)
        }
        .frame(minWidth: 400, minHeight: 300)
        .onHover { hovering in
            isHovering = hovering
        }
    }
}

struct FlipButton: View {
    let icon: String
    let label: String
    let isActive: Bool
    let action: () -> Void

    var body: some View {
        Button(action: action) {
            HStack(spacing: 8) {
                Image(systemName: icon)
                    .font(.system(size: 14, weight: .semibold))
                Text(label)
                    .font(.system(size: 13, weight: .medium))
            }
            .padding(.horizontal, 14)
            .padding(.vertical, 9)
            .background(
                RoundedRectangle(cornerRadius: 8)
                    .fill(isActive ? Color.accentColor : Color.primary.opacity(0.15))
            )
            .foregroundColor(isActive ? .white : .primary)
        }
        .buttonStyle(.plain)
        .animation(.easeInOut(duration: 0.15), value: isActive)
    }
}
