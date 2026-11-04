// The iOS demo app: one button runs the shared monet-conformance battery
// over the native transport (URLSession HTTP, NWConnection WebSocket,
// system trust store) inside a real app process. The console output is
// the same report the host tests and the Android demo print — whatever
// passes here passes everywhere, because it is the same battery.

import SwiftUI
import monet_demo

@main
struct MonetDemoApp: App {
  var body: some Scene {
    WindowGroup { ContentView() }
  }
}

@MainActor
final class BatteryModel: ObservableObject {
  @Published var report = "Tap Run to execute the transport battery."
  @Published var running = false

  func run() {
    guard !running else { return }
    running = true
    DispatchQueue.global(qos: .userInitiated).async {
      guard let raw = monet_demo_run_battery() else {
        print("battery returned nil")
        return
      }
      let text = String(cString: raw)
      monet_demo_free_string(raw)
      print(text)
      DispatchQueue.main.async {
        self.report = text
        self.running = false
      }
    }
  }
}

struct ContentView: View {
  @StateObject private var model = BatteryModel()

  var body: some View {
    VStack(spacing: 12) {
      Button(model.running ? "Running…" : "Run battery") { model.run() }
        .buttonStyle(.borderedProminent)
        .disabled(model.running)
      ScrollView {
        Text(model.report)
          .font(.system(size: 12, weight: .regular, design: .monospaced))
          .frame(maxWidth: .infinity, alignment: .leading)
          .textSelection(.enabled)
      }
    }
    .padding()
    .task { model.run() }
  }
}
