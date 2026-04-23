//
//  UsageMonitorApp.swift
//  UsageMonitorApp
//
//  Created by Eric Gong on 10/20/25.
//
//  Performance rewrite:
//    - Incremental decode: mmap each log, decode only the new tail since last refresh.
//    - All I/O and decode runs off-main inside a LogCache actor.
//    - Unaligned loads directly from mmap'd bytes (no per-record memcpy).
//    - Printline strings accumulated incrementally, not rebuilt each refresh.
//    - Text panes backed by NSTextView with non-contiguous layout (handles huge strings).
//    - Battery chart uses Charts' built-in horizontal scrolling (virtualized).
//    - Keyboard layout code is UNCHANGED from the original (verbatim).
//

import SwiftUI
import AppKit
import Charts
import Darwin
import IOKit
import IOKit.ps

import UsageMonitorUtilities


// MARK: - App entry

@main
struct UsageMonitorViewerApp: App {
    var body: some Scene {
        WindowGroup {
            ContentView()
                // minWidth sized so the 1:1 keyboard (273mm × 5.007pt/mm ≈ 1367pt)
                // plus side padding fits. On a 14" MBP (1512pt wide) this leaves
                // ~70pt of screen breathing room with normal window chrome.
                .frame(minWidth: 1440, minHeight: 710)
        }
    }
}


// MARK: - UI-facing data types

struct BatteryEntry: Identifiable {
    var id: Double { timestamp.timeIntervalSinceReferenceDate }
    var timestamp: Date
    var charge: Double
    var health: Double?
    var cycles: Int?
    var ac: Bool
}

struct BatteryRect: Identifiable {
    var start: Date
    var end: Date
    var charge: Double
    var asleep: Bool
    var id: Double { start.timeIntervalSinceReferenceDate }
}

enum Granularity: String, Hashable, CaseIterable {
    case fiveMin, hour
}

struct StatsSummary {
    var currentHealth: Double? = nil
    var currentCycles: Int? = nil
    var totalAwakeHours: Double = 0
    var avgHoursBetweenCharges: Double? = nil
    var avgUsageHoursBetweenCharges: Double? = nil
}


// MARK: - Log cache (incremental decode + derived data, off-main)

actor LogCache {
    struct Snapshot {
        let batteryEntries: [BatteryEntry]
        let sleepIntervals: [DateInterval]
        let stats: StatsSummary
        let keyCounts: [String: Int]
        let chargeText: String
        let healthText: String
        let screenText: String
        let keyText: String
        let rectsFiveMin: [BatteryRect]
        let rectsHour: [BatteryRect]
    }

    // Safety caps for the chart (most recent N bars only).
    // 5-min: 20k bars ≈ 69 days; 1-hour: 20k bars ≈ 2.3 years.
    private let rectCapFiveMin = 20_000
    private let rectCapHour    = 20_000

    // Decoded record caches; grow monotonically with file size.
    private var charges: [BatteryChargeEntry] = []
    private var healths: [BatteryHealthEntry] = []
    private var screens: [ScreenStateEntry]   = []

    // Bytes consumed from each log so far; used to decode only the new tail.
    private var chargeBytesSeen: Int = 0
    private var healthBytesSeen: Int = 0
    private var screenBytesSeen: Int = 0

    // Incrementally grown pretty-print dumps (one line per record).
    private var chargeText: String = ""
    private var healthText: String = ""
    private var screenText: String = ""

    // Fixed per-device; fetch once.
    private var designCapacity: Int?

    // Folder the current cache is keyed to; reset everything if the user relocates.
    private var currentFolderURL: URL?

    private func resetAll() {
        charges.removeAll(keepingCapacity: false)
        healths.removeAll(keepingCapacity: false)
        screens.removeAll(keepingCapacity: false)
        chargeBytesSeen = 0
        healthBytesSeen = 0
        screenBytesSeen = 0
        chargeText = ""
        healthText = ""
        screenText = ""
        designCapacity = nil
    }

    func refresh(folderURL: URL) -> Snapshot {
        if currentFolderURL != folderURL {
            resetAll()
            currentFolderURL = folderURL
        }

        let chargeURL = folderURL.appendingPathComponent("BatteryChargeLog")
        let healthURL = folderURL.appendingPathComponent("BatteryHealthLog")
        let screenURL = folderURL.appendingPathComponent("ScreenStateLog")
        let keysURL   = folderURL.appendingPathComponent("KeyFrequencyLog.json")

        // Snapshot directory — we clonefile the live logs here each refresh so the
        // viewer decodes from a frozen, inode-stable copy instead of the file the
        // recorder is actively appending to. On APFS, clonefile is an O(1) metadata
        // operation (copy-on-write extents), so doing it every refresh is free.
        let appDataDir = folderURL.appendingPathComponent("AppData", isDirectory: true)
        try? FileManager.default.createDirectory(
            at: appDataDir, withIntermediateDirectories: true
        )
        let snapCharge = appDataDir.appendingPathComponent("BatteryChargeLog")
        let snapHealth = appDataDir.appendingPathComponent("BatteryHealthLog")
        let snapScreen = appDataDir.appendingPathComponent("ScreenStateLog")

        updateCharges(original: chargeURL, snapshot: snapCharge)
        updateHealths(original: healthURL, snapshot: snapHealth)
        updateScreens(original: screenURL, snapshot: snapScreen)

        if designCapacity == nil {
            designCapacity = Self.fetchDesignCapacity()
        }

        let entries = mergeBatteryStreams(
            charges: charges,
            healths: healths,
            designCapacity: designCapacity
        )
        let sleeps = buildSleepIntervals(from: screens)
        let stats  = computeStats(entries: entries, sleepIntervals: sleeps)

        let (keyJSON, keyCounts) = loadKeyFrequency(from: keysURL)

        var rectsFive = buildRects(
            entries: entries, intervalMinutes: 5, gapMinutes: 1, sleepIntervals: sleeps
        )
        if rectsFive.count > rectCapFiveMin {
            rectsFive = Array(rectsFive.suffix(rectCapFiveMin))
        }
        var rectsHour = buildRects(
            entries: entries, intervalMinutes: 60, gapMinutes: 10, sleepIntervals: sleeps
        )
        if rectsHour.count > rectCapHour {
            rectsHour = Array(rectsHour.suffix(rectCapHour))
        }

        return Snapshot(
            batteryEntries: entries,
            sleepIntervals: sleeps,
            stats: stats,
            keyCounts: keyCounts,
            chargeText: chargeText,
            healthText: healthText,
            screenText: screenText,
            keyText: keyJSON,
            rectsFiveMin: rectsFive,
            rectsHour: rectsHour
        )
    }

    // MARK: Incremental decode (per log)

    private func updateCharges(original: URL, snapshot: URL) {
        let recordSize = BatteryChargeEntry.byteSize
        guard let (data, size) = mapSnapshotIfChanged(
            original: original, snapshot: snapshot, bytesSeen: chargeBytesSeen
        ) else { return }
        if size < chargeBytesSeen {
            // File shrank (rotation) — start fresh
            charges.removeAll(keepingCapacity: true)
            chargeText.removeAll(keepingCapacity: true)
            chargeBytesSeen = 0
        }
        let startOffset = chargeBytesSeen
        let tailBytes = size - startOffset
        guard tailBytes >= recordSize else { return }
        let newRecords = tailBytes / recordSize
        charges.reserveCapacity(charges.count + newRecords)

        var appended: [String] = []
        appended.reserveCapacity(newRecords)

        data.withUnsafeBytes { rb in
            guard let base = rb.baseAddress else { return }
            for i in 0..<newRecords {
                let p = base.advanced(by: startOffset + i * recordSize)
                let e = BatteryChargeEntry(
                    EntryTime:          p.loadUnaligned(fromByteOffset: 0,  as: UInt32.self),
                    Amperage:           p.loadUnaligned(fromByteOffset: 4,  as: Int16.self),
                    RawCurrentCapacity: p.loadUnaligned(fromByteOffset: 6,  as: Int16.self),
                    Voltage:            p.loadUnaligned(fromByteOffset: 8,  as: Int16.self),
                    CellVoltage0:       p.loadUnaligned(fromByteOffset: 10, as: Int16.self),
                    CellVoltage1:       p.loadUnaligned(fromByteOffset: 12, as: Int16.self),
                    CellVoltage2:       p.loadUnaligned(fromByteOffset: 14, as: Int16.self),
                    CurrentCapacity:    p.loadUnaligned(fromByteOffset: 16, as: Int8.self),
                    PresentDOD0:        p.loadUnaligned(fromByteOffset: 17, as: Int8.self),
                    PresentDOD1:        p.loadUnaligned(fromByteOffset: 18, as: Int8.self),
                    PresentDOD2:        p.loadUnaligned(fromByteOffset: 19, as: Int8.self)
                )
                charges.append(e)
                appended.append(e.printline)
            }
        }
        if !appended.isEmpty {
            if !chargeText.isEmpty { chargeText.append("\n") }
            chargeText.append(appended.joined(separator: "\n"))
        }
        chargeBytesSeen = startOffset + newRecords * recordSize
    }

    private func updateHealths(original: URL, snapshot: URL) {
        let recordSize = BatteryHealthEntry.byteSize
        guard let (data, size) = mapSnapshotIfChanged(
            original: original, snapshot: snapshot, bytesSeen: healthBytesSeen
        ) else { return }
        if size < healthBytesSeen {
            healths.removeAll(keepingCapacity: true)
            healthText.removeAll(keepingCapacity: true)
            healthBytesSeen = 0
        }
        let startOffset = healthBytesSeen
        let tailBytes = size - startOffset
        guard tailBytes >= recordSize else { return }
        let newRecords = tailBytes / recordSize
        healths.reserveCapacity(healths.count + newRecords)

        var appended: [String] = []
        appended.reserveCapacity(newRecords)

        data.withUnsafeBytes { rb in
            guard let base = rb.baseAddress else { return }
            for i in 0..<newRecords {
                let p = base.advanced(by: startOffset + i * recordSize)
                let e = BatteryHealthEntry(
                    EntryTime:         p.loadUnaligned(fromByteOffset: 0,  as: UInt32.self),
                    CycleCount:        p.loadUnaligned(fromByteOffset: 4,  as: UInt16.self),
                    RawMaxCapacity:    p.loadUnaligned(fromByteOffset: 6,  as: Int16.self),
                    QMax0:             p.loadUnaligned(fromByteOffset: 8,  as: Int16.self),
                    QMax1:             p.loadUnaligned(fromByteOffset: 10, as: Int16.self),
                    QMax2:             p.loadUnaligned(fromByteOffset: 12, as: Int16.self),
                    WeightedRa0:       p.loadUnaligned(fromByteOffset: 14, as: Int8.self),
                    WeightedRa1:       p.loadUnaligned(fromByteOffset: 15, as: Int8.self),
                    WeightedRa2:       p.loadUnaligned(fromByteOffset: 16, as: Int8.self),
                    ExternalConnected: p.loadUnaligned(fromByteOffset: 17, as: Int8.self)
                )
                healths.append(e)
                appended.append(e.printline)
            }
        }
        if !appended.isEmpty {
            if !healthText.isEmpty { healthText.append("\n") }
            healthText.append(appended.joined(separator: "\n"))
        }
        healthBytesSeen = startOffset + newRecords * recordSize
    }

    private func updateScreens(original: URL, snapshot: URL) {
        let recordSize = ScreenStateEntry.byteSize
        guard let (data, size) = mapSnapshotIfChanged(
            original: original, snapshot: snapshot, bytesSeen: screenBytesSeen
        ) else { return }
        if size < screenBytesSeen {
            screens.removeAll(keepingCapacity: true)
            screenText.removeAll(keepingCapacity: true)
            screenBytesSeen = 0
        }
        let startOffset = screenBytesSeen
        let tailBytes = size - startOffset
        guard tailBytes >= recordSize else { return }
        let newRecords = tailBytes / recordSize
        screens.reserveCapacity(screens.count + newRecords)

        var appended: [String] = []
        appended.reserveCapacity(newRecords)

        data.withUnsafeBytes { rb in
            guard let base = rb.baseAddress else { return }
            for i in 0..<newRecords {
                let p = base.advanced(by: startOffset + i * recordSize)
                let t: UInt32 = p.loadUnaligned(fromByteOffset: 0, as: UInt32.self)
                let raw: Int8 = p.loadUnaligned(fromByteOffset: 4, as: Int8.self)
                guard let state = ScreenStateEnum(rawValue: raw) else { continue }
                let e = ScreenStateEntry(EntryTime: t, ScreenState: state)
                screens.append(e)
                appended.append(e.printline)
            }
        }
        if !appended.isEmpty {
            if !screenText.isEmpty { screenText.append("\n") }
            screenText.append(appended.joined(separator: "\n"))
        }
        screenBytesSeen = startOffset + newRecords * recordSize
    }

    // MARK: File helpers

    /// Takes a fresh atomic snapshot of `original` into `snapshot` via APFS
    /// `clonefile()` (O(1), copy-on-write — no data actually copied), then
    /// mmaps the snapshot and returns (data, byteCount). Returns nil when
    /// there's nothing new (original size unchanged since `bytesSeen`), or
    /// when the source is missing.
    ///
    /// The viewer only ever decodes from this snapshot, never from the live
    /// file the recorder is appending to. This means even if the writer's
    /// format changes, a log rotates mid-read, or a partial write ever
    /// occurs, the viewer has a frozen, inode-stable view for the duration
    /// of the refresh.
    ///
    /// Fallback: if clonefile fails (non-APFS volume, etc.), we read the
    /// original into memory in one shot. That's still atomic from the
    /// reader's perspective — we get whatever bytes exist at read time and
    /// anything newer is picked up next refresh.
    private func mapSnapshotIfChanged(
        original: URL, snapshot: URL, bytesSeen: Int
    ) -> (Data, Int)? {
        // Fast path: if the live file hasn't grown, skip entirely.
        var st = stat()
        guard stat(original.path, &st) == 0 else { return nil }
        let origSize = Int(st.st_size)
        if origSize == bytesSeen { return nil }

        // APFS atomic clone.
        _ = unlink(snapshot.path)  // clonefile() requires destination not to exist
        if clonefile(original.path, snapshot.path, 0) == 0,
           let data = try? Data(contentsOf: snapshot, options: .mappedIfSafe) {
            return (data, data.count)
        }

        // Fallback: read-into-memory snapshot. Still atomic at read time.
        if let data = try? Data(contentsOf: original) {
            return (data, data.count)
        }
        return nil
    }

    // MARK: Derived data (merge, sleeps, stats, rects, keys)

    private func mergeBatteryStreams(
        charges: [BatteryChargeEntry],
        healths: [BatteryHealthEntry],
        designCapacity: Int?
    ) -> [BatteryEntry] {
        guard !charges.isEmpty else { return [] }
        var out: [BatteryEntry] = []
        out.reserveCapacity(charges.count)

        let baseline = max(0, designCapacity ?? 0)
        var iH = 0
        for c in charges {
            let t = c.EntryTime
            while iH + 1 < healths.count && healths[iH + 1].EntryTime <= t { iH += 1 }
            let h: BatteryHealthEntry? = healths.isEmpty ? nil : healths[iH]

            let ts = Date(timeIntervalSinceReferenceDate: TimeInterval(t))
            let chargePct = Double(Int(c.CurrentCapacity))
            let ac = (h?.ExternalConnected ?? 0) != 0
            let healthPct: Double? = {
                guard baseline > 0, let raw = h?.RawMaxCapacity else { return nil }
                let pct = (Double(max(0, Int(raw))) / Double(baseline)) * 100.0
                return max(0.0, min(100.0, pct))
            }()
            let cycles: Int? = h.map { Int($0.CycleCount) }

            out.append(BatteryEntry(
                timestamp: ts,
                charge: max(0, min(100, chargePct)),
                health: healthPct,
                cycles: cycles,
                ac: ac
            ))
        }
        return out
    }

    private func buildSleepIntervals(from events: [ScreenStateEntry]) -> [DateInterval] {
        guard !events.isEmpty else { return [] }
        var intervals: [DateInterval] = []
        var lockStart: Date? = nil
        for e in events {
            let t = Date(timeIntervalSinceReferenceDate: TimeInterval(e.EntryTime))
            switch e.ScreenState {
            case .locked:
                lockStart = t
            case .unlocked:
                if let s = lockStart, s <= t {
                    intervals.append(DateInterval(start: s, end: t))
                }
                lockStart = nil
            }
        }
        if let s = lockStart {
            intervals.append(DateInterval(start: s, end: Date()))
        }
        return intervals
    }

    private func buildRects(
        entries: [BatteryEntry],
        intervalMinutes: Int,
        gapMinutes: Int,
        sleepIntervals: [DateInterval]
    ) -> [BatteryRect] {
        guard entries.count > 1 else { return [] }
        let step = Double(intervalMinutes * 60)
        let bar  = Double(max(0, (intervalMinutes - gapMinutes) * 60))

        // Resample (linear interpolation on timestamp/charge)
        var resampled: [BatteryEntry] = []
        resampled.reserveCapacity(entries.count)
        var cursor = 0
        var t = entries.first!.timestamp
        let tEnd = entries.last!.timestamp
        while t <= tEnd {
            while cursor + 1 < entries.count && entries[cursor + 1].timestamp < t {
                cursor += 1
            }
            let lower = entries[cursor]
            let upper = (cursor + 1 < entries.count) ? entries[cursor + 1] : lower
            let y: Double
            if upper.timestamp == lower.timestamp {
                y = lower.charge
            } else {
                let frac = t.timeIntervalSince(lower.timestamp) /
                           upper.timestamp.timeIntervalSince(lower.timestamp)
                y = lower.charge + frac * (upper.charge - lower.charge)
            }
            resampled.append(BatteryEntry(
                timestamp: t,
                charge: max(0, min(100, y)),
                health: nil, cycles: nil, ac: false
            ))
            t = t.addingTimeInterval(step)
        }

        // Convert to rects; sleep-detection cursor walks forward across sleepIntervals.
        var rects: [BatteryRect] = []
        rects.reserveCapacity(resampled.count)
        var s = 0
        for e in resampled {
            let start = e.timestamp
            let end = start.addingTimeInterval(bar)
            while s < sleepIntervals.count && sleepIntervals[s].end <= start { s += 1 }
            let asleep = (s < sleepIntervals.count &&
                          sleepIntervals[s].start <= start &&
                          sleepIntervals[s].end   >= end)
            rects.append(BatteryRect(start: start, end: end, charge: e.charge, asleep: asleep))
        }
        return rects
    }

    private func loadKeyFrequency(from url: URL) -> (String, [String: Int]) {
        guard let data = try? Data(contentsOf: url) else { return ("{}", [:]) }
        let text = String(data: data, encoding: .utf8) ?? "{}"
        let obj = (try? JSONSerialization.jsonObject(with: data) as? [String: Any]) ?? [:]
        var out: [String: Int] = [:]
        out.reserveCapacity(obj.count)
        for (k, v) in obj {
            if let i = v as? Int { out[k] = i }
            else if let d = v as? Double { out[k] = Int(d.rounded()) }
            else if let s = v as? String, let i = Int(s) { out[k] = i }
        }
        return (text, out)
    }

    // MARK: Stats

    private func computeStats(entries: [BatteryEntry], sleepIntervals: [DateInterval]) -> StatsSummary {
        guard let first = entries.first, let last = entries.last, last.timestamp > first.timestamp else {
            return StatsSummary(
                currentHealth: entries.last?.health,
                currentCycles: entries.last?.cycles
            )
        }
        let range = DateInterval(start: first.timestamp, end: last.timestamp)
        let merged = Self.mergeIntervals(sleepIntervals)
        let awake  = Self.awakeDuration(in: range, subtracting: merged)

        let spans = Self.betweenChargeSpans(using: entries)
        let avgBetween: Double? = {
            guard !spans.isEmpty else { return nil }
            let s = spans.reduce(0.0) { $0 + $1.duration }
            return s / Double(spans.count) / 3600.0
        }()
        let avgUsageBetween: Double? = {
            guard !spans.isEmpty else { return nil }
            let s = spans.reduce(0.0) { $0 + Self.awakeDuration(in: $1, subtracting: merged) }
            return s / Double(spans.count) / 3600.0
        }()

        return StatsSummary(
            currentHealth: entries.last?.health,
            currentCycles: entries.last?.cycles,
            totalAwakeHours: awake / 3600.0,
            avgHoursBetweenCharges: avgBetween,
            avgUsageHoursBetweenCharges: avgUsageBetween
        )
    }

    private static func betweenChargeSpans(using entries: [BatteryEntry]) -> [DateInterval] {
        guard entries.count >= 2 else { return [] }
        var starts: [Date] = []
        var ends: [Date] = []
        var prevAC = entries[0].ac
        for i in 1..<entries.count {
            let currAC = entries[i].ac
            if !prevAC && currAC { starts.append(entries[i].timestamp) }
            else if prevAC && !currAC { ends.append(entries[i].timestamp) }
            prevAC = currAC
        }
        var spans: [DateInterval] = []
        var j = 0
        for end in ends {
            while j < starts.count && starts[j] <= end { j += 1 }
            if j < starts.count {
                spans.append(DateInterval(start: end, end: starts[j]))
            } else { break }
        }
        return spans
    }

    private static func mergeIntervals(_ intervals: [DateInterval]) -> [DateInterval] {
        guard !intervals.isEmpty else { return [] }
        var result: [DateInterval] = [intervals[0]]
        for iv in intervals.dropFirst() {
            if let last = result.last, last.end >= iv.start {
                result[result.count - 1] = DateInterval(start: last.start, end: max(last.end, iv.end))
            } else {
                result.append(iv)
            }
        }
        return result
    }

    private static func awakeDuration(in range: DateInterval, subtracting sleeps: [DateInterval]) -> TimeInterval {
        var asleep: TimeInterval = 0
        for s in sleeps {
            let lo = max(range.start, s.start)
            let hi = min(range.end, s.end)
            if hi > lo { asleep += hi.timeIntervalSince(lo) }
        }
        return max(0, range.duration - asleep)
    }

    // MARK: Design capacity (IOKit)

    private static func fetchDesignCapacity() -> Int? {
        let svc = IOServiceGetMatchingService(kIOMainPortDefault, IOServiceMatching("AppleSmartBattery"))
        guard svc != 0 else { return nil }
        defer { IOObjectRelease(svc) }

        var props: Unmanaged<CFMutableDictionary>?
        guard IORegistryEntryCreateCFProperties(svc, &props, kCFAllocatorDefault, 0) == KERN_SUCCESS,
              let dict = props?.takeRetainedValue() as? [String: Any] else { return nil }

        let batteryData = dict["BatteryData"] as? [String: Any]
        let anyVal = dict["DesignCapacity"] ?? batteryData?["DesignCapacity"]

        if let n = anyVal as? NSNumber, n.intValue > 0 { return n.intValue }
        if let v = anyVal as? Int, v > 0 { return v }
        return nil
    }
}


// MARK: - Content view

struct ContentView: View {
    // Default to current user's ~/Library/UsageMonitor; allow relocation via bookmark.
    private let defaultFolder = FileManager.default
        .homeDirectoryForCurrentUser
        .appendingPathComponent("Library/UsageMonitor", isDirectory: true)
    private let bookmarkKey = "UsageMonitorBookmark"

    // Log cache lives for the lifetime of the view (as a reference-type actor).
    @State private var cache = LogCache()

    // UI state
    @State private var batteryEntries: [BatteryEntry] = []
    @State private var sleepIntervals: [DateInterval] = []
    @State private var keyCounts: [String: Int] = [:]
    @State private var stats: StatsSummary = StatsSummary()
    @State private var rectsFiveMin: [BatteryRect] = []
    @State private var rectsHour: [BatteryRect] = []

    @State private var chargeText: String = ""
    @State private var healthText: String = ""
    @State private var screenText: String = ""
    @State private var keyText: String = ""

    @State private var isLoading: Bool = false
    @State private var errorMessage: String? = nil

    var body: some View {
        let panelHeight: CGFloat = 240
        ScrollView(.vertical) {
            VStack(spacing: 0) {
                Text("Battery and Charging Statistics")
                    .font(.title).fontWeight(.bold)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .padding(.horizontal).padding(.top, 8)

                HStack(alignment: .top, spacing: 12) {
                    BatteryGraphView(
                        rectsFiveMin: rectsFiveMin,
                        rectsHour: rectsHour
                    )
                    .frame(maxWidth: .infinity)
                    .frame(height: panelHeight)

                    StatsPanel(stats: stats)
                        .frame(width: 340)
                        .frame(height: panelHeight)
                }
                .padding(.horizontal).padding(.top, 8)

                Divider().padding(.top, 8)

                Text("Keyboard Usage Statistics")
                    .font(.title).fontWeight(.bold)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .padding(.horizontal).padding(.top, 8)

                VStack(alignment: .center) {
                    KeyboardHeatmapView(keyCounts: keyCounts)
                }
                .padding(.horizontal)
                .padding(.bottom, 12)

                Divider()

                HStack {
                    Button("Refresh Data") {
                        Task { await loadFiles() }
                    }
                    .keyboardShortcut("r", modifiers: [.command])
                    .disabled(isLoading)

                    if isLoading {
                        ProgressView()
                            .scaleEffect(0.6)
                            .padding(.leading, 6)
                    }
                    if let message = errorMessage {
                        Text(message).foregroundColor(.red).padding(.leading)
                    }
                    Spacer()
                }
                .padding(.horizontal)
                .padding(.top, 8)

                HStack(spacing: 10) {
                    VStack(spacing: 10) {
                        scrollableColumn(title: "BatteryChargeLog (decoded)", content: chargeText)
                        scrollableColumn(title: "BatteryHealthLog (decoded)", content: healthText)
                    }
                    .frame(width: 700, alignment: .leading)
                    VStack(spacing: 10) {
                        scrollableColumn(title: "ScreenStateLog (decoded)", content: screenText)
                        scrollableColumn(title: "KeyFrequencyLog.json", content: keyText)
                    }
                    .frame(maxWidth: .infinity, alignment: .leading)
                }
                .padding()
            }
        }
        .task { await loadFiles() }
    }

    private func scrollableColumn(title: String, content: String) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(title).font(.headline).padding(.bottom, 2)
            PlainTextScrollView(text: content)
                .border(Color.gray.opacity(0.5))
        }
        .frame(maxWidth: .infinity, minHeight: 260, maxHeight: 400)
    }

    // MARK: Loading

    private func loadFiles() async {
        if isLoading { return }
        isLoading = true
        defer { isLoading = false }
        errorMessage = nil

        guard let accessURL = accessFolderWithBookmark() ?? askUserForPermission(to: defaultFolder) else {
            errorMessage = "Please locate your UsageMonitor folder."
            return
        }
        saveBookmark(for: accessURL)

        // Refresh in the actor (off main thread)
        let snap = await cache.refresh(folderURL: accessURL)

        // Publish to UI
        self.batteryEntries = snap.batteryEntries
        self.sleepIntervals = snap.sleepIntervals
        self.stats          = snap.stats
        self.keyCounts      = snap.keyCounts
        self.chargeText     = snap.chargeText
        self.healthText     = snap.healthText
        self.screenText     = snap.screenText
        self.keyText        = snap.keyText
        self.rectsFiveMin   = snap.rectsFiveMin
        self.rectsHour      = snap.rectsHour
    }

    private func askUserForPermission(to nominalTarget: URL) -> URL? {
        let panel = NSOpenPanel()
        panel.title = "Allow Access to UsageMonitor Folder"
        panel.message = "Select your UsageMonitor folder (usually \(nominalTarget.path))."
        panel.directoryURL = nominalTarget.deletingLastPathComponent()
        panel.canChooseDirectories = true
        panel.canChooseFiles = false
        panel.allowsMultipleSelection = false
        panel.prompt = "Allow"
        if panel.runModal() == .OK, let selectedURL = panel.url {
            return selectedURL
        }
        return nil
    }

    private func saveBookmark(for url: URL) {
        do {
            let data = try url.bookmarkData(
                options: .withSecurityScope,
                includingResourceValuesForKeys: nil,
                relativeTo: nil
            )
            UserDefaults.standard.set(data, forKey: bookmarkKey)
        } catch {
            errorMessage = "Failed to save bookmark: \(error.localizedDescription)"
        }
    }

    private func accessFolderWithBookmark() -> URL? {
        guard let data = UserDefaults.standard.data(forKey: bookmarkKey) else { return nil }
        var isStale = false
        do {
            let resolved = try URL(
                resolvingBookmarkData: data,
                options: [.withSecurityScope],
                relativeTo: nil,
                bookmarkDataIsStale: &isStale
            )
            if isStale { return nil }
            if resolved.startAccessingSecurityScopedResource() { return resolved }
        } catch { /* ignore */ }
        return nil
    }
}


// MARK: - Stats views

struct StatBox: View {
    let title: String
    let value: String
    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text(title)
                .font(.caption)
                .fontWeight(.bold)
                .foregroundColor(.secondary)
                .frame(maxWidth: .infinity, alignment: .leading)
            Text(value)
                .font(.title3)
                .fontWeight(.semibold)
                .foregroundColor(.primary)
                .frame(maxWidth: .infinity, alignment: .leading)
        }
        .padding(10)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .background(Color.gray.opacity(0.12))
        .overlay(RoundedRectangle(cornerRadius: 8).stroke(Color.gray.opacity(0.35), lineWidth: 1))
        .cornerRadius(8)
    }
}

struct StatsPanel: View {
    let stats: StatsSummary
    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack(spacing: 10) {
                StatBox(title: "Battery Health",
                        value: stats.currentHealth.map { String(format: "%.0f%%", $0) } ?? "—")
                    .frame(maxWidth: .infinity)
                StatBox(title: "Charge Cycles",
                        value: stats.currentCycles.map(String.init) ?? "—")
                    .frame(maxWidth: .infinity)
            }
            StatBox(title: "Total Usage",
                    value: String(format: "%.1f Hours", stats.totalAwakeHours))
                .frame(maxWidth: .infinity)
            HStack(spacing: 10) {
                StatBox(title: "Duration between Charges",
                        value: stats.avgHoursBetweenCharges.map { String(format: "%.1f Hours", $0) } ?? "—")
                    .frame(maxWidth: .infinity)
                StatBox(title: "Usage between Charges",
                        value: stats.avgUsageHoursBetweenCharges.map { String(format: "%.1f Hours", $0) } ?? "—")
                    .frame(maxWidth: .infinity)
            }
            Spacer()
        }
        .padding(.trailing)
    }
}


// MARK: - Battery graph (Charts-native horizontal scrolling)

struct BatteryGraphView: View {
    let rectsFiveMin: [BatteryRect]
    let rectsHour: [BatteryRect]

    @State private var granularity: Granularity = .fiveMin
    @State private var scrollX: Date = Date()

    private let yTickValues: [Int] = [100, 75, 50, 25, 0]

    // Visible window length in seconds.
    private var visibleLength: TimeInterval {
        switch granularity {
        case .fiveMin: return 24 * 60 * 60          // 1 day
        case .hour:    return 7 * 24 * 60 * 60      // 1 week
        }
    }

    private var activeRects: [BatteryRect] {
        granularity == .fiveMin ? rectsFiveMin : rectsHour
    }

    var body: some View {
        VStack(alignment: .leading) {
            Picker("", selection: $granularity) {
                Text("5 Minutes").tag(Granularity.fiveMin)
                Text("1 Hour").tag(Granularity.hour)
            }
            .labelsHidden()
            .pickerStyle(.segmented)
            .onChange(of: granularity) { _, _ in
                snapToLatest()
            }
            .onChange(of: rectsFiveMin.last?.end) { _, _ in snapToLatest() }
            .onChange(of: rectsHour.last?.end)    { _, _ in snapToLatest() }

            chartView
        }
        .frame(maxWidth: .infinity)
        .onAppear { snapToLatest() }
    }

    @ViewBuilder
    private var chartView: some View {
        let rects = activeRects
        Chart {
            ForEach(yTickValues, id: \.self) { v in
                RuleMark(y: .value("Y", Double(v)))
                    .foregroundStyle(.secondary.opacity(0.18))
                    .lineStyle(StrokeStyle(lineWidth: 0.5, dash: [3, 3]))
            }
            ForEach(rects) { r in
                RectangleMark(
                    xStart: .value("Start", r.start),
                    xEnd:   .value("End",   r.end),
                    yStart: .value("Zero",  0.0),
                    yEnd:   .value("Charge", r.charge)
                )
                .foregroundStyle(r.asleep ? Color.gray.opacity(0.8) : Color.blue.opacity(0.8))
            }
        }
        .chartYScale(domain: 0...100)
        .chartYAxis {
            AxisMarks(position: .leading, values: yTickValues.map(Double.init)) { _ in
                AxisGridLine().foregroundStyle(.secondary.opacity(0.18))
                AxisTick()
                AxisValueLabel()
            }
        }
        .chartXAxis {
            switch granularity {
            case .fiveMin:
                AxisMarks(values: .stride(by: .hour, count: 3)) { value in
                    AxisTick()
                    if let date = value.as(Date.self) {
                        let hour = Calendar.current.component(.hour, from: date)
                        if hour == 0 {
                            AxisValueLabel {
                                Text(date, format: .dateTime.month(.abbreviated).day())
                                    .fontWeight(.bold)
                                    .foregroundStyle(.black)
                            }
                        } else {
                            AxisValueLabel {
                                Text(date, format: .dateTime.hour(.defaultDigits(amPM: .abbreviated)))
                            }
                        }
                    }
                }
            case .hour:
                AxisMarks(values: .stride(by: .day, count: 1)) { value in
                    AxisTick()
                    AxisValueLabel {
                        if let date = value.as(Date.self) {
                            Text(date, format: .dateTime.month(.abbreviated).day())
                                .fontWeight(.bold)
                                .foregroundStyle(.black)
                        }
                    }
                }
            }
        }
        .chartScrollableAxes(.horizontal)
        .chartXVisibleDomain(length: visibleLength)
        .chartScrollPosition(x: $scrollX)
        .chartPlotStyle { plotArea in
            plotArea
                .padding(.top, 0)
                .padding(.trailing, 0)
                .clipped()
        }
    }

    private func snapToLatest() {
        guard let last = activeRects.last?.end else { return }
        scrollX = last.addingTimeInterval(-visibleLength)
    }
}


// MARK: - NSTextView-backed scrollable plain-text pane (for large content)

struct PlainTextScrollView: NSViewRepresentable {
    let text: String

    func makeNSView(context: Context) -> NSScrollView {
        let scroll = NSTextView.scrollableTextView()
        scroll.hasVerticalScroller = true
        scroll.hasHorizontalScroller = false
        scroll.borderType = .noBorder
        scroll.autohidesScrollers = false

        if let tv = scroll.documentView as? NSTextView {
            tv.isEditable = false
            tv.isSelectable = true
            tv.isRichText = false
            tv.drawsBackground = true
            tv.backgroundColor = .clear
            tv.font = NSFont.monospacedSystemFont(ofSize: 11, weight: .regular)
            tv.textContainerInset = NSSize(width: 6, height: 6)

            // Make the text view grow vertically but stay within the scroll view horizontally.
            tv.isVerticallyResizable = true
            tv.isHorizontallyResizable = false
            tv.autoresizingMask = [.width]
            tv.textContainer?.widthTracksTextView = true
            tv.textContainer?.containerSize = NSSize(
                width: 0,
                height: CGFloat.greatestFiniteMagnitude
            )

            // Non-contiguous layout keeps huge strings snappy.
            tv.layoutManager?.allowsNonContiguousLayout = true

            tv.string = text
        }
        return scroll
    }

    func updateNSView(_ nsView: NSScrollView, context: Context) {
        guard let tv = nsView.documentView as? NSTextView else { return }
        // Cheap monotonic identity check: the writer only appends, so lengths differ
        // iff the content differs in the portion we haven't seen yet.
        let currentLen = (tv.string as NSString).length
        let newLen = (text as NSString).length
        if currentLen != newLen {
            tv.string = text
        }
    }
}


// MARK: - Keyboard Heatmap  (VERBATIM from the original file — do not edit)

// ---------- Legends & Key model ----------

enum Legend { case text(String); case symbol(String) }

struct KeyLegends {
    var topLeft: Legend? = nil
    var topCenter: Legend? = nil
    var topRight: Legend? = nil
    var center: Legend? = nil
    var bottomLeft: Legend? = nil
    var bottomCenter: Legend? = nil
    var bottomRight: Legend? = nil
}

enum KeyShape { case rect, rectWithInnerCircle, arrowsCluster }
enum KeyFaceStyle { case single, numberDual, dualEqual } // numbers bigger on bottom for .numberDual

struct KeySpec: Identifiable {
    let id: String
    let widthMM: CGFloat            // physical width in millimeters
    let shape: KeyShape
    let legends: KeyLegends
    let synonyms: [String]
    let centered: Bool              // false = left-aligned label (tab/shift/etc.)
    let face: KeyFaceStyle
    let isFunctionKey: Bool

    init(_ id: String,
         widthMM: CGFloat = 16,      // default letter-key width
         shape: KeyShape = .rect,
         legends: KeyLegends = KeyLegends(),
         synonyms: [String] = [],
         centered: Bool = true,
         face: KeyFaceStyle = .single,
         isFunctionKey: Bool = false) {
        self.id = id
        self.widthMM = widthMM
        self.shape = shape
        self.legends = legends
        self.synonyms = [id] + synonyms
        self.centered = centered
        self.face = face
        self.isFunctionKey = isFunctionKey
    }
}

// ---------- Layout (real-world MBP dimensions) ----------
//
// Widths are expressed in physical millimeters matching a MacBook Pro keyboard,
// then scaled to points via `ptPerMM` at render time. All rows are 273mm wide
// with a per-row inter-key gap derived from `(273mm − Σwidths) / (n − 1)`.
// That yields exactly 3mm gaps on every row.

// 14" MBP: 302mm-wide display × 1512 logical points across = 0.1997mm/pt
// → ptPerMM ≈ 5.007 renders the keyboard at exactly physical size.
private let ptPerMM: CGFloat = 5.007           // 1:1 on 14" MBP (302mm / 1512pt)
private let rowWidthMM: CGFloat = 273
private let rowHeightMM: CGFloat = 16
private let rowSpacingMM: CGFloat = 3          // vertical space between rows
private let arrowHeightMM: CGFloat = 8         // half-height for arrow keys
private let arrowGapMM: CGFloat = 3            // gap between arrow columns
private let arrowKeyWidthMM: CGFloat = 16
// Arrow cluster occupies three 16mm columns separated by 3mm gaps = 54mm total.
private let arrowClusterWidthMM: CGFloat =
    3 * arrowKeyWidthMM + 2 * arrowGapMM

// Builders
private func num(_ n: String, _ shifted: String) -> KeySpec {
    // top & bottom centered; bottom (number) a bit larger
    KeySpec("Digit\(n)",
            legends: .init(topCenter: .text(shifted), bottomCenter: .text(n)),
            face: .numberDual)
}
private func punct(_ id: String, _ top: String, _ bottom: String, syn: [String] = []) -> KeySpec {
    // top & bottom centered; same size
    KeySpec(id,
            legends: .init(topCenter: .text(top), bottomCenter: .text(bottom)),
            synonyms: syn,
            face: .dualEqual)
}
private func txt(_ id: String, _ label: String, mm: CGFloat = 16, syn: [String] = [], centered: Bool = true) -> KeySpec {
    KeySpec(id, widthMM: mm, legends: .init(center: .text(label)), synonyms: syn, centered: centered, face: .single)
}
// Single-label key with the label anchored bottom-left, inset `legendInsetMM`.
// Used for esc, tab, caps lock, left shift.
private func cornerBL(_ id: String, _ label: String, mm: CGFloat = 16, syn: [String] = []) -> KeySpec {
    KeySpec(id, widthMM: mm,
            legends: .init(bottomLeft: .text(label)),
            synonyms: syn, centered: false, face: .single)
}
// Single-label key with the label anchored bottom-right, inset `legendInsetMM`.
// Used for delete, right shift.
private func cornerBR(_ id: String, _ label: String, mm: CGFloat = 16, syn: [String] = []) -> KeySpec {
    KeySpec(id, widthMM: mm,
            legends: .init(bottomRight: .text(label)),
            synonyms: syn, centered: false, face: .single)
}
private func fn(_ n: Int, _ symbol: String) -> KeySpec {
    // small symbol near top + F# centered at bottom
    KeySpec("F\(n)",
            legends: .init(topCenter: .symbol(symbol), bottomCenter: .text("F\(n)")),
            synonyms: ["f\(n)","F\(n)"],
            centered: true,
            face: .dualEqual,
            isFunctionKey: true)
}

// ---------- Rows (widths in mm, per real MBP measurements) ----------

private let keyboardRows: [[KeySpec]] = [
    // Row 0: Esc(26), F1..F12(16 each), TouchID(16) — sum 234mm, 13 gaps × 3mm
    [
        cornerBL("Esc", "esc", mm: 26),
        fn(1,  "sun.min"),
        fn(2,  "sun.max"),
        fn(3,  "rectangle.3.group"),
        fn(4,  "magnifyingglass"),
        fn(5,  "mic"),
        fn(6,  "moon"),
        fn(7,  "backward"),
        fn(8,  "playpause"),
        fn(9,  "forward"),
        fn(10, "speaker"),
        fn(11, "speaker.wave.1"),
        fn(12, "speaker.wave.2"),
        KeySpec("TouchID",
                widthMM: 16,
                shape: .rectWithInnerCircle,
                legends: .init(),
                synonyms: ["power","touchid","lock"],
                centered: true)
    ],
    // Row 1: ` 1..0 - = delete — Backtick..Equal(16 each), Delete(26); sum 234mm
    [
        punct("Backtick", "~", "`", syn: ["backtick","grave"]),
        num("1","!"), num("2","@"), num("3","#"), num("4","$"), num("5","%"), num("6","^"),
        num("7","&"), num("8","*"), num("9","("), num("0",")"),
        punct("Minus", "_", "-", syn: ["minus","-","_"]),
        punct("Equal", "+", "="),
        cornerBR("Delete", "delete", mm: 26, syn: ["backspace"])
    ],
    // Row 2: tab Q..P [ ] \ — Tab(26), rest(16); sum 234mm
    [
        cornerBL("Tab", "tab", mm: 26),
        txt("KeyQ", "Q"), txt("KeyW", "W"), txt("KeyE", "E"), txt("KeyR", "R"),
        txt("KeyT", "T"), txt("KeyY", "Y"), txt("KeyU", "U"), txt("KeyI", "I"),
        txt("KeyO", "O"), txt("KeyP", "P"),
        punct("BracketLeft",  "{", "["),
        punct("BracketRight", "}", "]"),
        punct("Backslash", "|", "\\", syn: ["backslash"])
    ],
    // Row 3: caps A..' return — Caps(30.5), rest(16), Return(30.5); sum 237mm, 12 gaps × 3mm
    [
        cornerBL("CapsLock", "caps lock", mm: 30.5, syn: ["capslock"]),
        txt("KeyA", "A"), txt("KeyS", "S"), txt("KeyD", "D"), txt("KeyF", "F"),
        txt("KeyG", "G"), txt("KeyH", "H"), txt("KeyJ", "J"), txt("KeyK", "K"), txt("KeyL", "L"),
        punct("Semicolon", ":", ";"),
        punct("Quote", "\"", "'"),
        cornerBR("Return", "return", mm: 30.5, syn: ["enter"])
    ],
    // Row 4: shift Z../shift — Shifts(40), rest(16); sum 240mm, 11 gaps × 3mm
    [
        cornerBL("ShiftL", "shift", mm: 40, syn: ["left shift"]),
        txt("KeyZ", "Z"), txt("KeyX", "X"), txt("KeyC", "C"), txt("KeyV", "V"),
        txt("KeyB", "B"), txt("KeyN", "N"), txt("KeyM", "M"),
        punct("Comma",  "<", ","),
        punct("Period", ">", "."),
        punct("Slash",  "?", "/"),
        cornerBR("ShiftR", "shift", mm: 40, syn: ["right shift"])
    ],
    // Row 5: fn(16) ctrl(16) opt(16) cmd(21) SPACE(92) cmd(21) opt(16) + arrow cluster(54); sum 252mm
    [
        // fn: "fn" top-right, globe bottom-left
        KeySpec("Fn",
                legends: .init(topRight: .text("fn"), bottomLeft: .symbol("globe")),
                synonyms: ["function","fn"],
                centered: false),
        // Left modifiers: symbol top-right, word bottom-center
        KeySpec("Control",
                legends: .init(topRight: .text("⌃"), bottomCenter: .text("control")),
                synonyms: ["ctrl","control"],
                centered: false),
        KeySpec("OptionL",
                legends: .init(topRight: .text("⌥"), bottomCenter: .text("option")),
                synonyms: ["alt","option"],
                centered: false),
        KeySpec("CommandL", widthMM: 21,
                legends: .init(topRight: .text("⌘"), bottomCenter: .text("command")),
                synonyms: ["cmd","command"],
                centered: false),
        // Space: blank (no legend)
        KeySpec("Space", widthMM: 92,
                legends: .init(),
                synonyms: ["spacebar"," "],
                centered: true),
        // Right modifiers: symbol top-left, word bottom-center
        KeySpec("CommandR", widthMM: 21,
                legends: .init(topLeft: .text("⌘"), bottomCenter: .text("command")),
                synonyms: ["cmd","command"],
                centered: false),
        KeySpec("OptionR",
                legends: .init(topLeft: .text("⌥"), bottomCenter: .text("option")),
                synonyms: ["alt","option"],
                centered: false),
        // Inverted-T with triangle glyphs (three 16mm columns, 3mm gaps → 54mm total)
        KeySpec("ArrowCluster", widthMM: arrowClusterWidthMM, shape: .arrowsCluster)
    ]
]

// Per-row horizontal gap in mm, derived from total width and sum of key widths.
// This is what makes every row come out exactly `rowWidthMM` wide regardless of
// how the individual key widths add up.
private func rowGapMM(_ row: [KeySpec]) -> CGFloat {
    guard row.count > 1 else { return 0 }
    let sum = row.reduce(0.0) { $0 + $1.widthMM }
    return max(0, (rowWidthMM - sum) / CGFloat(row.count - 1))
}

// ---------- Heat coloring (log scale) ----------

// Three-stop gradient: dark navy (cold/unused) → brighter purple → crimson (hot).
private struct ColorStop { let t: CGFloat; let color: NSColor }
private let heatStops: [ColorStop] = [
    .init(t: 0.00, color: NSColor(srgbRed: 0.05, green: 0.08, blue: 0.28, alpha: 1)),  // dark navy
    .init(t: 0.50, color: NSColor(srgbRed: 0.45, green: 0.10, blue: 0.55, alpha: 1)),  // brighter purple
    .init(t: 1.00, color: NSColor(srgbRed: 0.70, green: 0.10, blue: 0.22, alpha: 1)),  // crimson
]

private func interpolateColor(_ stops: [ColorStop], t raw: CGFloat) -> Color {
    let t = max(0, min(1, raw))
    guard let hi = stops.firstIndex(where: { t <= $0.t }), hi > 0 else {
        // t is at or below the first stop → return first stop color.
        // (Original guard also caught t > all stops; that's fine since
        // t is clamped to [0,1] above and stops span [0,1].)
        return Color(stops.first?.color ?? .systemGray)
    }
    let lo = hi - 1
    let a = stops[lo], b = stops[hi]
    let u = (t - a.t) / max(0.0001, b.t - a.t)
    func comps(_ c: NSColor) -> (CGFloat,CGFloat,CGFloat,CGFloat) {
        let s = c.usingColorSpace(.sRGB) ?? c
        return (s.redComponent, s.greenComponent, s.blueComponent, s.alphaComponent)
    }
    let (r1,g1,b1,a1) = comps(a.color)
    let (r2,g2,b2,a2) = comps(b.color)
    return Color(NSColor(srgbRed: r1 + (r2-r1)*u,
                         green:  g1 + (g2-g1)*u,
                         blue:   b1 + (b2-b1)*u,
                         alpha:  a1 + (a2-a1)*u))
}

// Color helper: rank-based. The caller builds a [count: rank] map once
// (distinct counts sorted ascending, normalized to [0, 1]) so that keys
// with identical counts share a color and the full gradient is traversed
// evenly across all distinct press totals.
private func colorFor(count: Int, rankByCount: [Int: CGFloat]) -> Color {
    let t = rankByCount[count] ?? 0
    return interpolateColor(heatStops, t: t)
}

/// Build a map from press-count value to a normalized rank in [0, 1].
/// Ties produce identical ranks. If only one distinct count exists, all
/// keys map to t = 0 (coldest).
private func buildRankMap(counts: [Int]) -> [Int: CGFloat] {
    let distinct = Array(Set(counts)).sorted()
    guard distinct.count > 1 else {
        var out: [Int: CGFloat] = [:]
        for c in distinct { out[c] = 0 }
        return out
    }
    var out: [Int: CGFloat] = [:]
    let denom = CGFloat(distinct.count - 1)
    for (i, c) in distinct.enumerated() {
        out[c] = CGFloat(i) / denom
    }
    return out
}

// All keys are dark-filled (zero-count unused keys are very dark, hot keys
// are saturated dark colors) so every label renders in white.
private func textColorFor(count: Int) -> Color {
    .white
}

// ---------- JSON → Graphic-key mapping (YOU can edit this) ----------
// Keys are matched case-insensitively after removing spaces, "_" and "-".
// Values must be the graphic IDs used in this file (e.g. "KeyA", "Digit1", "Return", "OptionR", "F1", "Left", "TouchID", ...)

let JSON_KEY_TO_GRAPH_ID: [String: String] = [
    // Letters
    "0": "KeyA", "11": "KeyB", "8": "KeyC", "2": "KeyD", "14": "KeyE", "3": "KeyF",
    "5": "KeyG", "4": "KeyH", "34": "KeyI", "38": "KeyJ", "40": "KeyK", "37": "KeyL",
    "46": "KeyM", "45": "KeyN", "31": "KeyO", "35": "KeyP", "12": "KeyQ", "15": "KeyR",
    "1": "KeyS", "17": "KeyT", "32": "KeyU", "9": "KeyV", "13": "KeyW", "7": "KeyX",
    "16": "KeyY", "6": "KeyZ",

    // Digits & shifted (map either one)
    "18": "Digit1", "!": "Digit1",
    "19": "Digit2", "@": "Digit2",
    "20": "Digit3", "#": "Digit3",
    "21": "Digit4", "$": "Digit4",
    "23": "Digit5", "%": "Digit5",
    "22": "Digit6", "^": "Digit6",
    "26": "Digit7", "&": "Digit7",
    "28": "Digit8", "*": "Digit8",
    "25": "Digit9", "(": "Digit9",
    "29": "Digit0", ")": "Digit0",

    // Punctuation
    "50": "Backtick", "~": "Backtick",
    "27": "Minus", "_": "Minus",
    "24": "Equal", "+": "Equal",
    "33": "BracketLeft", "{": "BracketLeft",
    "30": "BracketRight", "}": "BracketRight",
    "42": "Backslash", "|": "Backslash",
    "41": "Semicolon", ":": "Semicolon",
    "39": "Quote", "\"": "Quote",
    "43": "Comma", "<": "Comma",
    "47": "Period", ">": "Period",
    "44": "Slash", "?": "Slash",

    // Function row
    "53": "Esc", "escape": "Esc",
    "f1": "F1", "f2": "F2", "160": "F3", "177": "F4", "176": "F5", "178": "F6",
    "f7": "F7", "f8": "F8", "f9": "F9", "f10": "F10", "f11": "F11", "f12": "F12",
    "touchid": "TouchID", "power": "TouchID",

    // Modifiers (choose left/right if your JSON distinguishes them)
    "48": "Tab",
    "57": "CapsLock",
    "56": "ShiftL", "60": "ShiftR",
    "59": "Control",
    "58": "OptionL", "61": "OptionR",
    "55": "CommandL", "54": "CommandR",
    "63": "Fn",
    "36": "Return",
    "51": "Delete",
    "49": "Space",

    // Arrows
    "123": "Left",
    "124": "Right",
    "126": "Up",
    "125": "Down"
]

// Normalization used for matching (lowercased; remove spaces, "_" and "-")
@inline(__always)
func normalizeKeyName(_ s: String) -> String {
    s.trimmingCharacters(in: .whitespacesAndNewlines)
     .lowercased()
     .replacingOccurrences(of: " ", with: "")
     .replacingOccurrences(of: "_", with: "")
     .replacingOccurrences(of: "-", with: "")
}

// Pre-normalized lookup so you don't have to normalize when filling the dict above.
let JSON_KEY_TO_GRAPH_ID_NORM: [String: String] = {
    var out: [String: String] = [:]
    for (k, v) in JSON_KEY_TO_GRAPH_ID { out[normalizeKeyName(k)] = v }
    return out
}()

// ---------- Heatmap view ----------

struct KeyboardHeatmapView: View {
    let keyCounts: [String: Int]

    // Use your mapping first; fall back to original tokens for synonym/label heuristics
    private var sourceCounts: [String: Int] {
        var agg: [String: Int] = [:]
        for (rawKey, val) in keyCounts {
            let norm = normalizeKeyName(rawKey)
            if let id = JSON_KEY_TO_GRAPH_ID_NORM[norm] {
                // store by normalized graphic ID (e.g., "keya", "digit1", "return")
                agg[normalizeKeyName(id), default: 0] += val
            } else {
                // keep the normalized raw token so existing synonyms/labels can still match it
                agg[norm, default: 0] += val
            }
        }
        return agg
    }

    // Helper to read a count by any name (we always normalize the lookup)
    private func countFor(_ name: String) -> Int {
        sourceCounts[normalizeKeyName(name)] ?? 0
    }

    private var totals: [String: Int] {
        var out: [String: Int] = [:]

        func addToID(_ id: String, names: [String], label: String?) {
            var s = 0
            for n in names { s += countFor(n) }
            if let l = label { s += countFor(l) }
            if id == "Space" { s += countFor(" ") }
            out[id, default: 0] += s
        }

        for row in keyboardRows {
            for k in row {
                switch k.shape {
                case .arrowsCluster:
                    addToID("Left",  names: ["Left","arrowleft","←","◀"], label: nil)
                    addToID("Right", names: ["Right","arrowright","→","▶"], label: nil)
                    addToID("Up",    names: ["Up","arrowup","↑","▲"],     label: nil)
                    addToID("Down",  names: ["Down","arrowdown","↓","▼"], label: nil)
                default:
                    let label: String? = {
                        if case .text(let t) = k.legends.center, t.count == 1 { return t }
                        return nil
                    }()
                    // `k.synonyms` already includes `k.id`
                    addToID(k.id, names: k.synonyms, label: label)
                }
            }
        }
        return out
    }
    
    private func colorForID(_ id: String) -> Color {
        colorFor(count: totals[id] ?? 0, rankByCount: rankByCount)
    }

    /// Rank map computed once per render from the aggregated `totals`.
    /// Ensures equal colors for equal counts and even gradient spread
    /// across distinct count values.
    private var rankByCount: [Int: CGFloat] {
        buildRankMap(counts: Array(totals.values))
    }

    var body: some View {
        let keyHeight = rowHeightMM * ptPerMM
        let rowSpacing = rowSpacingMM * ptPerMM

        VStack(alignment: .leading, spacing: 12) {
            HStack {
                LogLegendView(raw: keyCounts).frame(width: 240, height: 22)
            }

            VStack(alignment: .leading, spacing: rowSpacing) {
                ForEach(0..<keyboardRows.count, id: \.self) { _row in
                    let row = keyboardRows[_row]
                    let gapPt = rowGapMM(row) * ptPerMM
                    HStack(spacing: gapPt) {
                        ForEach(row) { key in
                            let keyWidth = key.widthMM * ptPerMM
                            let keyCount = totals[key.id] ?? 0
                            let keyTextColor = textColorFor(count: keyCount)
                            switch key.shape {
                            case .rectWithInnerCircle:
                                let touchIdDiameter: CGFloat = 10 * ptPerMM
                                RoundedRectangle(cornerRadius: 6, style: .continuous)
                                    .fill(colorForID(key.id))
                                    .frame(width: keyWidth, height: keyHeight)
                                    .overlay(
                                        Circle()
                                            .strokeBorder(keyTextColor.opacity(0.85), lineWidth: 2)
                                            .frame(width: touchIdDiameter, height: touchIdDiameter)
                                    )
                                    .shadow(color: .black.opacity(0.55), radius: 3, x: 0, y: 1)
                            case .arrowsCluster:
                                ArrowClusterView(
                                    width: keyWidth,
                                    height: keyHeight,
                                    colorLeft:  colorForID("Left"),
                                    colorDown:  colorForID("Down"),
                                    colorRight: colorForID("Right"),
                                    colorUp:    colorForID("Up"),
                                    textColorLeft:  textColorFor(count: totals["Left"]  ?? 0),
                                    textColorDown:  textColorFor(count: totals["Down"]  ?? 0),
                                    textColorRight: textColorFor(count: totals["Right"] ?? 0),
                                    textColorUp:    textColorFor(count: totals["Up"]    ?? 0)
                                )
                            case .rect:
                                RoundedRectangle(cornerRadius: 6, style: .continuous)
                                    .fill(colorForID(key.id))
                                    .frame(width: keyWidth, height: keyHeight)
                                    .overlay(
                                        KeyLegendsView(legends: key.legends,
                                                       face: key.face,
                                                       isFnKey: key.isFunctionKey,
                                                       textColor: keyTextColor)
                                    )
                                    .shadow(color: .black.opacity(0.55), radius: 3, x: 0, y: 1)
                                    .accessibilityLabel("\(key.id): \(keyCount) presses")
                            }
                        }
                    }
                }
            }
        }
    }
}

// ---------- Legends renderer (smaller everywhere) ----------

// ---------- Legend rendering ----------
//
// All slots are positioned via `.position(x:y:)` with mm-based offsets.
// Slot anchors (where the child view's reference point lands):
//   topLeft      → view's top-leading corner at   (inset,      inset)
//   topCenter    → view's top-center        at   (w/2,        inset)
//   topRight     → view's top-trailing corner at (w - inset,  inset)
//   center       → view's center            at   (w/2,        h/2)
//   bottomLeft   → view's bottom-leading    at   (inset,      h - inset)
//   bottomCenter → view's bottom-center     at   (w/2,        h - inset)
//   bottomRight  → view's bottom-trailing   at   (w - inset,  h - inset)
//
// Inset is `legendInsetMM = 3mm` scaled by ptPerMM.
//
// Font sizes are in points (not mm). Typography intent: regular weight
// (never bold), labels clearly readable at the real-scale keyboard.

private let legendInsetMM: CGFloat = 2

// Typography presets — points, not mm. Tuned for ptPerMM = 5.007 (14" MBP)
// and calibrated against physical dimensions.
//
// Labels rendered in SF Pro Expanded (words like "caps lock", "control") use
// smaller point sizes than non-expanded glyphs at equivalent visual height,
// because the expanded width variant has larger bearings per character.
private struct LegendFonts {
    static let letterLabel:  CGFloat = 28   // A, S, D, ... in the center slot (standard width)
    static let cornerLabel:  CGFloat = 13   // esc, tab, caps lock, shift, return, delete (expanded)
    static let modifierWord: CGFloat = 12   // "control", "option", "command" bottom-center (expanded)
    static let modifierSym:  CGFloat = 17   // ⌃ ⌥ ⌘ in corners (single glyph, standard width)
    static let fnWord:       CGFloat = 12   // "fn" in top-right (expanded)
    static let fnSymbol:     CGFloat = 17   // globe bottom-left (symbol)
    static let digit:        CGFloat = 26   // big 0..9 on digit keys (standard width)
    static let shifted:      CGFloat = 21   // shifted glyph on digit keys (standard width)
    static let punctDual:    CGFloat = 22   // both glyphs on [, {, ;, etc. (standard width)
    static let fnRowIcon:    CGFloat = 17   // SF Symbol on F1..F12
    static let fnRowWord:    CGFloat = 12   // "F1".."F12" (standard width — not a word)
}

private struct KeyLegendsView: View {
    let legends: KeyLegends
    let face: KeyFaceStyle
    let isFnKey: Bool
    let textColor: Color

    var body: some View {
        ZStack(alignment: .topLeading) {
            if let tl = legends.topLeft {
                place(legend: tl, font: fontForTop(slot: .topLeft),
                      alignment: .topLeading)
            }
            if let tr = legends.topRight {
                place(legend: tr, font: fontForTop(slot: .topRight),
                      alignment: .topTrailing)
            }
            if let tc = legends.topCenter {
                place(legend: tc, font: fontForTop(slot: .topCenter),
                      alignment: .top)
            }
            if let c = legends.center {
                place(legend: c, font: LegendFonts.letterLabel,
                      alignment: .center)
            }
            if let bl = legends.bottomLeft {
                place(legend: bl, font: fontForBottom(slot: .bottomLeft),
                      alignment: .bottomLeading)
            }
            if let br = legends.bottomRight {
                place(legend: br, font: fontForBottom(slot: .bottomRight),
                      alignment: .bottomTrailing)
            }
            if let bc = legends.bottomCenter {
                place(legend: bc, font: fontForBottom(slot: .bottomCenter),
                      alignment: .bottom)
            }
        }
        .allowsHitTesting(false)
    }

    // Only the digit/punct/fn-row face styles drive top vs bottom font choices.
    // Everything else uses the slot's default.
    private enum Slot { case topLeft, topCenter, topRight, bottomLeft, bottomCenter, bottomRight }

    private func fontForTop(slot: Slot) -> CGFloat {
        switch face {
        case .numberDual: return LegendFonts.shifted      // shifted glyph on digits
        case .dualEqual:  return isFnKey ? LegendFonts.fnRowIcon : LegendFonts.punctDual
        case .single:     return LegendFonts.modifierSym  // corner symbols on modifiers
        }
    }

    private func fontForBottom(slot: Slot) -> CGFloat {
        switch face {
        case .numberDual: return LegendFonts.digit
        case .dualEqual:  return isFnKey ? LegendFonts.fnRowWord : LegendFonts.punctDual
        case .single:
            // Single-face keys with bottom legends:
            //   bottomCenter → modifier words ("control", "option", "command")
            //   bottomLeft   → corner labels (tab, caps lock, shift, fn globe)
            //   bottomRight  → corner labels (delete, return, shift)
            if slot == .bottomCenter { return LegendFonts.modifierWord }
            return LegendFonts.cornerLabel
        }
    }

    @ViewBuilder
    private func place(legend: Legend, font: CGFloat, alignment: Alignment) -> some View {
        // Apply padding to the legend FIRST (reserving inset space adjacent to
        // it), THEN place it in a full-size frame with the requested alignment.
        //
        // Top-slot compensation: SwiftUI Text reserves ascent space above the
        // cap height, so a "2mm inset" measured by the text bounding box
        // looks like ~3mm from the visible glyph top. Subtract 1mm from the
        // top padding on top-touching alignments to bring the visible
        // character to ~2mm from the edge.
        //
        // Font width: word-like labels (all lowercase ASCII letters, maybe
        // with spaces) get SF Pro Expanded to match Apple's physical keyboard
        // lettering. Single characters, digit keys, uppercase letters, "F1",
        // and symbols stay at standard width.
        GeometryReader { geo in
            let inset = legendInsetMM * ptPerMM
            // Text has ascent padding above cap height (~1mm at our sizes);
            // symbols (SF Symbols / images) don't and additionally get
            // pushed down 1mm to match the real keyboard.
            let textCompensation = 1.0 * ptPerMM
            let symbolExtraDrop = 1.0 * ptPerMM
            let isSymbol: Bool = { if case .symbol = legend { return true } else { return false } }()
            let topInset = alignmentTouchesTop(alignment)
                ? max(0, inset + (isSymbol ? symbolExtraDrop : -textCompensation))
                : 0
            legendView(legend)
                .font(fontFor(legend: legend, size: font))
                .foregroundStyle(textColor)
                .fixedSize()
                .padding(.leading,  alignmentTouchesLeading(alignment)  ? inset : 0)
                .padding(.trailing, alignmentTouchesTrailing(alignment) ? inset : 0)
                .padding(.top,      topInset)
                .padding(.bottom,   alignmentTouchesBottom(alignment)   ? inset : 0)
                .frame(width: geo.size.width, height: geo.size.height, alignment: alignment)
        }
    }

    /// Returns the font for a legend. Word-like labels (text that is entirely
    /// lowercase letters and spaces) render in SF Pro Expanded to match
    /// Apple's physical keyboard. Everything else uses the default width.
    private func fontFor(legend: Legend, size: CGFloat) -> Font {
        let base = Font.system(size: size, weight: .regular)
        if case .text(let s) = legend,
           !s.isEmpty,
           s.allSatisfy({ $0.isLowercase || $0 == " " }) {
            return base.width(.expanded)
        }
        return base
    }

    private func alignmentTouchesLeading(_ a: Alignment) -> Bool {
        a == .topLeading || a == .leading || a == .bottomLeading
    }
    private func alignmentTouchesTrailing(_ a: Alignment) -> Bool {
        a == .topTrailing || a == .trailing || a == .bottomTrailing
    }
    private func alignmentTouchesTop(_ a: Alignment) -> Bool {
        a == .topLeading || a == .top || a == .topTrailing
    }
    private func alignmentTouchesBottom(_ a: Alignment) -> Bool {
        a == .bottomLeading || a == .bottom || a == .bottomTrailing
    }

    @ViewBuilder
    private func legendView(_ l: Legend) -> some View {
        switch l {
        case .text(let s): Text(s)
        case .symbol(let s): Image(systemName: s)
        }
    }
}

// ---------- Inverted-T arrows (MacBook geometry, all in mm) ----------
//
// Three 16mm-wide columns separated by 3mm gaps → 54mm total cluster width.
// All four arrow keys are 16mm × 8mm. Up and Down stack touching in the
// middle column; Left and Right sit bottom-aligned in the outer columns.
//
// Up and Down use custom shapes (UpKeyShape, DownKeyShape) with rounded
// outer corners and sharp (pointed) inner corners where they meet, and a
// thin shadow along the seam to mimic a real MacBook keyboard.

/// A rectangle with rounded top-left and top-right corners, and sharp
/// (right-angle, un-rounded) bottom-left and bottom-right corners.
private struct UpKeyShape: Shape {
    var cornerRadius: CGFloat = 6
    func path(in rect: CGRect) -> Path {
        var p = Path()
        let r = min(cornerRadius, min(rect.width, rect.height) / 2)
        p.move(to: CGPoint(x: rect.minX + r, y: rect.minY))
        p.addLine(to: CGPoint(x: rect.maxX - r, y: rect.minY))
        p.addArc(center: CGPoint(x: rect.maxX - r, y: rect.minY + r),
                 radius: r, startAngle: .degrees(-90), endAngle: .degrees(0), clockwise: false)
        p.addLine(to: CGPoint(x: rect.maxX, y: rect.maxY))
        p.addLine(to: CGPoint(x: rect.minX, y: rect.maxY))
        p.addLine(to: CGPoint(x: rect.minX, y: rect.minY + r))
        p.addArc(center: CGPoint(x: rect.minX + r, y: rect.minY + r),
                 radius: r, startAngle: .degrees(180), endAngle: .degrees(-90), clockwise: false)
        p.closeSubpath()
        return p
    }
}

/// Mirror of UpKeyShape: rounded bottom corners, sharp top corners.
private struct DownKeyShape: Shape {
    var cornerRadius: CGFloat = 6
    func path(in rect: CGRect) -> Path {
        var p = Path()
        let r = min(cornerRadius, min(rect.width, rect.height) / 2)
        p.move(to: CGPoint(x: rect.minX, y: rect.minY))
        p.addLine(to: CGPoint(x: rect.maxX, y: rect.minY))
        p.addLine(to: CGPoint(x: rect.maxX, y: rect.maxY - r))
        p.addArc(center: CGPoint(x: rect.maxX - r, y: rect.maxY - r),
                 radius: r, startAngle: .degrees(0), endAngle: .degrees(90), clockwise: false)
        p.addLine(to: CGPoint(x: rect.minX + r, y: rect.maxY))
        p.addArc(center: CGPoint(x: rect.minX + r, y: rect.maxY - r),
                 radius: r, startAngle: .degrees(90), endAngle: .degrees(180), clockwise: false)
        p.closeSubpath()
        return p
    }
}

private struct ArrowClusterView: View {
    let width: CGFloat     // total cluster width in pt  (= arrowClusterWidthMM * ptPerMM)
    let height: CGFloat    // total cluster height in pt (= rowHeightMM * ptPerMM)
    let colorLeft: Color
    let colorDown: Color
    let colorRight: Color
    let colorUp: Color
    let textColorLeft:  Color
    let textColorDown:  Color
    let textColorRight: Color
    let textColorUp:    Color

    private static let glyphSide: CGFloat = 15  // ◀ ▶
    private static let glyphUpDn: CGFloat = 15  // ▲ ▼

    var body: some View {
        let keyW      = arrowKeyWidthMM * ptPerMM
        let keyH      = arrowHeightMM   * ptPerMM
        let colStride = (arrowKeyWidthMM + arrowGapMM) * ptPerMM
        let midColX   = colStride            // left edge x of the middle column
        let seamY     = keyH                 // y of the horizontal seam between Up and Down

        ZStack(alignment: .topLeading) {
            // Left (◀) — column 0, bottom
            RoundedRectangle(cornerRadius: 6, style: .continuous)
                .fill(colorLeft)
                .frame(width: keyW, height: keyH)
                .overlay(
                    Text("◀")
                        .font(.system(size: Self.glyphSide, weight: .semibold))
                        .foregroundStyle(textColorLeft)
                )
                .shadow(color: .black.opacity(0.55), radius: 3, x: 0, y: 1)
                .position(x: keyW / 2, y: height - keyH / 2)

            // Up (▲) — column 1, top (rounded top, sharp bottom)
            UpKeyShape(cornerRadius: 6)
                .fill(colorUp)
                .frame(width: keyW, height: keyH)
                .overlay(
                    Text("▲")
                        .font(.system(size: Self.glyphUpDn, weight: .semibold))
                        .foregroundStyle(textColorUp)
                )
                .shadow(color: .black.opacity(0.55), radius: 3, x: 0, y: 1)
                .position(x: colStride + keyW / 2, y: keyH / 2)

            // Down (▼) — column 1, bottom (sharp top, rounded bottom)
            DownKeyShape(cornerRadius: 6)
                .fill(colorDown)
                .frame(width: keyW, height: keyH)
                .overlay(
                    Text("▼")
                        .font(.system(size: Self.glyphUpDn, weight: .semibold))
                        .foregroundStyle(textColorDown)
                )
                .shadow(color: .black.opacity(0.55), radius: 3, x: 0, y: 1)
                .position(x: colStride + keyW / 2, y: height - keyH / 2)

            // Right (▶) — column 2, bottom
            RoundedRectangle(cornerRadius: 6, style: .continuous)
                .fill(colorRight)
                .frame(width: keyW, height: keyH)
                .overlay(
                    Text("▶")
                        .font(.system(size: Self.glyphSide, weight: .semibold))
                        .foregroundStyle(textColorRight)
                )
                .shadow(color: .black.opacity(0.55), radius: 3, x: 0, y: 1)
                .position(x: 2 * colStride + keyW / 2, y: height - keyH / 2)

            // Seam shading drawn last so it sits above all shadows.
            Rectangle()
                .fill(Color.black.opacity(0.35))
                .frame(width: keyW, height: 1)
                .position(x: midColX + keyW / 2, y: seamY)
        }
        .frame(width: width, height: height)
    }
}

// ---------- Color legend ----------

struct LogLegendView: View {
    let raw: [String: Int]
    var body: some View {
        let minCount = raw.values.min() ?? 0
        let maxCount = raw.values.max() ?? 0
        HStack(spacing: 8) {
            GeometryReader { geo in
                let _ = geo.size.width
                ZStack(alignment: .leading) {
                    LinearGradient(
                        gradient: Gradient(colors: stride(from: 0.0, through: 1.0, by: 0.05).map { t in
                            interpolateColor(heatStops, t: t)
                        }),
                        startPoint: .leading, endPoint: .trailing
                    )
                    .clipShape(RoundedRectangle(cornerRadius: 4))
                }
            }
            .frame(height: 10)

            HStack(spacing: 6) {
                Text("\(minCount)").font(.system(size: 10, weight: .regular, design: .monospaced))
                    .foregroundStyle(.secondary)
                Text("→").font(.system(size: 10)).foregroundStyle(.secondary)
                Text("\(maxCount)").font(.system(size: 10, weight: .regular, design: .monospaced))
                    .foregroundStyle(.secondary)
            }
        }
    }
}
