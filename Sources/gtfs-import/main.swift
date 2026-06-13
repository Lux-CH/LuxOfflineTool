import Foundation
import GRDB
import ZIPFoundation

let datasetURL = URL(string: "https://data.opentransportdata.swiss/en/dataset/timetable-2026-gtfs2020/permalink")!

func log(_ message: String) {
    FileHandle.standardError.write(Data(("• " + message + "\n").utf8))
}

func sizeMB(_ url: URL) -> String {
    let bytes = (try? FileManager.default.attributesOfItem(atPath: url.path)[.size] as? Int) ?? 0 ?? 0
    return String(format: "%.1f MB", Double(bytes) / 1_048_576)
}

do {
    let outPath = CommandLine.arguments.count > 1
        ? CommandLine.arguments[1]
        : FileManager.default.currentDirectoryPath + "/offline.sqlite"
    let outURL = URL(fileURLWithPath: outPath)

    let work = URL(fileURLWithPath: NSTemporaryDirectory())
        .appendingPathComponent("gtfs-import-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: work, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: work) }

    log("Downloading national GTFS feed…")
    let (downloaded, response) = try await URLSession.shared.download(from: datasetURL)
    if let http = response as? HTTPURLResponse, !(200...299).contains(http.statusCode) {
        throw NSError(domain: "gtfs-import", code: http.statusCode,
                      userInfo: [NSLocalizedDescriptionKey: "Download failed: HTTP \(http.statusCode)"])
    }
    let zipURL = work.appendingPathComponent("gtfs.zip")
    try FileManager.default.moveItem(at: downloaded, to: zipURL)
    log("Downloaded \(sizeMB(zipURL)).")

    log("Unzipping…")
    let extractDir = work.appendingPathComponent("extract", isDirectory: true)
    try FileManager.default.createDirectory(at: extractDir, withIntermediateDirectories: true)
    try FileManager.default.unzipItem(at: zipURL, to: extractDir)
    try? FileManager.default.removeItem(at: zipURL)

    log("Importing (region: \(OfflineRegion.minLat)…\(OfflineRegion.maxLat) / \(OfflineRegion.minLon)…\(OfflineRegion.maxLon))…")
    for ext in ["", "-wal", "-shm"] {
        try? FileManager.default.removeItem(at: URL(fileURLWithPath: outPath + ext))
    }
    let store = try OfflineGTFSStore(url: outURL)
    var lastPct = -1
    try GTFSImporter.run(extractedDir: extractDir, into: store) { p in
        let pct = Int(p * 100)
        if pct != lastPct && pct % 5 == 0 { lastPct = pct; log("  \(pct)%") }
    }

    log("Compacting…")
    try store.dbQueue.writeWithoutTransaction { db in
        try db.execute(sql: "PRAGMA wal_checkpoint(TRUNCATE)")
        try db.execute(sql: "VACUUM")
    }

    let stops = store.stopCount()
    guard stops > 0 else {
        throw NSError(domain: "gtfs-import", code: 1,
                      userInfo: [NSLocalizedDescriptionKey: "No stops imported — feed empty or region wrong?"])
    }
    for ext in ["-wal", "-shm"] {
        try? FileManager.default.removeItem(at: URL(fileURLWithPath: outPath + ext))
    }

    log("Done: \(stops) stops → \(outPath) (\(sizeMB(outURL)))")
    print(outPath)
} catch {
    log("ERROR: \(error.localizedDescription)")
    exit(1)
}
