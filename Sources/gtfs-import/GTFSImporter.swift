//
//  GTFSImporter.swift
//  Lux
//
//  Imports an extracted GTFS feed into `OfflineGTFSStore`, keeping only the
//  region defined by `OfflineRegion`. Trips that merely pass through the box are
//  kept whole (all their stops), so trip detail views stay complete even when a
//  trip continues outside the box.
//
//  Pipeline (streaming, bounded memory):
//   1. stops.txt → insert ALL stops straight into SQLite (batched); keep only a
//      hashed set of in-region stop ids in memory
//   2. stop_times pass 1 → hashed set of trip ids that touch the region
//   3. trips.txt → kept trips, collecting route + service ids
//   4. routes.txt / calendar.txt / calendar_dates.txt → kept rows
//   5. stop_times pass 2 → insert kept stop_times
//
//  Memory notes: stops live in the DB, not the heap, so there is no in-memory
//  stop dictionary. The two large in-memory sets (in-region stop ids, kept trip
//  ids) hold 64-bit FNV-1a *hashes* rather than the (long) id strings — they are
//  used for membership tests only, and the 64-bit collision probability over a
//  national feed (~10⁻⁸) is negligible. This keeps peak RAM to a few MB even on
//  the multi-GB stop_times pass.
//

import Foundation
import GRDB

enum GTFSImportError: Error { case missingFile(String) }

/// FNV-1a 64-bit hash of a string's UTF-8 bytes. Stable within a run; used to
/// hold id membership sets compactly (no string storage) during import.
@inline(__always)
func gtfsHash(_ s: String) -> UInt64 {
    var h: UInt64 = 0xcbf2_9ce4_8422_2325
    for b in s.utf8 { h = (h ^ UInt64(b)) &* 0x0000_0100_0000_01b3 }
    return h
}

struct GTFSImporter {

    /// Rows per insert transaction. Bounds the transient array of boxed values.
    private static let batchSize = 20_000

    /// Run the import. `progress` receives 0...1 for the import stage only.
    /// `extractedDir` is the directory containing the GTFS .txt files.
    static func run(extractedDir: URL, into store: OfflineGTFSStore,
                    progress: @escaping (Double) -> Void) throws {
        let stopsURL = try locate("stops.txt", in: extractedDir)
        let stopTimesURL = try locate("stop_times.txt", in: extractedDir)
        let tripsURL = try locate("trips.txt", in: extractedDir)
        let routesURL = try locate("routes.txt", in: extractedDir)

        // --- Phase 1: stops.txt → insert ALL stops, collect in-region hashes -
        // Stops live in the DB (small table); we only keep a hashed membership
        // set of the in-region ids for Phase 2's filter.
        var inRegion = Set<UInt64>()
        var stopBatch = [[DatabaseValueConvertible?]]()
        stopBatch.reserveCapacity(batchSize)
        func flushStops() throws {
            guard !stopBatch.isEmpty else { return }
            try store.dbQueue.write { db in
                let stmt = try db.makeStatement(sql: """
                    INSERT OR REPLACE INTO stop
                        (id, norm_id, name, search_name, lat, lon, parent_station, location_type, level)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    """)
                for r in stopBatch { try stmt.execute(arguments: StatementArguments(r)) }
            }
            stopBatch.removeAll(keepingCapacity: true)
        }
        try forEachRow(stopsURL) { cols, row in
            guard let id = cols.value(row, "stop_id"),
                  let latS = cols.value(row, "stop_lat"), let lat = Double(latS),
                  let lonS = cols.value(row, "stop_lon"), let lon = Double(lonS) else { return }
            let name = cols.value(row, "stop_name") ?? id
            stopBatch.append([
                id, OfflineGTFSStore.normalize(id: id), name, OfflineGTFSStore.fold(name),
                lat, lon, cols.value(row, "parent_station") ?? "",
                Int(cols.value(row, "location_type") ?? "0") ?? 0,
                Double(cols.value(row, "level_id") ?? "") ?? 0
            ])
            if OfflineRegion.contains(lat: lat, lon: lon) { inRegion.insert(gtfsHash(id)) }
            if stopBatch.count >= batchSize { try flushStops() }
        }
        try flushStops()
        progress(0.05)

        // --- Phase 2: stop_times pass 1 → kept trip ids (hashed) -------------
        var keptTrips = Set<UInt64>()
        try forEachRow(stopTimesURL, progress: { progress(0.05 + 0.40 * $0) }) { cols, row in
            guard let stopId = cols.value(row, "stop_id"), inRegion.contains(gtfsHash(stopId)),
                  let tripId = cols.value(row, "trip_id") else { return }
            keptTrips.insert(gtfsHash(tripId))
        }
        inRegion.removeAll()
        progress(0.45)

        // --- Phase 3: trips.txt → kept trips, collect routes + services ------
        var keptRoutes = Set<String>()
        var keptServices = Set<String>()
        try store.dbQueue.write { db in
            let stmt = try db.makeStatement(sql:
                "INSERT OR REPLACE INTO trip (id, route_id, service_id, headsign) VALUES (?,?,?,?)")
            try forEachRow(tripsURL) { cols, row in
                guard let tripId = cols.value(row, "trip_id"), keptTrips.contains(gtfsHash(tripId)),
                      let routeId = cols.value(row, "route_id"),
                      let serviceId = cols.value(row, "service_id") else { return }
                keptRoutes.insert(routeId); keptServices.insert(serviceId)
                try stmt.execute(arguments: [tripId, routeId, serviceId, cols.value(row, "trip_headsign")])
            }
        }

        // --- Phase 4: routes.txt --------------------------------------------
        try store.dbQueue.write { db in
            let stmt = try db.makeStatement(sql:
                "INSERT OR REPLACE INTO route (id, short_name, route_type, agency_id, color) VALUES (?,?,?,?,?)")
            try forEachRow(routesURL) { cols, row in
                guard let routeId = cols.value(row, "route_id"), keptRoutes.contains(routeId) else { return }
                try stmt.execute(arguments: [
                    routeId,
                    cols.value(row, "route_short_name") ?? cols.value(row, "route_long_name") ?? "",
                    Int(cols.value(row, "route_type") ?? "3") ?? 3,
                    cols.value(row, "agency_id") ?? "",
                    cols.value(row, "route_color")
                ])
            }
        }

        // --- Phase 4b: calendars --------------------------------------------
        try importCalendars(dir: extractedDir, keptServices: keptServices, store: store)
        progress(0.50)

        // --- Phase 5: stop_times pass 2 → insert kept stop_times -------------
        // All stops are already in the DB (Phase 1), so we don't track referenced
        // stops here.
        var batch = [[DatabaseValueConvertible?]]()
        batch.reserveCapacity(batchSize)
        func flush() throws {
            guard !batch.isEmpty else { return }
            try store.dbQueue.write { db in
                let stmt = try db.makeStatement(sql: """
                    INSERT INTO stop_time (trip_id, stop_id, seq, arr_sec, dep_sec, headsign, pickup, dropoff)
                    VALUES (?,?,?,?,?,?,?,?)
                    """)
                for r in batch { try stmt.execute(arguments: StatementArguments(r)) }
            }
            batch.removeAll(keepingCapacity: true)
        }
        try forEachRow(stopTimesURL, progress: { progress(0.50 + 0.45 * $0) }) { cols, row in
            guard let tripId = cols.value(row, "trip_id"), keptTrips.contains(gtfsHash(tripId)),
                  let stopId = cols.value(row, "stop_id"),
                  let seqS = cols.value(row, "stop_sequence"), let seq = Int(seqS) else { return }
            batch.append([
                tripId, stopId, seq,
                gtfsSeconds(cols.value(row, "arrival_time")),
                gtfsSeconds(cols.value(row, "departure_time")),
                cols.value(row, "stop_headsign"),
                Int(cols.value(row, "pickup_type") ?? "0") ?? 0,
                Int(cols.value(row, "drop_off_type") ?? "0") ?? 0
            ])
            if batch.count >= batchSize { try flush() }
        }
        try flush()

        // --- Done: record import date ---------------------------------------
        try store.dbQueue.write { db in
            try db.execute(sql: "INSERT OR REPLACE INTO meta (key, value) VALUES ('import_date', ?)",
                           arguments: [String(Date().timeIntervalSince1970)])
        }
        progress(1.0)
    }

    // MARK: - Calendars

    private static func importCalendars(dir: URL, keptServices: Set<String>, store: OfflineGTFSStore) throws {
        if let calURL = try? locate("calendar.txt", in: dir) {
            try store.dbQueue.write { db in
                let stmt = try db.makeStatement(sql: """
                    INSERT OR REPLACE INTO calendar
                        (service_id, mon, tue, wed, thu, fri, sat, sun, start_date, end_date)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                    """)
                try forEachRow(calURL) { cols, row in
                    guard let sid = cols.value(row, "service_id"), keptServices.contains(sid) else { return }
                    func flag(_ c: String) -> Int { Int(cols.value(row, c) ?? "0") ?? 0 }
                    try stmt.execute(arguments: [
                        sid, flag("monday"), flag("tuesday"), flag("wednesday"), flag("thursday"),
                        flag("friday"), flag("saturday"), flag("sunday"),
                        Int(cols.value(row, "start_date") ?? "0") ?? 0,
                        Int(cols.value(row, "end_date") ?? "99999999") ?? 99999999
                    ])
                }
            }
        }
        if let datesURL = try? locate("calendar_dates.txt", in: dir) {
            try store.dbQueue.write { db in
                let stmt = try db.makeStatement(sql:
                    "INSERT INTO calendar_date (service_id, date, exception) VALUES (?,?,?)")
                try forEachRow(datesURL) { cols, row in
                    guard let sid = cols.value(row, "service_id"), keptServices.contains(sid),
                          let dateS = cols.value(row, "date"), let date = Int(dateS),
                          let excS = cols.value(row, "exception_type"), let exc = Int(excS) else { return }
                    try stmt.execute(arguments: [sid, date, exc])
                }
            }
        }
    }

    // MARK: - Row iteration

    /// Reads a GTFS CSV, parsing the header then calling `body(columns, row)` per data row.
    private static func forEachRow(_ url: URL, progress: ((Double) -> Void)? = nil,
                                   _ body: (CSVColumns, [String]) throws -> Void) throws {
        var columns: CSVColumns?
        try CSVReader.parse(url: url, onProgress: progress) { record in
            if columns == nil { columns = CSVColumns(header: record); return true }
            try body(columns!, record)
            return true
        }
    }

    private static func locate(_ name: String, in dir: URL) throws -> URL {
        let direct = dir.appendingPathComponent(name)
        if FileManager.default.fileExists(atPath: direct.path) { return direct }
        // Some feeds nest files in a subfolder — search one level deep.
        if let items = try? FileManager.default.contentsOfDirectory(at: dir, includingPropertiesForKeys: nil) {
            for item in items where item.hasDirectoryPath {
                let nested = item.appendingPathComponent(name)
                if FileManager.default.fileExists(atPath: nested.path) { return nested }
            }
        }
        throw GTFSImportError.missingFile(name)
    }
}

/// "HH:MM:SS" → seconds past service midnight (handles values ≥ 24:00:00). nil if empty/invalid.
func gtfsSeconds(_ value: String?) -> Int? {
    guard let value, !value.isEmpty else { return nil }
    let parts = value.split(separator: ":")
    guard parts.count == 3, let h = Int(parts[0]), let m = Int(parts[1]), let s = Int(parts[2]) else { return nil }
    return h * 3600 + m * 60 + s
}
