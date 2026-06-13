import Foundation
import GRDB

enum GTFSImportError: Error { case missingFile(String) }

@inline(__always)
func gtfsHash(_ s: String) -> UInt64 {
    var h: UInt64 = 0xcbf2_9ce4_8422_2325
    for b in s.utf8 { h = (h ^ UInt64(b)) &* 0x0000_0100_0000_01b3 }
    return h
}

struct GTFSImporter {

    private static let batchSize = 20_000

    static func run(extractedDir: URL, into store: OfflineGTFSStore,
                    progress: @escaping (Double) -> Void) throws {
        let stopsURL = try locate("stops.txt", in: extractedDir)
        let stopTimesURL = try locate("stop_times.txt", in: extractedDir)
        let tripsURL = try locate("trips.txt", in: extractedDir)
        let routesURL = try locate("routes.txt", in: extractedDir)

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

        var keptTrips = Set<UInt64>()
        try forEachRow(stopTimesURL, progress: { progress(0.05 + 0.40 * $0) }) { cols, row in
            guard let stopId = cols.value(row, "stop_id"), inRegion.contains(gtfsHash(stopId)),
                  let tripId = cols.value(row, "trip_id") else { return }
            keptTrips.insert(gtfsHash(tripId))
        }
        inRegion.removeAll()
        progress(0.45)

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

        try importCalendars(dir: extractedDir, keptServices: keptServices, store: store)
        progress(0.50)

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

        try store.dbQueue.write { db in
            try db.execute(sql: "INSERT OR REPLACE INTO meta (key, value) VALUES ('import_date', ?)",
                           arguments: [String(Date().timeIntervalSince1970)])
        }
        progress(1.0)
    }

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
        if let items = try? FileManager.default.contentsOfDirectory(at: dir, includingPropertiesForKeys: nil) {
            for item in items where item.hasDirectoryPath {
                let nested = item.appendingPathComponent(name)
                if FileManager.default.fileExists(atPath: nested.path) { return nested }
            }
        }
        throw GTFSImportError.missingFile(name)
    }
}

func gtfsSeconds(_ value: String?) -> Int? {
    guard let value, !value.isEmpty else { return nil }
    let parts = value.split(separator: ":")
    guard parts.count == 3, let h = Int(parts[0]), let m = Int(parts[1]), let s = Int(parts[2]) else { return nil }
    return h * 3600 + m * 60 + s
}
