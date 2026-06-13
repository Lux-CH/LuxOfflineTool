//
//  CSVReader.swift
//  Lux
//
//  Minimal streaming RFC-4180 CSV reader for GTFS files. Reads the file in
//  chunks (never loads the whole thing into memory — `stop_times.txt` is multiple
//  GB) and yields one record at a time. Handles quoted fields, escaped quotes
//  ("") and embedded commas/newlines.
//

import Foundation

enum CSVReaderError: Error { case cannotOpen }

struct CSVReader {

    /// Parse `url`, invoking `onRecord` for each row (including the header).
    /// Return `false` from `onRecord` to stop early. `onProgress` reports the
    /// fraction of the file consumed (0...1), throttled by the caller's needs.
    static func parse(
        url: URL,
        onProgress: ((Double) -> Void)? = nil,
        onRecord: ([String]) throws -> Bool
    ) throws {
        guard let handle = try? FileHandle(forReadingFrom: url) else { throw CSVReaderError.cannotOpen }
        defer { try? handle.close() }

        let totalBytes = (try? FileManager.default.attributesOfItem(atPath: url.path)[.size] as? Int) ?? nil
        let total = Double(totalBytes ?? 0)
        var bytesRead = 0
        var lastProgress = 0.0

        var field = [UInt8]()
        var record = [String]()
        var inQuotes = false
        var quoteJustClosed = false
        var sawAnyByteInRecord = false

        let comma: UInt8 = 0x2C, quote: UInt8 = 0x22, cr: UInt8 = 0x0D, lf: UInt8 = 0x0A
        let chunkSize = 1 << 20  // 1 MB

        func flushField() {
            record.append(String(decoding: field, as: UTF8.self))
            field.removeAll(keepingCapacity: true)
        }

        var stop = false
        while !stop {
            // autoreleasepool bounds the transient allocations from readData and
            // per-row String building over the multi-GB stop_times file.
            try autoreleasepool {
                let data = handle.readData(ofLength: chunkSize)
                if data.isEmpty { stop = true; return }
                bytesRead += data.count

                for byte in data {
                    if inQuotes {
                        if byte == quote {
                            inQuotes = false
                            quoteJustClosed = true
                        } else {
                            field.append(byte)
                        }
                        continue
                    }
                    if quoteJustClosed {
                        quoteJustClosed = false
                        if byte == quote { field.append(quote); inQuotes = true; continue } // escaped ""
                    }
                    switch byte {
                    case quote:
                        inQuotes = true; sawAnyByteInRecord = true
                    case comma:
                        flushField(); sawAnyByteInRecord = true
                    case lf, cr:
                        if sawAnyByteInRecord || !field.isEmpty || !record.isEmpty {
                            flushField()
                            if try !onRecord(record) { stop = true; return }
                            record.removeAll(keepingCapacity: true)
                        }
                        sawAnyByteInRecord = false
                    default:
                        field.append(byte); sawAnyByteInRecord = true
                    }
                }

                if total > 0 {
                    let p = Double(bytesRead) / total
                    if p - lastProgress >= 0.01 { lastProgress = p; onProgress?(min(p, 1.0)) }
                }
            }
        }

        // Trailing record with no final newline.
        if !field.isEmpty || !record.isEmpty {
            flushField()
            _ = try onRecord(record)
        }
        onProgress?(1.0)
    }
}

/// Maps a GTFS header row to column indices for tolerant, order-independent access.
struct CSVColumns {
    private let index: [String: Int]
    init(header: [String]) {
        var map = [String: Int]()
        for (i, name) in header.enumerated() {
            // Strip BOM and whitespace from the first/each header cell.
            let clean = name.replacingOccurrences(of: "\u{FEFF}", with: "")
                .trimmingCharacters(in: .whitespaces)
            map[clean] = i
        }
        self.index = map
    }
    func value(_ row: [String], _ column: String) -> String? {
        guard let i = index[column], i < row.count else { return nil }
        return row[i]
    }
}
