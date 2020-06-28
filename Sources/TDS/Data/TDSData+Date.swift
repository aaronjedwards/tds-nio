import Foundation
import NIO

/// Date/Times
/// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/786f5b8a-f87d-4980-9070-b9b7274c681d

extension TDSData {
//    public init(date: Date, type: TDSDataType = .datetime) {
//        var buffer = ByteBufferAllocator().buffer(capacity: 0)
//        let seconds = date.timeIntervalSince(_psqlDateStart) * Double(_microsecondsPerSecond)
//        buffer.writeInteger(Int64(seconds))
//        self.init(type: type, value: buffer)
//    }

    public var date: Date? {
        guard var value = self.value else {
            return nil
        }

        switch self.metadata.dataType {
        case .smallDateTime:
            guard
                // One 2-byte unsigned integer that represents the number of days since January 1, 1900.
                let daysSinceJan1900 = value.readInteger(endianness: .little, as: UInt16.self),
                // One 2-byte unsigned integer that represents the number of minutes elapsed since 12 AM that day.
                let minutesElapsed = value.readInteger(endianness: .little, as: UInt16.self)
            else {
                return nil
            }

            var secondsSinceJan1900 = Int64(daysSinceJan1900) * _secondsInDay
            secondsSinceJan1900 += Int64(minutesElapsed) * 60

            return Date(timeInterval: Double(secondsSinceJan1900), since: _jan1)

        case .datetime:
            guard
                // One 4-byte signed integer that represents the number of days since January 1, 1900. Negative numbers are allowed to represent dates since January 1, 1753.
                let daysSinceJan1900 = value.readInteger(endianness: .little, as: Int32.self),
                // One 4-byte unsigned integer that represents the number of one three-hundredths of a second (300 counts per second) elapsed since 12 AM that day.
                let oneThreeHundrethsOfASecondElapsed = value.readInteger(endianness: .little, as: UInt32.self)
            else {
                return nil
            }

            let secondsSinceJan1900 = Int64(daysSinceJan1900) * _secondsInDay
            let secondsSinceMidnight = Double(oneThreeHundrethsOfASecondElapsed) / 300
            let interval = Double(secondsSinceJan1900) + secondsSinceMidnight

            return Date(timeInterval: interval, since: _jan1900)

        case .date:
            // represented as one 3-byte unsigned integer that represents the number of days since January 1, year 1.
            guard let bytes = value.readBytes(length: 3) else {
                return nil
            }

            var daysSinceJan1: UInt32 = 0
            daysSinceJan1 += numericCast(bytes[0]) << 16
            daysSinceJan1 += numericCast(bytes[1]) << 8
            daysSinceJan1 += numericCast(bytes[2]) << 0

            let secondsSinceJan1 = Int64(daysSinceJan1) * _secondsInDay

            return Date(timeInterval: Double(secondsSinceJan1), since: _jan1)

        case .time:
            // time is represented as one unsigned integer that represents the number of 10-n second increments since 12 AM within a day. The length, in bytes, of that integer depends on the scale n as follows:
            // * 3 bytes if 0 <= n < = 2.
            // * 4 bytes if 3 <= n < = 4.
            // * 5 bytes if 5 <= n < = 7.
            fatalError("Unimplemented")
        case .datetime2:
            // datetime2(n) is represented as a concatenation of time(n) followed by date as specified above.
            fatalError("Unimplemented")
        case .datetimeOffset:
            // datetimeoffset(n) is represented as a concatenation of datetime2(n) followed by one 2-byte signed integer that represents the time zone offset as the number of minutes from UTC. The time zone offset MUST be between -840 and 840.
            fatalError("Unimplemented")
        default:
            return nil
        }
    }
}

//extension Date: TDSDataConvertible {
//    public static var tdsDataType: TDSDataType {
//        return .datetime
//    }
//
//    public init?(tdsData: TDSData) {
//        guard let date = tdsData.date else {
//            return nil
//        }
//        self = date
//    }
//
//    public var tdsData: TDSData? {
//        return .init(date: self)
//    }
//}

// MARK: Private
private let _microsecondsPerSecond: Int64 = 1_000_000
private let _secondsInDay: Int64 = 24 * 60 * 60
private let _jan1 = Date(timeIntervalSince1970: -62_135_742_702)
private let _jan1900 = Date(timeIntervalSince1970: -2_208_963_600)

