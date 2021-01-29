import Foundation
import NIO

/// Date/Times
/// https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/786f5b8a-f87d-4980-9070-b9b7274c681d

extension TDSData {
    public init(date: Date) {
        var buffer = ByteBufferAllocator().buffer(capacity: 0)
        buffer.writeDatetimeOffset(date: date)
        self.init(metadata: Date.tdsMetadata, value: buffer)
    }
    
    public var date: Date? {
        guard var value = self.value else {
            return nil
        }
        
        switch self.metadata.dataType {
        case .smallDateTime:
            guard
                value.readableBytes == 4,
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
                value.readableBytes == 8,
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
            return value.readDate()
        case .time:
            // time alone cannot be accurately represented with Swift's Date type
            return nil
        case .datetime2:
            return value.readDatetime2(bytes: value.readableBytes - 3, scale: metadata.scale)
        case .datetimeOffset:
            // datetimeoffset(n) is represented as a concatenation of datetime2(n) followed by one 2-byte signed integer that represents the time zone offset as the number of minutes from UTC. The time zone offset MUST be between -840 and 840.
            guard
                let datetime = value.readDatetime2(bytes: value.readableBytes - 5, scale: metadata.scale),
                let timezoneOffset = value.readInteger(as: Int16.self), timezoneOffset >= -840 && timezoneOffset <= 840
            else {
                return nil
            }
            
            let tz = TimeZone(secondsFromGMT: Int(timezoneOffset) * 60)
            let components = DateComponents(timeZone: tz)
            let datetimeOffset = Calendar.current.date(byAdding: components, to: datetime)
            return datetimeOffset
        default:
            return nil
        }
    }
}

extension ByteBuffer {
    fileprivate mutating func writeDatetimeOffset(date: Date) {
        let components = Calendar.current.dateComponents([.nanosecond, .timeZone], from: date)
        guard
            let nanoseconds = components.nanosecond,
            let secondsFromUTC = components.timeZone?.secondsFromGMT()
        else {
            return
        }
        
        // 10-n second increments since 12 AM within a day (5 bytes)
        let secondIncrements = UInt64(nanoseconds / 100)
        // The number of days since January 1, year 1. (3 bytes)
        let daysSinceJan1 = Int(date.timeIntervalSince(_jan1) / Double(_secondsInDay))
        // The time zone offset as the number of minutes from UTC (2 bytes)
        let minutesFromUTC = Int16(secondsFromUTC / 60)
        
        #warning("TODO: Should only write 5 bytes")
        self.writeInteger(secondIncrements)
        #warning("TODO: Should only write 3 bytes")
        self.writeInteger(daysSinceJan1)
        self.writeInteger(minutesFromUTC)
    }
    
    /// time(n) is represented as one unsigned integer that represents the number of 10-n second increments since 12 AM within a day.
    /// The length, in bytes, of that integer depends on the scale n as follows:
    /// * 3 bytes if 0 <= n < = 2.
    /// * 4 bytes if 3 <= n < = 4.
    /// * 5 bytes if 5 <= n < = 7.
    ///
    fileprivate mutating func readTimeComponents(bytes length: Int, scale: Int?) -> DateComponents? {
        guard var secondIncrements: Int = self.readByteLengthInteger(length: length), let scale = scale else {
            return nil
        }
        
        if scale < 7 {
            for _ in scale..<7 {
                secondIncrements = secondIncrements * 10
            }
        }
        
        return DateComponents.init(nanosecond: secondIncrements * 100)
    }
    
    /// represented as one 3-byte unsigned integer that represents the number of days since January 1, year 1.
    fileprivate mutating func readDate() -> Date? {
        
        guard let daysSinceJan1: UInt32 = self.readByteLengthInteger(length: 3) else {
            return nil
        }
        
        let secondsSinceJan1 = Int64(daysSinceJan1) * _secondsInDay
        
        return Date(timeInterval: Double(secondsSinceJan1), since: _jan1)
    }
    
    /// datetime2(n) is represented as a concatenation of time(n) followed by date as specified above.
    fileprivate mutating func readDatetime2(bytes length: Int, scale: Int?) -> Date? {
        
        guard
            let nanoseconds = self.readTimeComponents(bytes: length, scale: scale),
            let date = self.readDate()
        else {
            return nil
        }
        
        return Calendar.current.date(byAdding: nanoseconds, to: date)
    }
}

extension Date: TDSDataConvertible {
    public static var tdsMetadata: Metadata {
        return TypeMetadata(dataType: .datetimeOffset, scale: 7)
    }
    
    public init?(tdsData: TDSData) {
        guard let date = tdsData.date else {
            return nil
        }
        self = date
    }
    
    public var tdsData: TDSData? {
        return .init(date: self)
    }
}

// MARK: Private
private let _microsecondsPerSecond: Int64 = 1_000_000
private let _secondsInDay: Int64 = 24 * 60 * 60
private let _jan1 = Date(timeIntervalSince1970: -62_135_742_702)
private let _jan1900 = Date(timeIntervalSince1970: -2_208_963_600)

