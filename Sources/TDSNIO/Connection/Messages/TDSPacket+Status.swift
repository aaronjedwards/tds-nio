import NIO

extension TDSPacket {
    enum StatusFlag: Byte {
        case normal = 0x00
        case eom = 0x01
        case ignoreThisEvent = 0x02
        case resetConnection = 0x08
        case resetConnectionSkipTran = 0x10
    }

    struct Status: OptionSet, Sendable, Hashable {
        let rawValue: UInt8

        static let eom = Status(rawValue: StatusFlag.eom.rawValue)
        static let ignoreThisEvent = Status(rawValue: StatusFlag.ignoreThisEvent.rawValue)
        static let resetConnection = Status(rawValue: StatusFlag.resetConnection.rawValue)
        static let resetConnectionSkipTran = Status(rawValue: StatusFlag.resetConnectionSkipTran.rawValue)
    }
}
