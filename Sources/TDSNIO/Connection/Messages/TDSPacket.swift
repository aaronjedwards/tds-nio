import NIO

enum TDSPacket {
    public static let minimumPacketLength = 512
    public static let defaultPacketLength = 4096
    public static let maximumPacketLength = TDSPacket.defaultPacketLength
    public static let maximumNegotiatedPacketLength = 32_767
    public static let headerLength = 8
    public static let maximumPacketDataLength = TDSPacket.maximumPacketLength - headerLength

    static func clampedPacketLength(_ packetLength: Int) -> Int {
        min(
            max(packetLength, TDSPacket.minimumPacketLength),
            TDSPacket.maximumNegotiatedPacketLength
        )
    }
}
