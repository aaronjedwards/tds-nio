import XCTest
import NIO
@testable import TDS

final class TDSTests: XCTestCase {
    func testLoginOptionValidation() throws {
        let lopremipusm129 = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Vivamus gravida mauris eu tincidunt venenatis. Aliquam nec sem volutpat."
        let auth: TDSMessages.Login7Message = TDSMessages.Login7Message(
            hostname: lopremipusm129,
            username: "username",
            password: "password",
            appName: "TDSTester",
            serverName: "",
            clientInterfaceName: "SwiftTDS",
            language: "",
            database: "database",
            sspiData: "")
        var buffer = ByteBufferAllocator().buffer(capacity: 255)
        XCTAssertThrowsError(try auth.serialize(into: &buffer),
                             "MUST FAILED on invalid filed's lenght") { (err) in
                                let tdsErr: TDSError = err as! TDSError
                                switch tdsErr {
                                case let .invalidConnectionOptionValueLength(fieldName, limit):
                                    XCTAssertEqual(fieldName, "hostname")
                                    XCTAssertEqual(limit, 128)
                                default:
                                    assertionFailure("expected error was TDSError.invalidConnectionOptionValueLength")
                                }
                                let expectedDesc = "TDS error: The value's length for field 'hostname' exceeds it's limit of '128'"
                                XCTAssertEqual(tdsErr.description, expectedDesc)
        }
    }
    static var allTests = [
        ("testLoginOptionValidation", testLoginOptionValidation),
    ]
}
