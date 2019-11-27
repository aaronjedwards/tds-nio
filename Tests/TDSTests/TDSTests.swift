import XCTest
@testable import TDS

final class TDSTests: XCTestCase {
    func testExample() {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct
        // results.
        XCTAssertEqual(TDS().text, "Hello, World!")
    }

    static var allTests = [
        ("testExample", testExample),
    ]
}
