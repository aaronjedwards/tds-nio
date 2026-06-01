//===----------------------------------------------------------------------===//
//
// This source file is part of the TDSNIO open source project
//
// Copyright (c) 2026 Aaron Edwards and the TDSNIO project authors
// Licensed under Apache License v2.0
//
// See LICENSE for license information
// See CONTRIBUTORS.md for the list of TDSNIO project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//
import Testing

func expectEqual<T: Equatable>(
    _ expression1: @autoclosure () throws -> T,
    _ expression2: @autoclosure () throws -> T
) {
    do {
        #expect(try expression1() == expression2())
    } catch {
        Issue.record("Unexpected error while comparing values: \(error)")
    }
}

func expectEqual<T: FloatingPoint>(
    _ expression1: @autoclosure () throws -> T,
    _ expression2: @autoclosure () throws -> T,
    accuracy: T
) {
    do {
        #expect(abs(try expression1() - expression2()) <= accuracy)
    } catch {
        Issue.record("Unexpected error while comparing values: \(error)")
    }
}

func expectNotEqual<T: Equatable>(
    _ expression1: @autoclosure () throws -> T,
    _ expression2: @autoclosure () throws -> T
) {
    do {
        #expect(try expression1() != expression2())
    } catch {
        Issue.record("Unexpected error while comparing values: \(error)")
    }
}

func expectNil<T>(_ expression: @autoclosure () throws -> T?) {
    do {
        #expect(try expression() == nil)
    } catch {
        Issue.record("Unexpected error while checking nil: \(error)")
    }
}

func expectTrue(_ expression: @autoclosure () throws -> Bool) {
    do {
        #expect(try expression())
    } catch {
        Issue.record("Unexpected error while checking true: \(error)")
    }
}

func expectFalse(_ expression: @autoclosure () throws -> Bool) {
    do {
        #expect(try !expression())
    } catch {
        Issue.record("Unexpected error while checking false: \(error)")
    }
}

func expectGreaterThan<T: Comparable>(
    _ expression1: @autoclosure () throws -> T,
    _ expression2: @autoclosure () throws -> T
) {
    do {
        #expect(try expression1() > expression2())
    } catch {
        Issue.record("Unexpected error while comparing values: \(error)")
    }
}

func expectLessThan<T: Comparable>(
    _ expression1: @autoclosure () throws -> T,
    _ expression2: @autoclosure () throws -> T
) {
    do {
        #expect(try expression1() < expression2())
    } catch {
        Issue.record("Unexpected error while comparing values: \(error)")
    }
}

func expectLessThanOrEqual<T: Comparable>(
    _ expression1: @autoclosure () throws -> T,
    _ expression2: @autoclosure () throws -> T
) {
    do {
        #expect(try expression1() <= expression2())
    } catch {
        Issue.record("Unexpected error while comparing values: \(error)")
    }
}

func requireUnwrap<T>(_ expression: @autoclosure () throws -> T?) throws -> T {
    try #require(try expression())
}

func expectThrowsError<T>(
    _ expression: @autoclosure () throws -> T,
    _ errorHandler: (Error) -> Void = { _ in }
) {
    do {
        _ = try expression()
        Issue.record("Expected error to be thrown")
    } catch {
        errorHandler(error)
    }
}

func expectNoThrow<T>(_ expression: @autoclosure () throws -> T) {
    do {
        _ = try expression()
    } catch {
        Issue.record("Expected no error to be thrown, got \(error)")
    }
}
