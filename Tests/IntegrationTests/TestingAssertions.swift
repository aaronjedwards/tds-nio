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

func expectNil<T>(_ expression: @autoclosure () throws -> T?) {
    do {
        #expect(try expression() == nil)
    } catch {
        Issue.record("Unexpected error while checking nil: \(error)")
    }
}
