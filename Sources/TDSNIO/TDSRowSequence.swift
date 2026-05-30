//===----------------------------------------------------------------------===//
//
// This source file is part of the TDSNIO open source project
//
// Copyright (c) 2026 TDSNIO project authors
// Licensed under Apache License v2.0
//
// See LICENSE for license information
// See CONTRIBUTORS.md for the list of TDSNIO project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import NIOCore

/// An async sequence of ``TDSRow`` values.
public struct TDSRowSequence: AsyncSequence, Sendable {
    public typealias Element = TDSRow
    typealias BackingSequence = NIOThrowingAsyncSequenceProducer<
        TDSRow, Error, TDSAdaptiveRowBuffer, TDSRowStream
    >

    private enum Storage {
        case rows([TDSRow])
        case stream(BackingSequence)
    }

    private let storage: Storage

    public init(_ rows: [TDSRow]) {
        self.storage = .rows(rows)
    }

    init(_ stream: BackingSequence) {
        self.storage = .stream(stream)
    }

    public func makeAsyncIterator() -> AsyncIterator {
        switch self.storage {
        case .rows(let rows):
            return AsyncIterator(storage: .rows(rows, rows.startIndex))
        case .stream(let stream):
            return AsyncIterator(storage: .stream(stream.makeAsyncIterator()))
        }
    }
}

extension TDSRowSequence {
    public struct AsyncIterator: AsyncIteratorProtocol {
        public typealias Element = TDSRow

        fileprivate enum Storage {
            case rows([TDSRow], Array<TDSRow>.Index)
            case stream(BackingSequence.AsyncIterator)
        }

        private var storage: Storage

        fileprivate init(storage: Storage) {
            self.storage = storage
        }

        public mutating func next() async throws -> TDSRow? {
            switch self.storage {
            case .rows(let rows, let index):
                guard index < rows.endIndex else {
                    return nil
                }

                self.storage = .rows(rows, rows.index(after: index))
                return rows[index]
            case .stream(let iterator):
                let row = try await iterator.next()
                self.storage = .stream(iterator)
                return row
            }
        }
    }
}

struct TDSAdaptiveRowBuffer: NIOAsyncSequenceProducerBackPressureStrategy {
    static let defaultMinimum = 1
    static let defaultTarget = 256
    static let defaultMaximum = 16_384

    let minimum: Int
    let maximum: Int

    private var target: Int
    private var mayShrink = false

    init(
        minimum: Int = Self.defaultMinimum,
        maximum: Int = Self.defaultMaximum,
        target: Int = Self.defaultTarget
    ) {
        precondition(minimum <= target && target <= maximum)
        self.minimum = minimum
        self.maximum = maximum
        self.target = target
    }

    mutating func didYield(bufferDepth: Int) -> Bool {
        if bufferDepth > self.target, self.mayShrink, self.target > self.minimum {
            self.target &>>= 1
        }
        self.mayShrink = true
        return false
    }

    mutating func didConsume(bufferDepth: Int) -> Bool {
        if bufferDepth == 0, self.target < self.maximum {
            self.target *= 2
            self.mayShrink = false
        }
        return bufferDepth < self.target
    }
}

extension TDSRowSequence {
    public func collect() async throws -> [TDSRow] {
        var result: [TDSRow] = []
        for try await row in self {
            result.append(row)
        }
        return result
    }

    public func decode<T: TDSDecodable>(
        _ type: T.Type = T.self
    ) -> TDSDecodedRowSequence<T> {
        TDSDecodedRowSequence(self) { try $0.decode(type) }
    }

    public func decode<A: TDSDecodable, B: TDSDecodable>(
        _ type: (A, B).Type
    ) -> TDSDecodedRowSequence<(A, B)> {
        TDSDecodedRowSequence(self) { try $0.decode(type) }
    }

    public func decode<A: TDSDecodable, B: TDSDecodable, C: TDSDecodable>(
        _ type: (A, B, C).Type
    ) -> TDSDecodedRowSequence<(A, B, C)> {
        TDSDecodedRowSequence(self) { try $0.decode(type) }
    }

    public func decode<A: TDSDecodable, B: TDSDecodable, C: TDSDecodable, D: TDSDecodable>(
        _ type: (A, B, C, D).Type
    ) -> TDSDecodedRowSequence<(A, B, C, D)> {
        TDSDecodedRowSequence(self) { try $0.decode(type) }
    }

    public func decode<
        A: TDSDecodable,
        B: TDSDecodable,
        C: TDSDecodable,
        D: TDSDecodable,
        E: TDSDecodable
    >(
        _ type: (A, B, C, D, E).Type
    ) -> TDSDecodedRowSequence<(A, B, C, D, E)> {
        TDSDecodedRowSequence(self) { try $0.decode(type) }
    }
}

public struct TDSDecodedRowSequence<Element: Sendable>: AsyncSequence, Sendable {
    private let rows: TDSRowSequence
    private let transform: @Sendable (TDSRow) throws -> Element

    init(
        _ rows: TDSRowSequence,
        transform: @escaping @Sendable (TDSRow) throws -> Element
    ) {
        self.rows = rows
        self.transform = transform
    }

    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(
            rows: self.rows.makeAsyncIterator(),
            transform: self.transform
        )
    }

    public struct AsyncIterator: AsyncIteratorProtocol {
        private var rows: TDSRowSequence.AsyncIterator
        private let transform: @Sendable (TDSRow) throws -> Element

        fileprivate init(
            rows: TDSRowSequence.AsyncIterator,
            transform: @escaping @Sendable (TDSRow) throws -> Element
        ) {
            self.rows = rows
            self.transform = transform
        }

        public mutating func next() async throws -> Element? {
            guard let row = try await self.rows.next() else {
                return nil
            }
            return try self.transform(row)
        }
    }
}
