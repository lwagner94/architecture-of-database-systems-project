//
// Created by lukas on 07.01.21.
//

#pragma once

typedef uint32_t offset;
constexpr offset NO_CHILD = 0;

inline bool isNodePresent(offset o) {
    return o != NO_CHILD;
}

inline bool isL1Node(offset o) {
    return o & 0x40000000;
}

inline offset getL1IndexFromOffset(offset o) {
    return o & 0x3FFFFFFF;
}

inline offset getL1OffsetFromIndex(offset o) {
    return o | 0x40000000;
}

inline bool isNodeVisitable(offset o) {
    if ((o != NO_CHILD) && !(o & ~0x7FFFFFFF)) {
        return true;
    }
    return false;
}

inline offset getIndexFromOffset(offset o) {
    return o & 0x7FFFFFFF;
}

inline offset markAsVisitable(offset o) {
    return o & 0x7FFFFFFF;
}

inline offset markAsNotVisitable(offset o) {
    return o | ~0x7FFFFFFF;
}

constexpr size_t PREFIX_LENGTH = 4;

constexpr size_t SIZES[] = {
        [KeyType::SHORT] = 4,
        [KeyType::INT] = 8,
        [KeyType::VARCHAR] = MAX_VARCHAR_LEN
};

constexpr size_t max_size() {
    size_t max = 0;
    for (auto size : SIZES) {
        max = std::max(max, size);
    }

    return max;
}

constexpr size_t LEVELS[] = {
        [KeyType::SHORT] = SIZES[KeyType::SHORT] * 8 / PREFIX_LENGTH,
        [KeyType::INT] = SIZES[KeyType::INT] * 8 / PREFIX_LENGTH,
        [KeyType::VARCHAR] =  SIZES[KeyType::VARCHAR] * 8 / PREFIX_LENGTH,
};

constexpr size_t max_levels() {
    size_t max = 0;
    for (auto size : LEVELS) {
        max = std::max(max, size);
    }

    return max;
}

class MemDB;
class Tree;

struct IdxState {
    MemDB* memDbInstance;
    Tree* tree;
};

struct TxnState {
    explicit TxnState(uint32_t txnId): transactionId(txnId), firstCall(true), traversalTrace({}) {

    }

    uint32_t transactionId;
    bool firstCall;
    std::array<uint8_t, max_levels()> traversalTrace;
};

