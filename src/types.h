//
// Created by lukas on 07.01.21.
//

#pragma once

typedef uint32_t offset;
constexpr offset NO_CHILD = -1;

inline bool hasChild(offset o) {
    return o != NO_CHILD;
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
    std::shared_ptr<Tree> tree;
};

struct TxnState {
    explicit TxnState(uint32_t txnId): transactionId(txnId), l2Size(-1), l2Index(-1), l1Offset(NO_CHILD), firstCall(true), traversalTrace({}) {

    }

    uint32_t transactionId;
    uint32_t l2Size;
    uint32_t l2Index;
    offset l1Offset;
    bool firstCall;
    std::array<uint8_t, max_levels()> traversalTrace;
};

