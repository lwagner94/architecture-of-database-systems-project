//
// Created by lukas on 07.01.21.
//

#pragma once

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

class MemDB;
class Tree;

struct IdxState {
    MemDB* memDbInstance;
    std::shared_ptr<Tree> tree;
};

struct TxnState {
    uint32_t transactionId;
};