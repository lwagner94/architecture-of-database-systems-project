//
// Created by lukas on 07.01.21.
//

#pragma once

struct IdxState {
    MemDB* memDbInstance;
    std::shared_ptr<Tree> tree;
};

struct TxnState {
    uint32_t transactionId;
};