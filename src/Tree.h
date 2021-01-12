//
// Created by lukas on 07.01.21.
//

#pragma once

#include <map>
#include <string>
#include <vector>
#include <shared_mutex>
#include <mutex>
#include <stdint.h>
#include <memory>
#include "Spinlock.h"

#include "server.h"
#include "L0Item.h"
#include "L2Item.h"
#include "L1Item.h"
#include "Transaction.h"

class MemDB;

class Tree{
public:
    KeyType keyType;

    explicit Tree(KeyType keyType);

    ErrCode get(MemDB* db, TxnState *txn, Record *record);
    ErrCode insertRecord(MemDB* db, TxnState *txn, Key *k, const char* payload);
    ErrCode deleteRecord(MemDB* db, TxnState *txn, Record *record);

    void commit(int transactionId);
    void abort(int transactionId);

    L0Item* findL0Item(const uint8_t* data);

    static uint32_t calculateIndex(const uint8_t* data, uint32_t level);
private:

    std::map<int, std::vector<TransactionLogItem>> transactionLogItems;
    L0Item* rootElement;
    std::shared_mutex mutex;

    std::array<L0Item*, MAX_VARCHAR_LEN> jumperArray;

//    SpinLock mutex;
};

