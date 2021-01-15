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
#include "types.h"
class MemDB;



class Tree{
public:
    KeyType keyType;

    explicit Tree(KeyType keyType);

    ErrCode get(MemDB* db, TxnState *txn, Record *record);
    ErrCode getNext(MemDB* db, TxnState *txn, Record *record);
    ErrCode insertRecord(MemDB* db, TxnState *txn, Key *k, const char* payload);
    ErrCode deleteRecord(MemDB* db, TxnState *txn, Record *record);

    void commit(int transactionId);
    void abort(int transactionId);
    void visit(TxnState* txn);
private:
    offset findOrConstructL1Item(const std::array<uint8_t, max_size()>& keyData);
    offset findL1ItemWithSmallestKey();
    offset findL1Item(const uint8_t* data, TxnState* txn);

    std::map<int, std::vector<TransactionLogItem>> transactionLogItems;
    std::mutex mutex;

    std::vector<L0Item> l0Items;
    std::vector<L1Item> l1Items;
    offset rootElementOffset;

    L0Item& accessL0Item(offset i) {
        return l0Items[i];
//        return l0Items.at(i);
    }

    L1Item& accessL1Item(offset i) {
        return l1Items[i];
//        return l1Items.at(i);
    }

//    SpinLock mutex;


    offset recursive(TxnState *txn, uint32_t level, L0Item* l0Item, uint32_t* indexUpdate);
};

