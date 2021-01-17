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


enum class RecursiveDeleteResult {
    ONE_DELETED,
    ALL_DELETED,
    ENTRY_NOT_FOUND,
    KEY_NOT_FOUND
};

struct ReadPosition {
    ReadPosition(): l1Offset(NO_CHILD), l2Iterator({}), hasMoreL2Items(false) {

    }

    offset l1Offset;
    std::list<L2Item>::iterator l2Iterator;
    bool hasMoreL2Items;
};


class Tree{
public:
    KeyType keyType;

    explicit Tree(KeyType keyType);

    ErrCode get(MemDB* db, TxnState *txn, Record *record);
    ErrCode getNext(MemDB* db, TxnState *txn, Record *record);
    ErrCode insertRecord(MemDB* db, TxnState *txn, Key *k, const char* payload);
    ErrCode deleteRecord(MemDB* db, TxnState *txn, Record *record);

    void commit(uint32_t transactionId);
    void abort(uint32_t transactionId);

private:
    offset findOrConstructL1Item(const std::array<uint8_t, max_size()>& keyData);
    offset findL1ItemWithSmallestKey();
    offset findL1Item(const uint8_t* data, TxnState* txn);

    std::vector<TransactionLogItem> transactionLogItems;

    std::mutex mutex;
//    SpinLock mutex;

    std::vector<L0Item> l0Items;
    std::vector<L1Item> l1Items;
    std::vector<uint32_t> activeTransactionIDs;
    offset rootElementOffset;
    std::map<uint32_t, ReadPosition> readPositions;

    L0Item& accessL0Item(offset i) {
        return l0Items[getIndexFromOffset(i)];
//        return l0Items.at(i);
    }

    L1Item& accessL1Item(offset i) {
        return l1Items[getL1IndexFromOffset(i)];
//        return l1Items.at(i);
    }




    offset recursive(TxnState *txn, uint32_t level, L0Item* l0Item, uint32_t* indexUpdate);

    void removeTransaction(uint32_t transactionId);

    bool isTransactionActive(uint32_t transactionID);

    uint32_t getTransactionId(TxnState *txn, MemDB *db);

    L0Item *getItem(L0Item *currentL0Item, const uint8_t index);

    RecursiveDeleteResult recursiveDelete(uint32_t level, L0Item* l0Item, const uint8_t *keyData, const char* payload);
};

