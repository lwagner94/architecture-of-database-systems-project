//
// Created by lukas on 07.01.21.
//

#pragma once

#include <map>
#include <string>
#include <vector>
#include <shared_mutex>
#include <mutex>
#include <memory>

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
    explicit Tree(KeyType keyType, MemDB* memDb);
    ErrCode get(TxnState *txn, Record *record);
    ErrCode getNext(TxnState *txn, Record *record);
    ErrCode insertRecord(TxnState *txn, Key *k, const char* payload);
    ErrCode deleteRecord(TxnState *txn, Record *record);
    void commit(uint32_t transactionId);
    void abort(uint32_t transactionId);

private:
    MemDB* memDb;
    std::vector<TransactionLogItem> transactionLogItems;
    std::mutex mutex;
    std::vector<L0Item> l0Items;
    std::vector<L1Item> l1Items;
    offset rootElementOffset;
    std::map<uint32_t, ReadPosition> readPositions;


    offset findOrConstructL1Item(const std::array<uint8_t, max_size()>& keyData);
    offset findL1ItemWithSmallestKey();
    offset findL1Item(const uint8_t* data, TxnState* txn);
    offset recursiveFindL1(TxnState *txn, uint32_t level, L0Item* l0Item, uint32_t* indexUpdate);
    void removeTransaction(uint32_t transactionId);
    bool isTransactionActive(uint32_t transactionID);
    uint32_t getTransactionId(TxnState *txn);
    RecursiveDeleteResult recursiveDelete(uint32_t level, L0Item* l0Item, const uint8_t *keyData, const char* payload);

    L0Item& accessL0Item(offset i) {
        return l0Items[getIndexFromOffset(i)];
    }

    L1Item& accessL1Item(offset i) {
        return l1Items[getL1IndexFromOffset(i)];
    }
};

