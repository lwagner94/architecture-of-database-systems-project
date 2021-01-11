//
// Created by lukas on 07.01.21.
//

#pragma once

#include <map>
#include <string>
#include <vector>
#include <shared_mutex>
#include <stdint.h>
#include <memory>

#include "server.h"
#include "L0Item.h"
#include "L2Item.h"
#include "L1Item.h"
#include "Transaction.h"

class MemDB;

struct KeyCompare
{
    bool operator() (const Key& lhs, const Key& rhs) const
    {
        switch (lhs.type) {
            case KeyType::INT:
                return lhs.keyval.intkey < rhs.keyval.intkey;
            case KeyType::SHORT:
                return lhs.keyval.shortkey < rhs.keyval.shortkey;
            case KeyType::VARCHAR:
                return std::string(lhs.keyval.charkey) < std::string(rhs.keyval.charkey);
        }

        return false;
    }
};

class Tree{
public:
    KeyType keyType;

    explicit Tree(KeyType keyType);

    ErrCode get(MemDB* db, TxnState *txn, Record *record);
    ErrCode insertRecord(MemDB* db, TxnState *txn, Key *k, const char* payload);
    ErrCode deleteRecord(MemDB* db, TxnState *txn, Record *record);

    void commit(int transactionId);
    void abort(int transactionId);

    static uint32_t calculateIndex(const uint8_t* data, uint32_t level);

    static void int32ToByteArray(uint8_t* data, int32_t number);
    static void int64ToByteArray(uint8_t* data, int64_t number);

    static int32_t charArrayToInt32(const uint8_t* data);
    static int64_t charArrayToInt64(const uint8_t* data);

    static void varcharToByteArray(uint8_t* dest, const uint8_t* src);
private:

    std::map<int, std::vector<TransactionLogItem>> transactionLogItems;
    L0Item* rootElement;
    std::shared_mutex mutex;
};

