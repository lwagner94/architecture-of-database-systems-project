//
// Created by lukas on 07.01.21.
//

#include "Tree.h"

#include <algorithm>
#include <string.h>

#include <assert.h>
#include "MemDB.h"
#include "types.h"
#include "Transaction.h"

static inline int getTransactionId(TxnState *txn, MemDB* db) {
    if (!txn) {
        return db->getTransactionID();
    }
    else {
        return txn->transactionId;
    }
}

Tree::Tree(KeyType keyType) {
    this->keyType = keyType;
}

ErrCode Tree::get(MemDB* db, TxnState *txn, Record *record) {
    std::shared_lock lock(this->mutex);

    int transactionId = getTransactionId(txn, db);

    auto it = this->keyMap.find(record->key);

    if (it == this->keyMap.end()) {
        return KEY_NOTFOUND;
    }

    for (const auto& l2Item : it->second.items) {

        // TODO: Refactor!
        if (((l2Item.writeTimestamp < transactionId) && !db->isTransactionActive(l2Item.writeTimestamp)) || l2Item.writeTimestamp == transactionId) {
            auto& payload = l2Item.payload;
            assert(payload.length() <= MAX_PAYLOAD_LEN);
            payload.copy(record->payload, payload.length());
            record->payload[payload.length()] = 0;
            return SUCCESS;
        }
    }

    return KEY_NOTFOUND;
}

ErrCode Tree::insertRecord(MemDB* db, TxnState *txn, Key *k, const char *payload) {
    std::lock_guard lock(this->mutex);

    auto it = this->keyMap.find(*k);

    int transactionId = getTransactionId(txn, db);

    if (txn) {
        TransactionLogItem log;
        log.record.key = *k;
        strncpy(log.record.payload, payload, MAX_PAYLOAD_LEN);
        log.created = true;
        auto it = this->transactionLogItems.find(transactionId);

        if (it == this->transactionLogItems.end()) {
            this->transactionLogItems.emplace(std::make_pair(transactionId, std::vector {log}));
        }
        else {
            it->second.push_back(log);
        }
    }



    if (it == this->keyMap.end()) {
        L1Item l1Item;
        L2Item l2Item;
        l2Item.payload = payload;
        l2Item.writeTimestamp = transactionId;
        l1Item.items.push_back(l2Item);
        this->keyMap.insert(std::make_pair(*k, l1Item));
    }
    else {
        auto& l1Item = it->second;

        for (auto& l2Item : it->second.items) {
            if (l2Item.payload == payload) {
                return ENTRY_EXISTS;
            }
        }

        L2Item l2Item;

        l2Item.payload = std::string(payload);
        l2Item.writeTimestamp = transactionId;
        l1Item.items.push_back(l2Item);
    }
    return SUCCESS;
}

ErrCode Tree::deleteRecord(MemDB* db, TxnState *txn, Record *record) {
    std::lock_guard lock(this->mutex);
    auto it = this->keyMap.find(record->key);

    if (it == this->keyMap.end()) {
        return KEY_NOTFOUND;
    }

    if (strnlen(record->payload, 100) == 0) {
        this->keyMap.erase(it);
        return SUCCESS;
    }

    L1Item& l1Item = it->second;

    auto l2It = l1Item.items.begin();
    bool found = false;

    for (; l2It != l1Item.items.end(); l2It++) {
        if (l2It->payload == record->payload) {
            l1Item.items.erase(l2It);
            found = true;
            break;
        }
    }

    if (l1Item.items.empty()) {
        this->keyMap.erase(it);
    }

    return found ? SUCCESS : ENTRY_DNE;
}

void Tree::commit(int transactionId) {
    std::lock_guard lock(this->mutex);
    this->transactionLogItems.erase(transactionId);
}

void Tree::abort(int transactionId) {
    std::lock_guard lock(this->mutex);
    auto& logItems = this->transactionLogItems[transactionId];

    for (auto rit = logItems.rbegin(); rit != logItems.rend(); rit++) {
        if (rit->created) {
            auto it = this->keyMap.find(rit->record.key);
            L1Item& l1Item = it->second;

            auto l2It = l1Item.items.begin();

            for (; l2It != l1Item.items.end(); l2It++) {
                if (l2It->payload == rit->record.payload) {
                    l1Item.items.erase(l2It);
                    break;
                }
            }

            if (l1Item.items.empty()) {
                this->keyMap.erase(it);
            }
        }
    }
}

uint32_t Tree::calculateIndex(char* data, uint32_t level) {
    // Assuming prefix length = 4

    uint8_t byte = *(data + level / 2);
    uint8_t nibble = 0;

    if (level % 2 == 0) {
        nibble = (byte >> 4) & 0xF;
    }
    else {
        nibble = byte & 0xF;
    }

    return nibble;
}

void Tree::int32ToCharArray(char* data, int32_t number) {
    uint32_t temp = __builtin_bswap32(number);
    *reinterpret_cast<uint32_t*>(data) = temp;
}

void Tree::int64ToCharArray(char* data, int64_t number) {
    uint64_t temp = __builtin_bswap64(number);
    *reinterpret_cast<uint64_t*>(data) = temp;
}

int32_t Tree::charArrayToInt32(const char *data) {
    uint32_t temp = *reinterpret_cast<const uint32_t*>(data);
    return (int32_t) __builtin_bswap32(temp);
}

int64_t Tree::charArrayToInt64(const char *data) {
    uint64_t temp = *reinterpret_cast<const uint64_t*>(data);
    return (int64_t) __builtin_bswap64(temp);
}


void Tree::padVarchar(char* dest, const char* src) {
    // TODO: Feed this externally?
    size_t len = strnlen(src, MAX_VARCHAR_LEN);
    size_t offset = MAX_VARCHAR_LEN - len;

    memcpy(dest + offset, src, len);
    memset(dest, 0, offset);
}
