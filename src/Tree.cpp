//
// Created by lukas on 07.01.21.
//

#include "Tree.h"

#include <algorithm>
#include <iostream>
#include <string.h>

#include <assert.h>
#include "MemDB.h"
#include "types.h"
#include "Transaction.h"

constexpr size_t PREFIX_LENGTH = 4;

constexpr size_t SIZES[] = {
        [KeyType::SHORT] = 4,
        [KeyType::INT] = 8,
        [KeyType::VARCHAR] = MAX_VARCHAR_LEN
};

constexpr size_t LEVELS[] = {
        [KeyType::SHORT] = SIZES[KeyType::SHORT] * 8 / PREFIX_LENGTH,
        [KeyType::INT] = SIZES[KeyType::INT] * 8 / PREFIX_LENGTH,
        [KeyType::VARCHAR] =  SIZES[KeyType::VARCHAR] * 8 / PREFIX_LENGTH,
};

static inline uint32_t getTransactionId(TxnState *txn, MemDB* db) {
    if (!txn) {
        return db->getTransactionID();
    }
    else {
        return txn->transactionId;
    }
}

Tree::Tree(KeyType keyType) : keyType(keyType), rootElement(new L0Item) {

}

ErrCode Tree::get(MemDB* db, TxnState *txn, Record *record) {
    std::shared_lock lock(this->mutex);

    auto transactionId = getTransactionId(txn, db);

    // TODO: std::array?
    auto keyData = std::vector<uint8_t>(SIZES[this->keyType]);
    auto k = &record->key;

    switch (this->keyType) {
        case KeyType::SHORT:
            Tree::int32ToByteArray(keyData.data(), k->keyval.shortkey);
            break;
        case KeyType::INT:
            Tree::int64ToByteArray(keyData.data(), k->keyval.intkey);
            break;
        case KeyType::VARCHAR:
            Tree::varcharToByteArray(keyData.data(), (uint8_t *) k->keyval.charkey);
            break;
        default:
            return FAILURE;
    }

    L0Item* currentL0Item = this->rootElement;

    for (size_t level = 0; level < LEVELS[this->keyType]; level++) {
        uint32_t index = Tree::calculateIndex(keyData.data(), level);

        if (!currentL0Item->children[index]) {
            return KEY_NOTFOUND;
        }

        currentL0Item = currentL0Item->children[index];
    }

    for (const auto& l2Item : currentL0Item->l1Item->items) {

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

    auto transactionId = getTransactionId(txn, db);

    auto keyData = new uint8_t[SIZES[this->keyType]];

    switch (this->keyType) {
        case KeyType::SHORT:
            Tree::int32ToByteArray(keyData, k->keyval.shortkey);
            break;
        case KeyType::INT:
            Tree::int64ToByteArray(keyData, k->keyval.intkey);
            break;
        case KeyType::VARCHAR:
            Tree::varcharToByteArray(keyData, (uint8_t *) k->keyval.charkey);
            break;
        default:
            return FAILURE;
    }

    L0Item* currentL0Item = this->rootElement;

    for (size_t level = 0; level < LEVELS[this->keyType]; level++) {
        uint32_t index = Tree::calculateIndex(keyData, level);

        if (!currentL0Item->children[index]) {
            currentL0Item->children[index] = new L0Item;
        }

        currentL0Item = currentL0Item->children[index];
    }

    if (!currentL0Item->l1Item) {
        currentL0Item->l1Item = new L1Item(keyData);
    }

    for (const auto& l2 : currentL0Item->l1Item->items) {
        if (payload == l2.payload) {
            return ENTRY_EXISTS;
        }
    }

    currentL0Item->l1Item->items.emplace_back(L2Item {payload, transactionId, transactionId});


    if (txn) {
        TransactionLogItem log {currentL0Item->l1Item, payload, true};
        auto it = this->transactionLogItems.find(transactionId);

        if (it == this->transactionLogItems.end()) {
            this->transactionLogItems.emplace(std::make_pair(transactionId, std::vector {log}));
        }
        else {
            it->second.push_back(log);
        }
    }


    return SUCCESS;
}

ErrCode Tree::deleteRecord(MemDB* db, TxnState *txn, Record *record) {
    std::lock_guard lock(this->mutex);

    auto transactionId = getTransactionId(txn, db);

    // TODO: std::array?
    auto keyData = std::vector<uint8_t>(SIZES[this->keyType]);
    auto k = &record->key;

    switch (this->keyType) {
        case KeyType::SHORT:
            Tree::int32ToByteArray(keyData.data(), k->keyval.shortkey);
            break;
        case KeyType::INT:
            Tree::int64ToByteArray(keyData.data(), k->keyval.intkey);
            break;
        case KeyType::VARCHAR:
            Tree::varcharToByteArray(keyData.data(), (uint8_t *) k->keyval.charkey);
            break;
        default:
            return FAILURE;
    }

    L0Item* currentL0Item = this->rootElement;

    for (size_t level = 0; level < LEVELS[this->keyType]; level++) {
        uint32_t index = Tree::calculateIndex(keyData.data(), level);

        if (!currentL0Item->children[index]) {
            return KEY_NOTFOUND;
        }

        currentL0Item = currentL0Item->children[index];
    }

    if (strnlen(record->payload, MAX_PAYLOAD_LEN) == 0) {
        currentL0Item->l1Item->items.clear();
        return SUCCESS;
    }

    auto& items = currentL0Item->l1Item->items;
    auto it = items.begin();

    bool found = false;
    for (; it != items.end(); it++) {
        if (it->payload == record->payload) {
            items.erase(it);
            found = true;
            break;
        }
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
            auto l1Item = rit->l1Item;

            auto l2It = l1Item->items.begin();

            for (; l2It != l1Item->items.end(); l2It++) {
                if (l2It->payload == rit->payload) {
                    l1Item->items.erase(l2It);
                    break;
                }
            }
        }
    }
}

uint32_t Tree::calculateIndex(const uint8_t* data, uint32_t level) {
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

void Tree::int32ToByteArray(uint8_t * data, int32_t number) {
    uint32_t temp = __builtin_bswap32(number);
    *reinterpret_cast<uint32_t*>(data) = temp;
}

void Tree::int64ToByteArray(uint8_t* data, int64_t number) {
    uint64_t temp = __builtin_bswap64(number);
    *reinterpret_cast<uint64_t*>(data) = temp;
}

int32_t Tree::charArrayToInt32(const uint8_t *data) {
    uint32_t temp = *reinterpret_cast<const uint32_t*>(data);
    return (int32_t) __builtin_bswap32(temp);
}

int64_t Tree::charArrayToInt64(const uint8_t *data) {
    uint64_t temp = *reinterpret_cast<const uint64_t*>(data);
    return (int64_t) __builtin_bswap64(temp);
}


void Tree::varcharToByteArray(uint8_t* dest, const uint8_t* src) {
    // TODO: Feed this externally?
    size_t len = strnlen((char*) src, MAX_VARCHAR_LEN);
    size_t offset = MAX_VARCHAR_LEN - len;

    memcpy(dest + offset, src, len);
    memset(dest, 0, offset);
}
