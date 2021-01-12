//
// Created by lukas on 07.01.21.
//

#include "Tree.h"

#include <algorithm>
#include <iostream>
#include <string.h>

#include <assert.h>
#include "MemDB.h"

#include "Transaction.h"
#include "bitutils.h"



static inline uint32_t getTransactionId(TxnState *txn, MemDB* db) {
    if (!txn) {
        return db->getTransactionID();
    }
    else {
        return txn->transactionId;
    }
}

Tree::Tree(KeyType keyType) : keyType(keyType), rootElement(new L0Item), jumperArray({}) {
    this->jumperArray[0] = this->rootElement;
    L0Item* current = this->rootElement;

    if (keyType == KeyType::VARCHAR) {
        for (size_t level = 1; level < LEVELS[this->keyType]; level++) {
            auto newL0Item = new L0Item;
            current->children[0] = newL0Item;
            current = newL0Item;
            this->jumperArray[level] = newL0Item;
        }
    }
}

ErrCode Tree::get(MemDB* db, TxnState *txn, Record *record) {
//    std::shared_lock lock(this->mutex);
    std::lock_guard lock(this->mutex);
    auto transactionId = getTransactionId(txn, db);

    std::array<uint8_t, max_size()> keyData {};
    auto k = &record->key;
    size_t leadingZeros = 0;

    switch (this->keyType) {
        case KeyType::SHORT:
            int32ToByteArray(keyData.data(), k->keyval.shortkey);
            break;
        case KeyType::INT:
            int64ToByteArray(keyData.data(), k->keyval.intkey);
            break;
        case KeyType::VARCHAR:
            leadingZeros = varcharToByteArray(keyData.data(), (uint8_t *) k->keyval.charkey);
            break;
        default:
            return FAILURE;
    }

    L0Item* currentL0Item = this->findL0Item(keyData.data(), leadingZeros);
    if (!currentL0Item || !currentL0Item->l1Item) {
        return KEY_NOTFOUND;
    }

    for (const auto& l2Item : currentL0Item->l1Item->items) {

        // TODO: Refactor!
        if (((l2Item.writeTimestamp < transactionId) && !db->isTransactionActive(l2Item.writeTimestamp)) || l2Item.writeTimestamp == transactionId) {
            auto& payload = l2Item.payload;
//            assert(payload.length() <= MAX_PAYLOAD_LEN);
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
    std::array<uint8_t, max_size()> keyData {};

    size_t leadingZeros = 0;

    switch (this->keyType) {
        case KeyType::SHORT:
            int32ToByteArray(keyData.data(), k->keyval.shortkey);
            break;
        case KeyType::INT:
            int64ToByteArray(keyData.data(), k->keyval.intkey);
            break;
        case KeyType::VARCHAR:
            leadingZeros = varcharToByteArray(keyData.data(), (uint8_t *) k->keyval.charkey);
            break;
        default:
            return FAILURE;
    }

    L0Item* currentL0Item = this->rootElement;

    size_t level = 0;

    if (leadingZeros) {
        level = leadingZeros;
        currentL0Item = this->jumperArray[leadingZeros * 2 - 1];
    }


    for (; level < LEVELS[this->keyType] / 2; level++) {
        auto indices = calculateNextTwoIndices(keyData.data(), level);

        if (!currentL0Item->children[indices.first]) {
            currentL0Item->children[indices.first] = new L0Item;
        }
        currentL0Item = currentL0Item->children[indices.first];

        if (!currentL0Item->children[indices.second]) {
            currentL0Item->children[indices.second] = new L0Item;
        }

        currentL0Item = currentL0Item->children[indices.second];
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
//        TransactionLogItem log {currentL0Item->l1Item, payload, true};
//        auto it = this->transactionLogItems.find(transactionId);
//
//        if (it == this->transactionLogItems.end()) {
//            this->transactionLogItems.emplace(std::make_pair(transactionId, std::vector {log}));
//        }
//        else {
//            it->second.push_back(log);
//        }
    }


    return SUCCESS;
}

ErrCode Tree::deleteRecord(MemDB* db, TxnState *txn, Record *record) {
    std::lock_guard lock(this->mutex);

    auto transactionId = getTransactionId(txn, db);

    std::array<uint8_t, max_size()> keyData {};
    auto k = &record->key;

    switch (this->keyType) {
        case KeyType::SHORT:
            int32ToByteArray(keyData.data(), k->keyval.shortkey);
            break;
        case KeyType::INT:
            int64ToByteArray(keyData.data(), k->keyval.intkey);
            break;
        case KeyType::VARCHAR:
            varcharToByteArray(keyData.data(), (uint8_t *) k->keyval.charkey);
            break;
        default:
            return FAILURE;
    }

    // TODO
    L0Item* currentL0Item = this->findL0Item(keyData.data(), 0);
    if (!currentL0Item) {
        return KEY_NOTFOUND;
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

L0Item* Tree::findL0Item(const uint8_t *data, size_t leadingZeros) {
    L0Item* currentL0Item = this->rootElement;

    size_t level = 0;

    if (leadingZeros) {
        level = leadingZeros;
        currentL0Item = this->jumperArray[leadingZeros * 2 - 1];
    }

    for (; level < LEVELS[this->keyType] / 2; level++) {
        auto indices = calculateNextTwoIndices(data, level);

        if (!currentL0Item->children[indices.first]) {
            return nullptr;
        }
        currentL0Item = currentL0Item->children[indices.first];

        if (!currentL0Item->children[indices.second]) {
            return nullptr;
        }

        currentL0Item = currentL0Item->children[indices.second];
    }

    return currentL0Item;
}








