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

Tree::Tree(KeyType keyType) : keyType(keyType), l0Items(), l1Items(), rootElementOffset(0) {
    l0Items.emplace_back(L0Item {});
//    l0Items.reserve(1073741824 / 16);

}

ErrCode Tree::get(MemDB* db, TxnState *txn, Record *record) {
//    std::shared_lock lock(this->mutex);
    std::lock_guard lock(this->mutex);
    auto transactionId = getTransactionId(txn, db);

    std::array<uint8_t, max_size()> keyData {};
    prepareKeyData(&record->key, keyData.data());

    auto l1Offset = findL1Item(keyData.data(), txn);
    if (!hasChild(l1Offset)) {
        return KEY_NOTFOUND;
    }
    auto l1Item = &accessL1Item(l1Offset);

    for (uint32_t l2Index = 0; l2Index < l1Item->items.size(); l2Index++) {
        const auto& l2Item = l1Item->items[l2Index];

        // TODO: Refactor!
        if (((l2Item.writeTimestamp < transactionId) && !db->isTransactionActive(l2Item.writeTimestamp)) || l2Item.writeTimestamp == transactionId) {
            auto& payload = l2Item.payload;
//            assert(payload.length() <= MAX_PAYLOAD_LEN);
            payload.copy(record->payload, payload.length());
            record->payload[payload.length()] = 0;

            if (txn) {
                txn->l2Size = l1Item->items.size();
                txn->l2Index = l2Index + 1;
                txn->l1Offset = l1Offset;
                txn->firstCall = false;
            }

            return SUCCESS;
        }
    }

    return KEY_NOTFOUND;
}

ErrCode Tree::getNext(MemDB *db, TxnState *txn, Record *record) {
    std::lock_guard lock(this->mutex);
    auto transactionId = getTransactionId(txn, db);

    uint32_t l2Index = 0;

    offset l1Offset;
    if (!txn) {
        l1Offset = findL1ItemWithSmallestKey();

    }
    else if (txn->firstCall) {
        uint32_t _ = 0;
        l1Offset = recursive(txn, 0, &accessL0Item(rootElementOffset), &_);
        txn->firstCall = false;
    }
    else if (txn->l2Index < txn->l2Size) {
        l1Offset = txn->l1Offset;
        l2Index = txn->l2Index;
    }
    else {
        uint32_t _ = 0;
        l1Offset = recursive(txn, 0, &accessL0Item(rootElementOffset), &_);
    }

    if (!hasChild(l1Offset)) {
        return DB_END;
    }

    auto l1Item = &accessL1Item(l1Offset);

    for (; l2Index < l1Item->items.size(); l2Index++) {
        const auto& l2Item = l1Item->items[l2Index];

        // TODO: Refactor!
        if (((l2Item.writeTimestamp < transactionId) && !db->isTransactionActive(l2Item.writeTimestamp)) || l2Item.writeTimestamp == transactionId) {
            auto& payload = l2Item.payload;
//            assert(payload.length() <= MAX_PAYLOAD_LEN);
            payload.copy(record->payload, payload.length());
            record->payload[payload.length()] = 0;

            record->key.type = keyType;

            switch (keyType) {
                case KeyType::SHORT:
                    record->key.keyval.shortkey = charArrayToInt32(l1Item->keyData.data());
                    break;
                case KeyType::INT:

                    record->key.keyval.intkey = charArrayToInt64(l1Item->keyData.data());
                    break;
                case KeyType::VARCHAR: {
                    uint8_t* ptr = l1Item->keyData.data();
                    while (!*ptr) {
                        ptr++;
                    }
                    size_t len = MAX_VARCHAR_LEN - (ptr - l1Item->keyData.data());
                    strncpy(record->key.keyval.charkey, (char*) ptr, len);
                    record->key.keyval.charkey[len + 1] = '\0';
                }
                    break;
                default:
                    return FAILURE;
            }

            if (txn) {
                txn->l2Size = l1Item->items.size();
                txn->l2Index = l2Index + 1;
                txn->l1Offset = l1Offset;
            }

            return SUCCESS;
        }
    }


    return DB_END;
}


ErrCode Tree::insertRecord(MemDB* db, TxnState *txn, Key *k, const char *payload) {
    std::lock_guard lock(this->mutex);

    auto transactionId = getTransactionId(txn, db);
    std::array<uint8_t, max_size()> keyData {};

    prepareKeyData(k, keyData.data());
    auto l1Offset = findOrConstructL1Item(keyData);
    auto l1Item = &accessL1Item(l1Offset);

    for (const auto& l2 : l1Item->items) {
        if (payload == l2.payload) {
            return ENTRY_EXISTS;
        }
    }

    l1Item->items.emplace_back(L2Item (payload, transactionId, transactionId));

    if (txn) {
        TransactionLogItem log {l1Offset, payload, true};
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

    std::array<uint8_t, max_size()> keyData {};
    prepareKeyData(&record->key, keyData.data());

    // TODO
    auto l1Offset = findL1Item(keyData.data(), txn);
    if (!hasChild(l1Offset)) {
        return KEY_NOTFOUND;
    }

    auto l1Item = &accessL1Item(l1Offset);

    if (strnlen(record->payload, MAX_PAYLOAD_LEN) == 0) {
        l1Item->items.clear();
        return SUCCESS;
    }

    auto& items = l1Item->items;
    for (auto it = items.begin(); it != items.end(); it++) {
        if (it->payload == record->payload) {
            items.erase(it);
            return SUCCESS;
        }
    }

    return ENTRY_DNE;
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
            auto l1Item = &accessL1Item(rit->l1Offset);

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

offset Tree::findL1Item(const uint8_t *data, TxnState* txn) {
    auto currentL0Item = &accessL0Item(rootElementOffset);

    for (size_t level = 0; level < LEVELS[this->keyType] / 2; level++) {
        auto indices = calculateNextTwoIndices(data, level);

        offset i = currentL0Item->children[indices.first];
        if (!hasChild(i)) {
            return i;
        }

        currentL0Item = &accessL0Item(i);

        i = currentL0Item->children[indices.second];
        if (!hasChild(i)) {
            return i;
        }

        if (txn) {
            txn->traversalTrace[2*level] = indices.first;
            txn->traversalTrace[2 * level + 1] = indices.second;
        }

        currentL0Item = &accessL0Item(i);
    }

    if (txn) {
        txn->traversalTrace[LEVELS[this->keyType] - 1]++;
    }

    return currentL0Item->l1Item;
}

offset Tree::findL1ItemWithSmallestKey() {
    L0Item* current = &accessL0Item(rootElementOffset);

    for (size_t level = 0; level < LEVELS[this->keyType]; level++) {
        bool found = false;

        // TODO: Extract constant 16
        for (uint8_t i = 0; i < 16; i++) {
            offset idx = current->children[i];
            if (hasChild(idx)) {
                current =  &accessL0Item(idx);
                found = true;

                break;
            }
        }

        if (!found) {
            return NO_CHILD;
        }
    }

    return current->l1Item;
}

offset Tree::findOrConstructL1Item(const std::array<uint8_t, max_size()>& keyData) {
    auto currentL0Item = &accessL0Item(rootElementOffset);

    for (size_t level = 0; level < LEVELS[this->keyType] / 2; level++) {
        auto indices = calculateNextTwoIndices(keyData.data(), level);

        offset i = currentL0Item->children[indices.first];
        if (!hasChild(i)) {
            i = l0Items.size();
            currentL0Item->children[indices.first] = i;
            l0Items.emplace_back(L0Item ());
        }
        currentL0Item = &accessL0Item(i);

        i = currentL0Item->children[indices.second];
        if (!hasChild(i)) {
            i = l0Items.size();
            currentL0Item->children[indices.second] = i;
            l0Items.emplace_back(L0Item ());
        }
        currentL0Item = &accessL0Item(i);
    }

    if (!hasChild(currentL0Item->l1Item)) {
        currentL0Item->l1Item = l1Items.size();
        l1Items.emplace_back(L1Item {keyData});
    }

    return currentL0Item->l1Item;
}


void Tree::visit(TxnState* txn) {
    auto root = &accessL0Item(rootElementOffset);
    uint32_t idx = 0;

    recursive(txn, 0, root, &idx);
}

offset Tree::recursive(TxnState* txn, uint32_t level, L0Item* l0Item, uint32_t* indexUpdate) {
    if (level == LEVELS[this->keyType]) {
        *indexUpdate = *indexUpdate + 1;
        return l0Item->l1Item;
    }

    int initial = 0;
    if (txn) {
        initial = txn->traversalTrace[level];
    }

    for (int i = initial; i < 16; i++) {
        if (hasChild(l0Item->children[i])) {
            L0Item* next = &accessL0Item(l0Item->children[i]);

            uint32_t idx = i;
            offset l1Item = recursive(txn, level + 1, next, &idx);

            txn->traversalTrace[level] = idx;
            if (idx > 16) {
                *indexUpdate = *indexUpdate + 1;
                txn->traversalTrace[level] = 0;
            }
            if (hasChild(l1Item)) {
                return l1Item;
            }
        }
    }

    *indexUpdate = *indexUpdate + 1;
    txn->traversalTrace[level] = 0;
    return NO_CHILD;
}







