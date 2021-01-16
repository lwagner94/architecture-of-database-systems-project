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



uint32_t Tree::getTransactionId(TxnState *txn, MemDB* db) {
    if (!txn) {
        return db->getTransactionID();
    }
    else {
        if (std::find(activeTransactionIDs.begin(),
                      activeTransactionIDs.end(),
                      txn->transactionId) == activeTransactionIDs.end()) {
            activeTransactionIDs.push_back(txn->transactionId);
        }

        return txn->transactionId;
    }
}

Tree::Tree(KeyType keyType) : keyType(keyType), l0Items(), l1Items(), rootElementOffset(0) {
    std::array<uint8_t, max_size()> fakeKey {};

    l0Items.emplace_back(L0Item {});
    l1Items.emplace_back(L1Item {fakeKey});

    // TODO
    l0Items.reserve(1577798 + 1000);
    l1Items.reserve(129537 + 1000);
}

ErrCode Tree::get(MemDB* db, TxnState *txn, Record *record) {
//    std::shared_lock lock(this->mutex);
    std::lock_guard lock(this->mutex);
    auto transactionId = getTransactionId(txn, db);

    std::array<uint8_t, max_size()> keyData {};
    prepareKeyData(&record->key, keyData.data());

    auto l1Offset = findL1Item(keyData.data(), txn);
    if (!isNodeVisitable(l1Offset)) {
        return KEY_NOTFOUND;
    }
    auto l1Item = &accessL1Item(l1Offset);
    for (auto l2Item = l1Item->items.begin(); l2Item != std::end(l1Item->items); l2Item++) {

        // TODO: Refactor!
        if (((l2Item->writeTimestamp < transactionId) && !isTransactionActive(l2Item->writeTimestamp)) || l2Item->writeTimestamp == transactionId) {
            auto& payload = l2Item->payload;
//            assert(payload.length() <= MAX_PAYLOAD_LEN);
            payload.copy(record->payload, payload.length());
            record->payload[payload.length()] = 0;

            if (txn) {
                txn->firstCall = false;
                txn->l2Iterator = ++l2Item;
                if (txn->l2Iterator == std::end(l1Item->items)) {
                    txn->hasMoreL2Items = false;
                }
                else {
                    txn->hasMoreL2Items = true;
                }
                txn->l1Offset = l1Offset;
            }

            return SUCCESS;
        }
    }

    return KEY_NOTFOUND;
}

bool Tree::isTransactionActive(uint32_t transactionID) {
    return std::find(activeTransactionIDs.begin(), activeTransactionIDs.end(), transactionID) != activeTransactionIDs.end();
}

ErrCode Tree::getNext(MemDB *db, TxnState *txn, Record *record) {
    std::lock_guard lock(this->mutex);
    auto transactionId = getTransactionId(txn, db);

    while (true) {
        offset l1Offset;

        std::list<L2Item>::iterator l2Item {};

        if (!txn) {
            l1Offset = findL1ItemWithSmallestKey();

        }
        else if (txn->firstCall) {
            uint32_t _ = 0;
            l1Offset = recursive(txn, 0, &accessL0Item(rootElementOffset), &_);
            if (_) {
                return DB_END;
            }
            txn->firstCall = false;
        }
        else if (txn->hasMoreL2Items) {
            l1Offset = txn->l1Offset;
        }
        else {
            uint32_t _ = 0;
            l1Offset = recursive(txn, 0, &accessL0Item(rootElementOffset), &_);
            if (_) {
                return DB_END;
            }
        }

        if (!isNodeVisitable(l1Offset)) {
            return DB_END;
        }

        auto l1Item = &accessL1Item(l1Offset);

        if (txn && txn->hasMoreL2Items) {
            l2Item = txn->l2Iterator;
        }
        else {
            l2Item = l1Item->items.begin();
        }

        for (; l2Item != l1Item->items.end(); l2Item++) {

            // TODO: Refactor!
            if (((l2Item->writeTimestamp < transactionId) && !isTransactionActive(l2Item->writeTimestamp)) || l2Item->writeTimestamp == transactionId) {
                auto& payload = l2Item->payload;
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
                        uint32_t index = 0;
                        while (index < MAX_VARCHAR_LEN) {
                            if (ptr[index]) {
                                break;
                            }
                            index++;
                        }
                        size_t len = MAX_VARCHAR_LEN - index;
                        ptr += index;
                        strncpy(record->key.keyval.charkey, (char*) ptr, len);
                        record->key.keyval.charkey[len] = '\0';
                    }
                        break;
                    default:
                        return FAILURE;
                }

                if (txn) {
                    txn->l2Iterator = ++l2Item;
                    if (txn->l2Iterator == std::end(l1Item->items)) {
                        txn->hasMoreL2Items = false;
                    }
                    else {
                        txn->hasMoreL2Items = true;
                    }

                    txn->l1Offset = l1Offset;
                }

                return SUCCESS;
            }
        }

        if (txn) {
            txn->hasMoreL2Items = false;
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

//    // TODO
//    auto l1Offset = findL1Item(keyData.data(), txn);
//    if (!isNodeVisitable(l1Offset)) {
//        return KEY_NOTFOUND;
//    }
//
//    auto l1Item = &accessL1Item(l1Offset);
//
//    if (strnlen(record->payload, MAX_PAYLOAD_LEN) == 0) {
//        l1Item->items.clear();
//        return SUCCESS;
//    }
//
//    auto& items = l1Item->items;
//    for (auto it = items.begin(); it != items.end(); it++) {
//        if (it->payload == record->payload) {
//            items.erase(it);
//            return SUCCESS;
//        }
//    }
//
//    return ENTRY_DNE;

    char* payload = nullptr;
    if (strnlen(record->payload, MAX_PAYLOAD_LEN)) {
        payload = record->payload;
    }

    auto l0Item = &accessL0Item(rootElementOffset);
    auto result = recursiveDelete(0, l0Item, keyData.data(), payload);
    switch (result) {
        case RecursiveDeleteResult::ENTRY_NOT_FOUND:
            return ENTRY_DNE;
        case RecursiveDeleteResult::KEY_NOT_FOUND:
            return KEY_NOTFOUND;
        default:
            return SUCCESS;
    }
}

RecursiveDeleteResult Tree::recursiveDelete(uint32_t level, L0Item* l0Item, const uint8_t *keyData, const char* payload) {
    if (level == LEVELS[this->keyType]) {
        offset l1Offset = l0Item->l1Item;
        // Delete element!
        if (isNodeVisitable(l1Offset)) {
            auto l1Item = &accessL1Item(l1Offset);

            if (!payload) {
                l1Item->items.clear();
                l0Item->l1Item = markAsNotVisitable(l1Offset);
                return RecursiveDeleteResult::ALL_DELETED;
            }


            for (auto it = l1Item->items.begin(); it != l1Item->items.end(); it++) {
                if (it->payload == payload) {
                    l1Item->items.erase(it);
                    if (l1Item->items.empty()) {
                        return RecursiveDeleteResult::ALL_DELETED;
                    }
                    return RecursiveDeleteResult::ONE_DELETED;
                }
            }

            return RecursiveDeleteResult::ENTRY_NOT_FOUND;
        }
    }
    auto indices = calculateNextTwoIndices(keyData, level / 2);
    offset index = (level % 2 == 0) ? indices.first : indices.second;

    if (!isNodeVisitable(l0Item->children[index])) {
        return RecursiveDeleteResult::KEY_NOT_FOUND;
    }

    L0Item* next = &accessL0Item(l0Item->children[index]);
    auto result = recursiveDelete(level + 1, next, keyData, payload);

    switch (result) {
        case RecursiveDeleteResult::ALL_DELETED: {
            l0Item->children[index] = markAsNotVisitable(l0Item->children[index]);

            bool anyoneVisitable = false;
            for (auto i = 0; i < 16; i++) {
                if (isNodeVisitable(l0Item->children[i])) {
                    anyoneVisitable = true;
                    break;
                }
            }

            return anyoneVisitable ? RecursiveDeleteResult::ONE_DELETED : RecursiveDeleteResult::ALL_DELETED;
        }

        case RecursiveDeleteResult::ONE_DELETED:
            return result;
        case RecursiveDeleteResult::ENTRY_NOT_FOUND:
            return result;
        case RecursiveDeleteResult::KEY_NOT_FOUND:
            return result;

    }
}


void Tree::commit(uint32_t transactionId) {
    std::lock_guard lock(mutex);

    removeTransaction(transactionId);
}

void Tree::removeTransaction(uint32_t transactionId) {
    auto it = std::find(activeTransactionIDs.begin(), activeTransactionIDs.end(), transactionId);
    if (it != activeTransactionIDs.end()) {
        activeTransactionIDs.erase(it);
        transactionLogItems.erase(transactionId);
    }
}

void Tree::abort(uint32_t transactionId) {
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

    removeTransaction(transactionId);
}

offset Tree::findL1Item(const uint8_t *data, TxnState* txn) {
    auto currentL0Item = &accessL0Item(rootElementOffset);

    for (size_t level = 0; level < LEVELS[this->keyType] / 2; level++) {
        auto indices = calculateNextTwoIndices(data, level);

        offset i = currentL0Item->children[indices.first];
        if (!isNodeVisitable(i)) {
            return i;
        }

        currentL0Item = &accessL0Item(i);

        i = currentL0Item->children[indices.second];
        if (!isNodeVisitable(i)) {
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
            if (isNodeVisitable(idx)) {
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
        currentL0Item = getItem(currentL0Item, indices.first);
        currentL0Item = getItem(currentL0Item, indices.second);
    }

    // TODO: What to do if we have a duplicate payload? Then markAsVisitable is "wrong"
    offset l1Offset = currentL0Item->l1Item;

    if (!isNodePresent(l1Offset)) {
        l1Offset = l1Items.size();
        l1Items.emplace_back(L1Item {keyData});
    }

    currentL0Item->l1Item = markAsVisitable(l1Offset);
    return currentL0Item->l1Item;
}

L0Item *Tree::getItem(L0Item *currentL0Item, const uint8_t index) {
    offset i = currentL0Item->children[index];
    if (!isNodePresent(i)) {
        i = l0Items.size();
        currentL0Item->children[index] = markAsVisitable(i);
        l0Items.emplace_back(L0Item ());
    }
    else {
        currentL0Item->children[index] = markAsVisitable(i);
    }
    return &accessL0Item(i);
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
        if (isNodeVisitable(l0Item->children[i])) {
            L0Item* next = &accessL0Item(l0Item->children[i]);

            uint32_t idx = i;
            offset l1Item = recursive(txn, level + 1, next, &idx);

            if (txn) {
                txn->traversalTrace[level] = idx;
            }

            if (idx > 16) {
                *indexUpdate = *indexUpdate + 1;
                txn->traversalTrace[level] = 0;
            }
            if (isNodeVisitable(l1Item)) {
                return l1Item;
            }
        }
    }

    *indexUpdate = *indexUpdate + 1;
    txn->traversalTrace[level] = 0;
    return NO_CHILD;
}







