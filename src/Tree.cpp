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
//            auto& payload = l2Item->payload;
//            assert(payload.length() <= MAX_PAYLOAD_LEN);
//            payload.copy(record->payload, payload.length());
//            record->payload[payload.length()] = 0;
            strcpy(record->payload, l2Item->payload);

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
//            if (_) {
//                return DB_END;
//            }
            txn->firstCall = false;
        }
        else if (txn->hasMoreL2Items) {
            l1Offset = txn->l1Offset;
        }
        else {
            uint32_t _ = 0;
            l1Offset = recursive(txn, 0, &accessL0Item(rootElementOffset), &_);
//            if (_) {
//                return DB_END;
//            }
        }

        if (!isL1Node(l1Offset)) {
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
//                auto& payload = l2Item->payload;
//                payload.copy(record->payload, payload.length());
//                record->payload[payload.length()] = 0;
                strcpy(record->payload, l2Item->payload);
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
        if (strcmp(l2.payload, payload) == 0) {
            return ENTRY_EXISTS;
        }
    }

    l1Item->items.emplace_back(L2Item (payload, transactionId, transactionId));

    if (txn) {
        this->transactionLogItems.emplace_back(transactionId, l1Offset, payload, true);
    }


    return SUCCESS;
}

ErrCode Tree::deleteRecord(MemDB* db, TxnState *txn, Record *record) {
    std::lock_guard lock(this->mutex);

    auto transactionId = getTransactionId(txn, db);

    std::array<uint8_t, max_size()> keyData {};
    prepareKeyData(&record->key, keyData.data());

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
        assert(false);
    }
//    auto indices = calculateNextTwoIndices(keyData, level / 2);
//    offset index = (level % 2 == 0) ? indices.first : indices.second;
    offset index = calculateIndex(keyData, level);

    if (isL1Node(l0Item->children[index])) {
        offset l1Offset = l0Item->children[index];
        // Delete element!
        if (isNodeVisitable(l1Offset)) {
            auto l1Item = &accessL1Item(l1Offset);

            if (!payload) {
                l1Item->items.clear();
                l0Item->children[index] = markAsNotVisitable(l1Offset);
                return RecursiveDeleteResult::ALL_DELETED;
            }


            for (auto it = l1Item->items.begin(); it != l1Item->items.end(); it++) {
                if (strcmp(it->payload, payload) == 0) {
                    l1Item->items.erase(it);
                    if (l1Item->items.empty()) {
                        l0Item->children[index] = markAsNotVisitable(l1Offset);
                        return RecursiveDeleteResult::ALL_DELETED;
                    }
                    return RecursiveDeleteResult::ONE_DELETED;
                }
            }

            return RecursiveDeleteResult::ENTRY_NOT_FOUND;
        }
    }

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

        auto end = std::remove_if(transactionLogItems.begin(), transactionLogItems.end(), [=](const TransactionLogItem& t) {
            return t.transactionId == transactionId;
        });

        transactionLogItems.erase(end, transactionLogItems.end());
    }
}

void Tree::abort(uint32_t transactionId) {
    std::lock_guard lock(this->mutex);

    for (const auto& t : transactionLogItems) {
        if (t.transactionId == transactionId && t.created) {
            auto l1Item = &accessL1Item(t.l1Offset);

            for (auto it = l1Item->items.begin(); it != l1Item->items.end(); it++) {
                if (strcmp(it->payload, t.payload) == 0) {
                    l1Item->items.erase(it);
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
        if (txn) {
            txn->traversalTrace[2*level] = indices.first;
        }
        if (isL1Node(i)) {
            if (memcmp(data, &accessL1Item(i), SIZES[this->keyType]) == 0) {
                if (txn) {
                    txn->traversalTrace[level * 2]++;
                }
                return i;
            }
            else {
                return NO_CHILD;
            }

        }

        if (!isNodeVisitable(i)) {
            return NO_CHILD;
        }

        currentL0Item = &accessL0Item(i);

        i = currentL0Item->children[indices.second];
        if (txn) {
            txn->traversalTrace[2 * level + 1] = indices.second;
        }
        if (isL1Node(i)) {
            if (memcmp(data, &accessL1Item(i), SIZES[this->keyType]) == 0) {
                if (txn) {
                    txn->traversalTrace[level * 2 + 1]++;
                }
                return i;
            }
            else {
                return NO_CHILD;
            }
        }
        if (!isNodeVisitable(i)) {
            return NO_CHILD;
        }

        currentL0Item = &accessL0Item(i);
    }

    return NO_CHILD;
}

offset Tree::findL1ItemWithSmallestKey() {
    L0Item* current = &accessL0Item(rootElementOffset);

    for (size_t level = 0; level < LEVELS[this->keyType]; level++) {
        bool found = false;

        // TODO: Extract constant 16
        for (uint8_t i = 0; i < 16; i++) {
            offset idx = current->children[i];
            if (isL1Node(idx)) {
                return idx;
            }

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

    return NO_CHILD;
}

offset Tree::findOrConstructL1Item(const std::array<uint8_t, max_size()>& keyData) {
    auto currentL0Item = &accessL0Item(rootElementOffset);

    for (size_t level = 0; level < LEVELS[this->keyType]; level++) {
//        auto indices = calculateNextTwoIndices(keyData.data(), level);
//        currentL0Item = getItem(currentL0Item, indices.first);
//        currentL0Item = getItem(currentL0Item, indices.second);

//        offset i = currentL0Item->children[index];
//        if (!isNodePresent(i)) {
//            i = l0Items.size();
//            currentL0Item->children[index] = markAsVisitable(i);
//            l0Items.emplace_back(L0Item ());
//        }
//        else {
//            currentL0Item->children[index] = markAsVisitable(i);
//        }
//        i = currentL0Item->children[index];
//        if (!isNodePresent(i)) {
//            i = l0Items.size();
//            currentL0Item->children[index] = markAsVisitable(i);
//            l0Items.emplace_back(L0Item ());
//        }
//        else {
//            currentL0Item->children[index] = markAsVisitable(i);
//        }

        auto index = calculateIndex(keyData.data(), level);
        offset i = currentL0Item->children[index];

        if (!isNodePresent(i)) {
            // We have found an empty slot, we can construct L1 directly
            offset l1Offset = getL1OffsetFromIndex(l1Items.size());
            l1Items.emplace_back(L1Item {keyData});
            currentL0Item->children[index] = l1Offset;
            return l1Offset;
        }

        if (isL1Node(i)) {

            L1Item* l1Item = &accessL1Item(i);

            // Check if it already uses the same key
            if (memcmp(l1Item->keyData.data(), keyData.data(), SIZES[this->keyType]) == 0) {
                // Return the current L1 if we share the same key!
                return i;
            }
            else {
                // We do not share the same key, so we save the old l1Offset and construct a new L0Item
                offset oldL1 = i;

                auto newL0Offset = markAsVisitable(l0Items.size());
                currentL0Item->children[index] = newL0Offset;
                l0Items.emplace_back(L0Item ());
                currentL0Item = &accessL0Item(newL0Offset);

                for (size_t nestedLevel = level + 1; level < LEVELS[this->keyType]; nestedLevel++) {
                    auto newL1Index = calculateIndex(keyData.data(), nestedLevel);
                    auto oldL1Index = calculateIndex(accessL1Item(oldL1).keyData.data(), nestedLevel);

                    if (newL1Index == oldL1Index) {
                        newL0Offset = markAsVisitable(l0Items.size());
                        currentL0Item->children[newL1Index] = newL0Offset;
                        l0Items.emplace_back(L0Item ());
                        currentL0Item = &accessL0Item(newL0Offset);
                    }
                    else {
                        currentL0Item->children[oldL1Index] = oldL1;
                        offset l1Offset = getL1OffsetFromIndex(l1Items.size());
                        l1Items.emplace_back(L1Item {keyData});
                        currentL0Item->children[newL1Index] = l1Offset;
                        return l1Offset;
                    }
                }

            }
        }
        else {
            currentL0Item->children[index] = markAsVisitable(currentL0Item->children[index]);
            currentL0Item = &accessL0Item(currentL0Item->children[index]);
        }
    }

    return NO_CHILD;
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
    assert(txn);
    if (level == LEVELS[this->keyType]) {
//        *indexUpdate = *indexUpdate + 1;
//        return l0Item->l1Item;
        assert(false);
    }

    int initial = 0;
    if (txn) {
        initial = txn->traversalTrace[level];
    }

    for (int i = initial; i < 16; i++) {
        if (isL1Node(l0Item->children[i])) {

            txn->traversalTrace[level] = i + 1;
            if (txn->traversalTrace[level] > 15) {
                txn->traversalTrace[level] = 0;
                *indexUpdate = *indexUpdate + 1;
            }
            return l0Item->children[i];
        }

        if (isNodeVisitable(l0Item->children[i])) {
            L0Item* next = &accessL0Item(l0Item->children[i]);

            uint32_t idx = i;
            offset l1Item = recursive(txn, level + 1, next, &idx);

            if (txn) {
                txn->traversalTrace[level] = idx;
            }

            if ((idx > 15)) {
                *indexUpdate = *indexUpdate + 1;
                txn->traversalTrace[level] = 0;
            }
            if (isL1Node(l1Item)) {
                return l1Item;
            }
        }
    }

    *indexUpdate = *indexUpdate + 1;
    txn->traversalTrace[level] = 0;
    return NO_CHILD;
}







