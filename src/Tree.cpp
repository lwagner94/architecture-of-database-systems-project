//
// Created by lukas on 07.01.21.
//

#include "Tree.h"

#include <algorithm>

#include <assert.h>


Tree::Tree(KeyType keyType) {
    this->keyType = keyType;
}

ErrCode Tree::get(TxnState *txn, Record *record) {

    auto it = this->keyMap.find(record->key);

    if (it == this->keyMap.end()) {
        return KEY_NOTFOUND;
    }

    std::string payload = it->second.at(0);

    assert(payload.length() <= MAX_PAYLOAD_LEN);
    payload.copy(record->payload, payload.length());
    record->payload[payload.length()] = 0;
    return SUCCESS;
}

ErrCode Tree::insertRecord(TxnState *txn, Key *k, const char *payload) {
    auto it = this->keyMap.find(*k);

    if (it == this->keyMap.end()) {
        std::vector<std::string> vec;
        vec.emplace_back(payload);
        this->keyMap.insert(std::make_pair(*k, vec));
    }
    else {
        auto& vec = it->second;
        if (std::find(vec.begin(), vec.end(), payload) != vec.end()) {
            // Record exists
            return ENTRY_EXISTS;
        }

        vec.emplace_back(payload);
    }
    return SUCCESS;
}

ErrCode Tree::deleteRecord(TxnState *txn, Record *record) {
    return DB_END;
}
