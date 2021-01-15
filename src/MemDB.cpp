//
// Created by lukas on 07.01.21.
//

#include <algorithm>

#include "MemDB.h"
#include "types.h"


MemDB::MemDB() = default;

MemDB::~MemDB() = default;

ErrCode MemDB::create(KeyType type, char *name) {
    std::lock_guard<std::shared_mutex> l(this->mtx);

    if (this->index_dict.count(name) != 0) {
        return DB_EXISTS;
    }
    auto new_tree = std::make_shared<Tree>(type);
    this->index_dict.insert(std::make_pair(name, new_tree));

    return SUCCESS;
}

ErrCode MemDB::drop(char *name) {
    std::lock_guard<std::shared_mutex> l(this->mtx);

    if (this->index_dict.erase(name) > 0) {
        return SUCCESS;
    }

    return FAILURE;
}

ErrCode MemDB::openIndex(const char *name, IdxState **idxState) {
    auto state = new IdxState;

    auto it = this->index_dict.find(name);

    if (it == this->index_dict.end()) {
        return DB_DNE;
    }
    state->tree = it->second;
    *idxState = state;

    return SUCCESS;
}

ErrCode MemDB::closeIndex(IdxState *idxState) {
    delete idxState;

    return SUCCESS;
}

ErrCode MemDB::deleteRecord(IdxState *idxState, TxnState *txn, Record *record) {
    auto tree = idxState->tree;
    return tree->deleteRecord(this, txn, record);
}

ErrCode MemDB::insertRecord(IdxState *idxState, TxnState *txn, Key *k, const char *payload) {
    auto tree = idxState->tree;
    return tree->insertRecord(this, txn, k, payload);
}

ErrCode MemDB::getNext(IdxState *idxState, TxnState *txn, Record *record) {
    auto tree = idxState->tree;
    return tree->getNext(this, txn, record);
}

ErrCode MemDB::get(IdxState *idxState, TxnState *txn, Record *record) {
    auto tree = idxState->tree;
    return tree->get(this, txn, record);
}

ErrCode MemDB::commitTransaction(TxnState *txn) {
    std::lock_guard<std::shared_mutex> l(this->mtx);
    auto it = std::find(this->transactionIds.begin(), this->transactionIds.end(), txn->transactionId);

    if (it != this->transactionIds.end()) {
        this->transactionIds.erase(it);
    }

    for (auto& s : this->index_dict) {
        s.second->commit(txn->transactionId);
    }

    delete txn;

    return SUCCESS;
    return SUCCESS;
}

ErrCode MemDB::abortTransaction(TxnState *txn) {
    std::lock_guard<std::shared_mutex> l(this->mtx);
    auto it = std::find(this->transactionIds.begin(), this->transactionIds.end(), txn->transactionId);

    if (it != this->transactionIds.end()) {
        this->transactionIds.erase(it);
    }

    for (auto& s : this->index_dict) {
        s.second->abort(txn->transactionId);
    }

    delete txn;
    return SUCCESS;
    return SUCCESS;
}

ErrCode MemDB::beginTransaction(TxnState **txn) {
    TxnState* state;
    {
        std::lock_guard<std::shared_mutex> l(this->mtx);

        // TODO: Data type!
        int transactionId = this->getTransactionID();
        this->transactionIds.push_back(transactionId);

        state = new TxnState(transactionId);
    }
    *txn = state;
    return SUCCESS;
}

int MemDB::getTransactionID() {
    return __sync_fetch_and_add(&this->transactionIDCounter, 1);
}

bool MemDB::isTransactionActive(int transactionId) {
//    std::shared_lock<std::shared_mutex> l(this->mtx);
    if (std::find(this->transactionIds.cbegin(), this->transactionIds.cend(), transactionId) == this->transactionIds.cend()) {
        return false;
    }

    return true;
}
