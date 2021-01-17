//
// Created by lukas on 07.01.21.
//

#include <algorithm>

#include "MemDB.h"
#include "types.h"

MemDB::MemDB(): transactionIDCounter(0){

}


MemDB::~MemDB() {
    for (auto& it : this->tries) {
        delete it.second;
    }
}

ErrCode MemDB::create(KeyType type, char *name) {
    std::lock_guard<std::shared_mutex> l(this->mtx);

    if (this->tries.count(name) != 0) {
        return DB_EXISTS;
    }
    auto new_tree = new Tree(type);
    this->tries.insert(std::make_pair(name, new_tree));

    return SUCCESS;
}

ErrCode MemDB::drop(char *name) {
    std::lock_guard<std::shared_mutex> l(this->mtx);

    auto it = tries.find(name);

    if (it == tries.end()) {
        return FAILURE;
    }

    auto trie = it->second;
    tries.erase(it);
    delete trie;
    return SUCCESS;
}

ErrCode MemDB::openIndex(const char *name, IdxState **idxState) {
    std::shared_lock<std::shared_mutex> l(this->mtx);
    auto it = this->tries.find(name);
    if (it == this->tries.end()) {
        return DB_DNE;
    }

    auto state = new IdxState;
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

ErrCode MemDB::beginTransaction(TxnState **txn) {
    uint32_t transactionId = this->getTransactionID();
    *txn = new TxnState(transactionId);
    return SUCCESS;
}

ErrCode MemDB::commitTransaction(TxnState *txn) {
    std::shared_lock<std::shared_mutex> l(this->mtx);
    for (auto& s : this->tries) {
        s.second->commit(txn->transactionId);
    }
    delete txn;

    return SUCCESS;
}

ErrCode MemDB::abortTransaction(TxnState *txn) {
    std::shared_lock<std::shared_mutex> l(this->mtx);

    for (auto& s : this->tries) {
        s.second->abort(txn->transactionId);
    }
    delete txn;
    return SUCCESS;
}

uint32_t MemDB::getTransactionID() {
    return __sync_fetch_and_add(&this->transactionIDCounter, 1);
}