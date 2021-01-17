//
// Created by lukas on 07.01.21.
//

#pragma once

#include <map>
#include <string>
#include <memory>
#include <shared_mutex>
#include "Tree.h"
#include "server.h"

class MemDB {
public:
    MemDB();

    ~MemDB();
    ErrCode create(KeyType type, char *name);
    ErrCode drop(char *name);
    ErrCode openIndex(const char *name, IdxState **idxState);
    ErrCode closeIndex(IdxState *idxState);
    ErrCode beginTransaction(TxnState **txn);
    ErrCode abortTransaction(TxnState *txn);
    ErrCode commitTransaction(TxnState *txn);
    ErrCode get(IdxState *idxState, TxnState *txn, Record *record);
    ErrCode getNext(IdxState *idxState, TxnState *txn, Record *record);
    ErrCode insertRecord(IdxState *idxState, TxnState *txn, Key *k, const char* payload);
    ErrCode deleteRecord(IdxState *idxState, TxnState *txn, Record *record);

    uint32_t getTransactionID();

//    bool isTransactionActive(int transactionId);

private:


    std::map<std::string, Tree*> tries;
    std::shared_mutex mtx;

//    std::vector<int> transactionIds;

    uint32_t transactionIDCounter;
};
