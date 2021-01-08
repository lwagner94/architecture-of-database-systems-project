//
// Created by lukas on 07.01.21.
//

#pragma once

#include <map>
#include <string>
#include <vector>
#include <stdint.h>

#include "server.h"

struct KeyCompare
{
    bool operator() (const Key& lhs, const Key& rhs) const
    {
        switch (lhs.type) {
            case KeyType::INT:
                return lhs.keyval.intkey < rhs.keyval.intkey;
            case KeyType::SHORT:
                return lhs.keyval.shortkey < rhs.keyval.shortkey;
            case KeyType::VARCHAR:
                return std::string(lhs.keyval.charkey) < std::string(rhs.keyval.charkey);
        }

        return false;
    }
};

class Tree{
public:
    KeyType keyType;

    explicit Tree(KeyType keyType);

    ErrCode get(TxnState *txn, Record *record);
    ErrCode insertRecord(TxnState *txn, Key *k, const char* payload);
    ErrCode deleteRecord(TxnState *txn, Record *record);
private:

    std::map<Key, std::vector<std::string>, KeyCompare> keyMap;
};

