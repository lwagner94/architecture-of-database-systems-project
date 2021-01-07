//
// Created by lukas on 07.01.21.
//

#pragma once

#include <map>
#include <string>
#include <memory>
#include <shared_mutex>
#include "L0Tree.h"
#include "server.h"

class MemDB {
public:
    MemDB();
    virtual ~MemDB();

    ErrCode create(KeyType type, char *name);
    ErrCode drop(char *name);
    ErrCode openIndex(const char *name, IdxState **idxState);
    ErrCode closeIndex(IdxState *idxState);

private:
    std::map<std::string, std::shared_ptr<L0Tree>> index_dict;
    std::shared_mutex mtx;
};
