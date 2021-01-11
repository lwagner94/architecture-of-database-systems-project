//
// Created by lukas on 09.01.21.
//

#pragma once

#include "server.h"

struct TransactionLogItem {

    L1Item* l1Item;
    std::string payload;

    bool created;
};