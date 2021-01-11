//
// Created by lukas on 09.01.21.
//

#pragma once

#include "server.h"

struct TransactionLogItem {
    Record record;
    bool created;
};