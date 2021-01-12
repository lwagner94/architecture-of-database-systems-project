//
// Created by lukas on 08.01.21.
//

#pragma once
#include "server.h"
#include "L2Item.h"

#include <vector>
#include <list>
#include "types.h"

struct L1Item {
    explicit L1Item(std::array<uint8_t, max_size()> keyData): keyData(keyData), items({}) {

    }

//    uint8_t* keyData;
    std::array<uint8_t, max_size()> keyData {};
    std::vector<L2Item> items;
};


