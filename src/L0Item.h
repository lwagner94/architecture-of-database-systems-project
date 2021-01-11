//
// Created by lukas on 08.01.21.
//

#pragma once

#include <array>
#include "L1Item.h"

struct L0Item {
    L0Item() : children({}), l1Item(nullptr){
    }

    std::array<L0Item*, 16> children;
    L1Item* l1Item;
};

