//
// Created by lukas on 08.01.21.
//

#pragma once

#include <array>
#include "L1Item.h"
#include "types.h"



struct L0Item {
    L0Item() : children(), l1Item(NO_CHILD){
        children.fill(NO_CHILD);
    }

    std::array<offset, 16> children;
    offset l1Item;
};

