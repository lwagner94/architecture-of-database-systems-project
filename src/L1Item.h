//
// Created by lukas on 08.01.21.
//

#pragma once
#include "server.h"
#include "L2Item.h"

#include <vector>
#include <list>

struct L1Item {
    Key key;
    std::vector<L2Item> items;
};


