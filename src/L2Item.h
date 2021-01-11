//
// Created by lukas on 08.01.21.
//

#pragma once

#include <vector>
#include <string>

struct L2Item {
    L2Item(const char* payload, uint32_t readTimestamp, uint32_t writeTimestamp):
        payload(payload),
        readTimestamp(readTimestamp),
        writeTimestamp(writeTimestamp){

    };

    std::string payload;
    uint32_t readTimestamp;
    uint32_t writeTimestamp;
};