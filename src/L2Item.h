//
// Created by lukas on 08.01.21.
//

#pragma once

#include <vector>
#include <string.h>
#include "server.h"

struct L2Item {
    L2Item(const char* payload, uint32_t readTimestamp, uint32_t writeTimestamp):
        readTimestamp(readTimestamp),
        writeTimestamp(writeTimestamp){
        strcpy(this->payload, payload);
    };

    char payload[MAX_PAYLOAD_LEN + 1];
    uint32_t readTimestamp;
    uint32_t writeTimestamp;
};