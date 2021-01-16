//
// Created by lukas on 09.01.21.
//

#pragma once

#include "server.h"

struct TransactionLogItem {
    TransactionLogItem(uint32_t transactionId, offset l1Offset, const char *payload, bool created) :
        transactionId(transactionId),
        l1Offset(l1Offset),
        created(created) {
        strcpy(this->payload, payload);
    }

    uint32_t transactionId;
    offset l1Offset;
    char payload[MAX_PAYLOAD_LEN + 1];

    bool created;
};