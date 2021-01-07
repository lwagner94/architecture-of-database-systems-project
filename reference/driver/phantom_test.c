/*
 * speed_test.c
 * written by Elizabeth Reid
 * ereid@mit.edu
 *
 */

#import "../server.h"

#include <pthread.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

char *phantom_index = "phantom";

char *a_key = "a_key";
char *b_key = "b_key";
char *c_key = "c_key";

const char *value_one = "value one";
const char *value_two = "value two";

int READTXNBEGUN = 0;
int FAILEDLOOP = 0;

static void *read_loop()
{
    int errCode;
    IdxState *idx;
    TxnState *txn;
    Record record;
    memset(&record, 0, sizeof(Record));
    
    if ((errCode = openIndex(phantom_index, &idx)) != SUCCESS) {
        printf("cannot open phantom index in read_loop. ErrCode = %d\n", errCode);
        FAILEDLOOP = 1;
        READTXNBEGUN = 1;
        return;
    }
    
    if ((errCode = beginTransaction(&txn)) != SUCCESS) {
        printf("cannot open transaction in read_loop. ErrCode = %d\n", errCode);
        FAILEDLOOP = 1;
        READTXNBEGUN = 1;
        return;
    }
    
    int count = 0;
    while (count < 1000) {
        count++;
        record.key.type = VARCHAR;
        memcpy(record.key.keyval.charkey, a_key, strlen(a_key)+1);
        if ((errCode = get(idx, txn, &record)) != SUCCESS) {
            if (errCode == DEADLOCK) {
                printf("DEADLOCK in get() in read_loop\n");
            }
            printf("could not get() in read_loop. ErrCode = %d\n", errCode);
            FAILEDLOOP = 1;
            READTXNBEGUN = 1;
            return;
        }
        
        while (errCode != DB_END) {
            memset(&record, 0, sizeof(Record));
            if (((errCode = getNext(idx, txn, &record)) != SUCCESS) && (errCode != DB_END)) {
                if (errCode == DEADLOCK) {
                    printf("DEADLOCK in getNext() in read_loop\n");
                }
                printf("could not getNext() in read_loop. ErrCode = %d\n", errCode);
                FAILEDLOOP = 1;
                READTXNBEGUN = 1;
                return;
            }
            
            if (errCode == SUCCESS) {
                //check to make sure this transaction can't see b_key
                if (strcmp(b_key, record.key.keyval.charkey) == 0) {
                    printf("read_loop received b_key from getNext() call\n");
                    FAILEDLOOP = 1;
                    READTXNBEGUN = 1;
                    return;
                }
                
                //check to make sure this transaction can't see (c,2) tuple
                if ((strcmp(c_key, record.key.keyval.charkey) == 0) &&
                    (strcmp(value_two, record.payload) == 0)) {
                    printf("read_loop received (c,2) from getNext() call\n");
                    FAILEDLOOP = 1;
                    READTXNBEGUN = 1;
                    return;
                }
            }
        }
        READTXNBEGUN = 1;
    }
    
    //commit this transaction
    if ((errCode = commitTransaction(txn)) != SUCCESS) {
        printf("could not commit read_loop transaction\n");
        FAILEDLOOP = 1;
    }
}


#ifndef RUNNING_SPEED_TEST
int main(void)
{
    return run_phantomtest();
}
#endif

int run_phantomtest(void){
    int errCode;
    IdxState *idx;
    TxnState *txn;
    Record record;
    pthread_t back_loop_thread;
        
    static Key k_a;
    k_a.type = VARCHAR;
    memcpy(k_a.keyval.charkey, a_key, strlen(a_key)+1);
    
    Key k_b;
    k_b.type = VARCHAR;
    memcpy(k_b.keyval.charkey, b_key, strlen(b_key)+1);
    
    Key k_c;
    k_c.type = VARCHAR;
    memcpy(k_c.keyval.charkey, c_key, strlen(c_key)+1);
    
    if ((errCode = create(VARCHAR, phantom_index)) != SUCCESS) {
        printf("could not create phantom index\n");
        return EXIT_FAILURE;
    }
    
    if ((errCode = openIndex(phantom_index, &idx)) != SUCCESS) {
        printf("count not open phantom index\n");
        return EXIT_FAILURE;
    }
    
    //populate the DB with (a,1)
    if ((errCode = insertRecord(idx, NULL, &k_a, value_one)) != SUCCESS) {
        printf("could not insert (a,1) into DB. ErrCode = %d\n", errCode);
        return EXIT_FAILURE;
    }
    
    //populate with (c, 1)
    if ((errCode = insertRecord(idx, NULL, &k_c, value_one)) != SUCCESS) {
        printf("could not insert (c,1) into DB. ErrCode = %d\n", errCode);
        return EXIT_FAILURE;
    }
    
    //start background loop transaction
    if (pthread_create(&back_loop_thread, NULL, read_loop, NULL) != 0) {
        printf("could not create read_loop thread\n");
        return EXIT_FAILURE;
    }
    
    //wait until the read loop has gone through the DB once
    while (READTXNBEGUN == 0) {
        sleep(.5);
    }
    
insert_txn:
    //begin insert transaction
    if ((errCode = beginTransaction(&txn)) != SUCCESS) {
        if (errCode == DEADLOCK) {
            printf("DEADLOCK while beginning insert txn\n");
            goto insert_txn;
        }
        printf("failed to begin main insert transaction. ErrCode = %d\n", errCode);
        return EXIT_FAILURE;
    }
    
    int i = 0;
    for (i = 0; i < 200; i++) {
        char payload[MAX_PAYLOAD_LEN + 1];
        memset(payload, 0, sizeof(char)*(MAX_PAYLOAD_LEN + 1));
        sprintf(payload,"bpay%d",i);
        
        //insert (b,i)
        if ((errCode = insertRecord(idx, txn, &k_b, payload)) != SUCCESS) {
            if (errCode == DEADLOCK) {
                printf("insert (b,%d) produced DEADLOCK\n",i);
                if ((errCode = abortTransaction(txn)) != SUCCESS) {
                    printf("could not abort deadlocked transaction. ErrCode = %d\n", errCode);
                    return EXIT_FAILURE;
                }
                goto insert_txn;
            }
            printf("could not insert (b,1). ErrCode = %d\n", errCode);
            return EXIT_FAILURE;
        }
    }
    
    //insert (c,2)
    if ((errCode = insertRecord(idx, txn, &k_c, value_two)) != SUCCESS) {
        if (errCode == DEADLOCK) {
            printf("insert (c,2) produced DEADLOCK\n");
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction. ErrCode = %d\n", errCode);
                return EXIT_FAILURE;
            }
            goto insert_txn;
        }
        printf("could not insert (c,2). ErrCode = %d\n", errCode);
        return EXIT_FAILURE;
    }
    
    //commit the insert txn
    if ((errCode = commitTransaction(txn)) != SUCCESS) {
        if (errCode == DEADLOCK) {
            if ((errCode = abortTransaction(txn)) != SUCCESS) {
                printf("could not abort deadlocked transaction\n");
            }
            goto insert_txn;
        }
        printf("unable to commit insert_txn. ErrCode = %d\n", errCode);
        return EXIT_FAILURE;
    }
    
    
    if (FAILEDLOOP == 1) {
        return EXIT_FAILURE;
    } else {
        return EXIT_SUCCESS;
    }
}













