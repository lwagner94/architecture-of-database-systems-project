#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <pthread.h>
#include <assert.h>


#include "server.h"

int main(void) {
    assert(create(INT, "foobar") == SUCCESS);

    IdxState * state = NULL;
    assert(openIndex("foobar", &state) == SUCCESS);
    Key k;
    k.type = INT;
    k.keyval.intkey = 10;

    assert(insertRecord(state, NULL, &k, "foobar") == SUCCESS);

    IdxState * s1 = NULL;
    assert(openIndex("foobar", &s1) == SUCCESS);

    IdxState * s2 = NULL;
    assert(openIndex("foobar", &s2) == SUCCESS);

    TxnState* t1;
    TxnState* t2;

    assert(beginTransaction(&t1) == SUCCESS);
    assert(beginTransaction(&t2) == SUCCESS);

    Record r;
    r.key.type = INT;
    r.key.keyval.intkey = 10;
    strcpy(r.payload, "foobar");

    assert(deleteRecord(s1, t1, &r) == SUCCESS);
    assert(get(s1, t1, &r) == KEY_NOTFOUND);
    assert(get(s2, t2, &r) == KEY_NOTFOUND);

    assert(commitTransaction(t1) == SUCCESS);
    assert(commitTransaction(t2) == SUCCESS);

}
