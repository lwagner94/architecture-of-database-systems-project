//
// Created by lukas on 07.01.21.
//

#include "server.h"
#include <iostream>
#include "MemDB.h"


static MemDB db;




/**
 Creates a new index data structure to be used by any thread.

 @param type specifies what type of key the index will use
 @param name a unique name to be used to identify this index in any process
 @return ErrCode
 SUCCESS if successfully created index.
 DB_EXISTS if index with specified name already exists.
 FAILURE if could not create index for some other reason.
 */
ErrCode create(KeyType type, char *name) {
    return db.create(type, name);
}

/**
 Drops an existing index.

 @param name an existing index
 @return ErrCode
 SUCCESS if successfully dropped index.
 FAILURE index doesn't exist or some other reason.
 */
ErrCode drop(char *name) {
    return db.drop(name);
}

/**
 Opens a specific index data structure to be used by this thread.

 @param name the unique name specifying the index being opened
 @param idxState returns the state handle for the index being opened
 @return ErrCode
 SUCCESS if successfully opened index.
 DB_DNE if the name given does not have an associated DB that has been create()d.
 FAILURE if DB exists but could not be opened for some other reason.
 */
ErrCode openIndex(const char *name, IdxState **idxState) {
    return db.openIndex(name, idxState);
}

/**
 Terminate use of current index by this thread.

 @param idxState The state variable for the index being closed
 @return ErrCode
 SUCCESS if succesfully closed index.
 DB_DNE is the DB never existed or was already closed by someone else.
 FAILURE if could not close DB for some other reason.
 **/
ErrCode closeIndex(IdxState *idxState) {
    return db.closeIndex(idxState);
}

/**
 Signals the beginning of a transaction.  Each thread can have only
 one outstanding transaction running at a time.

 @param txn Returns the transaction state for the new transaction.
 @return ErrCode
 SUCCESS if successfully began transaction.
 TXN_EXISTS if there is already a transaction begun for this thread.
 DEADLOCK if this transaction had to be aborted because of deadlock.
 FAILURE if could not begin transaction for some other reason.
 */
ErrCode beginTransaction(TxnState **txn) {
    return db.beginTransaction(txn);
}

/**
 Forces the current transaction to abort, rolling back all changes
 made during the course of the transaction.

 @param txn The state variable for the transaction being aborted.
 @return ErrCode
 SUCCESS if successfully aborted transaction.
 TXN_DNE if there was no transaction to abort.
 DEADLOCK if the abort failed because of deadlock.
 FAILURE if could not abort transaction for some other reason.
 */
ErrCode abortTransaction(TxnState *txn) {
    return db.abortTransaction(txn);
}

/**
 Signals the end of the current transaction, committing
 all changes created in the transaction.

 @param txn The state variable for the transaction being committed.
 @return ErrCode
 SUCCESS if successfully ended transaction.
 TXN_DNE if there was no transaction currently open.
 DEADLOCK if this transaction could not be closed because of deadlock.
 FAILURE if could not end transaction for some other reason.
 */
ErrCode commitTransaction(TxnState *txn) {
    return db.commitTransaction(txn);
}

/**
 Retrieve the first record associated with the given key value; if
 more than one record exists with this key, return the first record
 with this key. Contents of the retrieved record are copied into
 the user supplied Record structure.

 Records with the same key may be returned in any order, but it must
 be that if there are n records with the same key k, a call to get
 followed by n-1 calls to getNext will return all n records with key k.

 If get returns KEY_NOTFOUND for a key k, the caller may invoke getNext
 to find the first key after key k.

 @param idxState The state variable for this thread
 @param txn The transaction state to be used (or NULL if not in a transaction)
 @param record Record containing the key being retrieved, into which the
 payload is copied.
 @return ErrCode
 SUCCESS if successfully retrieved and returned unique record.
 KEY_NOTFOUND if specified key value was not found in the DB.
 DEADLOCK if this call could not complete because of deadlock.
 FAILURE if could not retrieve unique record for some other reason.
 */
ErrCode get(IdxState *idxState, TxnState *txn, Record *record) {
    return db.get(idxState, txn, record);
}

/**
 Retrieve the record following the previous record retrieved by get or
 getNext. If no such call has occurred since the current transaction
 began, or if this is called from outside of a transaction, this
 returns the first record in the index. Records are ordered in ascending
 order by key.  Records with the same key but different payloads
 may be returned in any order.

 If get returned KEY_NOT_FOUND for a key k, invoking getNext will
 return the first key after k.

 If the index is closed and reopened, or a new transaction has begun
 since any previous call of get or getNext, getNext returns the first
 record in the index.


 @param idxState The state variable for the index whose next Record
 is to be returned
 @param txn The transaction state to be used (or NULL if not in a transaction)
 @param record Record through which the next key/payload pair is returned
 @return ErrCode
 SUCCESS if successfully retrieved and returned the next record in the DB.
 DB_END if reached the end of the DB.
 DEADLOCK if this call could not complete because of deadlock.
 FAILURE if could not retrieve next record for some other reason.
 */
ErrCode getNext(IdxState *idxState, TxnState *txn, Record *record) {
    return db.getNext(idxState, txn, record);
}

/**
 Insert a payload associated with the given key. An identical key can
 be used multiple times, but only with unique payloads.  If this is
 called from outside of a transaction, it should commit immediately.
 Records in an index are ordered in ascending order by key.  Records
 with the same key may be stored in any order.

 The implementation is responsible for making a copy of payload
 (e.g., it may not assume that the payload pointer continues
 to be valid after this routine returns.)

 @param idxState The state variable for this thread
 @param txn The transaction state to be used (or NULL if not in a transaction)
 @param k key value for insert
 @param payload Pointer to the beginning of the payload string
 @return ErrCode
 SUCCESS if successfully inserted record into DB.
 ENTRY_EXISTS if identical record already exists in DB.
 DEADLOCK if this call could not complete because of deadlock.
 FAILURE if could not insert entry for some other reason.
 */
ErrCode insertRecord(IdxState *idxState, TxnState *txn, Key *k, const char* payload) {
    return db.insertRecord(idxState, txn, k, payload);
}

/**
 Remove the record associated with the given key from the index
 structure.  If a payload is specified in the Record, then the
 key/payload pair specified is removed. Otherwise, the payload pointer
 is a length 0 string and all records with the given key are removed from the
 database.  If this is called from outside of a transaction, it should
 commit immediately.

 @param idxState The state variable for this thread
 @param txn The transaction state to be used (or NULL if not in a transaction)
 @param record Record struct containing a Key and a char* payload
 (or NULL pointer) describing what is to be deleted
 @return ErrCode
 SUCCESS if successfully deleted record from DB.
 ENTRY_DNE if the specified key/payload pair could not be found in the DB.
 KEY_NOTFOUND if the specified key could not be found in the DB, with only the key specified.
 DEADLOCK if this call could not complete because of deadlock.
 FAILURE if could not delete record for some other reason.
 */
ErrCode deleteRecord(IdxState *idxState, TxnState *txn, Record *record) {
    return db.deleteRecord(idxState, txn, record);
}
