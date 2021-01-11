#include <catch2/catch.hpp>

#include "MemDB.h"
#include <string>
#include <string.h>

TEST_CASE( "Basic create/drop tests", "[create]" ) {
    MemDB db;

    REQUIRE(db.create(INT, (char*) "hello") == SUCCESS);
    REQUIRE(db.create(INT, (char*) "hello") == DB_EXISTS);
    REQUIRE(db.drop((char*) "hello") == SUCCESS);
    REQUIRE(db.drop((char*) "hello") == FAILURE);
}

TEST_CASE( "Basic open/close tests", "[create]" ) {
    MemDB db;
    REQUIRE(db.create(INT, (char*) "hello") == SUCCESS);

    SECTION("open/close") {
        IdxState* state = nullptr;

        REQUIRE(db.openIndex("hello", &state) == SUCCESS);
        REQUIRE(db.closeIndex(state) == SUCCESS);
    }

    REQUIRE(db.drop((char*) "hello") == SUCCESS);

}

TEST_CASE( "Basic insert/get tests", "[create]" ) {
    MemDB db;
    REQUIRE(db.create(INT, (char*) "hello") == SUCCESS);
    IdxState* state = nullptr;
    REQUIRE(db.openIndex("hello", &state) == SUCCESS);

    Key k;
    k.type = INT;
    k.keyval.intkey = 0x1234ABCD1234ABCD;
    SECTION("single insert") {
        REQUIRE(db.insertRecord(state, nullptr, &k, "payload") == SUCCESS);
        Record r;
        r.key = k;
        REQUIRE(db.get(state, nullptr, &r) == SUCCESS);

        REQUIRE("payload" == std::string(r.payload));
    }

    SECTION("multiple insert") {
        REQUIRE(db.insertRecord(state, nullptr, &k, "payload") == SUCCESS);
        REQUIRE(db.insertRecord(state, nullptr, &k, "payload") == ENTRY_EXISTS);
        REQUIRE(db.insertRecord(state, nullptr, &k, "payload2") == SUCCESS);
        Record r;
        r.key = k;
        REQUIRE(db.get(state, nullptr, &r) == SUCCESS);

        REQUIRE("payload" == std::string(r.payload));
    }

    REQUIRE(db.closeIndex(state) == SUCCESS);
    REQUIRE(db.drop((char*) "hello") == SUCCESS);

}

TEST_CASE( "Basic deleteRecord tests", "[create]" ) {
    MemDB db;
    REQUIRE(db.create(INT, (char*) "hello") == SUCCESS);
    IdxState* state = nullptr;
    REQUIRE(db.openIndex("hello", &state) == SUCCESS);

    Key k;
    k.type = INT;
    k.keyval.intkey = 10;
    REQUIRE(db.insertRecord(state, nullptr, &k, "payload") == SUCCESS);

    SECTION("delete all") {
        REQUIRE(db.insertRecord(state, nullptr, &k, "payload2") == SUCCESS);

        Record r;
        r.key = k;
        r.payload[0] = 0;
        REQUIRE(db.deleteRecord(state, nullptr, &r) == SUCCESS);
        REQUIRE(db.get(state, nullptr, &r) == KEY_NOTFOUND);
    }

    SECTION("delete single") {
        REQUIRE(db.insertRecord(state, nullptr, &k, "payload2") == SUCCESS);

        Record r;
        r.key = k;
        strncpy(r.payload, "payload2", 100);
        REQUIRE(db.deleteRecord(state, nullptr, &r) == SUCCESS);
        REQUIRE(db.get(state, nullptr, &r) == SUCCESS);
    }

    REQUIRE(db.closeIndex(state) == SUCCESS);
    REQUIRE(db.drop((char*) "hello") == SUCCESS);
}

TEST_CASE( "Basic transaction tests", "[create]" ) {
    MemDB db;
    REQUIRE(db.create(INT, (char*) "hello") == SUCCESS);
    IdxState* state = nullptr;
    REQUIRE(db.openIndex("hello", &state) == SUCCESS);

    Key k;
    k.type = INT;
    k.keyval.intkey = 10;
    TxnState* txn = nullptr;

    SECTION("trivial transaction") {
        REQUIRE(db.beginTransaction(&txn) == SUCCESS);
        REQUIRE(db.commitTransaction(txn) == SUCCESS);
    }

    SECTION("trivial transaction") {
        REQUIRE(db.beginTransaction(&txn) == SUCCESS);

        REQUIRE(db.insertRecord(state, nullptr, &k, "payload") == SUCCESS);

        Record r;
        r.key = k;
        REQUIRE(db.get(state, txn, &r) == KEY_NOTFOUND);

        REQUIRE(db.commitTransaction(txn) == SUCCESS);
    }

    SECTION("trivial transaction") {
        REQUIRE(db.beginTransaction(&txn) == SUCCESS);

        REQUIRE(db.insertRecord(state, txn, &k, "payload") == SUCCESS);

        Record r;
        r.key = k;
        REQUIRE(db.get(state, nullptr, &r) == KEY_NOTFOUND);
        REQUIRE(db.get(state, txn, &r) == SUCCESS);

        REQUIRE(db.commitTransaction(txn) == SUCCESS);

        REQUIRE(db.get(state, nullptr, &r) == SUCCESS);
    }
    SECTION("trivial abort") {
        REQUIRE(db.beginTransaction(&txn) == SUCCESS);

        REQUIRE(db.insertRecord(state, txn, &k, "payload") == SUCCESS);

        Record r;
        r.key = k;
        REQUIRE(db.get(state, txn, &r) == SUCCESS);

        REQUIRE(db.abortTransaction(txn) == SUCCESS);

        REQUIRE(db.get(state, nullptr, &r) == KEY_NOTFOUND);
    }

    REQUIRE(db.closeIndex(state) == SUCCESS);
    REQUIRE(db.drop((char*) "hello") == SUCCESS);
}

TEST_CASE( "Endianness conversions 32 bit", "" ) {
    uint8_t data[4];

    SECTION("trivial") {
        Tree::int32ToByteArray(data, 0);
        REQUIRE(Tree::charArrayToInt32(data) == 0);
    }

    SECTION("small positive integer") {
        Tree::int32ToByteArray(data, 42);
        REQUIRE(Tree::charArrayToInt32(data) == 42);
    }

    SECTION("small negative integer") {
        Tree::int32ToByteArray(data, -42);
        REQUIRE(Tree::charArrayToInt32(data) == -42);
    }

    SECTION("MAX_INT") {
        Tree::int32ToByteArray(data, INT32_MAX);
        REQUIRE(Tree::charArrayToInt32(data) == INT32_MAX);
    }

    SECTION("MIN_INT") {
        Tree::int32ToByteArray(data, INT32_MIN);
        REQUIRE(Tree::charArrayToInt32(data) == INT32_MIN);
    }

    SECTION("Ordering") {
        Tree::int32ToByteArray(data, 0xAABBCCDD);
        char cmp[4] = {(char) 0xAA, (char) 0xBB, (char) 0xCC, (char) 0xDD};
        REQUIRE(memcmp(data, cmp, 4) == 0);
    }
}

TEST_CASE( "Endianness conversions 64 bit", "" ) {
    uint8_t data[8];

    SECTION("trivial") {
        Tree::int64ToByteArray(data, 0);
        REQUIRE(Tree::charArrayToInt64(data) == 0);
    }

    SECTION("small positive integer") {
        Tree::int64ToByteArray(data, 42);
        REQUIRE(Tree::charArrayToInt64(data) == 42);
    }

    SECTION("small negative integer") {
        Tree::int64ToByteArray(data, -42);
        REQUIRE(Tree::charArrayToInt64(data) == -42);
    }

    SECTION("MAX_INT") {
        Tree::int64ToByteArray(data, INT64_MAX);
        REQUIRE(Tree::charArrayToInt64(data) == INT64_MAX);
    }

    SECTION("MIN_INT") {
        Tree::int64ToByteArray(data, INT64_MIN);
        REQUIRE(Tree::charArrayToInt64(data) == INT64_MIN);
    }

    SECTION("Ordering") {
        Tree::int64ToByteArray(data, 0xAABBCCDD01020304);
        char cmp[8] = {(char) 0xAA, (char) 0xBB, (char) 0xCC, (char) 0xDD, (char) 0x01, (char) 0x02, (char) 0x03, (char) 0x04};
        REQUIRE(memcmp(data, cmp, 8) == 0);
    }
}

TEST_CASE( "Varchar padding", "" ) {
    uint8_t dest[MAX_VARCHAR_LEN];

    SECTION("trivial") {
        memset(dest, 1, MAX_VARCHAR_LEN);
        Tree::varcharToByteArray(dest, (uint8_t *) "");
        for (auto c : dest) {
            REQUIRE(c == 0);
        }
    }

    SECTION("normal string") {
        memset(dest, 1, MAX_VARCHAR_LEN);
        Tree::varcharToByteArray(dest, (uint8_t *) "foo");
        for (int i = 0; i < MAX_VARCHAR_LEN - 3; i++) {
            REQUIRE(dest[i] == 0);
        }

        REQUIRE(memcmp(dest + (MAX_VARCHAR_LEN - 3), "foo", 3) == 0);
    }

    SECTION("max length string") {
        memset(dest, 1, MAX_VARCHAR_LEN);

        // Length: 128
        const uint8_t* s = (uint8_t*) "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        Tree::varcharToByteArray(dest, s);
        REQUIRE(memcmp(dest, s, 128) == 0);
    }

    SECTION("maximum length") {
        memset(dest, 1, MAX_VARCHAR_LEN);

        // Length: 125
        const uint8_t * s = (uint8_t*) "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

        Tree::varcharToByteArray(dest, s);

        REQUIRE(dest[0] == 0);
        REQUIRE(dest[1] == 0);
        REQUIRE(dest[2] == 0);
        REQUIRE(dest[3] == 'a');
    }
}

TEST_CASE( "Tree::calculateIndex", "" ) {


    SECTION("trivial") {
        uint8_t data[4] = {0xAB, 0xCD, 0x12, 0x34};
        REQUIRE(Tree::calculateIndex(data, 0) == 0xA);
        REQUIRE(Tree::calculateIndex(data, 1) == 0xB);
        REQUIRE(Tree::calculateIndex(data, 2) == 0xC);
        REQUIRE(Tree::calculateIndex(data, 3) == 0xD);
        REQUIRE(Tree::calculateIndex(data, 4) == 0x1);
        REQUIRE(Tree::calculateIndex(data, 5) == 0x2);
        REQUIRE(Tree::calculateIndex(data, 6) == 0x3);
        REQUIRE(Tree::calculateIndex(data, 7) == 0x4);
    }

}

