#include <catch2/catch.hpp>

#include "MemDB.h"
#include <string>
#include <string.h>
#include "bitutils.h"

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
//    SECTION("trivial abort") {
//        REQUIRE(db.beginTransaction(&txn) == SUCCESS);
//
//        REQUIRE(db.insertRecord(state, txn, &k, "payload") == SUCCESS);
//
//        Record r;
//        r.key = k;
//        REQUIRE(db.get(state, txn, &r) == SUCCESS);
//
//        REQUIRE(db.abortTransaction(txn) == SUCCESS);
//
//        REQUIRE(db.get(state, nullptr, &r) == KEY_NOTFOUND);
//    }

    REQUIRE(db.closeIndex(state) == SUCCESS);
    REQUIRE(db.drop((char*) "hello") == SUCCESS);
}

TEST_CASE( "Endianness conversions 32 bit", "" ) {
    uint8_t data[4];

    SECTION("trivial") {
        int32ToByteArray(data, 0);
        REQUIRE(charArrayToInt32(data) == 0);
    }

    SECTION("small positive integer") {
        int32ToByteArray(data, 42);
        REQUIRE(charArrayToInt32(data) == 42);
    }

    SECTION("small negative integer") {
        int32ToByteArray(data, -42);
        REQUIRE(charArrayToInt32(data) == -42);
    }

    SECTION("MAX_INT") {
        int32ToByteArray(data, INT32_MAX);
        REQUIRE(charArrayToInt32(data) == INT32_MAX);
    }

    SECTION("MIN_INT") {
        int32ToByteArray(data, INT32_MIN);
        REQUIRE(charArrayToInt32(data) == INT32_MIN);
    }

    SECTION("Ordering") {
        int32ToByteArray(data, 0xAABBCCDD);
        char cmp[4] = {(char) 0xAA, (char) 0xBB, (char) 0xCC, (char) 0xDD};
        REQUIRE(memcmp(data, cmp, 4) == 0);
    }
}

TEST_CASE( "Endianness conversions 64 bit", "" ) {
    uint8_t data[8];

    SECTION("trivial") {
        int64ToByteArray(data, 0);
        REQUIRE(charArrayToInt64(data) == 0);
    }

    SECTION("small positive integer") {
        int64ToByteArray(data, 42);
        REQUIRE(charArrayToInt64(data) == 42);
    }

    SECTION("small negative integer") {
        int64ToByteArray(data, -42);
        REQUIRE(charArrayToInt64(data) == -42);
    }

    SECTION("MAX_INT") {
        int64ToByteArray(data, INT64_MAX);
        REQUIRE(charArrayToInt64(data) == INT64_MAX);
    }

    SECTION("MIN_INT") {
        int64ToByteArray(data, INT64_MIN);
        REQUIRE(charArrayToInt64(data) == INT64_MIN);
    }

    SECTION("Ordering") {
        int64ToByteArray(data, 0xAABBCCDD01020304);
        char cmp[8] = {(char) 0xAA, (char) 0xBB, (char) 0xCC, (char) 0xDD, (char) 0x01, (char) 0x02, (char) 0x03, (char) 0x04};
        REQUIRE(memcmp(data, cmp, 8) == 0);
    }
}

TEST_CASE( "Varchar padding", "" ) {
    std::array<uint8_t, MAX_VARCHAR_LEN> dest {};


    SECTION("normal string") {
        REQUIRE(varcharToByteArray(dest.data(), (uint8_t *) "foo") == 125);
        REQUIRE(memcmp(dest.data() + (MAX_VARCHAR_LEN - 3), "foo", 3) == 0);
    }

    SECTION("max length string") {
        // Length: 128
        const uint8_t* s = (uint8_t*) "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        REQUIRE(varcharToByteArray(dest.data(), s) == 0);
        REQUIRE(memcmp(dest.data(), s, 128) == 0);
    }

    SECTION("maximum length") {
        // Length: 125
        const uint8_t * s = (uint8_t*) "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

        REQUIRE(varcharToByteArray(dest.data(), s) == 3);

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

TEST_CASE( "calculateNextTwoIndices", "" ) {
    SECTION("trivial") {
        uint8_t data[4] = {0xAB, 0xCD, 0x12, 0x34};

        auto first = calculateNextTwoIndices(data, 0);
        auto second = calculateNextTwoIndices(data, 1);
        auto third = calculateNextTwoIndices(data, 2);
        auto fourth = calculateNextTwoIndices(data, 3);


        REQUIRE(first.first == 0xA);
        REQUIRE(first.second == 0xB);

        REQUIRE(second.first == 0xC);
        REQUIRE(second.second == 0xD);

        REQUIRE(third.first == 0x1);
        REQUIRE(third.second == 0x2);

        REQUIRE(fourth.first == 0x3);
        REQUIRE(fourth.second == 0x4);
    }
}

TEST_CASE( "jumpList tests", "[jumplist]" ) {
    MemDB db;
    REQUIRE(db.create(VARCHAR, (char*) "hello") == SUCCESS);
    IdxState* state = nullptr;
    REQUIRE(db.openIndex("hello", &state) == SUCCESS);

    Key k;
    k.type = VARCHAR;

    SECTION("single insert") {
        memset(k.keyval.charkey, 0, 129);
        REQUIRE(db.insertRecord(state, nullptr, &k, "payload") == SUCCESS);
        Record r;
        r.key = k;
        REQUIRE(db.get(state, nullptr, &r) == SUCCESS);

        REQUIRE("payload" == std::string(r.payload));
    }

//    SECTION("multiple insert") {
//        REQUIRE(db.insertRecord(state, nullptr, &k, "payload") == SUCCESS);
//        REQUIRE(db.insertRecord(state, nullptr, &k, "payload") == ENTRY_EXISTS);
//        REQUIRE(db.insertRecord(state, nullptr, &k, "payload2") == SUCCESS);
//        Record r;
//        r.key = k;
//        REQUIRE(db.get(state, nullptr, &r) == SUCCESS);
//
//        REQUIRE("payload" == std::string(r.payload));
//    }

    REQUIRE(db.closeIndex(state) == SUCCESS);
    REQUIRE(db.drop((char*) "hello") == SUCCESS);

}