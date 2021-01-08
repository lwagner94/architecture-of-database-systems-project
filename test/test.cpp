#include <catch2/catch.hpp>

#include "MemDB.h"
#include <string>

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

    SECTION("open/close") {
        Key k;
        k.type = INT;
        k.keyval.intkey = 10;

        db.insertRecord(state, nullptr, &k, "payload");
        Record r;
        r.key = k;
        db.get(state, nullptr, &r);

//        REQUIRE("payload" == std::string(r.payload));
    }

    REQUIRE(db.closeIndex(state) == SUCCESS);
    REQUIRE(db.drop((char*) "hello") == SUCCESS);

}