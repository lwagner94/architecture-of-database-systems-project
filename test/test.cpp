#include <catch2/catch.hpp>

#include "MemDB.h"

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