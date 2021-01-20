cmake_minimum_required(VERSION 3.15)



project(architecture_of_database_systems_project)

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()


set(CMAKE_CXX_FLAGS_RELEASE "-O3")
set(CMAKE_CXX_FLAGS "-Wall -Wextra")
set(CMAKE_CXX_FLAGS_DEBUG "-g -fsanitize=address")
set(CMAKE_C_COMPILER gcc)
set(CMAKE_CXX_COMPILER g++)
set(CMAKE_EXE_LINKER_FLAGS_DEBUG "-fsanitize=address" )


add_executable(speed_test reference/driver/speed_test.c)
add_executable(unittest reference/driver/unittests.c)
add_executable(Test reference/driver/Test.c)
add_executable(vary_high reference/driver/vary_high.c)
add_executable(vary_low reference/driver/vary_low.c)
add_executable(phantom_test reference/driver/phantom_test.c)
add_executable(double_lookup reference/driver/double_lookup.c)


add_library(memdb SHARED
        src/server.cpp
        src/Tree.cpp
        src/Tree.h
        src/MemDB.cpp
        src/MemDB.h
        src/types.h
        src/L2Item.h
        src/L1Item.h
        src/L0Item.h
        src/Transaction.h
        src/bitutils.h)

target_compile_features(memdb PRIVATE cxx_std_17)
set(CMAKE_CXX_STANDARD 17)

find_package(Threads REQUIRED)
target_link_libraries(speed_test PRIVATE Threads::Threads)
target_link_libraries(speed_test PRIVATE memdb)

target_link_libraries(unittest PRIVATE Threads::Threads)
target_link_libraries(unittest PRIVATE memdb)

target_link_libraries(Test PRIVATE Threads::Threads)
target_link_libraries(Test PRIVATE memdb)

target_link_libraries(vary_high PRIVATE Threads::Threads)
target_link_libraries(vary_high PRIVATE memdb)

target_link_libraries(vary_low PRIVATE Threads::Threads)
target_link_libraries(vary_low PRIVATE memdb)

target_link_libraries(phantom_test PRIVATE Threads::Threads)
target_link_libraries(phantom_test PRIVATE memdb)

target_link_libraries(double_lookup PRIVATE Threads::Threads)
target_link_libraries(double_lookup PRIVATE memdb)


include_directories(src)
include_directories(test)

add_library(catch2 STATIC test/catch_amalgamated.cpp test/catch_amalgamated.hpp)

add_executable(tests test/main.cpp test/test.cpp)
target_link_libraries(tests PRIVATE catch2)
target_link_libraries(tests PRIVATE memdb)
