cmake_minimum_required(VERSION 3.17)
project(architecture_of_database_systems_project)

set(CMAKE_CXX_FLAGS_RELEASE "-O3")
set(CMAKE_CXX_FLAGS "-Wall -Wextra")
set(CMAKE_CXX_FLAGS_DEBUG "-g")


add_executable(speed_test reference/driver/speed_test.c)
add_executable(unit_test reference/driver/unittests.c)




add_library(memdb STATIC src/memdb_server.cpp src/Tree.cpp src/Tree.h src/MemDB.cpp src/MemDB.h src/types.h src/L2Item.h src/L1Item.h src/L0Item.cpp src/L0Item.h src/Transaction.h)
target_compile_features(memdb PRIVATE cxx_std_17)


find_package(Threads REQUIRED)
target_link_libraries(speed_test PRIVATE Threads::Threads)
target_link_libraries(speed_test PRIVATE memdb)

target_link_libraries(unit_test PRIVATE Threads::Threads)
target_link_libraries(unit_test PRIVATE memdb)


set(CMAKE_CXX_STANDARD 17)


include_directories(src)

add_executable(main test/main.cpp)

find_package(Catch2 REQUIRED)
add_executable(tests test/test_main.cpp test/test.cpp)
target_link_libraries(tests PRIVATE Catch2::Catch2)
target_link_libraries(tests PRIVATE memdb)