cmake_minimum_required(VERSION 3.17)
project(architecture_of_database_systems_project)


add_executable(speed_test reference/driver/speed_test.c)

add_library(memdb STATIC src/memdb_server.cpp)


find_package(Threads REQUIRED)
target_link_libraries(speed_test PRIVATE Threads::Threads)
target_link_libraries(speed_test PRIVATE memdb)

set(CMAKE_CXX_STANDARD 17)

include_directories(reference)



add_executable(main test/main.cpp)