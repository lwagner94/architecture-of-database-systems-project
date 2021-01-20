
Requirements
------------
  - gcc (tested with v10.2.0), any C++17 capable gcc should suffice.
  - cmake (tested with 3.19.3)

Build steps
------------
  - Open project directory in terminal
  - `mkdir build`
  - `cd build`
  - `cmake ..`
  - `make`

In the `build` directory you will then find the following executables/libraries:
  - `speed_test`
  - `unittest`
  - `Test`
  - `double_lookup`
  - `vary_high`
  - `vary_low`
  - `tests`
  - `libmemdb.so`

Each executable corresponds to one of the drivers C files in `reference/driver`.
`libmemdb.so` contains the in-memory index implementation as a shared library.
`tests` executes my own unit-test suite (based on the catch2 framework, source code for those tests can be found in test/test.cpp)

Implementation
--------------
As a first step, the API defined in `server.h` was implemented in a very basic manner using STL data structures.
Of course, the performance wasn't quite comparable to the reference, so in the next step I decided to implement 
the same data structure as in the reference.
For this, I consulted the *"Efficient In-Memory Indexing with Generalized Prefix Trees"* paper that is linked from the lecture slides.
Using the information from the paper, I implemented the data structure in C++.
For some specific parts of my implemenation, namely some optimizations and transaction handling, 
I consulted the reference implementation to gain some insight on how it was implemented there.

`times.odt` contains some data on a few of my optimization steps.

Results
----------

On my local machine (`Arch Linux, gcc 10.2.0, Intel i7-6700K@4GHz, 16GB RAM`), 
my implementation is roughly 2.8x (5851ms vs 2079ms) slower than the reference.

Reference implementation:
```
./speed_test 1468 0 0 0 0 4000 16000
speed_test called with 4000 populate inserts per thread and 16000 tests per thread

Running the Speed Test, seed = 1468
Creating 17 indices.
Populating indices 17.
Time to populate: 29 milliseconds.
Testing the indices.
Time to test: 2049 milliseconds.
Testing complete.
NUM_DEADLOCK: 0
NUM_TXN_FAIL: 0
NUM_TXN_COMP: 800000
Overall time to run: 2079 milliseconds.
```

My implementation has the following performance:
```
./speed_test 1468 0 0 0 0 4000 16000
speed_test called with 4000 populate inserts per thread and 16000 tests per thread

Running the Speed Test, seed = 1468
Creating 17 indices.
Populating indices 17.
Time to populate: 62 milliseconds.
Testing the indices.
Time to test: 5788 milliseconds.
Testing complete.
NUM_DEADLOCK: 0
NUM_TXN_FAIL: 0
NUM_TXN_COMP: 800000
Overall time to run: 5851 milliseconds.

Cleaning up.

```
