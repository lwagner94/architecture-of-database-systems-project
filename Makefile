#/***********************************************
# DEXTER 1.4 - compilation targets
#************************************************/


DRIVERDIR = driver
#HEADERS   = $(INCDIR)/server.h  \
#	    IXStruct.h IX_Byte.h L2List.h TXManager.h IndexDictionary.h
#
#SRC = IXServer.c L2List.c IX_Byte.c TXManager.c IndexDictionary.c
DRIVER_TU = $(DRIVERDIR)/Test.c
DRIVER_UNIT = $(DRIVERDIR)/unittests.c
#DRIVER_DILT = $(DRIVERDIR)/double_lookup.c
#DRIVER_PHANTOM = $(DRIVERDIR)/phantom_test.c
#DRIVER_VHB = $(DRIVERDIR)/vary_high.c
#DRIVER_VLB = $(DRIVERDIR)/vary_low.c
DRIVER_BENCHMARK = $(DRIVERDIR)/speed_test.c
#
INCLUDE   = -I.
#DSYS      = BSD42
COMPILER = gcc #icc #gcc
#DVERSION  =
#CFLAGS = -O3 -Wall -mmmx -fPIC -combine
LDFLAGS= -lpthread -Wl,--no-relax
#MINFLAGS = -O3 -mmmx -fPIC -lpthread -combine
MAXFLAGS_UBUNTU = -O3 -fPIC -lpthread
#MAXFLAGS = -O3 -fPIC -lpthread -combine
#
#tu: $(HEADERS) IXServer.c
#	$(COMPILER) $(MAXFLAGS_UBUNTU) $(INCLUDE) $(DRIVER_TU) $(SRC) -o IXServer_tu $(LDFLAGS)
#
#harness_test: $(HEADERS)
#	$(COMPILER) $(MAXFLAGS_UBUNTU) $(INCLUDE) $(DRIVER_BENCHMARK) $(SRC) -o speed_test $(LDFLAGS)
#
#unittest: $(HEADERS) IXServer.c
#	$(COMPILER) $(MAXFLAGS_UBUNTU) $(INCLUDE) $(DRIVER_UNIT) $(SRC) -o IXServer_unit $(LDFLAGS)
#
#dilt: $(HEADERS) IXServer.c
#	$(COMPILER) $(MAXFLAGS) -g $(INCLUDE) $(DRIVER_DILT) $(SRC) -o IXServer_dilt
#
#phantom: $(HEADERS) IXServer.c
#	$(COMPILER) $(MAXFLAGS) -g $(INCLUDE) $(DRIVER_PHANTOM) $(SRC) -o IXServer_phantom
#
#vhb: $(HEADERS) IXServer.c
#	$(COMPILER) $(MAXFLAGS) $(INCLUDE) $(DRIVER_VHB) $(SRC) -o IXServer_vhb
#
#vlb: $(HEADERS) IXServer.c
#	$(COMPILER) $(MAXFLAGS) $(INCLUDE) $(DRIVER_VLB) $(SRC) -o IXServer_vlb
#
#lib.so:	$(HEADERS) IXServer.c
#	$(COMPILER) -shared $(MAXFLAGS) $(INCLUDE) $(SRC) -o lib.so
#
#all: unittest tu harness_test
#
#test: all
#  ifeq ($(OS),Windows_NT)
#	  ./IXServer_tu.exe
#	  ./IXServer_unit.exe
#	  ./speed_test.exe
#  else #Linux
#	  ./IXServer_tu
#	  ./IXServer_unit
#	  ./speed_test
#  endif
#
#clean:
#	- rm -f *.o *.so *.dyn XServer_* *.gcno *.gcda *.results *.exe
#	- rm IXServer_unit IXServer_tu speed_test

RUST_SRC = $(wildcard src/*.rs)
RUST_LIB = build/libmemdb.so

build/libmemdb.so : $(RUST_SRC)
	cargo build --release
	cp target/release/libmemdb.so build/libmemdb.so

clean:
	rm -f build/*

unittest: $(RUST_LIB)
	$(COMPILER) $(MAXFLAGS_UBUNTU) $(INCLUDE) $(DRIVER_UNIT) $(RUST_LIB) -o build/IXServer_unit $(LDFLAGS)

#tu: $(RUST_LIB)
#	$(COMPILER) $(MAXFLAGS_UBUNTU) $(INCLUDE) $(DRIVER_TU) $(RUST_LIB) -o build/IXServer_tu $(LDFLAGS)

harness_test: $(RUST_LIB)
	$(COMPILER) $(MAXFLAGS_UBUNTU) $(INCLUDE) $(DRIVER_BENCHMARK) $(RUST_LIB) -o build/speed_test $(LDFLAGS)

all: unittest harness_test #tu

test: all
	#./build/IXServer_tu
	./build/IXServer_unit
	./build/speed_test
