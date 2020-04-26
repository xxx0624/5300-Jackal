# Makefile
# Mark McKinnon
# Fuyuan Geng
# CPSC 5300 Milestone 2
# Kevin Lundeen's Makefile needed to run make on all milestone 2 files

CCFLAGS     = -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c -ggdb
COURSE      = /usr/local/db6
INCLUDE_DIR = $(COURSE)/include
LIB_DIR     = $(COURSE)/lib

# List of files to compile
OBJS       = sql5300.o heap_storage.o

# Link the heap storage and storage engine files
sql5300: $(OBJS)
	g++ -L$(LIB_DIR) -o $@ $(OBJS) -ldb_cxx -lsqlparser

sql5300.o : heap_storage.h storage_engine.h
heap_storage.o : heap_storage.h storage_engine.h

# Compilation of any .o file using g++
%.o: %.cpp
	g++ -I$(INCLUDE_DIR) $(CCFLAGS) -o "$@" "$<"

# Rule for removing all non-source files (so they can get rebuilt from scratch)
clean:
	rm -f sql5300 *.o
