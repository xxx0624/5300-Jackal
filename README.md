# 5300-Jackal
DB Relation Manager project for CPSC5300/4300 at Seattle U, Spring 2020

# Sprint Verano:

Authors:
Mark McKinnon
Fuyuan Geng

# Milestone 1: Skeletion
File relevant to this milestone is sql5300.cpp. Even though this file is also used in the second milestone, the parts that were not taken from Kevin Lundeen's code is our code for the first milestone.
Aim of this milestone is to create the skeleton for the database, and that skeleton just allows the user to type some simple SQL commands including create table, if not exists, insert into, select, from, where, join, left join, right join, not, values, and, or, as on, double, int, and text, and it will parse the SQL commands in an AST and spit back what the SQL is to the user. 
Typing quit ends the program.


# Milestone 2: Rudimentary Storage Engine
Files relevant to this milestone are sql5300.cpp, heap_storage.h, heap_storage.cpp, Makefile, and storage_engine.h/

storage_engine.h - This file contains the base classes and data types for the blocks, files, and relations that are used throughout the other programs.

heap_storage.h - This file contains the slottedPage, HeapFile, and HeapTable classes, and their data types, that inherit the base functionality from the classes defined in storage_engine.h (DbBlock, DbFile, and DbRelation respectively). The slottedPage is also inherited in the heapFile class to provide needed functionality. 

heap_storage.cpp - This file contains the implementation for the SlottedPage, HeapFile, and HeapTable classes as well as a test method to test the three classes from within sql5300.cpp.
However, the test function is not working properly, and it currenly throws an exception saying that Db::open is a directory when you type test into the SQL shell. 
All things considered, SlottedPage, and HeapFile are fully working, and HeapTable is fully working besides the testing methods not working.

There is a slotted page method commented out in the code, it should work, but since the other test method isn't working, we did not have time to test the slotted page test.
