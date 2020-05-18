# 5300-Jackal
DB Relation Manager project for CPSC5300/4300 at Seattle U, Spring 2020; 

# Milestone 3/4 Authors
Jietao Zhan and Thomas Ficca

#Milestone 3
Functions for CREATE TABLE, DROP TABLE, SHOW TABLE and SHOW COLUMNS have been added, we need to resolve some compiler issues before we can complete testing.

#Milestone 4 
Functions for CREATE INDEX, SHOW INDEX, and DROP INDEX have been added, Milestone 3 works perfectly now, unfortunately problems with CREATE INDEX kept us from fully testing Milestone 4

## Tags
- <code>Milestone1</code> is playing around with the AST returned by the HyLine parser and general setup of the command loop.
- <code>Milestone2h</code> has the instructor-provided files for Milestone 2. (Note that <code>heap_storage.cpp</code> is just a stub.)
- <code>Milestone2</code> is the instructor's attempt to complete the Milestone 2 assignment.
- <code>Milestone3_prep</code> has the instructor-provided files for Milestone 3.
- <code>Milestone4_prep</code> has the instructor-provided files for Milestone 4.

The students' work is in 
<code>SQLExec.cpp</code> labeled with <code>FIXME</code>.

## Unit Tests
Program is started by
./sql5300 ../data


There are some tests for SlottedPage and HeapTable. They can be invoked from the <clode>SQL</code> prompt:
```sql
SQL> test
```
Be aware that failed tests may leave garbage Berkeley DB files lingering in your data directory. 
If you don't care about any data in there, you are advised to just delete them all after a failed test.
```sh
$ rm -f data/*
``` 

## Valgrind (Linux)
To run valgrind (files must be compiled with -ggdb):
```sh
$ valgrind --leak-check=full --suppressions=valgrind.supp ./sql5300 data
```
Note that we've added suppression for the known issues with the Berkeley DB library <em>vis-à-vis</em> valgrind.

