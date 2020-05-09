# 5300-Instructor
DB Relation Manager project for CPSC5300/4300 at Seattle U, Spring 2020; Instructor Solution

## Tags
- <code>Milestone1</code> is playing around with the AST returned by the HyLine parser and general setup of the command loop.
- <code>Milestone2h</code> has the instructor-provided files for Milestone 2. (Note that <code>heap_storage.cpp</code> is just a stub.)
- <code>Milestone2</code> is the instructor's attempt to complete the Milestone 2 assignment.
- <code>Milestone3_prep</code> has the instructor-provided files for Milestone 3. The students' work is in 
<code>SQLExec.cpp</code> labeled with <code>FIXME</code>.

## Unit Tests
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

