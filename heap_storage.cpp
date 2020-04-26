// Mark McKinnon
// Fuyuan Geng
// CPSC 5300 Milestone 2
// heap_storage.cpp

#include "heap_storage.h"
#include "storage_engine.h"
#include "db_cxx.h"
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <cassert>
#include <cstring>

using namespace std;

typedef u_int16_t u16;

// Get 2-byte integer at given offset in block
u16 SlottedPage::get_n(u16 offset) {
  return *(u16*)this->address(offset);
}

// Put a 2-byte integer at given offset in block
void SlottedPage::put_n(u16 offset, u16 n) {
  *(u16*)this->address(offset) = n;
}

// Male a void* pointer for a given offset into the data block
void* SlottedPage::address(u16 offset) {
  return (void*)((char*)this->block.get_data() + offset);
}

// Gets the header of a given ID
void SlottedPage::get_header(u16 &size, u16 &loc, RecordID id) {
  size = get_n(4 * id);
  loc = get_n(4 * id + 2);
}

// Check and see if there is room in the current block
bool SlottedPage::has_room(u16 size) {
  int available = 0;
  available = this->end_free - (this->num_records + 1) * 4;
  return size <= available;
}

// Shifts the records over in memory after deletion
void SlottedPage::slide(u16 start, u16 end) {
  u16 shift = end - start;
  //  u16 size, loc = 0;
  RecordIDs* rec = ids();
  
  if (shift == 0) {
    return;
  }
  memcpy(this->address(this->end_free), this->address(shift), end);

  for (auto const &record_id : *rec) {
    u16 size, loc;
    get_header(size, loc, record_id);
    if (loc <= start) {
      loc += shift;
      put_header(record_id, size, loc);
    }
  }
  delete rec;
  this->end_free += shift;
  put_header();
}


// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
  if (id == 0) {
    size = this->num_records;
    loc = this->end_free;
  }
  put_n(4*id, size);
  put_n(4*id + 2, loc);

}

// Constructor that sets up the slotted page
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) :
  DbBlock(block, block_id, is_new) {
  if (is_new) {
    this->num_records = 0;
    this->end_free = DbBlock::BLOCK_SZ - 1;
    put_header();
  } else {
    get_header(this->num_records, this->end_free);
  }
}


// Add a record with given data
RecordID SlottedPage::add(const Dbt* data){
  if (!has_room(data->get_size())) {
    throw DbBlockNoRoomError("not enough room for new record");
  }
  u16 id = ++this->num_records;
  u16 size = (u16) data->get_size();
  this->end_free -= size;
  u16 loc = this->end_free + 1;
  put_header();
  put_header(id, size, loc);
  memcpy(this->address(loc), data->get_data(), size);
  return id;
}

// Gets a given record
Dbt* SlottedPage::get(RecordID record_id) {
  u16 size, loc;

  get_header(size, loc, record_id);

  if (loc == 0) {
    return nullptr;
  }

  return new Dbt(this->address(loc), size);

}

// Puts a given record in the slotted page with given data
void SlottedPage::put(RecordID record_id, const Dbt &data) {
  u16 size, loc;

  get_header(size, loc, record_id);
  u16 new_size = (u16) data.get_size();

  if (new_size > size) {
    u16 extra = new_size - size;
    if (!has_room(extra)) {
      throw DbBlockNoRoomError("Not enough room in block");
    }
    slide(loc - loc, extra);
    memcpy(this->address(loc - extra), data.get_data(), new_size);
  } else {
    memcpy(this->address(loc), data.get_data(), new_size);
    slide(loc + new_size, loc + size);
  }
  get_header(record_id, size, loc);
  put_header(record_id, new_size, loc);
}

// Deletes a given record
void SlottedPage::del(RecordID record_id) {
  u16 loc = 0;
  u16 size = 0;

  get_header(record_id, size, loc);
  put_header(record_id, 0, 0);
  slide(loc, loc + size);
}

// Returns list of ids in slotted page
RecordIDs* SlottedPage::ids(void) {
  RecordIDs* vec = new RecordIDs();
  u16 size, loc = 0;
  
  for (RecordID i = 0; i < 65535; i++) {    
    get_header(size, loc, i);
    if (size && loc != 0) {
      vec->push_back(i);
    } else {
      continue;
    }
  }
  return vec;
}


// HeapFile

// Gets a new page from the slotted page
SlottedPage* HeapFile::get_new(void) {
  char block[DbBlock::BLOCK_SZ];
  memset(block, 0, sizeof(block));
  Dbt data(block, sizeof(block));

  int block_id = ++this->last;
  Dbt key(&block_id, sizeof(block_id));

  // write out an empty block and read it back in so Berkeley DB is managing the memory
  SlottedPage* page = new SlottedPage(data, this->last, true);
  this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
  this->db.get(nullptr, &key, &data, 0);
  return page;
}


// Opens db
void HeapFile::db_open(uint flags) {
  if (!this->closed) {
    return;
  }
  
  this->db.set_re_len(DbBlock::BLOCK_SZ); // record length - will be ignored if file already$
  this->db.open(nullptr, this->dbfilename.c_str(), nullptr, DB_RECNO, flags, 0);
  this->closed = false;
}


// Creates a new file using slotted page
void HeapFile::create(void) {
  this->db_open(DB_CREATE | DB_EXCL);
  SlottedPage *page = this->get_new(); // force one page to exist
  delete page;
}


// Removes file
void HeapFile::drop(void){
  close();
  Db db(_DB_ENV, 0);
  db.remove(this->dbfilename.c_str(), nullptr, 0);
}

// Opens db
void HeapFile::open(void) {
  db_open();
}


// Closes db
void HeapFile::close(void) {
  this->db.close(0);
  this->closed = true;
}

// Gets page at specified block
SlottedPage* HeapFile::get(BlockID block_id) {
  Dbt key(&block_id, sizeof(block_id));
  Dbt data;
  this->db.get(nullptr, &key, &data, 0);
  return new SlottedPage(data, block_id, false);
}

// Puts page at specified block
void HeapFile::put(DbBlock *block) {
  int block_id = block->get_block_id();
  Dbt key(&block_id, sizeof(block_id));
  this->db.put(nullptr, &key, block->get_block(), 0);
}

// Gets a list of block ids
BlockIDs* HeapFile::block_ids() {
  BlockIDs* vec = new BlockIDs();

  for (BlockID block_id = 1; block_id <= this->last; block_id++) {
    vec->push_back(block_id);
  }
  return vec;
}

// HeapTable

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names,
                     ColumnAttributes column_attributes) :
  DbRelation(table_name, column_names, column_attributes), file(table_name) {
}

// Execute create statment
void HeapTable::create() {
  file.create();
}


// execute create table if not exists sql statement
void HeapTable::create_if_not_exists() {
  try {
    open();
  }
  catch (DbException& e) {
    create();
  }
}

// Execute drop table sql statement
void HeapTable::drop() {
  file.drop();
}


// Open existing table
void HeapTable::open() {
  file.open();
}


// Closes the table
void HeapTable::close() {
  file.close();
}

// Return the handle of the inserted row
Handle HeapTable::insert(const ValueDict *row) {
  open();
  ValueDict* full_row = validate(row);
  Handle handle = append(full_row);
  delete full_row;
  return handle;
}


// Update is not implemented yet
void HeapTable::update(const Handle handle, const ValueDict *new_values) {
  throw DbRelationError("Not implemented");
}


// Delete record
void HeapTable::del(const Handle handle) {
  open();
  BlockID block_id = handle.first;
  RecordID record_id = handle.second;
  SlottedPage* block = this->file.get(block_id);
  block->del(record_id);
  this->file.put(block);
  delete block;
}

// Returns a list of handles for qualifying rows
Handles* HeapTable::select() {
  return select(nullptr);
}

// Helper function for select
Handles* HeapTable::select(const ValueDict *where) {
  open();
  Handles* handles = new Handles();
  BlockIDs* block_ids = file.block_ids();
  for (auto const& block_id : *block_ids) {
    SlottedPage* block = file.get(block_id);
    RecordIDs* record_ids = block->ids();
    for (auto const& record_id : *record_ids) {
      Handle handle(block_id, record_id);
      handles->push_back(handle);
    }
    delete record_ids;
    delete block;
  }
  delete block_ids;
  return handles;
}

// Return a sequence of all values for handle.
ValueDict* HeapTable::project(Handle handle) {
  return project(handle, &this->column_names);
}

// Helper function for project
ValueDict* HeapTable::project(Handle handle, const ColumnNames *column_names) {
  BlockID block_id = handle.first;
  RecordID record_id = handle.second;
  SlottedPage* block = file.get(block_id);
  Dbt* data = block->get(record_id);
  ValueDict* row = unmarshal(data);
  delete data;
  delete block;
  if (column_names->empty()) {
    return row;
  }
  ValueDict* result = new ValueDict();
  for (auto const& column_name : *column_names) {
    if (row->find(column_name) == row->end()) {
      throw DbRelationError("table does not have column named '" + column_name + "'");
    }
    (*result)[column_name] = (*row)[column_name];
  }
  delete row;
  return result;
}

// Validate given row in table
ValueDict* HeapTable::validate(const ValueDict *row) {
  ValueDict* full_row = new ValueDict();
  for (auto const& column_name : this->column_names) {
    Value value;
    ValueDict::const_iterator column = row->find(column_name);
    if (column == row->end()) {
      throw DbRelationError("don't know how to handle NULLs, defaults, etc. yet");
    } else {
      value = column->second;
    (*full_row)[column_name] = value;
    }
  }
  return full_row;
}

// Appends a row into table
Handle HeapTable::append(const ValueDict *row) {
  Dbt* data = marshal(row);
  SlottedPage* block = this->file.get(this->file.get_last_block_id());
  RecordID record_id;
  try {
    record_id = block->add(data);
  }
  catch (DbBlockNoRoomError& e) {
    // need a new block
    block = this->file.get_new();
    record_id = block->add(data);
  }
  this->file.put(block);
  delete block;
  delete[](char*)data->get_data();
  delete data;
  return Handle(this->file.get_last_block_id(), record_id);
}

// Marshals data at given row
Dbt* HeapTable::marshal(const ValueDict *row) {
  char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
  uint offset = 0;
  uint col_num = 0;
  for (auto const& column_name : this->column_names) {
    ColumnAttribute ca = this->column_attributes[col_num++];
    ValueDict::const_iterator column = row->find(column_name);
    Value value = column->second;
    if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
      *(int32_t*)(bytes + offset) = value.n;
      offset += sizeof(int32_t);
    }
    else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
      uint size = value.s.length();
      *(u16*)(bytes + offset) = size;
      offset += sizeof(u16);
      memcpy(bytes + offset, value.s.c_str(), size); // assume ascii for now
      offset += size;
    }
    else {
      throw DbRelationError("Only know how to marshal INT and TEXT");
    }
  }
  char *right_size_bytes = new char[offset];
  memcpy(right_size_bytes, bytes, offset);
  delete[] bytes;
  Dbt *data = new Dbt(right_size_bytes, offset);
  return data;
}

// Unmarshals given data
ValueDict* HeapTable::unmarshal(Dbt *data) {
  ValueDict *row = new ValueDict();
  Value value;
  char *bytes = (char*)data->get_data();
  uint offset = 0;
  uint col_num = 0;
  for (auto const& column_name: this->column_names) {
    ColumnAttribute ca = this->column_attributes[col_num++];
    value.data_type = ca.get_data_type();
    if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
      value.n = *(int32_t*)(bytes + offset);
      offset += sizeof(int32_t);
    } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
      u16 size = *(u16*)(bytes + offset);
      offset += sizeof(u16);
      char buffer[DbBlock::BLOCK_SZ];
      memcpy(buffer, bytes+offset, size);
      buffer[size] = '\0';
      value.s = string(buffer);  // assume ascii for now
      offset += size;
      //    } else if (ca.get_data_type() == ColumnAttribute::DataType::BOOLEAN) {
      //value.n = *(uint8_t*)(bytes + offset);
      //offset += sizeof(uint8_t);
    } else {
      throw DbRelationError("Only know how to unmarshal INT, and TEXT");
    }
    (*row)[column_name] = value;
  }
  return row;
}


/**
 * Print out given failure message and return false.
 * @param message reason for failure
 * @return false
 **/
 
//bool assertion_failure(string message) {
//  cout << "FAILED TEST: " << message << endl;
//  return false;
//}

/**
 * Testing function for SlottedPage.
 * @return true if testing succeeded, false otherwise 
 **/

/**
bool test_slotted_page() {
  // construct one
  char blank_space[DbBlock::BLOCK_SZ];
  Dbt block_dbt(blank_space, sizeof(blank_space));
  SlottedPage slot(block_dbt, 1, true);

  // add a record
  char rec1[] = "hello";
  Dbt rec1_dbt(rec1, sizeof(rec1));
  RecordID id = slot.add(&rec1_dbt);
  if (id != 1) {
    return assertion_failure("add id 1");
  }

  // get it back
  Dbt *get_dbt = slot.get(id);
  string expected(rec1, sizeof(rec1));
  string actual((char *) get_dbt->get_data(), get_dbt->get_size());
  if (expected != actual) {
    return assertion_failure("get 1 back " + actual);
  }
  delete get_dbt;
  
  // add another record and fetch it back
  char rec2[] = "goodbye";
  Dbt rec2_dbt(rec2, sizeof(rec2));
  id = slot.add(&rec2_dbt);
  if (id != 2) {
    return assertion_failure("add id 2");
  }
  
  // get it back
  get_dbt = slot.get(id);
  expected = string(rec2, sizeof(rec2));
  actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
  if (expected != actual) {
    return assertion_failure("get 2 back " + actual);
  }
  delete get_dbt;

  // test put with expansion (and slide and ids)
  char rec1_rev[] = "something much bigger";
  rec1_dbt = Dbt(rec1_rev, sizeof(rec1_rev));
  slot.put(1, rec1_dbt);

  // check both rec2 and rec1 after expanding put
  get_dbt = slot.get(2);
  expected = string(rec2, sizeof(rec2));
  actual = string((char *) get_dbt->get_data(), get_dbt->get_size());

  if (expected != actual) {
    return assertion_failure("get 2 back after expanding put of 1 " + actual);
  }
  delete get_dbt;

  get_dbt = slot.get(1);
  expected = string(rec1_rev, sizeof(rec1_rev));
  actual = string((char *) get_dbt->get_data(), get_dbt->get_size());

  if (expected != actual) {
    return assertion_failure("get 1 back after expanding put of 1 " + actual);
  }
  delete get_dbt;

  // test put with contraction (and slide and ids)
  rec1_dbt = Dbt(rec1, sizeof(rec1));
  slot.put(1, rec1_dbt);

  // check both rec2 and rec1 after contracting put
  get_dbt = slot.get(2);
  expected = string(rec2, sizeof(rec2));
  actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
  if (expected != actual) {
    return assertion_failure("get 2 back after contracting put of 1 " + actual);
  }
  delete get_dbt;
  get_dbt = slot.get(1);
  expected = string(rec1, sizeof(rec1));
  actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
  if (expected != actual) {
    return assertion_failure("get 1 back after contracting put of 1 " + actual);
  }
  delete get_dbt;

  // test del (and ids)
  RecordIDs *id_list = slot.ids();
  if (id_list->size() != 2 || id_list->at(0) != 1 || id_list->at(1) != 2) {
    return assertion_failure("ids() with 2 records");
  }
  delete id_list;
  slot.del(1);
  id_list = slot.ids();
  if (id_list->size() != 1 || id_list->at(0) != 2) {
    return assertion_failure("ids() with 1 record remaining");
  }
  delete id_list;
  get_dbt = slot.get(1);
  if (get_dbt != nullptr) {
    return assertion_failure("get of deleted record was not null");
  }
  
  // try adding something too big
  rec2_dbt = Dbt(nullptr, DbBlock::BLOCK_SZ - 10); // too big, but only because we have a record in there
  try {
    slot.add(&rec2_dbt);
    return assertion_failure("failed to throw when add too big");
  } catch (const DbBlockNoRoomError &exc) {
    // test succeeded - this is the expected path
  } catch (...) {
    // Note that this won't catch segfault signals -- but in that case we also know the test failed
    return assertion_failure("wrong type thrown when add too big");
  }
  return true;
  }
**/
  
// test function -- returns true if all tests pass

bool test_heap_storage()
{
  ColumnNames column_names;
  column_names.push_back("a");
  column_names.push_back("b");
  ColumnAttributes column_attributes;
  ColumnAttribute ca(ColumnAttribute::INT);
  column_attributes.push_back(ca);
  ca.set_data_type(ColumnAttribute::TEXT);
  column_attributes.push_back(ca);
  HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
  table1.create();
  std::cout << "create ok" << std::endl;
  table1.drop();
  // drop makes the object unusable because of BerkeleyDB restriction
  // -- maybe want to fix this some day
  std::cout << "drop ok" << std::endl;

  HeapTable table("_test_data_cpp", column_names, column_attributes);
  table.create_if_not_exists();
  std::cout << "create_if_not_exsts ok" << std::endl;

  ValueDict row;
  row["a"] = Value(12);
  row["b"] = Value("Hello!");
  std::cout << "try insert" << std::endl;
  table.insert(&row);
  std::cout << "insert ok" << std::endl;
  Handles* handles = table.select();
  std::cout << "select ok " << handles->size() << std::endl;
  ValueDict *result = table.project((*handles)[0]);
  std::cout << "project ok" << std::endl;
  Value value = (*result)["a"];
  if (value.n != 12) {
    return false;
  }
  value = (*result)["b"];
  if (value.s != "Hello!") {
    return false;
  }
  table.drop();
  return true;
}

