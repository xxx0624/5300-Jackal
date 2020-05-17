/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 *
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Spring 2020"
 *
 * Additional functionality related to CREATE TABLE
 * DROP TABLE, SHOW TABLE, SHOW COLUMNS
 *
 * authored by Jietao Zhan and Thomas Ficca 5/8/2020
 * CPSC5300/4300 SQ20
 *
 */
#include "SQLExec.h"

using namespace std;
using namespace hsql;

// define static data
Tables* SQLExec::tables = nullptr;
Indices* SQLExec::indices = nullptr;

// make query result be printable
ostream& operator<<(ostream& out, const QueryResult& qres) {
    if (qres.column_names != nullptr) {
        for (auto const& column_name : *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const& row : *qres.rows) {
            for (auto const& column_name : *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                case ColumnAttribute::INT:
                    out << value.n;
                    break;
                case ColumnAttribute::TEXT:
                    out << "\"" << value.s << "\"";
                    break;
                default:
                    out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

/*
 * Destructor
 */

QueryResult::~QueryResult() {
    if (column_names != nullptr)
        delete column_names;
    if (column_attributes != nullptr)
        delete column_attributes;
    if (rows != nullptr) {
        for (auto row : *rows)
            delete row;
        delete rows;

    }
}

/*
 * Execute SQL statement based on statement type
 * @param SQL statement
 * @return QueryResult
 */

QueryResult* SQLExec::execute(const SQLStatement* statement) {
  if (SQLExec::tables == nullptr) {
      SQLExec::tables = new Tables();
    }

    try {
        switch (statement->type()) {
        case kStmtCreate:
            return create((const CreateStatement*)statement);
        case kStmtDrop:
            return drop((const DropStatement*)statement);
        case kStmtShow:
            return show((const ShowStatement*)statement);
        default:
            return new QueryResult("not implemented");
        }
    }
    catch (DbRelationError & e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

/*
 * Pull out column name and attributes from AST's ColDef clause
 * @param col                AST column defintion
 * @param column_name        returned by reference
 * @param column_attributes  returned by reference
 */

void SQLExec::column_definition(const ColumnDefinition* col, Identifier& column_name, ColumnAttribute& column_attribute) {
    column_name = col->name; // Hold column name

    // Check column type
    switch (col->type) {
    case ColumnDefinition::INT:
        column_attribute.set_data_type(ColumnAttribute::INT);
        break;
    case ColumnDefinition::TEXT:
        column_attribute.set_data_type(ColumnAttribute::TEXT);
        break;
    default:
        throw SQLExecError("data not implemented");

    }
}


/*
 * Create function establishing tables, indexs and such
 * @param given statement for creation of something
 * @return QueryResult
*/

QueryResult* SQLExec::create(const CreateStatement* statement) {
    switch (statement->type) {
    case CreateStatement::kTable:
        return create_table(statement);
    case CreateStatement::kIndex:
        return create_index(statement);
    default:
        return new QueryResult("Only CREATE TABLE and CREATE INDEX are implemented");
    }
}

/*
 * helper function, creates a table in conjunction with create
 * @param given statement for table creation
 * @return QueryResult
 */

QueryResult* SQLExec::create_table(const CreateStatement* statement) {
    if (statement->type != CreateStatement::kTable) {
        return new QueryResult("Only handle CREATE TABLE");
    } // Check the statement

    Identifier table_name = statement->tableName;

    ValueDict row;
    row["table_name"] = Value(table_name);  // set the table name in the dictionary

    Handle table_handle = tables->insert(&row); // update table schema


    ColumnNames column_names;
    ColumnAttributes column_attributes;
    ColumnAttribute column_attribute;
    Identifier column_name;
    for (ColumnDefinition* col : *statement->columns) {
        column_definition(col, column_name, column_attribute);
        column_names.push_back(column_name);
        column_attributes.push_back(column_attribute);
    }

    try {
        Handles column_handles;
        DbRelation& cols = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try {
            for (unsigned int i = 0; i < column_names.size(); i++) {
                row["column_name"] = column_names[i];
                row["data_type"] = Value(column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
                column_handles.push_back(cols.insert(&row));
            }

            DbRelation& table = SQLExec::tables->get_table(table_name);

            // check if statement exisits
            if (statement->ifNotExists)
                table.create_if_not_exists();
            else
                table.create();
        }
        catch (exception & e) {
            try {
                for (unsigned int i = 0; i < column_handles.size(); i++) {
                    cols.del(column_handles.at(i));
                }// remove from columns 
            }
            catch (...) {
            }
            throw;
        }
    }
    catch (exception & e) {
        try {
            // remove from _tables
            SQLExec::tables->del(table_handle);
        }
        catch (...) {
        }
        throw;
    }


    return new QueryResult("Created " + table_name);
}

/*
 * helper function, creates an index in conjunction with create
 * @param given statement for index creation
 * @return QueryResult
 */

QueryResult* SQLExec::create_index(const CreateStatement* statement) {
    if (statement->type != CreateStatement::kIndex) {
        return new QueryResult("Only handles CREATE INDEX");
    }
    Identifier table_name = statement->tableName;
    ColumnNames column_names;
    Identifier index_name = statement->indexName;
    Identifier index_type;

    bool isUnique;

    ValueDict row;


    try {
        index_type = statement->indexType;
    }
    catch (exception & e) {
        index_type = "BTREE";
    }


    if (index_type == "BTREE") {
        isUnique = true;
    }
    else {
        isUnique = false;
    }


    // row setup
    row["table_name"] = table_name;
    row["index_name"] = index_name;
    row["index_type"] = index_type;
    row["is_unique"] = isUnique;
    row["seq_in_index"] = 0;

    Handles index_handles;
    try {
        for (auto const& col : *statement->indexColumns) {
            row["seq_in_index"].n += 1;
            row["column_name"] = string(col);
            index_handles.push_back(SQLExec::indices->insert(&row));
        }


        DbIndex& index = SQLExec::indices->get_index(table_name, index_name);
        index.create();
    }
    catch (exception & e) {
        try {
            for (unsigned int i = 0; i < index_handles.size(); i++) {
                SQLExec::indices->del(index_handles.at(i));
            }
        }
        catch (...) {
        }
        throw;
    }

    return new QueryResult("Created "+ index_name);
}

/*
 * Drop function, drops tables, indexs and such......
 * @param given statement for removal of things
 * @return QueryResult
 */

QueryResult* SQLExec::drop(const DropStatement* statement) {
    switch (statement->type) {
    case DropStatement::kTable:
        return drop_table(statement);
    case DropStatement::kIndex:
        return drop_index(statement);
    default:
        return new QueryResult("Drop table and index implemented only");
    }
}


/*
 * helper function, drops tables when used in conjunction with drop
 * @param given statement for table removal
 * @return QueryResult
 */

QueryResult* SQLExec::drop_table(const DropStatement* statement) {
    Identifier table_name = statement->name;

    if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME) {
        throw SQLExecError("Cannot drop a schema table");
    }

    ValueDict where;
    where["table_name"] = Value(table_name);

    DbRelation& table = SQLExec::tables->get_table(table_name);

    DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

    Handles* handles = columns.select(&where);
    for (auto const& handle : *handles)
        columns.del(handle); // remove from column
    delete handles;


    table.drop();
    SQLExec::tables->del(*SQLExec::tables->select(&where)->begin());

    return new QueryResult("Dropped table " + table_name);
}


/*
 * helper function, drops index when used in conjunction with drop
 * @param given statement for index removal
 * @return QueryResult
 */
QueryResult* SQLExec::drop_index(const hsql::DropStatement* statement) {

  //get table name and index name
  Identifier table_name = statement->name;
  Identifier index_name = statement->indexName;

  //get index from database
  DbIndex& index = SQLExec::indices->get_index(table_name, index_name);
  ValueDict where;
  where["table_name"] = Value(table_name);
  where["index_name"] = Value(index_name);
    
  
  Handles *index_handles = SQLExec::indices->select(&where);
  
  //remove index
  index.drop();

  for (auto const &handle : *index_handles)
    {
      SQLExec::indices->del(handle);
    }
  delete index_handles;
  return new QueryResult("dropped index " + index_name);
}


 /*
  * Show function, shows tables, columns, indexs and such
  * @param given statement for show table or columns
  * @return QueryResult
  */
QueryResult* SQLExec::show(const ShowStatement* statement) {
    switch (statement->type) {
    case ShowStatement::kTables:
        return show_tables(statement);
    case ShowStatement::kColumns:
        return show_columns(statement);
    case ShowStatement::kIndex:
    default:
        throw SQLExecError("unrecognized SHOW type");
    }
}


/*
  * helper function, used in conjunction with Show to show table
  * @param given statement for show table
  * @return QueryResult
  */
QueryResult* SQLExec::show_tables(const ShowStatement* statement) {
    ColumnNames* column_names = new ColumnNames;
    column_names->push_back("table_name");
    ColumnAttributes* column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    Handles* handles = SQLExec::tables->select();
    u_long n = handles->size() - 3; // -3 for table, columns, and indices of schema

    ValueDicts* rows = new ValueDicts;
    for (auto const& handle : *handles) {
        ValueDict* row = SQLExec::tables->project(handle, column_names);
        Identifier table_name = row->at("table_name").s;
        if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME && table_name != Indices::TABLE_NAME)
            rows->push_back(row);
        else
            delete row;
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(n) + " rows");

}

/*
  * Show columns function,
  * @param given statement for show columns
  * @return QueryResult
  */
QueryResult* SQLExec::show_columns(const ShowStatement* statement) {
    DbRelation &cols = SQLExec::tables->get_table(Columns::TABLE_NAME);

    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("column_name");
    column_names->push_back("data_type");
    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles* handles = cols.select(&where);
    u_long n = handles->size();

    ValueDicts* rows = new ValueDicts;
    for (auto const& handle : *handles) {
        ValueDict* row = cols.project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;

    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(n) + " rows");
}

/*
  * Show index function,
  * @param given statement for show index
  * @return QueryResult
  */
QueryResult* SQLExec::show_index(const hsql::ShowStatement* statement) {
    Identifier table_name = statement->tableName;
    ColumnNames* column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("index_name");
    column_names->push_back("column_name");
    column_names->push_back("index_type");
    column_names->push_back("is_unique");
    column_names->push_back("seq_in_index");

    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles* handles = SQLExec::indices->select(&where);
    u_long rowNum = handles->size();
    ValueDicts* rows = new ValueDicts;
    for (unsigned int i = 0; i < handles->size(); i++) {
        ValueDict* row = SQLExec::indices->project(handles->at(i), column_names);
        rows->push_back(row);
    }
    delete handles;

    return new QueryResult(column_names, nullptr, rows, "successfully returned " + to_string(rowNum) + " rows");
    //return new QueryResult("not implemented");
}

