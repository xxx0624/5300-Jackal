/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 *
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Spring 2020"
 *
 * Additional functionality related to CREATE TABLE
 * DROP TABLE, SHOW TABLE, SHOW COLUMNS authored by
 * Jietao Zhan and Thomas Ficca 5/8/2020
 * CPSC5300/4300 SQ20
 *
 */
#include "SQLExec.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;
//Indices* SQLExec::indices = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
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


QueryResult *SQLExec::execute(const SQLStatement *statement) {
    if (tables == nullptr) {
        tables = new Tables();
    }

    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

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
 *
*/

QueryResult *SQLExec::create(const CreateStatement *statement) {
  switch (statement->type) {
  case CreateStatement::kTable:
    return create_table(statement);
  case CreateStatement::kIndex:
    return create_index(statement);
  default:
    return new QueryResult("Only CREATE TABLE and CREATE INDEX are implemented");
  }
}


// CREATE TABLE
QueryResult *SQLExec::create_table(const CreateStatement *statement) {
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
                }
            }
            catch (...) {
            }
            throw;
        }
    }
    catch (exception & e) {
        try {
            //attempt to remove from _tables
            SQLExec::tables->del(table_handle);
        }
        catch (...) {
        }
        throw;
    }


    return new QueryResult("Created " + table_name);
}


// CREATE INDEX
QueryResult *SQLExec::create_index(const CreateStatement* statement){
    if (statement->type != CreateStatement::kIndex) {
        return new QueryResult("Only handles CREATE INDEX");
    }
  Identifier table_name = statement->tableName;
  ColumnNames column_names;
  Identifier index_name = statement->indexName;
  Identifier index_type;

  bool is_unique;

  ValueDict row;
 

  try {
      index_type = statement->indexType;
  }
  catch (exception & e) {
      index_type = "BTREE";
  }


  if (index_type == "BTREE") {
      is_unique = true;
  }
  else {
      is_unique = false;
  }

  row["table_name"] = table_name;
  row["index_name"] = index_name;
  row["index_type"] = index_type;
  row["is_unique"] = is_unique;
  row["seq_in_index"] = 0;

  Handles index_handles;
  //try {
  //    for (auto const& col : *statement->indexColumns) {
  //        row["seq_in_index"].n += 1;
  //        row["column_name"] = string(col);
  //        index_handles.push_back(SQLExec::indices->insert(&row));
  //    }
      
      //index.create();
  //}
  
  return new QueryResult("Working on it");
}

/*
 * Drop function, drops tables, indexs and such......
 *
 */

QueryResult* SQLExec::drop(const DropStatement* statement) {
    switch (statement->type) {
    case DropStatement::kTable:
        return drop_table(statement);
    //case DropStatement::kIndex:
    //    return drop_index(statement);
    default:
        return new QueryResult("Drop implemented");
    }
}



QueryResult *SQLExec::drop_table(const DropStatement *statement) {
    Identifier table_name = statement->name;

    if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME ) {
        throw SQLExecError("Cannot drop a schema table");
    }

    ValueDict where;
    where["table_name"] = Value(table_name);

    DbRelation& table = SQLExec::tables->get_table(table_name);

    // DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);


    table.drop();
    SQLExec::tables->del(*SQLExec::tables->select(&where)->begin());

    return new QueryResult("Droped table " + table_name);
}


//QueryResult* drop_index(const hsql::DropStatement* statement) {
//    // FIXME
//    return new QueryResult("not implemented");
//}


/*
 * Show function, shows tables, columns, indexs and such.
 *
 */
QueryResult *SQLExec::show(const ShowStatement *statement) {
  switch (statement->type){
  case ShowStatement::kTables:
    return show_tables(statement);
  case ShowStatement::kColumns:
    return show_columns(statement);
  case ShowStatement::kIndex:
  default:
    throw SQLExecError("unrecognized SHOW type");
  }
}

QueryResult *SQLExec::show_tables(const ShowStatement* statement) {
    //ColumnNames column_names;
    //column_names->push_back("table_names");
    //ColumnAttributes column_attributes;
    //column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    //return new QueryResult(column_names + "   " + column_attributes);
    return new QueryResult("working on it");

}

/*
 * Helper function, used in conjunction with show to show columns  
 *
*/
 
QueryResult *SQLExec::show_columns(const ShowStatement* statement) {
    //DbRelation& cols = SQLExec::tables->get_table(Columns::TABLE_NAME);
    //ColumnNames column_names;
    //column_names->push_back("table_name");
    //column_names->push_back("column_name");
    //column_names->push_back("data_type");
    //ColumnAttributes column_attributes;
    //column_attributes->push_back(ColumnAttributes::TEXT);

    return new QueryResult("working on it");


}

