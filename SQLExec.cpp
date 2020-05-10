/**
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
  if (rows != nullptr){
    for(auto row : *rows)
      delete row;
    delete rows;
    
  }
}


QueryResult *SQLExec::execute(const SQLStatement *statement) {
    // FIXME: initialize _tables table, if not yet present

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

void SQLExec::column_definition(const ColumnDefinition *col,
                                Identifier &column_name,
                                ColumnAttribute &column_attribute) {
  column_name = col->name;

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

/*
 * Helper function used in conjunction with create to....create tables
 *
 */

QueryResult *SQLExec::create_table(const CreateStatement *statement) {
  Identifier table_name = statement->tableName;
  ColumnNames column_names;
  ColumnAttributes column_attributes;
  Identifier column_name;
  for (ColumnDefinition* col : *statement->columns) {
    column_definition(col, column_names, column_attributes);
    column_name.push_back(column_name);
    column_attributes.push_back(column_attributes);
  }
  return new QueryResult("Created" + table_name);
}

/*
 * Helper function used in conjunction with create to....create indexs
 *
*/

QueryResult *SQLExec::create_index(const CreateStatement* statement){
  //Identifier table_name = statement->tableName;
  //ColumnNames column_names;
  //Identifier index_name = statement->indexName;
  //for (ColumnDefinition* col : *statement->indexColumns){
  //  index.create();
  //}
  //return new QueryResult("Created" + index_name);
  return new QueryResult("not implemented");
}

/*
 * Drop function, drops tables, indexs and such......
 *
 */

QueryResult *SQLExec::drop(const DropStatement *statement) {
  /* switch (statement->type) {
  case DropStatement::kTable:
    return drop_table(statement);
  case DropStatement::kIndex:
    return drop_index(statement);
  default:
    return new QueryResult("Only DROP TABLE and DROP INDEX are implemented");
    }*/
  return new QueryResult("not implemented");
}

/*
 * Helper function, used in conjunction with drop to.....drop tables
 *
 */

QueryResult *SQLExec::drop_table(const DropStatement *statement) {
  /*Identifier table_name = statement->tableName;
  if ( table_name == Table::TABLE_NAME || table_name == Columns::TABLE_NAME){
    throw SQLExecError("Schema table cannot be droped");
  }
  DbRelation& table = SQLExec::tables->get_table(table_name);
  table.drop();

  return new QueryResult("droped" + table_name);*/
  return new QueryResult("not implemented");
}


/*
 * Show function, shows tables, columns, indexs and such.
 *
 */
QueryResult *SQLExec::show(const ShowStatement *statement) {
  switch (statement->type){
  case ShowStatement::kTables:
    return show_tables();
  case ShowStatement::kColumns:
    return show_columns();
  case ShowStatement::kIndex:
  default:
    throw SQLExecError("unrecognized SHOW type");
  }

    
/*
*  Helper function, used in conjunction with Show to....show tables
*
*/


}

QueryResult *SQLExec::show_tables() {
  /*ColumnNames column_names;
  column_names->push_back("table_names");
  ColumnAttributes column_attributes;
  column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));
  
  return new QueryResult(column_names, column_attributes);*/
  return new QueryResult("not implemented");
}

/*
 * Helper function, used in conjunction with show to show columns  
 *
*/
 
QueryResult *SQLExec::show_columns() {
  /*DbRelation& cols = SQLExec::tables->get_table(Columns::TABLE_NAME);
  ColumnNames column_names;
  column_names->push_back("table_name");
  column_names->push_back("column_name");
  column_names->push_back("data_type");
  ColumnAttributes column_attributes;
  column_attributes->push_back(ColumnAttributes::TEXT);

  return new QueryResult(column_names, column_attributes);*/
  return new QueryResult("not implemented");
}

