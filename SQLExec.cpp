/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Spring 2020"
 */
#include "SQLExec.h"
#include "ParseTreeToString.h"
#include "EvalPlan.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;
Indices *SQLExec::indices = nullptr;

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
                    case ColumnAttribute::BOOLEAN:
                        out << (value.n == 0 ? "false" : "true");
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
        for (auto row: *rows)
            delete row;
        delete rows;
    }
}


QueryResult *SQLExec::execute(const SQLStatement *statement) {
    // initialize _tables table, if not yet present
    if (SQLExec::tables == nullptr) {
        SQLExec::tables = new Tables();
        SQLExec::indices = new Indices();
    }

    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            case kStmtInsert:
                return insert((const InsertStatement *) statement);
            case kStmtDelete:
                return del((const DeleteStatement *) statement);
            case kStmtSelect:
                return select((const SelectStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

QueryResult *SQLExec::insert(const InsertStatement *statement) {
    Identifier table_name = statement->tableName;
    if(!table_exist(table_name)){
        throw SQLExecError(table_name + " not exist");
    }

    ValueDict row;
    ColumnNames column_names;
    DbRelation &table = tables->get_table(table_name);
    if (statement->columns != NULL) {
		for (char* column : *statement->columns) {
			column_names.push_back(column);
		}
	}
	else {
		column_names = table.get_column_names();
	}
    vector<Value> records;
	for (auto const record : *statement->values) {
		switch (record->type) {
		case kExprLiteralString:
			records.push_back(Value(record->name));
			break;
		case kExprLiteralInt:
			records.push_back(Value(record->ival));
			break;
		default:
			throw DbRelationError("Unsupported Data type!");
		}
	}
    
    uint size;
	// hold handle for inserting row 
	Handle record_handle;
	try {
		ValueDict row;
		Identifier column_name;
		for (u_int16_t i = 0; i < column_names.size(); i++) {
			column_name = column_names.at(i);
			row[column_name] = records.at(i);
		}
		// insert row to table
		record_handle = table.insert(&row);

		// update index
		IndexNames index_names = indices->get_index_names(table_name);
		size = index_names.size();
		try {
			for (u_int16_t i = 0; i < index_names.size(); i++) {
				DbIndex &index = indices->get_index(table_name, index_names[i]);
				index.insert(record_handle);
			}
		}
		catch (exception& e) {
			for (u_int16_t i = 0; i < index_names.size(); i++) {
				DbIndex &index = indices->get_index(table_name, index_names[i]);
				index.del(record_handle);
			}
		}
	}
	catch (exception& e) {
		try {
			table.del(record_handle);
		}
		catch (...) {}
		throw;
	}
    return new QueryResult("successfully inserted 1 row into " + table_name + " and " + to_string(size) + " indices");
}

QueryResult *SQLExec::del(const DeleteStatement *statement) {
    Identifier table_name = statement->tableName;
    DbRelation &table = SQLExec::tables->get_table(table_name);
    
    //start base of plan at a TableScan
    EvalPlan *plan = new EvalPlan(table);
    //enclose that in a delete if we have a where clause
    if(statement->expr != nullptr)
        plan = new EvalPlan(fetch_where_clause(statement->expr), plan);
    //optimize the plan
    EvalPlan *optimized = plan->optimize();
    EvalPipeline pipeline = optimized->pipeline();
    delete plan;
    delete optimized;

    // now delete all the handles
    IndexNames index_names = SQLExec::indices->get_index_names(table_name);
	// to hold hanles from piepleline
	Handles *handles = pipeline.second;
	// iterate to delete index from index table
	for (auto const &index_name : index_names) {
		DbIndex& index = SQLExec::indices->get_index(table_name, index_name);
		for (auto const &handle : *handles) {
			index.del(handle);
		}
	}
	// to hold suffix string statement for query result
	string suffix;
	// in case of no index deletion
	if (index_names.size() == 0)
		suffix = "";
	// in case of index/indices is/are deleted
	else
		suffix = " and " + to_string(index_names.size()) + " from indices";
	// delete from table
	for (auto const &handle : *handles) {
		pipeline.first->del(handle);
	}

    int size = handles->size();
    delete handles;
	return new QueryResult("successfully deleted " + to_string(size) + " rows from " + table_name + suffix);
}

ValueDict *SQLExec::fetch_where_clause(const Expr *expr){
    ValueDict *val = new ValueDict();
    if(expr == NULL){
        return val;
    }

    vector<Value>* res = new vector<Value>();
    expression(expr, res);
    for(int i = 0; i < (int)res->size(); i += 2){
        (*val)[res->at(i).s] = res->at(i + 1);
    }
    delete res;
    return val;
}

void SQLExec::operator_expression(const Expr *expr, vector<Value> *res){
    expression(expr->expr, res);
    if(expr->opType == Expr::SIMPLE_OP || expr->opType == Expr::AND){
        // for now, only support AND & EQUAL op
    } else {
        throw SQLExecError("not support this op");
    }
    if(expr->expr2 != NULL){
        expression(expr->expr2, res);
    }
}

void SQLExec::expression(const Expr *expr, vector<Value> *res){
    if(expr->type == kExprColumnRef || expr->type == kExprLiteralString){
        res->push_back(Value(expr->name));
    } else if(expr->type == kExprLiteralInt){
        res->push_back(Value(expr->ival));
    } else if(expr->type == kExprOperator){
        operator_expression(expr, res);
    }
    else {
        throw SQLExecError("not support this op");
    }
}


QueryResult *SQLExec::select(const SelectStatement *statement) {
    Identifier table_name = statement->fromTable->name;
    if(!table_exist(table_name)){
        throw SQLExecError(table_name + " not exist");
    }

    DbRelation &table = tables->get_table(table_name);
    const ColumnNames &all_cols = table.get_column_names();
    ColumnAttributes all_col_attrs = table.get_column_attributes();
    ColumnNames *cols = new ColumnNames;
    ColumnAttributes *attrs = new ColumnAttributes;
    // fetch all specified cols
    for(Expr *expr: *statement->selectList){
        if(expr->type == kExprStar){
            for(Identifier name : all_cols){
                cols->push_back(name);
            }
            break;
        } else {
            cols->push_back(string(expr->name));
        }
    }
    // fetch all specified attrs for cols
    for(Identifier name : *cols){
        for(int i = 0; i < (int)all_cols.size(); i ++){
            if(name == all_cols[i]){
                attrs->push_back(all_col_attrs[i]);
                break;
            }
        }
    }
    EvalPlan *plan = new EvalPlan(table);
    if(statement->whereClause != NULL){
        ValueDict *where = fetch_where_clause(statement->whereClause);
        plan = new EvalPlan(where, plan);
    }
    plan = new EvalPlan(cols, plan);
    plan = plan->optimize();
    ValueDicts *rows = plan->evaluate();
    delete plan;
    return new QueryResult(cols, attrs, rows, "successfully returned " + to_string(rows->size()) + " rows");
}

void
SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {
    column_name = col->name;
    switch (col->type) {
        case ColumnDefinition::INT:
            column_attribute.set_data_type(ColumnAttribute::INT);
            break;
        case ColumnDefinition::TEXT:
            column_attribute.set_data_type(ColumnAttribute::TEXT);
            break;
        case ColumnDefinition::DOUBLE:
        default:
            throw SQLExecError("unrecognized data type");
    }
}

// CREATE ...
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

QueryResult *SQLExec::create_table(const CreateStatement *statement) {
    Identifier table_name = statement->tableName;
    ColumnNames column_names;
    ColumnAttributes column_attributes;
    Identifier column_name;
    ColumnAttribute column_attribute;
    for (ColumnDefinition *col : *statement->columns) {
        column_definition(col, column_name, column_attribute);
        column_names.push_back(column_name);
        column_attributes.push_back(column_attribute);
    }

    // Add to schema: _tables and _columns
    ValueDict row;
    row["table_name"] = table_name;
    Handle t_handle = SQLExec::tables->insert(&row);  // Insert into _tables
    try {
        Handles c_handles;
        DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try {
            for (uint i = 0; i < column_names.size(); i++) {
                row["column_name"] = column_names[i];
                row["data_type"] = Value(column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
                c_handles.push_back(columns.insert(&row));  // Insert into _columns
            }

            // Finally, actually create the relation
            DbRelation &table = SQLExec::tables->get_table(table_name);
            if (statement->ifNotExists)
                table.create_if_not_exists();
            else
                table.create();

        } catch (...) {
            // attempt to remove from _columns
            try {
                for (auto const &handle: c_handles)
                    columns.del(handle);
            } catch (...) {}
            throw;
        }

    } catch (exception &e) {
        try {
            // attempt to remove from _tables
            SQLExec::tables->del(t_handle);
        } catch (...) {}
        throw;
    }
    return new QueryResult("created " + table_name);
}

QueryResult *SQLExec::create_index(const CreateStatement *statement) {
    Identifier index_name = statement->indexName;
    Identifier table_name = statement->tableName;

    // get underlying relation
    DbRelation &table = SQLExec::tables->get_table(table_name);

    // check that given columns exist in table
    const ColumnNames &table_columns = table.get_column_names();
    for (auto const &col_name: *statement->indexColumns)
        if (find(table_columns.begin(), table_columns.end(), col_name) == table_columns.end())
            throw SQLExecError(string("Column '") + col_name + "' does not exist in " + table_name);

    // insert a row for every column in index into _indices
    ValueDict row;
    row["table_name"] = Value(table_name);
    row["index_name"] = Value(index_name);
    row["index_type"] = Value(statement->indexType);
    row["is_unique"] = Value(string(statement->indexType) == "BTREE"); // assume HASH is non-unique --
    int seq = 0;
    Handles i_handles;
    try {
        for (auto const &col_name: *statement->indexColumns) {
            row["seq_in_index"] = Value(++seq);
            row["column_name"] = Value(col_name);
            i_handles.push_back(SQLExec::indices->insert(&row));
        }

        DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
        index.create();

    } catch (...) {
        // attempt to remove from _indices
        try {  // if any exception happens in the reversal below, we still want to re-throw the original ex
            for (auto const &handle: i_handles)
                SQLExec::indices->del(handle);
        } catch (...) {}
        throw;  // re-throw the original exception (which should give the client some clue as to why it did
    }
    return new QueryResult("created index " + index_name);
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch (statement->type) {
        case DropStatement::kTable:
            return drop_table(statement);
        case DropStatement::kIndex:
            return drop_index(statement);
        default:
            return new QueryResult("Only DROP TABLE and CREATE INDEX are implemented");
    }
}

QueryResult *SQLExec::drop_table(const DropStatement *statement) {
    Identifier table_name = statement->name;
    if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME)
        throw SQLExecError("cannot drop a schema table");

    ValueDict where;
    where["table_name"] = Value(table_name);

    // get the table
    DbRelation &table = SQLExec::tables->get_table(table_name);

    // remove any indices
    for (auto const &index_name: SQLExec::indices->get_index_names(table_name)) {
        DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
        index.drop();  // drop the index
    }
    Handles *handles = SQLExec::indices->select(&where);
    for (auto const &handle: *handles)
        SQLExec::indices->del(handle);  // remove all rows from _indices for each index on this table
    delete handles;

    // remove from _columns schema
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    handles = columns.select(&where);
    for (auto const &handle: *handles)
        columns.del(handle);
    delete handles;

    // remove table
    table.drop();

    // finally, remove from _tables schema
    handles = SQLExec::tables->select(&where);
    SQLExec::tables->del(*handles->begin()); // expect only one row from select
    delete handles;

    return new QueryResult(string("dropped ") + table_name);
}

QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    Identifier table_name = statement->name;
    Identifier index_name = statement->indexName;

    // drop index
    DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
    index.drop();

    // remove rows from _indices for this index
    ValueDict where;
    where["table_name"] = Value(table_name);
    where["index_name"] = Value(index_name);
    Handles *handles = SQLExec::indices->select(&where);
    for (auto const &handle: *handles)
        SQLExec::indices->del(handle);
    delete handles;

    return new QueryResult("dropped index " + index_name);
}

// SHOW ...
QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type) {
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        case ShowStatement::kIndex:
            return show_index(statement);
        default:
            throw SQLExecError("unrecognized SHOW type");
    }
}

QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    ColumnNames *column_names = new ColumnNames;
    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_names->push_back("table_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("index_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("column_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("seq_in_index");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::INT));

    column_names->push_back("index_type");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("is_unique");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::BOOLEAN));

    ValueDict where;
    where["table_name"] = Value(string(statement->tableName));
    Handles *handles = SQLExec::indices->select(&where);
    u_long n = handles->size();

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = SQLExec::indices->project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(n) + " rows");
}

QueryResult *SQLExec::show_tables() {
    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");

    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    Handles *handles = SQLExec::tables->select();
    u_long n = handles->size() - 3;

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = SQLExec::tables->project(handle, column_names);
        Identifier table_name = row->at("table_name").s;
        if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME && table_name != Indices::TABLE_NAME)
            rows->push_back(row);
        else
            delete row;
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(n) + " rows");
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("column_name");
    column_names->push_back("data_type");

    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles *handles = columns.select(&where);
    u_long n = handles->size();

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = columns.project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(n) + " rows");
}


/**
 * check if the table exists
 * @param table_name  name of the table you want to check
 */
bool SQLExec::table_exist(Identifier table_name){
    QueryResult *query_result = show_tables();
    bool res = false;
    for(auto const &row : *(query_result->get_rows())){
        if(row->at("table_name") == Value(table_name)){
            res = true;
            break;
        }
    }
    delete query_result;
    return res;
}