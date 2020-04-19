//Milestone1
//Fuyuan Geng/	McKinnon, Mark D.

#include <string.h>
#include "db_cxx.h"
#include "SQLParser.h"

using namespace std;
using namespace hsql;


string executeCreate(const CreateStatement *stmt){
	string ret("CREATE TABLE ");
	if(stmt->type != CreateStatement::kTable)
		return ret+"...";
	if(stmt->ifNotExists)
		ret+="If NOT EXISTS ";
	ret+= string(stmt->tableName)+"(..)";
	return ret;
}

string execute(const SQLStatement *stmt){
	switch(stmt->type()){
		case kStmtSelect:
		 	return "SELECT ...":
		case kStmtCreate:
			return executeCreate((const CreateStatement *)stmt);
		case kStmtInsert:
			return "INSERT ...";
		default:
			return "Not implemented";
	 }
}


int main(int argc, char *argv[]) {
	if(arge !=2){
	cerr<<"Usage: ./sql5300 dbenvapth" <<endl ;
	return EXIT_FAILURE;
}
 	char *envHome=argv[1]; 	
	cout<<"(sql5300:running with database environment at" <<envHome<<")"<<endl;
	
	DbEnv env(0U);
	env.set_message_stream(&std::cout);
	env.set_error_stream(&std::cerr);
	try{
	env.open(envHome,DB_CREATE  | DB_INIT_MPOOL, 0);
	}catch(DbException& exc){
		cerr<<"(sql5300:"<<exc.what()<<")"<<endl;
		exit(1);
	}

	while(true){
		cout<<" SQL >" ;
		string query;
		getline(cin,guery);
		if(query.length()==0)
			continue;
                if(query=="quit")
			break;
	
	//USE sql parser to get on ASI
	SQLParserResult *result=SQLParser::parseSQLString(query);
	if(!result->isValid()){
		cout<<"invalid SQL:"<<query<<endl;
		delete result;
		//continue;
	}else{
	 	cout<<"valid"<<endl;
  	}
	for(uint i=0;i<result->size();i++){
	    cout<<execute(result->getStatement(i))<<endl;
	}
	
	//cout<<" your query was: "<< query <<endl;
	delete result;
     }
	return EXIT_SUCCESS;
}

