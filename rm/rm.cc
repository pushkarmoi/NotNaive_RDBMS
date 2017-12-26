#include "rm.h"
#include <cmath>

RelationManager* RelationManager::instance()
{
    static RelationManager _rm;
    return &_rm;
}


RC RelationManager::createCatalog()
{
		RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();

		// NOTE - can't call getRecordDescriptor. Because it queries 'Tables' and 'Columns'
		Attribute matr;

		vector<Attribute> tablesRecDes;
		matr.name = "table-id"; matr.type = TypeInt; matr.length = 4;
		tablesRecDes.push_back(matr);
		matr.name = "table-name"; matr.type = TypeVarChar; matr.length = 50;
		tablesRecDes.push_back(matr);
		matr.name = "file-name"; matr.type = TypeVarChar; matr.length = 50;
		tablesRecDes.push_back(matr);

		vector<Attribute> columnsRecDes;
		matr.name = "table-id"; matr.type = TypeInt; matr.length = 4;
		columnsRecDes.push_back(matr);
		matr.name = "column-name"; matr.type = TypeVarChar; matr.length = 50;
		columnsRecDes.push_back(matr);
		matr.name = "column-type"; matr.type = TypeInt; matr.length = 4;
		columnsRecDes.push_back(matr);
		matr.name = "column-length"; matr.type = TypeInt; matr.length = 4;
		columnsRecDes.push_back(matr);
		matr.name = "column-position"; matr.type = TypeInt; matr.length = 4;
		columnsRecDes.push_back(matr);


		RID mrid;	// a useless value


		/*
		 *
		 * INSERT Hardcoded rows in 'Tables' table
		 *
		 */


		FileHandle tabFileHandler;
		if(rbfm->createFile("Tables") != (RC) 0)
			return (RC) -1;
		if(rbfm->openFile("Tables", tabFileHandler) != (RC) 0)
			return (RC) -1;


		char* data = new char[13 + 6 + 6];
		prepareRecordTables(data, 1, "Tables", "Tables");
		if(rbfm->insertRecord(tabFileHandler, tablesRecDes, data, mrid) != (RC) 0){
			delete[] data;
			return (RC) -1;
		}

		delete[] data;
		data = new char[13 + 7 + 7];
		prepareRecordTables(data, 2, "Columns", "Columns");
		if(rbfm->insertRecord(tabFileHandler, tablesRecDes, data, mrid) != (RC) 0){
			delete[] data;
			return (RC) -1;
		}

		delete[] data;
		data = new char[13 + 7 + 7];
		prepareRecordTables(data, 3, "Indexes", "Indexes");
		if(rbfm->insertRecord(tabFileHandler, tablesRecDes, data, mrid) != (RC) 0){
			delete[] data;
			return (RC) -1;
		}

		delete[] data;

		if(rbfm->closeFile(tabFileHandler) != (RC) 0){
			return (RC) -1;
		}


		/*
		 *
		 * INSERT into 'Columns' table
		 *
		 */

		FileHandle colFileHandler;
		if(rbfm->createFile("Columns") != (RC) 0){
			return (RC) -1;
		}

		if(rbfm->openFile("Columns", colFileHandler) != (RC) 0){
			return (RC) -1;
		}


		data = new char[21 + 8];	// "table-id"
		prepareRecordColumns(data, 1, "table-id", 0, 4, 1);
		if(rbfm->insertRecord(colFileHandler, columnsRecDes, data, mrid) != (RC) 0){
			delete[] data;
			return (RC) -1;
		}

		delete[] data;
		data = new char[21 + 10];		// "table-name"
		prepareRecordColumns(data, 1, "table-name", 2, 50, 2);
		if(rbfm->insertRecord(colFileHandler, columnsRecDes, data, mrid) != (RC) 0){
			delete[] data;
			return (RC) -1;
		}

		delete[] data;
		data = new char[21 + 9];		// "file-name"
		prepareRecordColumns(data, 1, "file-name", 2, 50, 3);
		if(rbfm->insertRecord(colFileHandler, columnsRecDes, data, mrid) != (RC) 0){
			delete[] data;
			return (RC) -1;
		}



		delete[] data;
		data = new char[21 + 8];	// "table-id"
		prepareRecordColumns(data, 2, "table-id", 0, 4, 1);
		if(rbfm->insertRecord(colFileHandler, columnsRecDes, data, mrid) != (RC) 0){
			delete[] data;
			return (RC) -1;
		}

		delete[] data;
		data = new char[21 + 11];	// "column-name"
		prepareRecordColumns(data, 2, "column-name", 2, 50, 2);
		if(rbfm->insertRecord(colFileHandler, columnsRecDes, data, mrid) != (RC) 0){
			delete[] data;
			return (RC) -1;
		}

		delete[] data;
		data = new char[21 + 11];	// "column-type"
		prepareRecordColumns(data, 2, "column-type", 0, 4, 3);
		if(rbfm->insertRecord(colFileHandler, columnsRecDes, data, mrid) != (RC) 0){
			delete[] data;
			return (RC) -1;
		}


		delete[] data;
		data = new char[21 + 13];	// "column-length"
		prepareRecordColumns(data, 2, "column-length", 0, 4, 4);
		if(rbfm->insertRecord(colFileHandler, columnsRecDes, data, mrid) != (RC) 0){
			delete[] data;
			return (RC) -1;
		}


		delete[] data;
		data = new char[21 + 15];	// "column-position"
		prepareRecordColumns(data, 2, "column-position", 0, 4, 5);
		if(rbfm->insertRecord(colFileHandler, columnsRecDes, data, mrid) != (RC) 0){
			delete[] data;
			return (RC) -1;
		}

		delete[] data;
		data = new char[21 + 8];	// "table-id"
		prepareRecordColumns(data, 3, "table-id", 0, 4, 1);
		if(rbfm->insertRecord(colFileHandler, columnsRecDes, data, mrid) != (RC) 0){
			delete[] data;
			return (RC) -1;
		}

		delete[] data;
		data = new char[21 + 10];	// "table-name"
		prepareRecordColumns(data, 3, "table-name", 2, 50, 2);
		if(rbfm->insertRecord(colFileHandler, columnsRecDes, data, mrid) != (RC) 0){
			delete[] data;
			return (RC) -1;
		}

		delete[] data;
		data = new char[21 + 8];	// "key-name"
		prepareRecordColumns(data, 3, "key-name", 2, 50, 3);
		if(rbfm->insertRecord(colFileHandler, columnsRecDes, data, mrid) != (RC) 0){
			delete[] data;
			return (RC) -1;
		}

		delete[] data;

		if(rbfm->closeFile(colFileHandler) != (RC) 0){
			return (RC) -1;
		}


		/*
		 *
		 * CREATE 'Indexes' table file. No need to insert anything.
		 *
		 */

		if(rbfm->createFile("Indexes") != (RC) 0){
			return (RC) -1;
		}

		return (RC) 0;
}

// only delete the system tables.
RC RelationManager::deleteCatalog()
{
	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();

	FileHandle mFileHandler1;
	FileHandle mFileHandler2;
	FileHandle mFileHandler3;


	if((rbfm->openFile("Tables", mFileHandler1) != (RC) 0) ||
			(rbfm->openFile("Columns", mFileHandler2) != (RC) 0) ||
				(rbfm->openFile("Indexes", mFileHandler3) != (RC) 0)){

		// a catalog wasn't created
		//rbfm->destroyFile("Tables");
		//rbfm->destroyFile("Columns");
		//rbfm->destroyFile("Indexes");
		return (RC) -1;
	}

	rbfm->closeFile(mFileHandler1);
	rbfm->closeFile(mFileHandler2);
	rbfm->closeFile(mFileHandler3);

	if(rbfm->destroyFile("Columns") != (RC) 0)
		return (RC) -1;

	if(rbfm->destroyFile("Tables") != (RC) 0)
		return (RC) -1;

	if(rbfm->destroyFile("Indexes") != (RC) 0)
		return (RC) -1;

	return (RC) 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	if((tableName == "Tables") || (tableName == "Columns") || (tableName == "Indexes"))
		return (RC) -1;

	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();

	FileHandle mFileHandler;
	if(rbfm->createFile(tableName) != (RC) 0)
		return (RC) -1;

	if(rbfm->openFile(tableName, mFileHandler) != (RC) 0)
		return (RC) -1;

	if(rbfm->closeFile(mFileHandler) != (RC) 0)
		return (RC) -1;

	// GET new table-id
	int tableid = 0;
	RM_ScanIterator mIterator1;
	vector<string> mAttributes;
	mAttributes.push_back("table-id");	// fetch table-id's

	// SELECT * FROM TABLES
	if(scan("Tables", "", NO_OP, NULL, mAttributes, mIterator1) != (RC) 0){
		//cout << "In create table. Error in SELECT * FROM Tables" << endl;
		return (RC) -1;
	}

	// Generate the table-id

	// fetch, get maximum, increment by 1
	RID xrid;
	char* tempTableIds = new char[1+4];
	while(mIterator1.getNextTuple(xrid, tempTableIds) == (RC) 0){

		char n;
		std::memcpy(&n, tempTableIds, 1);
		if(n != (char)0){
			delete[] tempTableIds;
			mIterator1.close();
			return (RC) -1;
		}

		int f;
		std::memcpy((char*) &f, tempTableIds + 1, 4);
		if(f > tableid){
			tableid = f;
		}
	}
	delete[] tempTableIds;
	tableid++;
	mIterator1.close();


	/*
	 *
	 * Now, we have the table-id, as:    int tableid;
	 *
	 */



	// insert into Tables table
	char* mRecord = new char[17 + tableName.length() + tableName.length()];	// 2 times coz filename = tablename
	prepareRecordTables(mRecord, tableid, tableName, tableName);	// fileName = tableName

	FileHandle fhTables;
	if(rbfm->openFile("Tables", fhTables) != (RC) 0){
		delete[] mRecord;
		return (RC) -1;
	}

	RID mrid;

	vector<Attribute> recDesTables;
	if(getAttributes("Tables", recDesTables) != (RC) 0){
		//cout << "Error in fetching recdes for Tables" << endl;
		delete[] mRecord;
		rbfm->closeFile(fhTables);
		return (RC) -1;
	}
	if(rbfm->insertRecord(fhTables, recDesTables, mRecord, mrid) != (RC) 0){
		delete[] mRecord;
		rbfm->closeFile(fhTables);
		return (RC) -1;
	}

	if(rbfm->closeFile(fhTables) != (RC) 0){
		delete[] mRecord;
		return (RC) -1;
	}

	delete[] mRecord;

	// insert into Columns table
	FileHandle fhColumns;
	if(rbfm->openFile("Columns", fhColumns) != (RC) 0){
		return (RC) -1;
	}

	vector<Attribute> recDesCols;
	if(getAttributes("Columns", recDesCols) != (RC) 0){
		rbfm->closeFile(fhColumns);
		return (RC) -1;
	}

	char* mNewRecord = new char[21 + 50];
	for (int i = 0; i < attrs.size(); i++){
		prepareRecordColumns(mNewRecord, tableid, attrs[i].name, (int) attrs[i].type, (int) attrs[i].length, i+1);
		if(rbfm->insertRecord(fhColumns, recDesCols, mNewRecord, mrid) != (RC) 0){
			delete[] mNewRecord;
			return (RC) -1;
		}
	}

	delete[] mNewRecord;
    return (RC) rbfm->closeFile(fhColumns);

}

RC RelationManager::deleteTable(const string &tableName)
{
	if((tableName == "Tables") || (tableName == "Columns") || (tableName == "Indexes"))
		return (RC) -1;

	// ******************************************** DELETION FROM system tables starts now

	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();

	RM_ScanIterator mIterator;

	const int tblnmlength = tableName.length();
	char* value = new char[tblnmlength + 4];
	std::memcpy(value, (char*) &tblnmlength, 4);
	std::memcpy(value + 4, tableName.c_str(), tblnmlength);

	vector<string> mAttributes;
	mAttributes.push_back("table-id");

	if (scan("Tables", "table-name", EQ_OP, value, mAttributes, mIterator) != (RC) 0){
		delete[] value;
		return (RC) -1;
	}

	// there will only be 1 record
	char* mRecord = new char[1 + 4];	// null byte and integer

	// now, mrid will be the RID of record with table-name = tableName, in 'Tables' table.
	RID mrid;
	if(mIterator.getNextTuple(mrid, mRecord) != (RC) 0){
		// such a table doesn't exist
		mIterator.close();
		delete[] value;
		delete[] mRecord;
		return (RC) -1;
	}

	mIterator.close();

	char nb;
	std::memcpy(&nb, mRecord, 1);
	if(nb != 0){
		delete[] value;
		delete[] mRecord;
		return (RC) -1;
	}

	delete[] value;

	// table-id of the table to be deleted
	int tableid;
	std:memcpy((char*) &tableid, mRecord + 1, 4);

	delete[] mRecord;

	// delete records from tables-table

	FileHandle mFileHandler;
	if(rbfm->openFile("Tables", mFileHandler) != (RC) 0)
		return (RC) -1;
	vector<Attribute> recDesTables;
	if(getAttributes("Tables", recDesTables) != (RC) 0)
		return (RC) -1;
	if(rbfm->deleteRecord(mFileHandler, recDesTables, mrid) != (RC) 0)
		return (RC) -1;
	if(rbfm->closeFile(mFileHandler) != (RC) 0)
		return (RC) -1;


	// delete records from columns-table

	FileHandle mNewFileHandler;
	RM_ScanIterator mNewIterator;
	mAttributes.clear();
	mAttributes.push_back("table-id");	// actually doesn't matter. We only need the RIDs

	if(rbfm->openFile("Columns", mNewFileHandler) != (RC) 0)
		return (RC) -1;

	if (scan("Columns", "table-id", EQ_OP, (char*) &tableid, mAttributes, mNewIterator) != (RC) 0)
		return (RC) -1;


	vector<Attribute> recDesCols;
	if(getAttributes("Columns", recDesCols) != (RC) 0)
		return (RC) -1;


	vector<RID> toDelete;
	char* useless = new char[1+4];
	RID drid;
	while(mNewIterator.getNextTuple(drid, useless) == (RC) 0){
		toDelete.push_back(drid);
	}
	delete[] useless;
	mNewIterator.close();

	for(int i = 0; i < toDelete.size(); i++){
		rbfm->deleteRecord(mNewFileHandler, recDesCols, toDelete[i]);
	}

	if(rbfm->closeFile(mNewFileHandler) == -1)
		return (RC) -1;

	// ******************************************** DELETION FROM system tables done


	// delete the actual table
	return (RC) rbfm->destroyFile(tableName); // filename and table-name are the same
}

// returns 'keys' on which indexes have been made on this table
RC RelationManager::getAllIndexFiles(const string &tableName, vector<string> &fileAttributes){

	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
	FileHandle mFileHandler;

	vector<string> results;

	if(rbfm->openFile("Indexes", mFileHandler) != (RC) 0)
		return (RC) -1;


	// scan to get all the records, match tableName
	RBFM_ScanIterator mIterator;
	vector<Attribute> indexRecDes;
	if(getAttributes("Indexes", indexRecDes) != (RC) 0){
		return (RC) -1;
	}

	vector<string> fields;
	fields.push_back("key-name");

	char value[4 + tableName.length()];
	int lov = tableName.length();
	std::memcpy(value, (char*) &lov, 4);
	std::memcpy(value + 4, tableName.c_str(), lov);

	if(rbfm->scan(mFileHandler, indexRecDes, "table-name", EQ_OP, value, fields, mIterator) != (RC) 0){
		return (RC) -1;
	}

	while(true){
		RID mrid;
		char input_data[4 + 50];

		if(mIterator.getNextRecord(mrid, input_data) != -1){
			// create an index file handler using this
			char nb;
			// skip the null byte
			int lov;
			std::memcpy((char*) &lov, input_data + 1, 4);
			char tk[lov + 1];
			std::memcpy(tk, input_data + 1 + 4, lov);
			tk[lov] = '\0';
			string keyName(tk);

			results.push_back(keyName);
		}else{
			break;
		}

	}

	mIterator.close();
	if(rbfm->closeFile(mFileHandler) != (RC) 0)
		return (RC) -1;

	fileAttributes = results;
	return (RC) 0;
}

// projects only one key (without the null bytes)
RC RelationManager::projectIndexKeyData(const void* allData, const string keyName, const vector<Attribute> recDes, void* projection){

	const int nullBytes = (int) ceil( (float) recDes.size() / 8.0);
	unsigned char nb[nullBytes];
	std::memcpy(nb, allData, nullBytes);

	int scanOffset = nullBytes;

	for (int i = 0; i < recDes.size(); i++){

		// determine if null or not
		unsigned char toConsider = nb[(int) floor((float) i / 8.0)];
		toConsider = toConsider << i;
		toConsider &= (unsigned char) 128;

		if (recDes[i].name != keyName){
			// skip
			if(toConsider == (unsigned char) 0){
				// not null
				if (recDes[i].type != TypeVarChar){
					scanOffset += 4;
				}else{
					int lov;
					std::memcpy((char*) &lov, allData + scanOffset, 4);
					scanOffset += (4 + lov);
				}
			}else{
				// null
				continue;
			}
		}else{
			// this is the data
			if(toConsider == 0){
				// not null

				if (recDes[i].type != TypeVarChar){
					std::memcpy(projection, allData + scanOffset, 4);
				}else{
					int lov;
					std::memcpy((char*) &lov, allData + scanOffset, 4);
					scanOffset += 4;

					std::memcpy(projection, (char*) &lov, 4);
					std::memcpy(projection + 4, allData + scanOffset, lov);
				}

			}else{
				// is null

				return (RC) -1;
				// cant insert a null value in the index table
			}
			break;
		}


	}

	return (RC) 0;
}



RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{

	if((tableName == "Tables") || (tableName == "Columns") || (tableName == "Indexes"))
		return (RC) -1;

	vector<Attribute> mRecDes;
	if(getAttributes(tableName, mRecDes) != (RC) 0){
		return (RC) -1;
	}

	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
	FileHandle mFileHandler;
	if(rbfm->openFile(tableName, mFileHandler) != (RC) 0)
		return (RC) -1;

	if(rbfm->insertRecord(mFileHandler, mRecDes, data, rid) != (RC) 0)
		return (RC) -1;

	//cout << "INserted record: " << endl;


	if(rbfm->closeFile(mFileHandler) != (RC) 0)
		return (RC) -1;


	// insert into index files
	vector<string> indexKeys;
	if(getAllIndexFiles(tableName, indexKeys) != (RC) 0)
		return (RC) -1;


	IndexManager* im = IndexManager::instance();
	for(int i = 0; i < indexKeys.size(); i++){

		IXFileHandle ixHandler;
		if(im->openFile("ix_" + tableName + "_" + indexKeys[i], ixHandler) != (RC) 0)
			return (RC) -1;

		Attribute matr;
		for(int j = 0; j < mRecDes.size(); j++){
			if(mRecDes[j].name == indexKeys[i]){
				matr = mRecDes[j];
				break;
			}
		}


		/*
		 *
		 * PROJECTION
		 *
		 *
		 */

		char indexingData[2000];
		if(projectIndexKeyData(data, matr.name, mRecDes, indexingData) != (RC) 0){
			return (RC) -1;
		}

		if(im->insertEntry(ixHandler, matr, indexingData, rid) != (RC) 0)
			return (RC) -1;


		if(im->closeFile(ixHandler) != (RC) 0)
			return (RC) -1;
	}

	return (RC) 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{

	if((tableName == "Tables") || (tableName == "Columns") || (tableName == "Indexes"))
		return (RC) -1;


	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
	FileHandle mFileHandler;
	if(rbfm->openFile(tableName, mFileHandler) != (RC) 0)
		return (RC) -1;


	vector<Attribute> mRecDes;
	if(getAttributes(tableName, mRecDes) != (RC) 0)
		return (RC) -1;


	// DELETE FROM index files FIRST
	vector<string> indexKeys;
	if(getAllIndexFiles(tableName, indexKeys) != (RC) 0)
		return (RC) -1;


	IndexManager* im = IndexManager::instance();
	for(int i = 0; i < indexKeys.size(); i++){

		IXFileHandle ixHandler;
		if(im->openFile("ix_" + tableName + "_" + indexKeys[i], ixHandler) != (RC) 0)
			return (RC) -1;

		Attribute matr;
		for(int j = 0; j < mRecDes.size(); j++){
			if(mRecDes[j].name == indexKeys[i]){
				matr = mRecDes[j];
				break;
			}
		}

		// read this particular attribute;
		char* data;
		if(matr.type != TypeVarChar){
			data = new char[1 + 4];
		}else{
			data = new char[1 + 4 + matr.length];
		}

		if(rbfm->readAttribute(mFileHandler, mRecDes, rid, matr.name, data) != (RC) 0){
			delete[] data;
			return (RC) -1;
		}

		if(im->deleteEntry(ixHandler, matr, data + 1, rid) != (RC) 0){
			delete[] data;
			return (RC) -1;
		}


		if(im->closeFile(ixHandler) != (RC) 0){
			delete[] data;
			return (RC) -1;
		}

		delete[] data;
	}

	// delete from the heap file..

	if(rbfm->deleteRecord(mFileHandler, mRecDes, rid) != (RC) 0)
		return (RC) -1;

	if(rbfm->closeFile(mFileHandler) != (RC) 0)
		return (RC) -1;


	return (RC) 0;
}



RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	if((tableName == "Tables") || (tableName == "Columns"))
		return (RC) -1;

	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
	FileHandle mFileHandler;
	if(rbfm->openFile(tableName, mFileHandler) == -1)
		return (RC) -1;

	vector<Attribute> mRecDes;
	if(getAttributes(tableName, mRecDes) == -1)
		return (RC) -1;


	// DELETE FROM index files FIRST
	vector<string> indexKeys;
	if(getAllIndexFiles(tableName, indexKeys) != (RC) 0)
		return (RC) -1;


	IndexManager* im = IndexManager::instance();
	for(int i = 0; i < indexKeys.size(); i++){

		IXFileHandle ixHandler;
		if(im->openFile("ix_" + tableName + "_" + indexKeys[i], ixHandler) != (RC) 0)
			return (RC) -1;

		Attribute matr;
		for(int j = 0; j < mRecDes.size(); j++){
			if(mRecDes[j].name == indexKeys[i]){
				matr = mRecDes[j];
				break;
			}
		}

		// read this particular attribute;
		char* todeletedata;		// THIS should not consist of any null bytes. BUt i add this case in deleteEntry (+1)
		if(matr.type != TypeVarChar){
			todeletedata = new char[1 + 4];
		}else{
			todeletedata = new char[1 + 4 + matr.length];
		}

		if(rbfm->readAttribute(mFileHandler, mRecDes, rid, matr.name, todeletedata) != (RC) 0){
			delete[] todeletedata;
			return (RC) -1;
		}

		// remove null bytes from todeletedata

		// delete and insert
		if(im->deleteEntry(ixHandler, matr, todeletedata + 1, rid) != (RC) 0){
			delete[] todeletedata;
			return (RC) -1;
		}

		char indexingData[2000];
		if(projectIndexKeyData(data, matr.name, mRecDes, indexingData) != (RC) 0){
			delete[] todeletedata;
			return (RC) -1;
		}

		if(im->insertEntry(ixHandler, matr, indexingData, rid) != (RC) 0){
			delete[] todeletedata;
			return (RC) -1;
		}


		if(im->closeFile(ixHandler) != (RC) 0){
			delete[] todeletedata;
			return (RC) -1;
		}


		delete[] todeletedata;
	}


	// now update
	if(rbfm->updateRecord(mFileHandler, mRecDes, data, rid) != (RC) 0)
		return (RC) -1;

	if(rbfm->closeFile(mFileHandler) == -1)
		return (RC) -1;

	return (RC) 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	vector<Attribute> mRecDes;
	if(getAttributes(tableName, mRecDes) != (RC) 0){
		return (RC) -1;
	}

	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
	FileHandle mFileHandler;
	if(rbfm->openFile(tableName, mFileHandler) != (RC) 0)
		return (RC) -1;
	RC result = rbfm->readRecord(mFileHandler, mRecDes, rid, data);

	if(rbfm->closeFile(mFileHandler) != (RC) 0)
		return (RC) -1;

	return result;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
	return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	vector<Attribute> mRecDes;
	if(getAttributes(tableName, mRecDes) != (RC) 0)
		return (RC) -1;

	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
	FileHandle mFileHandler;
	if(rbfm->openFile(tableName, mFileHandler) != (RC) 0)
		return (RC) -1;

	RC result = rbfm->readAttribute(mFileHandler, mRecDes, rid, attributeName, data);

	if(rbfm->closeFile(mFileHandler) != (RC) 0)
		return (RC) -1;

	return result;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{

	vector<Attribute> recDes;
	if (getAttributes(tableName, recDes) != (RC) 0){
		return (RC) -1;
	}

	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
	FileHandle mFileHandler;
	if(rbfm->openFile(tableName, mFileHandler) != (RC) 0)
		return (RC) -1;

	//rm_ScanIterator.close();

	RC result = rbfm->scan(mFileHandler, recDes, conditionAttribute, compOp, value,
			attributeNames, rm_ScanIterator.rbfm_iterator);

	if(rbfm->closeFile(mFileHandler) != (RC) 0)
		return (RC) -1;

	return result;
}

RC RelationManager::prepareRecordTables(void* data, int tableId, string tableName, string fileName){
	// pass in *data of correct length
	char* output = new char[13 + tableName.length() + fileName.length()];
	char nb = 0;
	std::memcpy(output, &nb, 1);

	std::memcpy(output + 1, (char*) &tableId, 4);

	int lov1 = tableName.length();
	std::memcpy(output + 5, (char*) &lov1, 4);
	std::memcpy(output + 9, tableName.c_str(), lov1);


	int lov2 = fileName.length();
	std::memcpy(output + 9 + lov1, (char*) &lov2, 4);
	std::memcpy(output + 9 + lov1 + 4, fileName.c_str(), lov2);

	// output data is ready
	std::memcpy(data, output, 13 + tableName.length() + fileName.length());
	delete[] output;
	return (RC) 0;
}

RC RelationManager::prepareRecordColumns(void* data, int tableId, string columnName, int columnType,
		  int columnLength, int columnPosition){
	// pass in *data of correct length
	char* output = new char[21 + columnName.length()];
	char nb = 0;
	std::memcpy(output, &nb, 1);
	std::memcpy(output + 1, (char*) &tableId, 4);
	int lov1 = columnName.length();
	std::memcpy(output + 5, (char*) &lov1, 4);
	std::memcpy(output + 9, columnName.c_str(), lov1);
	std::memcpy(output + 9 + lov1, (char*) &columnType, 4);
	std::memcpy(output + 9 + lov1 + 4, (char*) &columnLength, 4);
	std::memcpy(output + 9 + lov1 + 8, (char*) &columnPosition, 4);

	// output data is ready
	std::memcpy(data, output, 21 + columnName.length());
	delete[] output;
	return (RC) 0;
}


RC RelationManager::prepareRecordIndexes(void* data, int tableId, string tableName, string keyName){
	// pass in *data of correct length
	char* output = new char[13 + tableName.length() + keyName.length()];

	char nb = 0;
	std::memcpy(output, &nb, 1);

	std::memcpy(output + 1, (char*) &tableId, 4);

	int lov1 = tableName.length();
	std::memcpy(output + 5, (char*) &lov1, 4);
	std::memcpy(output + 9, tableName.c_str(), lov1);

	int lov2 = keyName.length();
	std::memcpy(output + 9 + lov1, (char*) &lov2, 4);
	std::memcpy(output + 9 + lov1 + 4, keyName.c_str(), lov2);

	// output data is ready
	std::memcpy(data, output, 13 + tableName.length() + keyName.length());
	delete[] output;
	return (RC) 0;
}

// only opens 'Tables' and 'Columns' file (for i/o. That is first scan(), then getNextRecord() //
RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs){


	RC result = (RC) -1;
	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();

	vector<Attribute> recDescriptorTables;
	Attribute matr;
	matr.name = "table-id"; matr.type = TypeInt; matr.length = (AttrLength) 4;
	recDescriptorTables.push_back(matr);
	matr.name = "table-name"; matr.type = TypeVarChar; matr.length = (AttrLength) 50;
	recDescriptorTables.push_back(matr);
	matr.name = "file-name"; matr.type = TypeVarChar; matr.length = (AttrLength) 50;
	recDescriptorTables.push_back(matr);

	vector<Attribute> recDescriptorColumns;
	matr.name = "table-id"; matr.type = TypeInt; matr.length = (AttrLength) 4;
	recDescriptorColumns.push_back(matr);
	matr.name = "column-name"; matr.type = TypeVarChar; matr.length = (AttrLength) 50;
	recDescriptorColumns.push_back(matr);
	matr.name = "column-type"; matr.type = TypeInt; matr.length = (AttrLength) 4;
	recDescriptorColumns.push_back(matr);
	matr.name = "column-length"; matr.type = TypeInt; matr.length = (AttrLength) 4;
	recDescriptorColumns.push_back(matr);
	matr.name = "column-position"; matr.type = TypeInt; matr.length = (AttrLength) 4;
	recDescriptorColumns.push_back(matr);

	vector<Attribute> indexesRecDes;
	matr.name = "table-id"; matr.type = TypeInt; matr.length = 4;
	indexesRecDes.push_back(matr);
	matr.name = "table-name"; matr.type = TypeVarChar; matr.length = 50;
	indexesRecDes.push_back(matr);
	matr.name = "key-name"; matr.type = TypeVarChar; matr.length = 50;
	indexesRecDes.push_back(matr);


	if(tableName == "Tables"){
		attrs = recDescriptorTables;
		result = (RC) 0;
		return result;
	}

	if(tableName == "Columns"){
		attrs = recDescriptorColumns;
		result = (RC) 0;
		return result;
	}

	if(tableName == "Indexes"){
		attrs = indexesRecDes;
		result = (RC) 0;
		return result;
	}


	/*
	 *
	 * Check from cache
	 *
	 *
	 *   bool cachedRDset;
  string cachedRDtable;
  vector<Attribute> cachedRD;
	 *
	 *
	 */

	if (cachedRDset && (cachedRDtable == tableName)){
		attrs = cachedRD;
		result = (RC) 0;
		return result;
	}

	cachedRDset = false;



	const int tblnmlength = tableName.length();

	/*
	 *
	 * QUERYING THE 'Tables' FILE. To get table-id
	 *
	 *
	 */


	// Query 'Tables', to get the table-id of the requested table
	char* value1 = new char[tblnmlength + 4];
	std::memcpy(value1, (char*) &tblnmlength, 4);
	std::memcpy(value1 + 4, tableName.c_str(), tblnmlength);

	vector<string> mAttributes1;
	mAttributes1.push_back("table-id");


	FileHandle mHandler;
	if(rbfm->openFile("Tables", mHandler) != 0){
		//cout << "Premtive return - 9";
		delete[] value1;
		//cout << "Couldn't open Tables file." << endl;
		result = (RC) -1;
		return result;
	}

	RBFM_ScanIterator mIterator1;
	if(rbfm->scan(mHandler, recDescriptorTables, "table-name", EQ_OP, value1, mAttributes1, mIterator1) != (RC) 0){
		//cout << "Premptive return! from getRecDes 8";
		delete[] value1;
		rbfm->closeFile(mHandler);
		//cout << "Couldn't scan Tables (matching table-name)" << endl;
		result = (RC) -1;
		return result;
	}
	delete[] value1;


	char* mRecord1 = new char[1 + 4];	// table-id
	RID mrid;
	if (mIterator1.getNextRecord(mrid, mRecord1) != (RC) 0){
		//cout << "Premptive return! from getRecDes 7";
		delete[] mRecord1;
		//cout << "NO matching records in Tables. getNextRecord has 0 set of RIDs";
		//cout << "Number of records in Tables scan() = " << mIterator1.getNumberOfRecordsInIterator() << endl;
		mIterator1.close();
		rbfm->closeFile(mHandler);
		result = (RC) -1;
		return result;
	}
	char nullByte = 0;
	std::memcpy(&nullByte, mRecord1, 1);
	if (nullByte != ((char) 0)){
		//cout << "Premptive return! from getRecDes 6.";
		rbfm->closeFile(mHandler);
		mIterator1.close();
		delete[] mRecord1;
		//cout << "Table-id is NULL (of supplied tableName)." << endl;
		result = (RC) -1;
		return result;
	}
	int tableid = 0;
	std::memcpy((char*) &tableid, mRecord1 + 1, 4);
	delete[] mRecord1;
	mIterator1.close();
	if(rbfm->closeFile(mHandler) != (RC) 0){
		//cout << "Premptive return! from getRecDes 5. Couldn't close Tables file" << endl;
		result = (RC) -1;
		return result;
	}




	/*
	 *
	 *
	 * Fetching from 'Columns', where, table-id equals tableid
	 *
	 *
	 *
	 */


	FileHandle mNewHandler;
	RBFM_ScanIterator mIterator2;
	if(rbfm->openFile("Columns", mNewHandler) != (RC) 0){
		//cout << "Premptive return! from getRecDes 4. Couldnt open Columns file." << endl;
		result = (RC) -1;
		return result;
	}

	char* value2 = new char[4];
	std::memcpy(value2, (char*) &tableid, 4);

	vector<string> mAttributes2;
	mAttributes2.push_back("column-name");	// max 50
	mAttributes2.push_back("column-type");	// integer
	mAttributes2.push_back("column-length"); // integer

	// use the new iterator
	if(rbfm->scan(mNewHandler, recDescriptorColumns, "table-id", EQ_OP, value2, mAttributes2, mIterator2) != (RC) 0){
		//cout << "Premptive return! from getRecDes 3";
		rbfm->closeFile(mNewHandler);
		delete[] value2;
		mIterator2.close();
		result = (RC) -1;
		//cout << "No matching rows in columns table" << endl;
		return result;
	}

	delete[] value2;


	char* mRecord2 = new char[1 + 4 + 50 + 4 + 4];
	RID orid;
	while(mIterator2.getNextRecord(orid, mRecord2) == (RC) 0){
		// mRecord2 is a valid record
		char nb;
		std::memcpy(&nb, mRecord2, 1);
		if(nb != (char)0){
			// some field is null
			//cout << "Premptive return! from getRecDes 2";
			mIterator2.close();
			rbfm->closeFile(mNewHandler);
			delete[] mRecord2;
			//cout << "SOme field is null while fetching from Columns table" << endl;
			result = (RC) -1;
			return result;
		}

		int lov;
		std::memcpy((char*) &lov, mRecord2 + 1, 4);

		char* cn = new char[lov + 1];	// need to add null character at the end
		std::memcpy(cn, mRecord2 + 1 + 4, lov);
		cn[lov] = '\0';
		string columnName(cn);
		delete[] cn;

		int columnType;
		int columnLength;
		std::memcpy((char*) &columnType, mRecord2 + 1 + 4 + lov, 4);
		std::memcpy((char*) &columnLength, mRecord2 + 1 + 4 + lov + 4, 4);

		Attribute toAdd;
		toAdd.name = columnName;
		toAdd.type = (AttrType) columnType;
		toAdd.length = (AttrLength) columnLength;
		attrs.push_back(toAdd);
	}

	delete[] mRecord2;
	mIterator2.close();
	if(rbfm->closeFile(mNewHandler) == -1){
		//cout << "Premptive return! from getRecDes 1. Couldnt close Columns file." << endl;
		result = (RC) -1;
		return result;
	}

	result = (RC) 0;


	/*
	 *
	 * Set the cache
	 *
	 */

	cachedRDset = true;
	cachedRDtable = tableName;
	cachedRD = attrs;

	return result;
}


RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{

	//cout << "CREATING INDEX FOR TABLE: " << tableName << "   " << attributeName << endl;

	IndexManager* im_instance = IndexManager::instance();
	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();

	// insert into the Indexes catalog table
	FileHandle catalogHandler;
	if(rbfm->openFile("Indexes", catalogHandler) != (RC) 0)
		return (RC) -1;

	vector<Attribute> catalogRecDes;
	if(getAttributes("Indexes", catalogRecDes) != (RC) 0){
		return (RC) -1;
	}

	RID mrid;

	char data[13 + tableName.length() + attributeName.length()];
	prepareRecordIndexes(data, 3, tableName, attributeName);
	if(rbfm->insertRecord(catalogHandler, catalogRecDes, data, mrid) != (RC) 0){
		return (RC) -1;
	}

	if(rbfm->closeFile(catalogHandler) != (RC) 0)
		return (RC) -1;


	// creating the B-TREE file
	string fileName = "ix_";
	fileName += (tableName + "_");
	fileName += attributeName;
	if(im_instance->createFile(fileName) != (RC) 0)
		return (RC) -1;


	//**************************************************************** populating entries to b-tree
	// Populate these entries
	// scan and insertEntry
	FileHandle heapFile;
	if(rbfm->openFile(tableName, heapFile) != (RC) 0)
		return (RC) -1;

	RBFM_ScanIterator mIterator;
	vector<Attribute> heapRecDes;
	if(getAttributes(tableName, heapRecDes) != (RC) 0)
		return (RC) -1;

	vector<string> attrReq;
	attrReq.push_back(attributeName);

	if(rbfm->scan(heapFile, heapRecDes, "", NO_OP, NULL, attrReq, mIterator) != (RC) 0)
		return (RC) -1;

	Attribute indexingAttr;
	for(int i = 0; i < heapRecDes.size(); i++){
		if (attributeName == heapRecDes[i].name){
			indexingAttr = heapRecDes[i];
			break;
		}
	}

	IXFileHandle ixfh;
	if(im_instance->openFile(fileName, ixfh) != (RC) 0)
		return (RC) -1;

	char alldata[PAGE_SIZE];
	while(mIterator.getNextRecord(mrid, alldata) != -1){
		/*
		 * NO NEED TO DO PROJECTION... AS WE ONLY FETCH REQUIRED DATA, only remove the null byte
		 *
		 */

		unsigned char nb;
		std::memcpy(&nb, alldata, 1);

		if (nb != (unsigned char) 0)
			return (RC) -1;			// a field to be indexed is NULL

		char* keytoinsert;
		if(indexingAttr.type != TypeVarChar){
			keytoinsert = new char[4];
			std::memcpy(keytoinsert, alldata + 1, 4);
		}else{
			int lov;
			std::memcpy((char*) &lov, alldata + 1, 4);
			keytoinsert = new char[4 + lov];
			std::memcpy(keytoinsert, alldata + 1, 4 + lov);
		}


		if(im_instance->insertEntry(ixfh, indexingAttr, keytoinsert, mrid) != (RC) 0){
			delete[] keytoinsert;
			return (RC) -1;
		}

		delete[] keytoinsert;
	}

	mIterator.close();
	if(im_instance->closeFile(ixfh) != (RC) 0)
		return (RC) -1;

	if(rbfm->closeFile(heapFile) != (RC) 0)
		return (RC) -1;


	return (RC) 0;
}


RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
	IndexManager* im_instance = IndexManager::instance();
	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();

	string fileName = "ix_";
	fileName += (tableName + "_");
	fileName += attributeName;

	if(im_instance->destroyFile(fileName) != (RC) 0)
		return (RC) -1;


	// delete entry from the Indexes table. do it manually.
	RM_ScanIterator mIterator;

	vector<string> fields;
	fields.push_back("key-name");

	char data[4 + tableName.length()];
	int lov = tableName.length();
	std::memcpy(data, (char*) &lov, 4);
	std::memcpy(data + 4, tableName.c_str(), lov);

	if(scan("Indexes", "table-name", EQ_OP, data, fields, mIterator) != (RC) 0){
		return (RC) -1;
	}


	bool deleted = false;
	while(!deleted){

		char data[4 + 50];	// max length possible
		RID mrid;

		if(mIterator.getNextTuple(mrid, data) != -1){
			// compare if this is same as attributeName
			int lov;
			std::memcpy((char*) &lov, data, 4);
			char tk[lov + 1];
			std::memcpy(tk, data + 4, lov);
			tk[lov] = '\0';

			string temp(tk);
			if (temp == attributeName){
				FileHandle inFileHandler;
				if(rbfm->openFile("Indexes", inFileHandler) != (RC) 0)
					return (RC) -1;

				vector<Attribute> indexesRecDes;
				if(getAttributes("Indexes", indexesRecDes) != (RC) 0)
					return (RC) -1;

				if(rbfm->deleteRecord(inFileHandler, indexesRecDes, mrid) != (RC) 0)
					return (RC) -1;

				if(rbfm->closeFile(inFileHandler) != (RC) 0)
					return (RC) -1;

				deleted = true;
				break;
			}else{
				continue;
			}


		}else{
			break;
		}

	}	// end of while loop

	mIterator.close();
	if(!deleted){
		return (RC) -1;
	}else{
		return (RC) 0;
	}
}


// indexScan returns an iterator to allow the caller to go through qualified entries in index
RC RelationManager::indexScan(const string &tableName,
                      const string &attributeName,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      RM_IndexScanIterator &rm_IndexScanIterator)
{

	IndexManager* im_instance = IndexManager::instance();
	IXFileHandle ix_fh;

	string fileName = "ix_";
	fileName += (tableName + "_");
	fileName += attributeName;

	if(im_instance->openFile(fileName, ix_fh) != (RC) 0){
		return (RC) -1;
	}

	// build the attribute to pass
	vector<Attribute> recDes;
	if(getAttributes(tableName, recDes) != (RC) 0){
		return (RC) -1;
	}

	Attribute matr;
	bool found = false;
	for (int i = 0; i< recDes.size(); i++){
		if(recDes[i].name == attributeName){
			matr = recDes[i];	// bit wise copy
			found = true;
			break;
		}
	}

	if(!found){
		return (RC) -1;
	}

	RC result = im_instance->scan(ix_fh, matr, lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator.ix_iterator);
	return result;
}

















// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}


