#include "rbfm.h"
#include <iostream>
#include <cstdint>
#include <cstring>
#include <vector>
#include <cmath>
using namespace std;

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance(){
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager(){
}

RecordBasedFileManager::~RecordBasedFileManager(){
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    PagedFileManager* pfm = PagedFileManager::instance();
    return (RC) (pfm->createFile(fileName));
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    PagedFileManager* pfm = PagedFileManager::instance();
    return (RC) (pfm->destroyFile(fileName));
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    // *** meta-data will be null for this
    PagedFileManager* pfm = PagedFileManager::instance();
    return (RC) (pfm->openFile(fileName, fileHandle));
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    PagedFileManager* pfm = PagedFileManager::instance();
    return (RC) (pfm->closeFile(fileHandle));
}


RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {

    char* inputData = (char*) data;

    //******************************** changing representation of data ********************//
    // calculate length of record //
    const int fields = recordDescriptor.size();
    const int nullBytes = (int)ceil((double)fields/8);
    char* actualData = inputData + nullBytes;

    // destination for pointerData;
    int* myPointerData = new int[fields];

    // destination for actualData;
    char* myActualData = new char[PAGE_SIZE];

    char* mSource = actualData;
    char* mDestination = myActualData;

    char* mad;

    for(int j = 0; j < fields; j++){
        mad = inputData +  (int)(floor((double)j/8));
        char consider = *mad;
        int l = j - (8 * floor((double)j/8));
        consider <<= l;
        char ote = 128;
        consider = consider & ote;
        if(consider != ote){
            // that field is not null
            // get data type from recordDescriptor.
            Attribute mAtr = recordDescriptor[j];
            if(mAtr.type == 0){
                // integer type
                myPointerData[j] = 4 + nullBytes + (4*fields) + (int)(mDestination - myActualData);
                std::memcpy(mDestination, mSource, 4);
                mSource += 4;
                mDestination += 4;
            }else{
                if(mAtr.type == 1){
                    // real type (floating)
                    myPointerData[j] = 4 + nullBytes + (4*fields) + (int)(mDestination - myActualData);
                    std::memcpy(mDestination, mSource, 4);
                    mSource += 4;
                    mDestination += 4;
                }else{
                    // VARCHAR type
                    myPointerData[j] = 4 + nullBytes + (4*fields) + (int)(mDestination - myActualData);
                    int lov = 0;
                    std::memcpy(&lov, mSource, 4);
                    std::memcpy(mDestination, mSource, 4);
                    mSource += 4;
                    mDestination += 4;

                    std::memcpy(mDestination, mSource, lov);
                    mSource += lov;
                    mDestination += lov;
                }
            }
        }else{
            // field is null.
            myPointerData[j] = -1;
        }
    }

    const int theMeat = (int)(mDestination - myActualData);
    const int newRecordLength = 4 + nullBytes + (4*fields) + theMeat;

    // combine the result -> version + nullbytes + pointerdata + actualdata
    char* mRecord = new char[newRecordLength];
    char* mr = mRecord;


    // insert the version number. Starts from 1, -1 for tombstone
    int ver = (fileHandle.getMetaData()).getVersion();
    std::memcpy(mr, (char*) &ver, 4);
    mr = mr + 4;

    std::memcpy(mr, inputData, nullBytes);
    mr = mr + nullBytes;

    std::memcpy(mr, (char*) myPointerData, 4*fields);
    mr = mr + (4 * fields);

    std::memcpy(mr, myActualData, theMeat);


    delete[] myPointerData;
    delete[] myActualData;

    //******************************** changing representation of data done ********************//

    // get freespace vector
    vector<FreeSpaceNode> fsVector = fileHandle.getFSpace();

    // free space node for that page.
    FreeSpaceNode fsNode;

    int pgNo = -1; // indicates there is no page, and an append page needs to be done
    // starting from last-page, start decrementing
    for (int i = fileHandle.getNumberOfPages(); i >= 1; i--){
        if( (fsVector[i]).getSpace() > (newRecordLength + sizeof(SlotTableNode) + 0) ){
            pgNo = fsVector[i].getPageNumber();   // this is the actual number. Means 0th page has metadata.
            fsNode = fsVector[i];
            break;
        }
    }

    // read that page into m/y
    char* mPage = new char[PAGE_SIZE];
    vector<SlotTableNode> mSlotTable;   // create new or if old page, set it after read.

    char *temp;

    if (pgNo == -1){
        // no page found

    	// no need to set fsNode. All properties have been set later in the function
        // mSlotTable is an empty vector
    }else{
        // page found, do a read page

    	if(fileHandle.readPage(pgNo - 1, mPage) != (RC) 0){
    		//cout << "Preempt in rbfm->insert.";
    		delete[] mRecord;
    		delete[] mPage;
    		//cout << "Error in read page. " << pgNo-1 << endl;
    		return (RC) -1;
    	}

        // read mSlotTable from the page
        mSlotTable.resize( (PAGE_SIZE- fsNode.getTableStart()) / sizeof(SlotTableNode) );
        std::memcpy((char*) &mSlotTable[0], mPage + fsNode.getTableStart(), PAGE_SIZE- fsNode.getTableStart());
        // mSlotTable is now set.
    }

    SlotTableNode newSlot;
    int newRecordStart;

    int foundDeletedSlot = 0;
    int addInExistingPage = 0;	// 0 for append new page, 1 for yes insert in existing

    if(mSlotTable.size()!= 0){
    	// existing page
    	addInExistingPage = 1;
    	for (int i = 0; i < mSlotTable.size(); i++){
    		if(mSlotTable[i].getRecordLength() == 0){
    			// found a re-usable slot...
    			//cout << "Inserting,.... Found a reusable slot.." << endl;
    			foundDeletedSlot = 1;
    			newSlot = mSlotTable[i];
    			newSlot.setRecordLength(newRecordLength);
    			newRecordStart = mSlotTable[i].getRecordStart();
    			break;
    		}
    	}

    	if(foundDeletedSlot == 0){
    		// no re-usable slot found
			//cout << "Inserting,.... no reusable slot.." << endl;
    		newSlot = SlotTableNode(0, newRecordLength, 0);
            newRecordStart = mSlotTable.back().getRecordStart() + mSlotTable.back().getRecordLength();
            newSlot.setSlotValue(mSlotTable.back().getSlotValue() + 1);
            newSlot.setRecordStart(newRecordStart);
    	}

    }else{
    	// making new page
		//cout << "Inserting,.... Appending page" << endl;
    	addInExistingPage = 0;
        newSlot.setSlotValue(0);
        newRecordStart = 0;
        newSlot.setRecordStart(newRecordStart);
        newSlot.setRecordLength(newRecordLength);
    }

    // add record to page.

    if(foundDeletedSlot == 0){
    	// existing page at end OR append page case
    	std::memcpy(mPage + newRecordStart, mRecord, newRecordLength);
    	mSlotTable.push_back(newSlot);
        const int slottablelength = mSlotTable.size()*sizeof(SlotTableNode);
        const int k = PAGE_SIZE - (mSlotTable.back().getRecordStart() + mSlotTable.back().getRecordLength()) - slottablelength;

        // write slottable to page
        // **************************** possible bug in the line below.
        std::memcpy(mPage + newRecordStart + newRecordLength + k, (char*) &mSlotTable[0], slottablelength);

        fsNode.setSpace(k);
        fsNode.setTableStart(newRecordStart + newRecordLength + k);



        if(addInExistingPage == 1){
            for(int j = 1; j < fsVector.size(); j++){
                if(fsVector[j].getPageNumber() == fsNode.getPageNumber()){
                    fsVector[j] = fsNode;
                    break;
                }
            }

            fileHandle.setFSpace(fsVector);
            if(fileHandle.writePage(pgNo - 1, mPage) != (RC) 0){
            	//cout << "Preempt in rbfm->insert.";
            	delete[] mRecord;
            	delete[] mPage;
            	//cout << "couldn't write page: " << pgNo - 1 << endl;
            	return (RC) -1;
            }

        }else{
        	//append page
        	fsNode.setPageNumber(fsVector.size());
        	fsVector.push_back(fsNode);
        	fileHandle.setFSpace(fsVector);
            if(fileHandle.appendPage(mPage) != (RC) 0){
            	//cout << "Preempt in rbfm->insert.";
            	delete[] mRecord;
            	delete[] mPage;
            	//cout << "couldn't append page: " << endl;
            	return (RC) -1;
            }
        }

        rid.pageNum = fsNode.getPageNumber() - 1;
        rid.slotNum = newSlot.getSlotValue();

        // custom check to detect overflow
        if((fsNode.getSpace() < 0) || (fsNode.getSpace() > PAGE_SIZE)){
        	delete[] mPage; delete[] mRecord;
        	//cout << "*******************" << endl;
        	//cout << "INCONSISTENCY IN insert. Freespace < 0 OR > PAGE_SIZE" << endl;
        	//cout << "Normal insert. RID: " << rid.pageNum << ", " << rid.slotNum << endl;
        	return (RC) -1;
        }

        if(fsNode.getTableStart() > PAGE_SIZE){
        	delete[] mPage; delete[] mRecord;
        	//cout << "*******************" << endl;
        	//cout << "INCONSISTENCY IN insert. tableoffset > PAGE_SIZE" << endl;
        	//cout << "Reusable insert. RID: " << rid.pageNum << ", " << rid.slotNum << endl;
        	return (RC) -1;
        }



    }else{
    	// move existing records on the existing page
    	// CANNOT call update record here
		const int length_rightPortion = (mSlotTable.back().getRecordStart() + mSlotTable.back().getRecordLength()
										- newSlot.getRecordStart() - 0);


		char* rightPortion = new char[length_rightPortion];
		std::memcpy(rightPortion, mPage + newSlot.getRecordStart(), length_rightPortion);

		// writing newrecord to its place
		std::memcpy(mPage + newSlot.getRecordStart(), mRecord, newRecordLength);

		// shift all other slots to the right
		std::memcpy(mPage + newSlot.getRecordStart() + newRecordLength, rightPortion, length_rightPortion);
		for (int i = newSlot.getSlotValue() + 1; i < mSlotTable.size(); i++){
			SlotTableNode temp = mSlotTable[i];
			temp.setRecordStart(temp.getRecordStart() + newRecordLength);
			mSlotTable[i] = temp;
		}

		// write the new slot back to the slotTable
		mSlotTable[newSlot.getSlotValue()] = newSlot;

		const int k = fsNode.getSpace() - newRecordLength;
		const int slottablestart = PAGE_SIZE - (mSlotTable.size() * sizeof(SlotTableNode));

		//const int slottablestart = mSlotTable.back().getRecordStart() +	mSlotTable.back().getRecordLength() + k;

		std::memcpy(mPage + slottablestart, (char*) &mSlotTable[0], mSlotTable.size() * sizeof(SlotTableNode));


		fsNode.setSpace(k);
		fsNode.setTableStart(slottablestart);
        for(int j = 1; j < fsVector.size(); j++){
            if(fsVector[j].getPageNumber() == fsNode.getPageNumber()){
                fsVector[j] = fsNode;
                break;
            }
        }
		fileHandle.setFSpace(fsVector);
        if(fileHandle.writePage(pgNo - 1, mPage) != (RC) 0){
        	//cout << "Preempt in rbfm->insert.";
        	delete[] mRecord;
        	delete[] mPage;
        	delete[] rightPortion;
        	//cout << "(inserting in reusable) couldn't write page: " << pgNo - 1 << endl;
        	return (RC) -1;
        }

        rid.pageNum = fsNode.getPageNumber() - 1;
        rid.slotNum = newSlot.getSlotValue();

        // custom check to detect overflow
        if((fsNode.getSpace() < 0) || (fsNode.getSpace() > PAGE_SIZE)){
        	delete[] mPage; delete[] mRecord; delete[] rightPortion;
        	//cout << "*******************" << endl;
        	//cout << "INCONSISTENCY IN insert. Freespace < 0 OR > PAGE_SIZE" << endl;
        	//cout << "Reusable insert. RID: " << rid.pageNum << ", " << rid.slotNum << endl;
        	return (RC) -1;
        }

        if(fsNode.getTableStart() > PAGE_SIZE){
        	delete[] mPage; delete[] mRecord; delete[] rightPortion;
        	//cout << "*******************" << endl;
        	//cout << "INCONSISTENCY IN insert. tableoffset > PAGE_SIZE" << endl;
        	//cout << "Reusable insert. RID: " << rid.pageNum << ", " << rid.slotNum << endl;
        	return (RC) -1;
        }

        delete[] rightPortion;

    }

    // release m/y for all.
   delete[] mPage;
   delete[] mRecord;


   return (RC) 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {


	int pageNum = rid.pageNum + 1;	// to incorporate the internal page numbering
	int slotNum = rid.slotNum;

	vector<FreeSpaceNode> fsVector = fileHandle.getFSpace();
	FreeSpaceNode fsNode;

    char* mPage = new char[PAGE_SIZE];
    //cout << "Allocated for mPage:" << 4096 << endl;

    vector<SlotTableNode> mSlotTable;
    SlotTableNode mSlot;

    char* mRecord = new char[2000];
    //cout << "Allocated for mRecord:" << 2000 << endl;

   	int status = -1; // -1 for moved to another page

	while(status == -1){

		// setup fsNode.
		if(fsVector.size() <= pageNum){
			// page doesn't exist
			//cout << "Broke in rbfm-> readRecord";
			delete[] mPage;
			delete[] mRecord;
			//cout << "page number out of index. " << pageNum << " in " << fileHandle.fileName << endl;
			return (RC) -1;
		}
		fsNode = fsVector[pageNum];

		// read the page into m/y
		if(fileHandle.readPage(pageNum - 1, mPage) != (RC) 0){
			//cout << "Broke in rbfm-> readRecord";
			delete[] mPage;
			delete[] mRecord;
			//cout << "couldnt read page. " << pageNum - 1 << " in " << fileHandle.fileName << endl;
			return (RC) -1;
		}

		// form the slot table
		mSlotTable.clear();
		mSlotTable.resize( (PAGE_SIZE - fsNode.getTableStart()) / (sizeof(SlotTableNode)) );
		std::memcpy((char*) &mSlotTable[0], mPage + fsNode.getTableStart(), PAGE_SIZE - fsNode.getTableStart());

		// find corresponding slot
		if(mSlotTable.size() <= slotNum){
			// slot doesn't exist
			//cout << "Broke in rbfm-> readRecord";
			delete[] mPage;
			delete[] mRecord;
			//cout << "slot out of index " << slotNum << " in " << fileHandle.fileName << endl;
			return (RC) -1;
		}
		mSlot = mSlotTable[slotNum];

		// check if record has a non-zero length or not
		if(mSlot.getRecordLength() == 0){
			// slot deleted
			//cout << "Broke in rbfm-> readRecord";
			delete[] mPage;
			delete[] mRecord;
			//cout << "deleted record" << endl;
			return (RC) -1;
		}

		// read record into m/y
		std::memcpy(mRecord, mPage + mSlot.getRecordStart(), mSlot.getRecordLength());

		// read version. if version = -1, then loop again
		std::memcpy((char*) &status, mRecord, 4);
		if(status != -1){
			break;
		}else{
			// update pageNum and slotNum
			std::memcpy((char*) &pageNum, mRecord + 4, 4);
			std::memcpy((char*) &slotNum, mRecord + 8, 4);
		}
	}

	// here, everything corresponds to the final page.

    // get record
    const int recordLength = mSlot.getRecordLength();

    const int fields = recordDescriptor.size();
    const int nullBytes = (int) ceil((double)fields/8);

    char* outputData = new char[recordLength]; // a little more, but okay.
    //cout << "Allocated for outputData:" << recordLength << endl;

    // add the null bytes array
    std::memcpy(outputData, mRecord + 4, nullBytes);

    char* mSource = mRecord + 4 + nullBytes + (4*fields);

    // add the actual data
    std::memcpy(outputData + nullBytes, mSource, recordLength - 4 - nullBytes - (fields * 4));

    // WRITE outputData to void* data
    std::memcpy((char*) data, outputData, recordLength - 4 - (fields * 4));

    delete[] mPage;
    delete[] mRecord;
    delete[] outputData;

    return (RC) 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {

    char* inputData = (char*) data;

    const int fields = recordDescriptor.size();
    const int nullBytes = (int) ceil((double)fields/8);

    char* actualData = inputData + nullBytes;
    char* mSource = actualData;
    char* mad;

    for(int j = 0; j < fields; j++){
        mad = inputData +  (int)(floor((double)j/8));
        char consider = *mad;
        int l = j - (8 * floor((double)j/8));
        consider <<= l;
        char ote = 128;
        consider = consider & ote;
        if(consider != ote){
            // field is not null
            Attribute mAtr = recordDescriptor[j];
            if(mAtr.type == 0){
                // integer type
                int value = 0;
                std::memcpy((char*)&value, mSource, 4);
                cout << mAtr.name << ":\t" << value << "\t" << endl;
                mSource += 4;
            }else{
                if(mAtr.type == 1){
                    // real type
                    float value = 0;
                    std::memcpy((char*)&value, mSource, 4);
                    cout << mAtr.name << ":\t" << value << "\t" << endl;
                    mSource += 4;
                }else{
                    // VARCHAR.
                    int lov = 0;
                    std::memcpy((char*)&lov, mSource, 4);
                    mSource += 4;
                    char value[lov + 1];
                    std::memcpy((void*)value, mSource, lov);
                    mSource += lov;
                    value[lov] = '\0';
                    cout << mAtr.name << ":\t" << value << "\t" << endl;
                }
            }
        }else{
            // field is null.
            Attribute mAtr = recordDescriptor[j];
            cout << mAtr.name << ":\t" << "NULL" << "\t" << endl;
        }
    }

    return (RC) 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){

	// find the actual page where the record is located
	// using a loop.
	int pageNum = rid.pageNum + 1;	// to incorporate the internal page numbering
	int slotNum = rid.slotNum;
	char* mPage = new char[PAGE_SIZE];

	vector<FreeSpaceNode> fsVector = fileHandle.getFSpace();
	FreeSpaceNode fsNode;

	vector<SlotTableNode> mSlotTable;

	SlotTableNode mSlot;

	char* mRecord = new char[PAGE_SIZE];

	int status = -1; // -1 for moved to another page

	while(status == -1){

		// setup fsNode.
		if(fsVector.size() <= pageNum){
			// page doesn't exist
			//cout << "Broke in deleteRecord.";
			delete[] mPage;
			delete[] mRecord;
			//cout << "pagenumber out of index. " << pageNum << endl;
			return (RC) -1;
		}
		fsNode = fsVector[pageNum];

		// read the page into m/y
		if(fileHandle.readPage(pageNum - 1, mPage) != (RC) 0){
			//cout << "Broke in deleteRecord.";
			delete[] mPage;
			delete[] mRecord;
			//cout << "couldnt read page" << pageNum-1 << endl;
			return (RC) -1;
		}

		// form the slot table
		mSlotTable.clear();
		mSlotTable.resize((PAGE_SIZE - fsNode.getTableStart()) / sizeof(SlotTableNode));
		std::memcpy((char*) &mSlotTable[0], mPage + fsNode.getTableStart(), PAGE_SIZE - fsNode.getTableStart());

		// find corresponding slot
		if(mSlotTable.size() <= slotNum){
			// slot doesn't exist
			//cout << "Broke in deleteRecord.";
			delete[] mPage;
			delete[] mRecord;
			//cout << "SLot out of index." << pageNum-1 << endl;
			return (RC) -1;
		}
		mSlot = mSlotTable[slotNum];

		// check if record has a non-zero length or not
		if(mSlot.getRecordLength() == 0){
			// slot already deleted
			//cout << "Broke in deleteRecord.";
			delete[] mPage;
			delete[] mRecord;
			//cout << "Already deleted" << endl;
			return (RC) -1;
		}

		// read record into m/y
		std::memcpy(mRecord, mPage + mSlot.getRecordStart(), mSlot.getRecordLength());

		// read version. if version = -1, then loop again
		std::memcpy((char*) &status, mRecord, 4);
		if(status != -1){
			break;
		}else{
			// update pageNum and slotNum
			std::memcpy((char*) &pageNum, mRecord + 4, 4);
			std::memcpy((char*) &slotNum, mRecord + 8, 4);
		}
	}

	// here, everything corresponds to the final page.

	const int length_leftPortion = mSlot.getRecordStart();
	char* leftPortion = new char[length_leftPortion];
	std::memcpy(leftPortion, mPage, length_leftPortion);


	const int length_rightPortion = mSlotTable.back().getRecordStart() + mSlotTable.back().getRecordLength()
									- length_leftPortion - mSlot.getRecordLength();


	char* rightPortion = new char[length_rightPortion];
	std::memcpy(rightPortion, mPage + mSlot.getRecordStart() + mSlot.getRecordLength(), length_rightPortion);

	// new free space
	int k = fsNode.getSpace() + mSlot.getRecordLength();

	// new slot table size
	int slotTableLength = mSlotTable.size() * sizeof(SlotTableNode);

	// rewrite the page
	//std::memcpy(mPage, leftPortion, length_leftPortion);
	std::memcpy(mPage + length_leftPortion, rightPortion, length_rightPortion);
	for(int i = slotNum + 1; i < mSlotTable.size(); i++){
		SlotTableNode tempNode = mSlotTable[i];
		tempNode.setRecordStart(tempNode.getRecordStart() - mSlot.getRecordLength());
		mSlotTable[i] = tempNode;
	}
	mSlot.setRecordLength(0); // no need to change record start and slot value
	mSlotTable[slotNum] = mSlot;
	std::memcpy(mPage + length_leftPortion + length_rightPortion + k, (char*) &mSlotTable[0], slotTableLength);

	if(fileHandle.writePage(pageNum - 1, mPage) != (RC) 0){
		//cout << "Broke in deleteRecord.";
		delete[] mPage;
		delete[] mRecord;
		//cout << "Couldnt write page" << pageNum - 1 << endl;
		return (RC) -1;
	}


	// update fsVector
	fsNode.setSpace(k);
	fsNode.setTableStart(length_leftPortion + length_rightPortion + k);
	fsVector[pageNum] = fsNode;

	// set fsVector
	fileHandle.setFSpace(fsVector);

	// relieve allocated m/y
	delete[] mPage;
	delete[] mRecord;
	delete[] leftPortion;
	delete[] rightPortion;

	return (RC) 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid){

	//cout << "Entered updateRecord, updating RID: (" << rid.pageNum << ", " << rid.slotNum << ")"  << endl;

	// find the actual page where the record is located
	// using a loop.
	int pageNum = rid.pageNum + 1;	// to incorporate the internal page numbering
	int slotNum = rid.slotNum;
	char* mPage = new char[PAGE_SIZE];

	vector<FreeSpaceNode> fsVector = fileHandle.getFSpace();
	FreeSpaceNode fsNode;

	vector<SlotTableNode> mSlotTable;

	SlotTableNode mSlot;

	char* mRecord = new char[PAGE_SIZE];

	int status = -1; // 1 for record is on this page, -1 for moved to another page


	while(status == -1){

		// setup fsNode.
		if(fsVector.size() <= pageNum){
			// page doesn't exist
			//cout << "Broke in update Record..";
			delete[] mRecord;
			delete[] mPage;
			//cout << "page out of index error. Page no=" << pageNum << endl;
			return (RC) -1;
		}
		fsNode = fsVector[pageNum];

		// read the page into m/y
		if(fileHandle.readPage(pageNum - 1, mPage) != (RC) 0){
			//cout << "Broke in update Record..";
			delete[] mRecord;
			delete[] mPage;
			//cout << "Couldn't read page into m/y. Page num:" << pageNum - 1 << ", file: " << fileHandle.fileName << endl;
			return (RC) -1;
		}

		// form the slot table
		mSlotTable.clear();
		mSlotTable.resize((PAGE_SIZE - fsNode.getTableStart()) / sizeof(SlotTableNode));
		std::memcpy((char*) &mSlotTable[0], mPage + fsNode.getTableStart(), PAGE_SIZE - fsNode.getTableStart());

		// find corresponding slot
		if(mSlotTable.size() <= slotNum){
			// slot doesn't exist
			//cout << "Broke in update Record..";
			delete[] mRecord;
			delete[] mPage;
			//cout << "slot out of index error." << endl;
			return (RC) -1;
		}
		mSlot = mSlotTable[slotNum];


		// check if record has a non-zero length or not
		if(mSlot.getRecordLength() == 0){
			// slot deleted
			//cout << "Broke in update Record..";
			delete[] mRecord;
			delete[] mPage;
			//cout << "deleted record" << endl;
			return (RC) -1;
			return -1;
		}

		// read record into m/y
		std::memcpy(mRecord, mPage + mSlot.getRecordStart(), mSlot.getRecordLength());

		// read version. If version != -1, then record has been found.
		std::memcpy((char*) &status, mRecord, 4);
		if(status != -1){
			break;
		}else{
			// update pageNum and slotNum
			std::memcpy((char*) &pageNum, mRecord + 4, 4);
			std::memcpy((char*) &slotNum, mRecord + 8, 4);
		}
	}

	// here, everything corresponds to the final page.
	delete[] mRecord;


	char* inputData = (char*) data;

    //******************************** changing representation of data ********************//
    // calculate length of record //
    const int fields = recordDescriptor.size();
    const int nullBytes = (int)ceil((double)fields/8);
    char* actualData = inputData + nullBytes;

    // destination for pointerData;
    int* myPointerData = new int[fields];

    // destination for actualData;
    char* myActualData = new char[PAGE_SIZE];

    char* mSource = actualData;
    char* mDestination = myActualData;

    char* mad;

    for(int j = 0; j < fields; j++){
        mad = inputData +  (int)(floor((double)j/8));
        char consider = *mad;
        int l = j - (8 * floor((double)j/8));
        consider <<= l;
        char ote = 128;
        consider = consider & ote;
        if(consider != ote){
            // that field is not null
            // get data type from recordDescriptor.
            Attribute mAtr = recordDescriptor[j];
            if(mAtr.type == 0){
                // integer type
                myPointerData[j] = 4 + nullBytes + (4*fields) + (int)(mDestination - myActualData);
                std::memcpy(mDestination, mSource, 4);
                mSource += 4;
                mDestination += 4;
            }else{
                if(mAtr.type == 1){
                    // real type (floating)
                    myPointerData[j] = 4 + nullBytes + (4*fields) + (int)(mDestination - myActualData);
                    std::memcpy(mDestination, mSource, 4);
                    mSource += 4;
                    mDestination += 4;
                }else{
                    // VARCHAR type
                    myPointerData[j] = 4 + nullBytes + (4*fields) + (int)(mDestination - myActualData);
                    int lov = 0;
                    std::memcpy(&lov, mSource, 4);
                    std::memcpy(mDestination, mSource, 4);
                    mSource += 4;
                    mDestination += 4;

                    std::memcpy(mDestination, mSource, lov);
                    mSource += lov;
                    mDestination += lov;
                }
            }
        }else{
            // field is null.
            myPointerData[j] = -1;
        }
    }

    const int theMeat = (int)(mDestination - myActualData);
    int newRecordLength = 4 + nullBytes + (4*fields) + theMeat;

    // combine the result -> version + nullbytes + pointerdata + actualdata
    mRecord = new char[newRecordLength];
    char* mr = mRecord;


    // insert the version number. Starts from 1, -1 for tombstone
    int ver = (fileHandle.getMetaData()).getVersion();
    std::memcpy(mr, (char*) &ver, 4);
    mr = mr + 4;

    std::memcpy(mr, inputData, nullBytes);
    mr = mr + nullBytes;

    std::memcpy(mr, (char*) myPointerData, 4*fields);
    mr = mr + (4 * fields);

    std::memcpy(mr, myActualData, theMeat);

    delete[] myPointerData;
    delete[] myActualData;

    //******************************** changing representation of data done ********************//

	const int currentRecordLength = mSlot.getRecordLength();


	if((newRecordLength - currentRecordLength) <= fsNode.getSpace()){
		// new record can fit into this page
		const int length_rightPortion = (mSlotTable.back().getRecordStart() + mSlotTable.back().getRecordLength()
										- mSlot.getRecordStart() - mSlot.getRecordLength());
		char* rightPortion = new char[length_rightPortion];
		std::memcpy(rightPortion, mPage + mSlot.getRecordStart() + mSlot.getRecordLength(), length_rightPortion);

		std::memcpy(mPage + mSlot.getRecordStart(), mRecord, newRecordLength);

		// shift other slots to the right OR left. depends on +ve or -ve.
		std::memcpy(mPage + mSlot.getRecordStart() + newRecordLength, rightPortion, length_rightPortion);
		for (int i = mSlot.getSlotValue() + 1; i < mSlotTable.size(); i++){
			SlotTableNode temp = mSlotTable[i];
			temp.setRecordStart(temp.getRecordStart() + (newRecordLength - currentRecordLength));
			mSlotTable[i] = temp;
		}

		mSlot.setRecordLength(newRecordLength);

		// set the edited mSlot, back into the mSlotTable
		mSlotTable[mSlot.getSlotValue()] = mSlot;

		const int k = fsNode.getSpace() - (newRecordLength - currentRecordLength);
		//int slottablestart = mSlotTable.back().getRecordStart() + mSlotTable.back().getRecordLength() + k;
		const int slottablestart = PAGE_SIZE - (sizeof(SlotTableNode) * mSlotTable.size());

		std::memcpy(mPage + slottablestart, (char*) &mSlotTable[0], mSlotTable.size() * sizeof(SlotTableNode));

		if(fileHandle.writePage(pageNum - 1, mPage) != (RC) 0){
			//cout << "broke in update record";
			delete[] mPage;
			delete[] mRecord;
			delete[] rightPortion;
			//cout << "couldnt write page: " << pageNum - 1 << endl;
			return (RC) -1;
		}

		fsNode.setSpace(k);
		fsNode.setTableStart(slottablestart);
		fsVector[pageNum] = fsNode;
		fileHandle.setFSpace(fsVector);

		if( (k>PAGE_SIZE) || (k<0) || (slottablestart>=PAGE_SIZE) || (slottablestart<0) ){
			//cout << "broke in update record";
			delete[] mPage;
			delete[] mRecord;
			delete[] rightPortion;
			//cout << "INCOSTISENCY. Same page update " << pageNum - 1 << endl;
			return (RC) -1;
		}

		delete[] rightPortion;

	}else{
		// new record canNOT fit
		// make tombstone on this page

		// check if there is space to make a tombstone
		if(fsNode.getSpace() < (12 - currentRecordLength)){
			//cout << "Inside update. No space for tombstone" << endl;
			delete[] mPage;
			delete[] mRecord;
			return (RC) -1;
		}

		char* mTombstone = new char[12];
		int stat = -1;
		std::memcpy(mTombstone, (char*) &stat, 4);

		RID mRid;
		// don't need to update fsVector before calling this
		// because existing page will not have space anyway
		if(insertRecord(fileHandle, recordDescriptor, data, mRid) == -1){
			//cout << "Inserting record (called from update) has failed..." << endl;
			delete[] mTombstone;
			delete[] mPage;
			delete[] mRecord;
			return (RC) -1;
		}

		// check if returned mrid resides on the same page or not..
		if(mRid.pageNum + 1 == (pageNum)){
			//cout << "Broke in update. TO be chained record was inserted in the same page.." << endl;
			delete[] mTombstone;
			delete[] mPage;
			delete[] mRecord;
			return (RC) -1;
		}

		// *********** IMPORTANT ****************************************************
		// now insert record has modified the fsVector
		// *********** IMPORTANT ****************************************************

		// So,
		fsVector = fileHandle.getFSpace();

		int pn = mRid.pageNum + 1;
		int sn = mRid.slotNum;
		std::memcpy(mTombstone + 4, (char*) &pn, 4);
		std::memcpy(mTombstone + 8, (char*) &sn, 4);

		newRecordLength = 12; 	// newRecordisthetombstone

		const int length_rightPortion = (mSlotTable.back().getRecordStart() + mSlotTable.back().getRecordLength()
													- mSlot.getRecordStart() - mSlot.getRecordLength());
		char* rightPortion = new char[length_rightPortion];
		std::memcpy(rightPortion, mPage + mSlot.getRecordStart() + mSlot.getRecordLength(), length_rightPortion);


		// copy the tombstone to the page
		std::memcpy(mPage + mSlot.getRecordStart(), mTombstone, newRecordLength);

		// shift the slots
		std::memcpy(mPage + mSlot.getRecordStart() + newRecordLength, rightPortion, length_rightPortion);

		for (int i = mSlot.getSlotValue() + 1; i < mSlotTable.size(); i++){
			SlotTableNode temp = mSlotTable[i];
			temp.setRecordStart(temp.getRecordStart() - (currentRecordLength - newRecordLength));
			mSlotTable[i] = temp;
		}

		mSlot.setRecordLength(newRecordLength);
		// set the edited mSlot, back into the mSlotTable
		mSlotTable[slotNum] = mSlot;

		//const int k = fsNode.getSpace() + (mSlot.getRecordLength() - newRecordLength);
		const int k = fsNode.getSpace() - (newRecordLength - currentRecordLength);
		//const int slottablestart = mSlotTable.back().getRecordStart() + mSlotTable.back().getRecordLength() + k;
		const int slottablestart = PAGE_SIZE - (sizeof(SlotTableNode) * mSlotTable.size());


		std::memcpy(mPage + slottablestart, (char*) &mSlotTable[0], mSlotTable.size() * sizeof(SlotTableNode));


		if(fileHandle.writePage(pageNum - 1, mPage) != (RC) 0){
			//cout << "broke in update record (making tombstone)";
			delete[] mPage;
			delete[] mRecord;
			delete[] rightPortion;
			delete[] mTombstone;
			//cout << "couldnt write page: " << pageNum - 1 << endl;
			return (RC) -1;
		}

		fsNode.setSpace(k);
		fsNode.setTableStart(slottablestart);
		fsVector[pageNum] = fsNode;
		fileHandle.setFSpace(fsVector);

		if( (k>PAGE_SIZE) || (k<0) || (slottablestart>=PAGE_SIZE) || (slottablestart<0) ){
			//cout << "broke in update record";
			delete[] mPage;
			delete[] mRecord;
			delete[] rightPortion;
			//cout << "INCOSTISENCY. Tombstone insertion " << pageNum - 1 << endl;
			return (RC) -1;
		}


		delete[] mTombstone;
		delete[] rightPortion;

	}

	delete[] mPage;
	delete[] mRecord;

	return (RC) 0;
}







RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
		const RID &rid, const string &attributeName, void *data){

	int pageNum = rid.pageNum + 1;	// to incorporate the internal page numbering
	int slotNum = rid.slotNum;

	vector<FreeSpaceNode> fsVector = fileHandle.getFSpace();
	FreeSpaceNode fsNode;
	vector<SlotTableNode> mSlotTable;
	SlotTableNode mSlot;

	char* mRecord = new char[PAGE_SIZE];
	char* mPage = new char[PAGE_SIZE];

	int status = -1; // -1 for moved to another page

	while(status == -1){
		// setup fsNode.
		if(fsVector.size() <= pageNum){
			// page doesn't exist
			//cout << "Error in readAttribute";
			delete[] mPage;
			delete[] mRecord;
			//cout << "Page doesn't exist. (Page > no. of pages in file): " << fileHandle.fileName << endl;
			return (RC) -1;
		}
		fsNode = fsVector[pageNum];

		// read the page into m/y
		if(fileHandle.readPage(pageNum - 1, mPage) != (RC) 0){
			//cout << "Error in readAttribute";
			delete[] mPage;
			delete[] mRecord;
			//cout << "Couldn't read page: "<< pageNum << " in file: " << fileHandle.fileName << endl;
			return (RC) -1;
		}


		// form the slot table
		mSlotTable.clear();
		mSlotTable.resize((PAGE_SIZE - fsNode.getTableStart()) / sizeof(SlotTableNode));
		std::memcpy((char*) &mSlotTable[0], mPage + fsNode.getTableStart(), PAGE_SIZE - fsNode.getTableStart());

		// find corresponding slot
		if(mSlotTable.size() <= slotNum){
			// slot doesn't exist
			//cout << "Error in readAttribute";
			delete[] mPage;
			delete[] mRecord;
			//cout << "Couldn't create slot: "<< slotNum << " in page: " << pageNum << "in file: " << fileHandle.fileName << endl;
			return (RC) -1;
		}
		mSlot = mSlotTable[slotNum];

		// check if record has a non-zero length or not
		if(mSlot.getRecordLength() == 0){
			// slot already deleted
			delete[] mPage;
			delete[] mRecord;
			return (RC) -1;
		}

		// read record into m/y
		std::memcpy(mRecord, mPage + mSlot.getRecordStart(), mSlot.getRecordLength());

		// read version. if version = -1, then loop again
		std::memcpy((char*) &status, mRecord, 4);
		if(status != -1){
			break;
		}else{
			// update pageNum and slotNum
			std::memcpy((char*) &pageNum, mRecord + 4, 4);
			std::memcpy((char*) &slotNum, mRecord + 8, 4);
		}
	}

	// here, everything corresponds to the final page

    const int fields = recordDescriptor.size();
    const int nullBytes = (int)ceil((double)fields/8.0);

    int* directAccess = new int[fields];
    std::memcpy((char*) directAccess, mRecord + 4 + nullBytes, 4 * fields);

    // compute attribute position

    int atrPosition = 0;
    while(atrPosition < fields){
    	if(attributeName == recordDescriptor[atrPosition].name){
    		// record matches.
    		break;
    	}
    	atrPosition++;
    }

    if(atrPosition == fields){
    	// attribute doesn't exist
    	//cout << "Error in readAttribute";
    	delete[] mPage;
    	delete[] mRecord;
    	delete[] directAccess;
    	//cout << "Attribute out of Index error.." << endl;
    	return (RC) -1;
    }

    // this is offset from start of record
    int offset = directAccess[atrPosition];

    if(offset == -1){
    	// field is null
    	// just return the null byte,  i.e. '10000000'
    	char toreturn = 128;
    	std::memcpy(data, &toreturn, 1);
    	return (RC) 0;
    }

    if(recordDescriptor[atrPosition].type == TypeInt){
    	// int
    	char nb = 0;
    	std::memcpy(data, &nb, 1);
    	std::memcpy(data + 1, mRecord + offset, 4);
    }else if(recordDescriptor[atrPosition].type == TypeReal){
    	// float
    	char nb = 0;
    	std::memcpy(data, &nb, 1);
    	std::memcpy(data + 1, mRecord + offset, 4);
    }else{
    	// varchar
    	char nb = 0;
    	std::memcpy(data, &nb, 1);

    	int lov = 0;
    	std::memcpy((char*) &lov, mRecord + offset, 4);
    	std::memcpy(data + 1, (char*) &lov, 4);

    	std::memcpy(data + 1 + 4, mRecord + offset + 4, lov);
    }

    delete[] directAccess;
    delete[] mRecord;
    delete[] mPage;

	return (RC) 0;
}

RC RecordBasedFileManager::projectKey(const void* record, const string keyName, const vector<Attribute> recDes, void* projection){

	const int nullBytes = (int) ceil( (float) recDes.size() / 8.0);
	int scanOffset = nullBytes + 4;

	int constAccess[recDes.size()];
	std::memcpy((char*) constAccess, record + scanOffset, 4 * recDes.size());

	for(int i = 0; i < recDes.size(); i++){
		if(recDes[i].name == keyName){

			// check if null or not
			if (constAccess[i] == -1){
				char nb = 128;
				std::memcpy(projection, &nb, 1);
				return (RC) 0;
			}else{
				char nb = 0;
				std::memcpy(projection, &nb, 1);
			}

			const int recordStart = constAccess[i];

			AttrType mType = recDes[i].type;
			if (mType != TypeVarChar){
				std::memcpy(projection + 1, record + recordStart, 4);
			}else{
				int lov;
				std::memcpy((char*) &lov, record + recordStart, 4);

				std::memcpy(projection + 1, (char*) &lov, 4);
				std::memcpy(projection + 1 + 4, record + recordStart + 4, lov);
 			}



			break;
		}
	}



	return (RC) 0;
}



RC RecordBasedFileManager::scan(FileHandle &fileHandle,
	      const vector<Attribute> &recordDescriptor,
	      const string &conditionAttribute,
	      const CompOp compOp,
	      const void *value,
	      const vector<string> &attributeNames,
	      RBFM_ScanIterator &rbfm_ScanIterator){


	vector<RID> mFinalRecords;	// to return RIDs of qualifying records
	string modifiedConditionAttribute = conditionAttribute;

	vector<FreeSpaceNode> fsVector = fileHandle.getFSpace();
	FreeSpaceNode fsNode;
	vector<SlotTableNode> mSlotTable;

	// handle for "", NO_OP case. That is, SELECT *
	if((modifiedConditionAttribute == "") && (compOp == NO_OP)){
		modifiedConditionAttribute = (string) recordDescriptor[0].name;
	}

	// determine type of modifiedConditionAttribute (int/float/varchar)
	int type = -1;
	int maxLov = -1; // only for varchar

	for(int i = 0; i < recordDescriptor.size(); i++){
		if(modifiedConditionAttribute == (string) recordDescriptor[i].name){
			type = (int) recordDescriptor[i].type;
			if(type == 2){
				// for varchar
				maxLov = (int) recordDescriptor[i].length;
			}
			break;
		}
	}

	if(type == -1){
		//cout << "Specified condition attribute is invlid" << endl;
		return (RC) -1;
	}

	int pageNum;	// internal format
	char* mPage = new char[PAGE_SIZE];

	// iterate through pages.
	for(int i = 1; i < fsVector.size(); i++){
		pageNum = i;	// internal format
		fsNode = fsVector[i];

		// read the page into m/y from disk
		if(fileHandle.readPage(pageNum - 1, mPage) != (RC) 0){
			//cout << "Scan shits the bed - 1.";
			delete[] mPage;
			//cout << "COuldnt read page" << endl;
			return (RC) -1;
		}

		// form the slot table
		mSlotTable.clear();
		mSlotTable.resize((PAGE_SIZE - fsNode.getTableStart()) / sizeof(SlotTableNode));
		std::memcpy((char*) &mSlotTable[0], mPage + fsNode.getTableStart(), PAGE_SIZE - fsNode.getTableStart());

		// iterate through the slots on the current page
		for (int j = 0; j < mSlotTable.size(); j++){

			SlotTableNode mSlot = mSlotTable[j];
			char* mRecordValue = new char[1 + 4 + maxLov];

			// deleted record
			if(mSlot.getRecordLength() == 0){
				delete[] mRecordValue;
				continue;
			}

			// updated record (moved to another page)
			int version = -1;
			std::memcpy((char*) &version, mPage + mSlot.getRecordStart(), 4);
			if(version == -1){
				delete[] mRecordValue;
				continue;
			}

			RID mrid;
			mrid.pageNum = pageNum - 1; 	// because pageNum is internal format. Convert to external
			mrid.slotNum = j;


			// TODO : Change readAttribute
			// load entire record into m/y
			char entireRecord[mSlot.getRecordLength()];
			std::memcpy(entireRecord, mPage + mSlot.getRecordStart(), mSlot.getRecordLength());

			if(projectKey(entireRecord, modifiedConditionAttribute, recordDescriptor, mRecordValue) != (RC) 0){
				delete[] mPage;
				delete[] mRecordValue;
				return (RC) -1;
			}


			/*

			if(readAttribute(fileHandle, recordDescriptor, mrid, modifiedConditionAttribute, mRecordValue) != (RC) 0){
				//cout << "Scan shits the bed - 2. ";
				delete[] mPage;
				delete[] mRecordValue;
				//cout << "Error in reading attribute: " << modifiedConditionAttribute << ", from Table:" << fileHandle.fileName << endl;
				return (RC) -1;
			}

			*/


			// check if satisfies condition
			bool satisfies = false;

			if(!value){
				// passed in is NULL
				if(type < 2){
					// int or real
					char nb;
					std::memcpy(&nb, mRecordValue, 1);

					if(nb == (char)0){
						// stored value NOT null, and passed in is NULL
						int mrv;
						std::memcpy((char*) &mrv, mRecordValue + 1, 4);

						switch((int) compOp){
						case 5:
							satisfies = true;
							break;
						case 6:
							satisfies = true;
							break;
						default:
							satisfies = false;
							break;
						}


					}else{
						// stored value null, and passed in is NULL
						switch((int) compOp){
						case 0:
							satisfies = true;
							break;
						case 6:
							satisfies = true;
							break;
						default:
							satisfies = false;
							break;
						}
					}

				}else{
					// varchar
					char nb;
					std::memcpy(&nb, mRecordValue, 1);

					if(nb == (char)0){
						// stored value is NOT null, passed in is null

						int lov;
						std::memcpy((char*) &lov, mRecordValue + 1, 4);

						char* tempmrv = new char[lov + 1];
						std::memcpy(tempmrv, mRecordValue + 1 + 4, lov);
						tempmrv[lov] = '\0';
						string mrv(tempmrv);
						delete[] tempmrv;

						switch((int)compOp){
						case 5:
							satisfies = true;
							break;
						case 6:
							satisfies = true;
							break;
						default:
							satisfies = false;
							break;
						}
					}else{
						// stored value is null, passed in is null
						switch((int) compOp){
						case 0:
							satisfies = true;
							break;
						case 6:
							satisfies = true;
							break;
						default:
							satisfies = false;
							break;
						}
					}
				}

			}



			if(value){
				// passed in is NOT NULL
				if(type < 2){
					// int or real
					char isNull;
					std::memcpy(&isNull, mRecordValue, 1);

					if(isNull == (char)0){
						// stored value NOT null, and passed also not null
						int mrv;
						std::memcpy((char*) &mrv, mRecordValue + 1, 4);
						int given;
						std::memcpy((char*) &given, value, 4);

						switch((int) compOp){
						case 0:
							if(mrv == given)
								satisfies = true;
							break;
						case 1:
							if(mrv < given)
								satisfies = true;
							break;
						case 2:
							if(mrv <= given)
								satisfies = true;
							break;
						case 3:
							if(mrv > given)
								satisfies = true;
							break;
						case 4:
							if(mrv >= given)
								satisfies = true;
							break;
						case 5:
							if(mrv != given)
								satisfies = true;
							break;
						case 6:
							satisfies = true;
							break;
						default:
							satisfies = false;
							break;
						}


					}else{
						// stored value is null, passed in not null
						switch((int) compOp){
						case 5:
							satisfies = true;
							break;
						case 6:
							satisfies = true;
							break;
						default:
							satisfies = false;
							break;
						}
					}


				}else{
					// varchar
					char isNull;
					std::memcpy(&isNull, mRecordValue, 1);

					if(isNull == (char)0){
						// stored value is NOT null, passed in not null

						int lov;
						std::memcpy((char*) &lov, mRecordValue + 1, 4);

						char* tempmrv = new char[lov + 1];
						std::memcpy(tempmrv, mRecordValue + 1 + 4, lov);
						tempmrv[lov] = '\0';
						string mrv(tempmrv);
						delete[] tempmrv;

						std::memcpy((char*) &lov, value, 4);

						char* tempgiven = new char[lov + 1];
						std::memcpy(tempgiven, ((char*)value) + 4, lov);
						tempgiven[lov] = '\0';
						string given(tempgiven);
						delete[] tempgiven;

						switch((int)compOp){
						case 0:
							if(mrv == given)
								satisfies = true;
							break;
						case 1:
							if(mrv < given)
								satisfies = true;
							break;
						case 2:
							if(mrv <= given)
								satisfies = true;
							break;
						case 3:
							if(mrv > given)
								satisfies = true;
							break;
						case 4:
							if(mrv >= given)
								satisfies = true;
							break;
						case 5:
							if(mrv != given)
								satisfies = true;
							break;
						case 6:
							satisfies = true;
							break;
						default:
							satisfies = false;
							break;
						}
					}else{
						// stored value is null, passed in not null
						switch((int) compOp){
						case 5:
							satisfies = true;
							break;
						case 6:
							satisfies = true;
							break;
						default:
							satisfies = false;
							break;
						}
					}

				}
			}

			if(satisfies){
				mrid.pageNum = pageNum -1;
				mrid.slotNum = j;
				mFinalRecords.push_back(mrid);
			}

			delete[] mRecordValue;
		}
		// iterating through slots on page finished
	}
	// iterating through pages finished


	// set the records
	rbfm_ScanIterator.setRecords(mFinalRecords, fileHandle.fileName, recordDescriptor);
	//cout << "Tuples in table (: " << fileHandle.fileName << ") " << mFinalRecords.size() << endl;
	rbfm_ScanIterator.attributes = attributeNames;
	delete[] mPage;
	return ((RC) 0);
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data){

	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();

	// mFileHandler (a class-field) is already associated with an open file
	currentIndex++;
	if(currentIndex >= records.size()){
		return (RC) -1;
	}

	// set the input rid
	rid = records[currentIndex];

	char* mRecord = new char[PAGE_SIZE];
	if(rbfm->readRecord(mFileHandler, mRecordDescriptor, records[currentIndex], mRecord) != (RC) 0){
		//cout << "Breaks in getNextRecord.";
		delete[] mRecord;
		//cout << " Not able to rbfm->readRecord. RID:" << rid.pageNum << ", " << rid.slotNum << endl;
		return (RC) -1;
	}

	// filter this mRecord to suit your needs
	// now, attributes might not be in the same order as recordDescriptor

	// create a new vector<Attribute> that stores the actual records as each element. Length of varchar should
	// be exact, as in data

	vector<Field> mFields;

	const int fields = mRecordDescriptor.size();
    const int nullBytes = (int)ceil((double)fields/8.0);
    char* mSource = mRecord + nullBytes;
    char* mad;

    for(int j = 0; j < fields; j++){
        mad = mRecord +  (int)(floor((double)j/8));
        char consider = *mad;
        int l = j - (8 * floor((double)j/8));
        consider <<= l;
        char ote = 128;
        consider = consider & ote;

        Field toAdd;
        Attribute mAtr = mRecordDescriptor[j];
        if(consider != ote){
            // that field is not null
        	// add a new element to mFields

            toAdd.valid = 1;
            toAdd.properties.name = mAtr.name;
            toAdd.properties.type = mAtr.type;

            if(mAtr.type == TypeInt){
                // integer type
            	toAdd.properties.length = 4;
            	int temp;
            	std::memcpy((char*) &toAdd.integer_value, mSource, 4);
                mSource += 4;
            }else{
                if(mAtr.type == TypeReal){
                    // real type (floating)
                	toAdd.properties.length = 4;
                	std::memcpy((char*) &toAdd.float_value, mSource, 4);
                    mSource += 4;

                }else{
                    // varchar type
                	std::memcpy((char*) &toAdd.properties.length, mSource, 4);
                    mSource += 4;
                    char* temporary = new char[toAdd.properties.length + 1];
                    std::memcpy(temporary, mSource, toAdd.properties.length);
                    temporary[toAdd.properties.length] = '\0';
                    string str(temporary);
                    toAdd.varchar_value = str;
                    delete[] temporary;
                    mSource += toAdd.properties.length;
                }
            }
        }else{
            // field is null.
            toAdd.valid = 0;
            toAdd.properties.name = mAtr.name;
            toAdd.properties.type = mAtr.type;
        }
        mFields.push_back(toAdd);
    }

    // all fields have been added
    char* outputData = new char[PAGE_SIZE];
    char* mDestination = outputData;

    // create new null bytes, initialize all of them as 000000000000000000000...
    const int filteredNullBytes = (int) ceil((double)(attributes.size())/8.0);
    for(int i = 0; i < filteredNullBytes; i++){
    	unsigned char zero = (unsigned char) 0;
    	std::memcpy(mDestination, &zero, 1);
    	mDestination++;
    }

    for(int i = 0; i < attributes.size(); i++){	// iterating the vector<String> containing names
    	for(int j = 0; j < mFields.size(); j++){	// iterating the vector<Field> containing real data

    		if (attributes[i] == mFields[j].properties.name){

    			Field theField = mFields[j];
    			if(theField.valid == 1){
    				// not null
    				if(theField.properties.type == TypeInt){
    					// int
    					std::memcpy(mDestination, (char*) &theField.integer_value, 4);
    					mDestination += 4;
    				}else if(theField.properties.type == TypeReal){
    					//real
    					std::memcpy(mDestination, (char*) &theField.float_value, 4);
    					mDestination += 4;
    				}else{
    					//varchar
    					std::memcpy(mDestination, (char*) &theField.properties.length, 4);
    					mDestination += 4;
    					std::memcpy(mDestination, theField.varchar_value.c_str(), theField.properties.length);
    					mDestination += theField.properties.length;
    				}

    			}else {
    				// null
    				// set that corresponding bit to '1' in filterednullbytes
    				// this would be the 'i th' bit from left
    				// set it to 1

    				int considerOffset = (int) floor(((float)i)/8.0);
    				unsigned char consider;
    				std::memcpy(&consider, outputData + considerOffset, 1);

    				int bitToSet = i - (8*considerOffset);	// starting from left
    				consider |= ( ((unsigned char)128) >> bitToSet );

    				std::memcpy(outputData + considerOffset, &consider, 1);
    			}




    		}

    	}
    }

    // output data is ready
    int recLenActual = (int) (mDestination - outputData);
    std::memcpy(data, outputData, recLenActual);

    delete[] outputData;
    delete[] mRecord;

    return (RC) 0;
}

RC RBFM_ScanIterator::close(){

	currentIndex = -1;
	records.clear();
	mRecordDescriptor.clear();
	attributes.clear();

	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
	rbfm->closeFile(mFileHandler);

	return (RC) 0;
}

RC RBFM_ScanIterator::setRecords(vector<RID> rids, string fileName, vector<Attribute> recDes){
	  // set the filename with mFileHandler
	  RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();

	  if(mFileHandler.mFile.is_open()){
		  rbfm->closeFile(mFileHandler);
	  }

	  if(rbfm->openFile(fileName, mFileHandler) != (RC) 0){
		  return (RC) -1;
	  }
	  records = rids;
	  mRecordDescriptor = recDes;
	  return (RC) 0;
}


















