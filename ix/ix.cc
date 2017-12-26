#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    PagedFileManager* pfm = PagedFileManager::instance();
    return (RC) (pfm->createFile(fileName));
}

RC IndexManager::destroyFile(const string &fileName)
{
    PagedFileManager* pfm = PagedFileManager::instance();
    return (RC) (pfm->destroyFile(fileName));
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
	PagedFileManager* pfm = PagedFileManager::instance();
	RC result = (RC) (pfm->openFile(fileName, ixfileHandle.lowerFileHandler));

	// also initialize the counters
	if(result == (RC) 0){
		if(ixfileHandle.lowerFileHandler.collectCounterValues(
				ixfileHandle.ixReadPageCounter, ixfileHandle.ixWritePageCounter,
				ixfileHandle.ixAppendPageCounter) != (RC) 0){
			result = (RC) -1;
		}
	}
	// but these are not updated on every operation. only updated during call to collectCounter

	return result;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    PagedFileManager* pfm = PagedFileManager::instance();
    return (RC) (pfm->closeFile(ixfileHandle.lowerFileHandler));
}



// returns the lowest NONEMPTY page
RC IndexManager::getLowestKey(IXFileHandle &ixfileHandle, const Attribute &attribute, int &leafPage){

	vector<FreeSpaceNode> fsVector = ixfileHandle.lowerFileHandler.getFSpace();

	// no path possible - (no root)
	if(ixfileHandle.lowerFileHandler.getNumberOfPages() == 0){
		return (RC) 0;
	}

	// root always starts from '1'.
	int current_pointer = 1;
	bool search = true;

	while(search){

		char* currentNode = new char[PAGE_SIZE];
		if(ixfileHandle.lowerFileHandler.readPage(current_pointer - 1, currentNode) != (RC) 0){
			delete[] currentNode;
			search = false;
			return (RC) -1;
		}

		// CHECK IF THIS PAGE IS NON-LEAF OR LEAF PAGE
		char pageType = 0;
		std::memcpy(&pageType, currentNode, 1);
		if(pageType == (char) 1){
			// i.e. pageType == 1, means ki leaf
			delete[] currentNode;
			search = false;
			break;
		}



		std::memcpy((char*) &current_pointer, currentNode + 1, 4);

		delete[] currentNode;

	}



	// traverse till you get a NONEMPTY PAGE
	char mPage[PAGE_SIZE];
	bool foundData = false;

	while(!foundData){
		// check the current page
		FreeSpaceNode cfsn = fsVector[current_pointer];
		if(cfsn.getSpace() < (PAGE_SIZE - 1 - 4)){
			foundData = true;
			break;
		}

		if(ixfileHandle.lowerFileHandler.readPage(current_pointer - 1, mPage) != (RC) 0){
			return (RC) -1;
		}

		std::memcpy((char*) &current_pointer, mPage + 1, 4);
		if(current_pointer == -1){
			//cout << "Every page is empty!!!" << endl;
			return (RC) -1;
		}
	}


	leafPage = current_pointer;

	return (RC) 0;
}

RC IndexManager::getPathToLeaf(IXFileHandle &ixfileHandle, const Attribute &attribute,
		const void *key, vector<int> &path, int &leafPage){

	vector<int> parents;
	vector<FreeSpaceNode> fsVector = ixfileHandle.lowerFileHandler.getFSpace();

	// no path possible - (no root)
	if(ixfileHandle.lowerFileHandler.getNumberOfPages() == 0){
		path = parents;
		return (RC) 0;
	}

	int key_int = 0;
	float key_real = 0;
	string key_varchar = "";

	if(attribute.type == TypeInt){
		std::memcpy((char*) &key_int, key, 4);
	}else if(attribute.type == TypeReal){
		std::memcpy((char*) &key_real, key, 4);
	}else{
		int lov = 0;
		std::memcpy((char*) &lov, key, 4);
		char temp[lov+1];
		std::memcpy(temp, key + 4, lov);
		temp[lov] = '\0';
		string temp1(temp);
		key_varchar = temp1;
	}

	// root always starts from '1'.
	int current_pointer = 1;

	bool search = true;

	// search stops before the final LEAF node
	while(search){

		if((current_pointer < 1) || (current_pointer > ixfileHandle.lowerFileHandler.getNumberOfPages())){
			//cout << "Such a page doesnt exist::: current_pointer" << current_pointer << endl;
			return (RC) -1;
		}

		char currentNode[PAGE_SIZE];
		if(ixfileHandle.lowerFileHandler.readPage(current_pointer - 1, currentNode) != (RC) 0){
			//delete[] currentNode;
			search = false;
			return (RC) -1;
		}

		// CHECK IF THIS PAGE IS NON-LEAF OR LEAF PAGE
		char pageType = 0;
		std::memcpy(&pageType, currentNode, 1);
		if(pageType == 1){
			// i.e. pageType == 1, means ki leaf
			//delete[] currentNode;
			search = false;
			break;
		}

		parents.push_back(current_pointer);		// PUSHING the current node under consideration to stack
		FreeSpaceNode fsNode = fsVector[current_pointer];

		int scanPosition = 1 + 4;	// page-indicator + left-pointer
		bool found_next_pointer = false;

		while(scanPosition < (PAGE_SIZE - fsNode.getSpace())){

			if(attribute.type == TypeInt){

				int temp = 0;
				std::memcpy((char*) &temp, currentNode + scanPosition, 4);
				if (temp > key_int){
					// found the correct position
					found_next_pointer = true;
					std::memcpy((char*) &current_pointer, currentNode + scanPosition - 4, 4);
					break;
				}else{
					scanPosition += (4 + 4);	// skip this key, then a pointer
				}

			}else if(attribute.type == TypeReal){

				float temp = 0;
				std::memcpy((char*) &temp, currentNode + scanPosition, 4);
				if (temp > key_real){
					// found the correct position
					found_next_pointer = true;
					std::memcpy((char*) &current_pointer, currentNode + scanPosition - 4, 4);
					break;
				}else{
					scanPosition += (4 + 4);	// skip this key, then a pointer
				}


			}else{

				int lov = 0;
				std::memcpy((char*) &lov, currentNode + scanPosition, 4);
				char temp0[lov + 1];
				std::memcpy(temp0, currentNode + scanPosition + 4, lov);
				temp0[lov] = '\0';
				string temp(temp0);
				if(temp > key_varchar){
					// found the correct position
					found_next_pointer = true;
					std::memcpy((char*) &current_pointer, currentNode + scanPosition - 4, 4);
					break;
				}else{
					scanPosition += (4 + lov + 4);	// skip this key (4+xyz), then a pointer
				}


			}

		}

		if(!found_next_pointer){
			// take the last pointer
			scanPosition -= 4;
			std::memcpy((char*) &current_pointer, currentNode + scanPosition, 4);
		}

	}


	// here, currentPointer marks the final leaf node
	path = parents;
	leafPage = current_pointer;

	return (RC) 0;
}

RC IndexManager::initializeTree(IXFileHandle &ixfileHandle, const Attribute &attribute,
		const void *key, const RID &rid){

	if(ixfileHandle.lowerFileHandler.getNumberOfPages() != 0){
		//cout << "Tree already initialized" << endl;
		return (RC) -1;
	}

	vector<FreeSpaceNode> fsVector = ixfileHandle.lowerFileHandler.getFSpace();

	char* root = new char[PAGE_SIZE];
	int root_pointer = 1;

	char* left_leaf = new char[PAGE_SIZE];
	int left_leaf_pointer = 2;

	char* right_leaf = new char[PAGE_SIZE];
	int right_leaf_pointer = 3;


	/*
	 * FORM THE ROOT PAGE
	 */


	FreeSpaceNode rootFS(1, PAGE_SIZE, -1); 	// pgno, space, table-offset
	int freespace = PAGE_SIZE;

	char indicator = (char) 0;
	std::memcpy(root, (char*) &indicator, 1);
	freespace -= 1;

	std::memcpy(root + 1, (char*) &left_leaf_pointer, 4);
	freespace -= 4;

	if((attribute.type == TypeInt) || (attribute.type == TypeReal)){
		std::memcpy(root + 1 + 4, key, 4);
		freespace -= 4;
		std::memcpy(root + 1 + 4 + 4, (char*) &right_leaf_pointer, 4);
		freespace -= 4;
	}else{
		int lov = 0;
		std::memcpy((char*) &lov, key, 4);
		char ok[lov + 1];
		std::memcpy(ok, key + 4, lov);
		ok[lov] = '\0';

		string originalkey(ok);

		std::memcpy(root + 1 + 4, (char*) &lov, 4);
		freespace -= 4;
		std::memcpy(root + 1 + 4 + 4, originalkey.c_str(), lov);
		freespace -= lov;
		std::memcpy(root + 1 + 4 + 4 + lov, (char*) &right_leaf_pointer, 4);
		freespace -= 4;
	}

	rootFS.setSpace(freespace);
	fsVector.push_back(rootFS);

	/*
	 * SET THE LEFT AND RIGHT LEAF NODES (only linked lists and indicators)
	 */

	indicator = (char) 1;

	std::memcpy(left_leaf, (char*) &indicator, 1);
	std::memcpy(left_leaf + 1, (char*) &right_leaf_pointer, 4);
	FreeSpaceNode llFS(left_leaf_pointer, PAGE_SIZE - 5, -1);
	fsVector.push_back(llFS);

	std::memcpy(right_leaf, (char*) &indicator, 1);
	int rightright = -1;
	std::memcpy(right_leaf + 1, (char*) &rightright, 4);
	FreeSpaceNode rrFS(right_leaf_pointer, PAGE_SIZE - 5, -1);
	fsVector.push_back(rrFS);

	/*
	 * WRITE ALL PAGES TO DISK
	 */

	if((ixfileHandle.lowerFileHandler.appendPage(root) != (RC) 0) || (ixfileHandle.lowerFileHandler.appendPage(left_leaf) != (RC) 0) || (ixfileHandle.lowerFileHandler.appendPage(right_leaf) != (RC) 0)){
		delete[] root;
		delete[] left_leaf;
		delete[] right_leaf;
		return (RC) -1;
	}

	ixfileHandle.lowerFileHandler.setFSpace(fsVector);
	delete[] root;
	delete[] left_leaf;
	delete[] right_leaf;
	return (RC) 0;
}


// TODO : no need to check for deleted records
// gets the split position, based on non-leaf OR leaf page
RC IndexManager::getSplitPosition(const Attribute &attribute, const void *data,
		const int dataLength, int &splitPosition){

	if(dataLength < PAGE_SIZE){
		// NO NEED TO SPLIT
		splitPosition = -1;
		return (RC) 0;
	}

	bool foundSplit = false;
	int startSearchingFrom = ((int) dataLength / 2);
	while(true){
		foundSplit = false;
		startSearchingFrom -= 200;
		if(startSearchingFrom < 0){
			break;
		}

		char indicator;
		std::memcpy(&indicator, data, 1);

		if(indicator == 0){
			// non-leaf page
			int scanOffset = 1 + 4;		// this start point is different (from other functions)

			while(scanOffset < startSearchingFrom){

				if(attribute.type != TypeVarChar){
					scanOffset += (4 + 4);
				}else{
					int lov;
					std::memcpy((char*) &lov, data + scanOffset, 4);
					scanOffset += (4 + lov + 4);
				}
			}

			// no case of duplicate/deleted records
			splitPosition = scanOffset;
			foundSplit = true;
			break;


		}else if(indicator == 1){
			// leaf page

			int scanOffset = 1 + 4;

			// check if this offset > startSearchingFrom
			// if not go to next, till you increase more than startSearchingFrom

			while(scanOffset < startSearchingFrom){
				if(attribute.type != TypeVarChar){
					scanOffset += (1 + 4 + 8);
				}else{
					int lov;
					std::memcpy((char*) &lov, data + scanOffset + 1, 4);
					scanOffset += (1 + 4 + lov + 8);
				}
			}

			// scan offset is at a location greater or equal to 1900
			// check if this and the next have different keys or not.

			//bool foundSplit = false;
			while(!foundSplit && (scanOffset < dataLength)){

				// if this and the next are VALID non equal records. split at the mid.
				if(attribute.type != TypeVarChar){

					// next records scanOffset should also be less..
					if((scanOffset + 1 + 4 + 8) >= dataLength){
						break;
					}

					// compare their keys (should be non-equal)
					int res = std::memcmp(data + scanOffset + 1, data + scanOffset + 1 + 4 + 8 + 1, 4);
					if(res != 0){
						foundSplit = true;
						scanOffset += (1 + 4 + 8);	// this is also the split position
						break;
					}else{
						scanOffset += (1 + 4 + 8);
						continue;
					}
				}else if(attribute.type == TypeVarChar){

					int lov1;
					int lov2;
					std::memcpy((char*) &lov1, data + scanOffset + 1, 4);

					// next records scanOffset should also be less..
					if((scanOffset + 1 + 4 + lov1 + 8) >= dataLength){
						break;
					}

					std::memcpy((char*) &lov2, data + scanOffset + 1 + 4 + lov1 + 8 + 1, 4);

					//compare their keys (should not be equal)
					char tk1[lov1 + 1];
					char tk2[lov2 + 1];
					std::memcmp(tk1, data + scanOffset + 1 + 4, lov1);
					std::memcmp(tk2, data + scanOffset + 1 + 4 + lov1 + 8 + 1 + 4, lov2);
					tk1[lov1] = '\0';
					tk2[lov2] = '\0';

					string key1(tk1);
					string key2(tk2);

					if(key1 != key2){
						foundSplit = true;
						scanOffset += (1 + 4 + lov1 + 8);
						break;
					}else{
						scanOffset += (1 + 4 + lov1 + 8);
						continue;
					}

				}
			}

			if(foundSplit){
				splitPosition = scanOffset;
				break;
			}else{
				// looks like all keys are equal...
			}
		}

	}	// loop for decrementing start scan value

	if(!foundSplit){
		return (RC) -1;
	}

	return (RC) 0;
}


RC IndexManager::insertInParent(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *data,
		vector<int> parents, const int parentID){

	// check to know if sent in data is correct or not...


	// calculate if this page has enough space or not.
	vector<FreeSpaceNode> fsVector = ixfileHandle.lowerFileHandler.getFSpace();

	// form the record
	char* mRecord;
	int newRecordLength;

	int key_int;
	float key_real;
	string key_varchar;

	int maxnp = ixfileHandle.lowerFileHandler.getNumberOfPages() + 1;

	if(attribute.type == TypeInt){
		newRecordLength = 4 + 4 + 4;
		mRecord = new char[newRecordLength];
		std::memcpy(mRecord, data, 4 + 4 + 4);


		std::memcpy((char*) &key_int, data + 4, 4);
	}else if(attribute.type == TypeReal){
		newRecordLength = 4 + 4 + 4;
		mRecord = new char[newRecordLength];
		std::memcpy(mRecord, data, 4 + 4 + 4);

		std::memcpy((char*) &key_real, data + 4, 4);
	}else if(attribute.type == TypeVarChar){
		int lov;
		std::memcpy((char*) &lov, data + 4, 4);

		newRecordLength = 4 + 4 + lov + 4;
		mRecord = new char[newRecordLength];
		std::memcpy(mRecord, data, 4 + 4 + lov + 4);
		char tk[lov + 1];
		std::memcpy(tk, data + 4 + 4, lov);
		tk[lov] = '\0';
		key_varchar = string(tk);
	}

	const int spRequired = newRecordLength - 4;			// actual record length minus 4

	if(fsVector[parentID].getSpace() >= spRequired){

		// HAS space
		char* mPage = new char[PAGE_SIZE];
		if(ixfileHandle.lowerFileHandler.readPage(parentID - 1, mPage) != (RC) 0){
			delete[] mPage;
			delete[] mRecord;
			return (RC) -1;
		}

		// find position
		int scanOffset = 1 + 4;
		bool foundPosition = false;

		const int endOfPage = PAGE_SIZE - fsVector[parentID].getSpace();

		while(!foundPosition && (scanOffset < endOfPage)){

			if(attribute.type == TypeInt){
				int key;
				std::memcpy((char*) &key, mPage + scanOffset, 4);

				if(key_int < key){
					// insert here.. scan offset points to the correct location
					foundPosition = true;
					break;
				}else{
					scanOffset += (4 + 4);
					continue;
				}

			}else if(attribute.type == TypeReal){
				float key;
				std::memcpy((char*) &key, mPage + scanOffset, 4);

				if(key_real < key){
					// insert here.. scan offset points to the correct location
					foundPosition = true;
					break;
				}else{
					scanOffset += (4 + 4);
					continue;
				}

			}else if(attribute.type == TypeVarChar){
				int lov;
				std::memcpy((char*) &lov, mPage + scanOffset, 4);
				char tk[lov + 1];
				std::memcpy(tk, mPage + scanOffset + 4, lov);
				tk[lov] = '\0';

				string key(tk);
				if(key_varchar < key){
					// insert here.. scan offset points to the correct location
					foundPosition = true;
					break;
				}else{
					scanOffset += (4 + lov + 4);
					continue;
				}
			}

		}

		// check if inserted
		if(foundPosition){
			// insert in b/w
			const int lengthRP = endOfPage - scanOffset;
			char rightPortion[lengthRP];
			std::memcpy(rightPortion, mPage + scanOffset, lengthRP);

			// insert the sent in record
			scanOffset -= 4;
			std::memcpy(mPage + scanOffset, mRecord, newRecordLength);

			// insert the right portion
			std::memcpy(mPage + scanOffset + newRecordLength, rightPortion, lengthRP);
		}else{
			// insert at the end
			scanOffset -= 4;
			std::memcpy(mPage + scanOffset, mRecord, newRecordLength);
		}

		// set new free-space
		int k = fsVector[parentID].getSpace() - spRequired;
		fsVector[parentID].setSpace(k);
		ixfileHandle.lowerFileHandler.setFSpace(fsVector);

		// write page to disk
		if(ixfileHandle.lowerFileHandler.writePage(parentID - 1, mPage) != (RC) 0){
			delete[] mPage;
			delete[] mRecord;
			return (RC) -1;
		}

		delete[] mPage;



	}else{
		// split this up
		//cout << "PARENT HAS TO BE SPLIT UP!.. parentID is " << parentID << endl;

		const int originalPageLength = PAGE_SIZE - fsVector[parentID].getSpace();
		char* originalPage = new char[PAGE_SIZE];

		if(ixfileHandle.lowerFileHandler.readPage(parentID - 1, originalPage) != (RC) 0){
			delete[] originalPage;
			delete[] mRecord;
			return (RC) -1;
		}

		// insert original page into big page
		char* bigPage = new char[originalPageLength + spRequired];
		std::memcpy(bigPage, originalPage, originalPageLength);

		// insert given slot to this big page
		// find position
		int scanOffset = 1 + 4;
		bool foundPosition = false;

		while(!foundPosition && (scanOffset < originalPageLength)){

			if(attribute.type == TypeInt){
				int key;
				std::memcpy((char*) &key, bigPage + scanOffset, 4);

				if(key_int < key){
					// insert here.. scan offset points to the correct location
					foundPosition = true;
					break;
				}else{
					scanOffset += (4 + 4);
					continue;
				}

			}else if(attribute.type == TypeReal){
				float key;
				std::memcpy((char*) &key, bigPage + scanOffset, 4);

				if(key_real < key){
					// insert here.. scan offset points to the correct location
					foundPosition = true;
					break;
				}else{
					scanOffset += (4 + 4);
					continue;
				}

			}else if(attribute.type == TypeVarChar){
				int lov;
				std::memcpy((char*) &lov, bigPage + scanOffset, 4);
				char tk[lov + 1];
				std::memcpy(tk, bigPage + scanOffset + 4, lov);
				tk[lov] = '\0';

				string key(tk);
				if(key_varchar < key){
					// insert here.. scan offset points to the correct location
					foundPosition = true;
					break;
				}else{
					scanOffset += (4 + lov + 4);
					continue;
				}
			}

		}

		if(foundPosition){
			// insert in b/w
			const int lengthRP = originalPageLength - scanOffset;
			char rightPortion[lengthRP];
			std::memcpy(rightPortion, bigPage + scanOffset, lengthRP);

			// insert the sent in record
			scanOffset -= 4;
			std::memcpy(bigPage + scanOffset, mRecord, newRecordLength);

			// insert the right portion
			std::memcpy(bigPage + scanOffset + newRecordLength, rightPortion, lengthRP);

		}else{
			// insert at the end of big page
			// insert at the end
			scanOffset -= 4;
			std::memcpy(bigPage + scanOffset, mRecord, newRecordLength);
		}

		// compute the split position
		// call getSplitPosition
		int splitAt;
		if(getSplitPosition(attribute, bigPage, originalPageLength + spRequired, splitAt) != (RC) 0){
			delete[] mRecord;
			delete[] bigPage;
			delete[] mRecord;
			return (RC) -1;
		}

		if(splitAt == -1){
			// no need to split..
			delete[] mRecord;
			delete[] bigPage;
			delete[] mRecord;
			return (RC) -1;
		}

		// create the new fresh page.
		int newPagePointer = ixfileHandle.lowerFileHandler.getNumberOfPages() + 1;
		if(1 == 1){
			// scope for new-page
			char* newPage = new char[PAGE_SIZE];
			char indicator = 0;
			std::memcpy(newPage, &indicator, 1);
			int bigPageLength = originalPageLength + spRequired;

			if(attribute.type != TypeVarChar){
				std::memcpy(newPage + 1, bigPage + splitAt + 4, bigPageLength - (splitAt + 4));
				FreeSpaceNode fsn(newPagePointer, PAGE_SIZE - 1 - (bigPageLength - (splitAt + 4)), -1);
				fsVector.push_back(fsn);
			}else{
				int lov;
				std::memcpy((char*) &lov, bigPage + splitAt, 4);
				std::memcpy(newPage + 1, bigPage + splitAt + 4 + lov, bigPageLength - (splitAt + 4 + lov));
				FreeSpaceNode fsn(newPagePointer, PAGE_SIZE - 1 - (bigPageLength - (splitAt + 4 + lov)), -1);
				fsVector.push_back(fsn);
			}

			ixfileHandle.lowerFileHandler.setFSpace(fsVector);
			if(ixfileHandle.lowerFileHandler.appendPage(newPage) != (RC) 0){
				delete[] newPage;
				delete[] bigPage;
				delete[] mRecord;
				return (RC) -1;
			}
			delete[] newPage;
		}

		// write back the original page
		if(parentID == 1){
			// is root.
			// create a new page to fill in the original data
			char* leftNewPage = new char[PAGE_SIZE];
			std::memcpy(leftNewPage, bigPage, splitAt);

			const int leftNewPagePointer = ixfileHandle.lowerFileHandler.getNumberOfPages() + 1;
			FreeSpaceNode fsn(leftNewPagePointer, PAGE_SIZE - splitAt, -1);
			fsVector.push_back(fsn);
			if(ixfileHandle.lowerFileHandler.appendPage(leftNewPage) != (RC) 0){
				delete[] bigPage;
				delete[] mRecord;
				delete[] leftNewPage;
				delete[] originalPage;
				return (RC) -1;
			}

			delete[] leftNewPage;

			// create a new root page.
			// with pointer-key-pointer
			//char theNewRoot[PAGE_SIZE];
			char* theNewRoot = new char[PAGE_SIZE];
			int k = PAGE_SIZE;	// freespace

			char indicator = 0;
			std::memcpy(theNewRoot, &indicator, 1);
			std::memcpy(theNewRoot + 1, (char*) &leftNewPagePointer, 4);
			k -= (1 + 4);

			// fill in the key
			if(attribute.type != TypeVarChar){
				std::memcpy(theNewRoot + 1 + 4, bigPage + splitAt, 4);
				k -= 4;
				std::memcpy(theNewRoot + 1 + 4 + 4, (char*) &newPagePointer, 4);
				k -= 4;
			}else{
				int lov;
				std::memcpy((char*) &lov, bigPage + splitAt, 4);
				std::memcpy(theNewRoot + 1 + 4, bigPage + splitAt, 4 + lov);
				k -= (4 + lov);
				std::memcpy(theNewRoot + 1 + 4 + 4 + lov, (char*) &newPagePointer, 4);
				k -= 4;
			}

			// edit FSPACE for root !!!!!!!!
			fsVector[1].setSpace(k);
			ixfileHandle.lowerFileHandler.setFSpace(fsVector);

			// write this into PAGE 1.
			if(ixfileHandle.lowerFileHandler.writePage(1 - 1, theNewRoot) != (RC) 0){
				delete[] originalPage;
				delete[] bigPage;
				delete[] mRecord;
				delete[] theNewRoot;
				return (RC) -1;
			}else{
				//cout << "new root wrtitten to disk!!!" << endl;
			}

			delete[] theNewRoot;

		}else{
			// was something else.
			// just fill back in the same page
			std::memcpy(originalPage, bigPage, splitAt);
			fsVector[parentID].setSpace(PAGE_SIZE - splitAt);
			ixfileHandle.lowerFileHandler.setFSpace(fsVector);

			char* nodeToSendAbove;
			if(attribute.type != TypeVarChar){
				nodeToSendAbove = new char[4 + 4 + 4];
				std::memcpy(nodeToSendAbove, (char*) &parentID, 4);
				std::memcpy(nodeToSendAbove + 4, bigPage + splitAt, 4);
				std::memcpy(nodeToSendAbove + 4 + 4, (char*) &newPagePointer, 4);
			}else{
				int lov;
				std::memcpy((char*) &lov, bigPage + splitAt, 4);
				nodeToSendAbove = new char[4 + 4 + lov + 4];
				std::memcpy(nodeToSendAbove, (char*) &parentID, 4);
				std::memcpy(nodeToSendAbove + 4, bigPage + splitAt, 4 + lov);
				std::memcpy(nodeToSendAbove + 4 + 4 + lov, (char*) &newPagePointer, 4);
			}

			if(parents.size() < 1){
				delete[] nodeToSendAbove;
				delete[] bigPage;
				delete[] originalPage;
				delete[] mRecord;
				return (RC) -1;
			}

			const int immparent = parents.back();
			parents.pop_back();
			if(insertInParent(ixfileHandle, attribute, nodeToSendAbove, parents, immparent) != (RC) 0){
				delete[] nodeToSendAbove;
				delete[] bigPage;
				delete[] originalPage;
				delete[] mRecord;
				return (RC) -1;
			}

			delete[] nodeToSendAbove;
		}

		delete[] bigPage;
		delete[] originalPage;

	}

	delete[] mRecord;
	return (RC) 0;
}



// update arguments being sent in this method
RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute,
		const void *key, const RID &rid)
{

	vector<int> parents; // page_numbers (internal), of all parents
	int final_leaf_pointer;

	if(ixfileHandle.lowerFileHandler.getNumberOfPages() == 0){
		if(initializeTree(ixfileHandle, attribute, key, rid) != (RC) 0){
			return (RC) -1;
		}
		parents.push_back(1);
		final_leaf_pointer = 3;
	}else{
		if(getPathToLeaf(ixfileHandle, attribute, key, parents, final_leaf_pointer) != (RC) 0){
			return (RC) -1;
		}
	}

	vector<FreeSpaceNode> fsVector = ixfileHandle.lowerFileHandler.getFSpace();

	// Leaf page format: page-indicator, next-pointer, record(null-indicator, key, RID)

	int original_pointer = final_leaf_pointer;
	char original_leaf[PAGE_SIZE];

	if(ixfileHandle.lowerFileHandler.readPage(original_pointer - 1, original_leaf) != (RC) 0){
		return (RC) -1;
	}

	FreeSpaceNode fsn_original = fsVector[original_pointer];

	// space required: 1 + 4/(4+lov) + 8
	int space_required = 1 + 4;

	int key_int;
	float key_float;
	string key_varchar;

	if(attribute.type == TypeInt){
		std::memcpy((char*) &key_int, key, 4);
	}else if(attribute.type == TypeReal){
		std::memcpy((char*) &key_float, key, 4);
	}else{
		int lov;
		std::memcpy((char*) &lov, key, 4);
		char tempp[lov+1];
		std::memcpy(tempp, key + 4, lov);
		tempp[lov] = '\0';
		string mcbc(tempp);
		key_varchar = mcbc;
		space_required += lov;	 // no need to add space for length of varchar. stored above
	}

	space_required += 8;	// for rid.

	// create the record
	char* mRecord = new char[space_required];
	char indicator = 1;
	std::memcpy(mRecord, &indicator, 1);

	if(attribute.type != TypeVarChar){
		std::memcpy(mRecord + 1, key, 4);
		std::memcpy(mRecord + 1 + 4, (char*) &rid, 8);
	}else{
		int lov;
		std::memcpy((char*) &lov, key, 4);

		std::memcpy(mRecord + 1, (char*) &lov, 4);
		std::memcpy(mRecord + 1 + 4, key + 4, lov);
		std::memcpy(mRecord + 1 + 4 + lov, (char*) &rid, 8);
	}


	if(fsn_original.getSpace() >= space_required){
		// Same page insert. // maintain an ordered format (even for deleted)

		int scanOffset = 1 + 4;
		while(scanOffset < (PAGE_SIZE - fsn_original.getSpace())){


			if(attribute.type == TypeInt){
				int temp;
				std::memcpy((char*) &temp, original_leaf + scanOffset + 1, 4);
				if(key_int < temp){
					// insert here
					break;
				}else{
					scanOffset += (1 + 4 + 8);
				}
			}else if(attribute.type == TypeReal){
				float temp;
				std::memcpy((char*) &temp, original_leaf + scanOffset + 1, 4);
				if(key_float < temp){
					// insert here
					break;
				}else{
					scanOffset += (1 + 4 + 8);
				}
			}else{
				int lov;
				std::memcpy((char*) &lov, original_leaf + scanOffset + 1, 4);
				char temp0[lov + 1];
				std::memcpy(temp0, original_leaf + scanOffset + 1 + 4, lov);
				temp0[lov] = '\0';
				string temp(temp0);

				if(key_varchar < temp){
					// insert here
					break;
				}else{
					scanOffset += (1 + 4 + lov + 8);
				}
			}


		}

		// position to insert is indicated by scanOffset

		if(scanOffset == (PAGE_SIZE - fsn_original.getSpace())){
			// inserting after the last
			std::memcpy(original_leaf + scanOffset, mRecord, space_required);
		}else if(scanOffset < (PAGE_SIZE - fsn_original.getSpace())){
			// inserting in between
			int length_rightPortion = (PAGE_SIZE - fsn_original.getSpace()) - scanOffset;
			char rightPortion[length_rightPortion];
			std::memcpy(rightPortion, original_leaf + scanOffset, length_rightPortion);

			std::memcpy(original_leaf + scanOffset, mRecord, space_required);
			std::memcpy(original_leaf + scanOffset + space_required, rightPortion, length_rightPortion);
		}else{
			// someone fucked up
			//cout << "someone fucked up somewhere" << endl;
			delete[] mRecord;
			return (RC) -1;
		}

		fsn_original.setSpace(fsn_original.getSpace() - space_required);
		fsVector[original_pointer] = fsn_original;
		ixfileHandle.lowerFileHandler.setFSpace(fsVector);

		// write the page back to disk
		if(ixfileHandle.lowerFileHandler.writePage(original_pointer - 1, original_leaf) != (RC) 0){
			delete[] mRecord;
			return (RC) -1;
		}



	}else{
		// SPLIT !!!
		//cout << "Splitting leaf node" << endl;

		// create a temporary page in m/y to store the correctly merged BIG page.
		int originalPageLength = PAGE_SIZE - fsn_original.getSpace();
		//char tempMergedPage[originalPageLength + space_required];	// exact length
		char* tempMergedPage = new char[originalPageLength + space_required];

		// fill this page. (original page)
		std::memcpy(tempMergedPage, original_leaf, originalPageLength);

		// insert the new record in the correct position, in this page.
		int scanOffset = 1 + 4;
		bool foundPosition = false;

		while(!foundPosition && (scanOffset < originalPageLength)){
			// key should be greater than this recrd's key

			if(attribute.type == TypeInt){

				int key;
				std::memcpy((char*) &key, tempMergedPage + scanOffset + 1, 4);

				if(key_int < key){
					// insert here
					foundPosition = true;
					break;
				}else{
					scanOffset += (1 + 4 + 8);
					continue;
				}

			}else if(attribute.type == TypeReal){

				float key;
				std::memcpy((char*) &key, tempMergedPage + scanOffset + 1, 4);

				if(key_float < key){
					// insert here
					foundPosition = true;
					break;
				}else{
					scanOffset += (1 + 4 + 8);
					continue;
				}

			}else if(attribute.type == TypeVarChar){

				int lov;
				std::memcpy((char*) &lov, tempMergedPage + scanOffset + 1, 4);
				char tk[lov + 1];
				std::memcpy(tk, tempMergedPage + scanOffset + 1 + 4, lov);
				tk[lov] = '\0';

				string key(tk);

				if(key_varchar < key){
					// insert here
					foundPosition = true;
					break;
				}else{
					scanOffset += (1 + 4 + lov + 8);
					continue;
				}

			}

		}

		if(!foundPosition){
			// insert at the end
			std::memcpy(tempMergedPage + scanOffset, mRecord, space_required);
		}else{
			// insert in b/w
			//int lengthRP = originalPageLength + space_required - scanOffset;
			int lengthRP = originalPageLength - scanOffset;
			char rightPortion[lengthRP];
			std::memcpy(rightPortion, tempMergedPage + scanOffset, lengthRP);
			std::memcpy(tempMergedPage + scanOffset, mRecord, space_required);
			std::memcpy(tempMergedPage + scanOffset + space_required, rightPortion, lengthRP);
		}

		// call getSplitPosition
		int splitAt;
		if(getSplitPosition(attribute, tempMergedPage, originalPageLength + space_required, splitAt) != (RC) 0){
			delete[] mRecord;
			delete[] tempMergedPage;
			return (RC) -1;
		}

		if(splitAt == -1){
			// no need to split..
			delete[] mRecord;
			delete[] tempMergedPage;
			return (RC) -1;
		}

		// fill the old-page back
		std::memcpy(original_leaf, tempMergedPage, splitAt);

		int original_next_page_pointer;	// to be inserted in the fresh page
		std::memcpy((char*) &original_next_page_pointer, original_leaf + 1, 4);

		const int new_page_pointer = ixfileHandle.lowerFileHandler.getNumberOfPages() + 1;	// internal format
		std::memcpy(original_leaf + 1, (char*) &new_page_pointer, 4);
		fsn_original.setSpace(PAGE_SIZE - splitAt);
		fsVector[original_pointer] = fsn_original;
		if(ixfileHandle.lowerFileHandler.writePage(original_pointer - 1, original_leaf) != (RC) 0){
			delete[] original_leaf;
			delete[] mRecord;
			return (RC) -1;
		}

		// fresh page
		char freshPage[PAGE_SIZE];
		char indicator = 1;
		std::memcpy(freshPage, &indicator, 1);
		std::memcpy(freshPage + 1, (char*) &original_next_page_pointer, 4);
		std::memcpy(freshPage + 1 + 4, tempMergedPage + splitAt, originalPageLength + space_required - splitAt);
		int k = PAGE_SIZE - 1 - 4 - (originalPageLength + space_required - splitAt);
		FreeSpaceNode newFSN(new_page_pointer, k , -1);
		fsVector.push_back(newFSN);

		if(ixfileHandle.lowerFileHandler.appendPage(freshPage) != (RC) 0){
			delete[] tempMergedPage;
			delete[] original_leaf;
			delete[] mRecord;
			return (RC) -1;
		}


		ixfileHandle.lowerFileHandler.setFSpace(fsVector);

		// form the node to be inserted
		char* nodeToSendAbove;
		if(attribute.type == TypeInt){
			nodeToSendAbove = new char[4 + 4 + 4];
			std::memcpy(nodeToSendAbove, (char*) &original_pointer, 4);
			std::memcpy(nodeToSendAbove + 4, freshPage + 1 + 4 + 1, 4);
			std::memcpy(nodeToSendAbove + 4 + 4, (char*) &new_page_pointer, 4);
		}else if(attribute.type == TypeReal){
			nodeToSendAbove = new char[4 + 4 + 4];
			std::memcpy(nodeToSendAbove, (char*) &original_pointer, 4);
			std::memcpy(nodeToSendAbove + 4, freshPage + 1 + 4 + 1, 4);
			std::memcpy(nodeToSendAbove + 4 + 4, (char*) &new_page_pointer, 4);
		}else{
			// send varchar element
			int lov;
			std::memcpy((char*) &lov, freshPage + 1 + 4 + 1, 4);
			nodeToSendAbove = new char[4 + (4 + lov) + 4];
			std::memcpy(nodeToSendAbove, (char*) &original_pointer, 4);
			std::memcpy(nodeToSendAbove + 4, freshPage + 1 + 4 + 1, 4 + lov);
			std::memcpy(nodeToSendAbove + 4 + 4 + lov, (char*) &new_page_pointer, 4);
		}

		//  send a node to be inserted above

		if(parents.size() < 1){
			//cout << "No immediate parent!!!" << endl;
			delete[] tempMergedPage;
			delete[] mRecord;
			delete[] nodeToSendAbove;
			return (RC) -1;
		}

		const int immParent = parents.back();
		parents.pop_back();
		if(insertInParent(ixfileHandle, attribute, nodeToSendAbove, parents, immParent) != (RC) 0){
			delete[] mRecord;
			delete[] tempMergedPage;
			delete[] nodeToSendAbove;
			return (RC) -1;
		}

		delete[] nodeToSendAbove;
		delete[] tempMergedPage;
	}


	delete[] mRecord;
    return (RC) 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{

	// get path to leaf page
	vector<int> parents;
	int finalLeafPointer;

	int key_int;
	float key_real;
	string key_varchar;
	if(attribute.type == TypeInt){
		std::memcpy((char*) &key_int, key, 4);
	}else if(attribute.type == TypeReal){
		std::memcpy((char*) &key_real, key, 4);
	}else{
		int lov;
		std::memcpy((char*) &lov, key, 4);
		char tk[lov + 1];
		std::memcpy(tk, key + 4, lov);
		tk[lov] = '\0';
		key_varchar = string(tk);
	}


	if(getPathToLeaf(ixfileHandle, attribute, key, parents, finalLeafPointer) != (RC) 0){
		return (RC) -1;
	}

	char currentPage[PAGE_SIZE];
	if(ixfileHandle.lowerFileHandler.readPage(finalLeafPointer - 1, currentPage) != (RC) 0){
		return (RC) -1;
	}

	int scanOffset = 1 + 4;
	bool recordFound = false;

	FreeSpaceNode cfsn = ixfileHandle.lowerFileHandler.getFSpace()[finalLeafPointer];

	while((!recordFound) && (scanOffset < (PAGE_SIZE - cfsn.getSpace()))){

		char indicator;
		std::memcpy(&indicator, currentPage + scanOffset, 1);

		if(attribute.type == TypeInt){
			if(indicator == (char) 0){
				scanOffset += (1 + 4 + 8);
				continue;
			}

			int key;
			std::memcpy((char*) &key, currentPage + scanOffset + 1, 4);
			RID mrid;
			std::memcpy((char*) &mrid, currentPage + scanOffset + 1 + 4, 8);

			if ((key == key_int) && (rid.pageNum == mrid.pageNum) && (rid.slotNum == mrid.slotNum)){
				// found it!
				recordFound = true;
				break;
				// do deletion later.... (outside loop)
			}else{
				scanOffset += (1 + 4 + 8);
				continue;
			}


		}else if(attribute.type == TypeReal){

			if(indicator == (char) 0){
				scanOffset += (1 + 4 + 8);
				continue;
			}

			float key;
			std::memcpy((char*) &key, currentPage + scanOffset + 1, 4);
			RID mrid;
			std::memcpy((char*) &mrid, currentPage + scanOffset + 1 + 4, 8);

			if ((key == key_real) && (rid.pageNum == mrid.pageNum) && (rid.slotNum == mrid.slotNum)){
				// found it!
				recordFound = true;
				break;
				// do deletion later.... (outside loop)
			}else{
				scanOffset += (1 + 4 + 8);
				continue;
			}


		}else if(attribute.type == TypeVarChar){

			string key;
			int lov;
			std::memcpy((char*) &lov, currentPage + scanOffset + 1, 4);
			char tk[lov + 1];
			std::memcpy(tk, currentPage + scanOffset + 1 + 4, lov);
			tk[lov] = '\0';
			key = string(tk);

			RID mrid;
			std::memcpy((char*) &mrid, currentPage + scanOffset + 1 + 4 + lov, 8);

			if(indicator == (char) 0){
				scanOffset += (1 + 4 + lov + 8);
				continue;
			}

			if ((key == key_varchar) && (rid.pageNum == mrid.pageNum) && (rid.slotNum == mrid.slotNum)){
				// found it!
				recordFound = true;
				break;
				// do deletion later.... (outside loop)
			}else{
				scanOffset += (1 + 4 + lov + 8);
				continue;
			}

		}

	}

	// check if recordfound or not.
	if(recordFound){
		// delete it at the scanoffset
		char del_indicator = 0;
		std::memcpy(currentPage + scanOffset, &del_indicator, 1);

		// FREE SPACE SHOULD NOT BE UPDATED!!!!!!! BECAUSE THE RECORD IS STILL THERE
		return (RC) ixfileHandle.lowerFileHandler.writePage(finalLeafPointer - 1,currentPage);

	}else{
		// either no record
		// or deleted before
		return (RC) -1;
	}
}


void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const{
	if(ixfileHandle.lowerFileHandler.getNumberOfPages() > 0){
		const int root = 1;
		const int tabs = 0;
		cout << endl;
		recursivePrint(ixfileHandle, attribute, root, tabs, false);
	}
}

void IndexManager::recursivePrint(IXFileHandle &ixfileHandle, const Attribute &attribute,
		const int root, const int tabs, const bool printComma) const{

	// check if non-leaf or leaf node

	char mPage[PAGE_SIZE];
	if(ixfileHandle.lowerFileHandler.readPage(root - 1, mPage) != (RC) 0){
		//delete[] mPage;
		return;
	}

	FreeSpaceNode fsn = (ixfileHandle.lowerFileHandler.getFSpace())[root];

	char indicator;
	std::memcpy((char*)&indicator, mPage, 1);

	if(indicator == (char) 0){
		// non-leaf
		int scanOffset = 1;
		vector<string> keys_varchar;
		vector<int> keys_int;
		vector<float> keys_real;

		vector<int> child_pointers;

		// pointer and key traversal
		while(scanOffset < (PAGE_SIZE - fsn.getSpace())){

			int lp;
			std::memcpy((char*) &lp, mPage + scanOffset, 4);
			child_pointers.push_back(lp);
			scanOffset += 4;

			if(scanOffset >= (PAGE_SIZE - fsn.getSpace())){
				break;	// last pointer case
			}

			if(attribute.type == TypeInt){
				int key;
				std::memcpy((char*) &key, mPage + scanOffset, 4);
				keys_int.push_back(key);
				scanOffset += 4;
			}else if(attribute.type == TypeReal){
				float key;
				std::memcpy((char*) &key, mPage + scanOffset, 4);
				keys_real.push_back(key);
				scanOffset += 4;
			}else if(attribute.type == TypeVarChar){
				int lov;
				std::memcpy((char*) &lov, mPage + scanOffset, 4);
				char tk[lov + 1];
				std::memcpy(tk, mPage + scanOffset + 4, lov);
				tk[lov] = '\0';
				string key(tk);
				keys_varchar.push_back(key);
				scanOffset += (4 + lov);
			}

		}

		// BEGIN PRINTING..
		for(int i = 0; i < tabs; i++){
			//cout << "\t";
			cout << "    ";
		}
		cout << "{";	// no need to print endl. Wrong description online.
		cout << " \"keys\": [";
		// for each key, print that
		if(attribute.type == TypeInt){
			for(int i = 0; i < keys_int.size(); i++){
				cout << " \"" << keys_int[i] << "\"";
				if(i < (keys_int.size() - 1)){
					cout << ",";
				}
			}
			cout << "]," << endl;
		}else if(attribute.type == TypeReal){
			for(int i = 0; i < keys_real.size(); i++){
				cout << " \"" << keys_real[i] << "\"";
				if(i < (keys_real.size() - 1)){
					cout << ",";
				}
			}
			cout << "]," << endl;
		}else{
			for(int i = 0; i < keys_varchar.size(); i++){
				cout << " \"" << keys_varchar[i] << "\"";
				if(i < (keys_varchar.size() - 1)){
					cout << ",";
				}
			}
			cout << "]," << endl;
		}

		for(int i = 0; i < tabs; i++){
			//cout << "\t";
			cout << "    ";
		}
		cout << " \"children\": [" << endl;

		// for all children, call recursively
		for(int i = 0; i < child_pointers.size(); i++){
			if (i < (child_pointers.size() - 1)){
				recursivePrint(ixfileHandle, attribute, child_pointers[i], tabs + 1, true);
			}else{
				recursivePrint(ixfileHandle, attribute, child_pointers[i], tabs + 1, false);
			}
		}

		for(int i = 0; i < tabs; i++){
			cout << "    ";
		}
		cout << "]}";
		if (printComma){
			cout << ",";
		}
		cout << endl;

	}else{
		// leaf
		int scanOffset = 1 + 4;
		cout << "{\"keys\": [";

		while(scanOffset < (PAGE_SIZE - fsn.getSpace())){

			if(attribute.type == TypeInt){
				int key;
				std::memcpy((char*) &key, mPage + scanOffset + 1, 4);
				vector<RID> validRIDs;
				while(true){

					if(scanOffset >= (PAGE_SIZE - fsn.getSpace())){
						break;
					}

					int this_key;
					std::memcpy((char*) &this_key, mPage + scanOffset + 1, 4);
					if(this_key != key){
						// this is a new key
						break;
					}
					char indicator;
					std::memcpy(&indicator, mPage + scanOffset, 1);
					if(indicator == (char) 0){
						scanOffset += (1 + 4 + 8);
						break;
					}
					RID mrid;
					std::memcpy((char*) &mrid, mPage + scanOffset + 1 + 4, 8);
					validRIDs.push_back(mrid);
					scanOffset += (1 + 4 + 8);
				}

				if(validRIDs.size() > 0){
					// print 'this' key
					cout << " \"" << key << " : [";
					for(int i = 0; i < validRIDs.size(); i++){
						cout << "(" << validRIDs[i].pageNum << "," << validRIDs[i].slotNum << ")";
						if(i < (validRIDs.size() - 1))
							cout << ", ";
					}
					cout << "]\"";

					if(scanOffset < (PAGE_SIZE - fsn.getSpace())){
						cout << ", ";
					}
				}


			}else if(attribute.type == TypeReal){
				float key;
				std::memcpy((char*) &key, mPage + scanOffset + 1, 4);
				vector<RID> validRIDs;
				while(true){

					if(scanOffset >= (PAGE_SIZE - fsn.getSpace())){
						break;
					}

					float this_key;
					std::memcpy((char*) &this_key, mPage + scanOffset + 1, 4);
					if(this_key != key){
						// this is a new key
						break;
					}
					char indicator;
					std::memcpy(&indicator, mPage + scanOffset, 1);
					if(indicator == (char) 0){
						scanOffset += (1 + 4 + 8);
						break;
					}
					RID mrid;
					std::memcpy((char*) &mrid, mPage + scanOffset + 1 + 4, 8);
					validRIDs.push_back(mrid);
					scanOffset += (1 + 4 + 8);
				}

				if(validRIDs.size() > 0){
					// print 'this' key
					cout << " \"" << key << " : [";
					for(int i = 0; i < validRIDs.size(); i++){
						cout << "(" << validRIDs[i].pageNum << "," << validRIDs[i].slotNum << ")";
						if(i < (validRIDs.size() - 1))
							cout << ", ";
					}
					cout << "]\"";

					if(scanOffset < (PAGE_SIZE - fsn.getSpace())){
						cout << ", ";
					}
				}



			}else if(attribute.type == TypeVarChar){
				int lov0;
				std::memcpy((char*) &lov0, mPage + scanOffset + 1, 4);
				char key[lov0 + 1];
				std::memcpy(key, mPage + scanOffset + 1 + 4, lov0);
				key[lov0] = '\0';
				vector<RID> validRIDs;
				while(true){

					if(scanOffset >= (PAGE_SIZE - fsn.getSpace())){
						break;
					}

					int lov1;
					std::memcpy((char*) &lov1, mPage + scanOffset + 1, 4);
					char this_key[lov1 + 1];
					std::memcpy(this_key, mPage + scanOffset + 1 + 4, lov1);
					this_key[lov1] = '\0';
					if(strcmp(key, this_key) != 0){
						// this is a new_key
						break;
					}
					char indicator;
					std::memcpy(&indicator, mPage + scanOffset, 1);
					if(indicator == (char) 0){
						scanOffset += (1 + 4 + lov1 + 8);
						break;
					}
					RID mrid;
					std::memcpy((char*) &mrid, mPage + scanOffset + 1 + 4 + lov1, 8);
					validRIDs.push_back(mrid);
					scanOffset += (1 + 4 + lov1 + 8);
				}

				if(validRIDs.size() > 0){
					// print 'this' key
					cout << " \"" << key << " : [";
					for(int i = 0; i < validRIDs.size(); i++){
						cout << "(" << validRIDs[i].pageNum << "," << validRIDs[i].slotNum << ")";
						if(i < (validRIDs.size() - 1))
							cout << ", ";
					}
					cout << "]\"";
					if(scanOffset < (PAGE_SIZE - fsn.getSpace())){
						cout << ", ";
					}
				}


			}
		}

		cout << "]}";
		if(printComma){
			cout << ",";
		}
		cout << endl;
	}
}



RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{



	if(ixfileHandle.lowerFileHandler.getNumberOfPages() == 0){
		//cout << "no root" << endl;
		return (RC) -1;
	}

	// fields to set in the ix_scanIterator
	vector<int> intkeys;
	vector<float> realkeys;
	vector<string> varcharkeys;
	vector<RID> finalRIDs;

	vector<int> parents;
	int start_leaf_pointer;

	int low_int; int high_int;
	float low_real; float high_real;
	string low_varchar; string high_varchar;

	if(attribute.type == TypeInt){

		if(lowKey){
			// some value
			std::memcpy((char*) &low_int, lowKey, 4);
			getPathToLeaf(ixfileHandle, attribute, lowKey, parents, start_leaf_pointer);
		}else{
			// -infinity
			getLowestKey(ixfileHandle, attribute, start_leaf_pointer);
		}

		if(highKey){
			std::memcpy((char*) &high_int, highKey, 4);
		}else{
			// + infinity
		}


	}else if(attribute.type == TypeReal){

		if(lowKey){
			std::memcpy((char*) &low_real, lowKey, 4);
			//cout << "Scanning for: " << low_real << ", inside index b-tree (rhs)" << endl;
			getPathToLeaf(ixfileHandle, attribute, lowKey, parents, start_leaf_pointer);
		}else{
			getLowestKey(ixfileHandle, attribute, start_leaf_pointer);
		}

		if(highKey){
			std::memcpy((char*) &high_real, highKey, 4);
			//cout << "High key == " << high_real << endl;
		}

	}else{
		int lov;

		if(lowKey){
			std::memcpy((char*) &lov, lowKey, 4);
			char temp1[lov + 1];
			std::memcpy(temp1, lowKey + 4, lov);
			temp1[lov] = '\0';
			low_varchar = string(temp1);
			getPathToLeaf(ixfileHandle, attribute, lowKey, parents, start_leaf_pointer);
		}else{
			getLowestKey(ixfileHandle, attribute, start_leaf_pointer);
		}

		if(highKey){
			std::memcpy((char*) &lov, highKey, 4);
			char temp2[lov + 1];
			std::memcpy(temp2, highKey + 4, lov);
			temp2[lov] = '\0';
			high_varchar = string(temp2);
		}

	}


	char currentPage[PAGE_SIZE];
	if(ixfileHandle.lowerFileHandler.readPage(start_leaf_pointer - 1, currentPage) != (RC) 0){
		//cout << "couldnt read page (start)" << endl;
		return (RC) -1;
	}
	FreeSpaceNode currentFSN = ixfileHandle.lowerFileHandler.getFSpace()[start_leaf_pointer];



	// find the start position within this page.
	int scanOffset = 1 + 4;
	bool recordFound = false;

	if(!lowKey){
		recordFound = true;
	}

	int endOfPage = PAGE_SIZE - currentFSN.getSpace();
	while(!recordFound && (scanOffset < endOfPage)){

			char indicator;
			std::memcpy(&indicator, currentPage + scanOffset, 1);

			if(attribute.type == TypeInt){

				if(indicator == (char) 0){
					scanOffset += (1 + 4 + 8);
					continue;
				}

				int key;
				std::memcpy((char*) &key, currentPage + scanOffset + 1, 4);

				if(lowKeyInclusive){
					if((key == low_int) || (low_int < key)){
						recordFound = true;
						break;
					}else{
						scanOffset += (1 + 4 + 8);
						continue;
					}
				}else{
					if(low_int < key){
						recordFound = true;
						break;
					}else{
						scanOffset += (1 + 4 + 8);
						continue;
					}
				}

			}else if(attribute.type == TypeReal){

				if(indicator == (char) 0){
					scanOffset += (1 + 4 + 8);
					continue;
				}

				float key;
				std::memcpy((char*) &key, currentPage + scanOffset + 1, 4);


				if(lowKeyInclusive){
					if(key >= low_real){
						recordFound = true;
						break;
					}else{
						scanOffset += (1 + 4 + 8);
						continue;
					}
				}else{
					if(low_real < key){
						recordFound = true;
						break;
					}else{
						scanOffset += (1 + 4 + 8);
						continue;
					}
				}

			}else if(attribute.type == TypeVarChar){

				int lov;
				std::memcpy((char*) &lov, currentPage + scanOffset + 1, 4);
				char tk[lov + 1];
				std::memcpy(tk, currentPage + scanOffset + 1 + 4, lov);
				tk[lov] = '\0';
				string key(tk);

				if(indicator == (char) 0){
					scanOffset += (1 + 4 + lov + 8);
					continue;
				}

				if(lowKeyInclusive){
					if((key == low_varchar) || (low_varchar < key)){
						recordFound = true;
						break;
					}else{
						scanOffset += (1 + 4 + lov + 8);
						continue;
					}
				}else{
					if(low_varchar < key){
						recordFound = true;
						break;
					}else{
						scanOffset += (1 + 4 + lov + 8);
						continue;
					}
				}

			}

	}


	int currentPagePointer;

	if(!recordFound){
		// if i didn't find the starting offset on this page,
		// my starting page is the next page to this page....
		scanOffset = 1 + 4;
		std::memcpy((char*) &currentPagePointer, currentPage + 1, 4);
	}else{
		currentPagePointer = start_leaf_pointer;
	}

	// get all other records
	bool searchPages = true;
	while(searchPages){

		if(currentPagePointer == -1){
			searchPages = false;
			break;
		}

		currentFSN = ixfileHandle.lowerFileHandler.getFSpace()[currentPagePointer];
		if(ixfileHandle.lowerFileHandler.readPage(currentPagePointer - 1, currentPage) != (RC) 0){
			//cout << "couldnt read page" << endl;
			return (RC) -1;
		}

		endOfPage = PAGE_SIZE - currentFSN.getSpace();

		// search within pages // scanOffset is already set
		while(scanOffset < endOfPage){
			char indicator;
			std::memcpy(&indicator, currentPage + scanOffset, 1);

			if(attribute.type == TypeInt){

				if(indicator == (char) 0){
					scanOffset += (1 + 4 + 8);
					continue;
				}

				int key;
				std::memcpy((char*) &key, currentPage + scanOffset + 1, 4);

				if(highKey){
					// you have to consider high key

					if(highKeyInclusive){
						if(key <= high_int){
							// add the record, increment scanOffset
							intkeys.push_back(key);
							RID mrid;
							std::memcpy((char*) &mrid, currentPage + scanOffset + 1 + 4, 8);
							finalRIDs.push_back(mrid);
							scanOffset += (1 + 4 + 8);
						}else{
							// DONE. no more to add
							break;
						}
					}else{
						if(key < high_int){
							// add the record
							intkeys.push_back(key);
							RID mrid;
							std::memcpy((char*) &mrid, currentPage + scanOffset + 1 + 4, 8);
							finalRIDs.push_back(mrid);
							scanOffset += (1 + 4 + 8);
						}else{
							// DONE. no more to add
							//cout << "highkey open. found a record bigger than given high key.. stopping." << endl;
							break;
						}
					}

				}else{
					// +infinity
					// add the record
					intkeys.push_back(key);
					RID mrid;
					std::memcpy((char*) &mrid, currentPage + scanOffset + 1 + 4, 8);
					finalRIDs.push_back(mrid);
					scanOffset += (1 + 4 + 8);
				}

			}else if(attribute.type == TypeReal){

				if(indicator == (char) 0){
					scanOffset += (1 + 4 + 8);
					continue;
				}

				float key;
				std::memcpy((char*) &key, currentPage + scanOffset + 1, 4);

				//cout << "scanning till upper limit.. this key = " << key << endl;

				if(highKey){
					// you have to consider high key

					if(highKeyInclusive){
						if(key <= high_real){
							// add the record, increment scanOffset
							realkeys.push_back(key);
							RID mrid;
							std::memcpy((char*) &mrid, currentPage + scanOffset + 1 + 4, 8);
							finalRIDs.push_back(mrid);
							scanOffset += (1 + 4 + 8);
						}else{
							// DONE. no more to add
							break;
						}
					}else{
						if(key < high_real){
							// add the record
							realkeys.push_back(key);
							RID mrid;
							std::memcpy((char*) &mrid, currentPage + scanOffset + 1 + 4, 8);
							finalRIDs.push_back(mrid);
							scanOffset += (1 + 4 + 8);
						}else{
							// DONE. no more to add
							break;
						}
					}

				}else{
					// +infinity
					// add the record
					realkeys.push_back(key);
					RID mrid;
					std::memcpy((char*) &mrid, currentPage + scanOffset + 1 + 4, 8);
					finalRIDs.push_back(mrid);
					scanOffset += (1 + 4 + 8);
				}

			}else if(attribute.type == TypeVarChar){

				int lov;
				std::memcpy((char*) &lov, currentPage + scanOffset + 1, 4);
				char tk[lov + 1];
				std::memcpy(tk, currentPage + scanOffset + 1 + 4, lov);
				tk[lov] = '\0';

				string key(tk);

				if(indicator == (char) 0){
					scanOffset += (1 + 4 + lov + 8);
					continue;
				}

				if(highKey){
					// you have to consider high key

					if(highKeyInclusive){
						if(key <= high_varchar){
							// add the record, increment scanOffset
							varcharkeys.push_back(key);
							RID mrid;
							std::memcpy((char*) &mrid, currentPage + scanOffset + 1 + 4 + lov, 8);
							finalRIDs.push_back(mrid);
							scanOffset += (1 + 4 + lov + 8);
						}else{
							// DONE. no more to add
							break;
						}
					}else{
						if(key < high_varchar){
							// add the record
							varcharkeys.push_back(key);
							RID mrid;
							std::memcpy((char*) &mrid, currentPage + scanOffset + 1 + 4 + lov, 8);
							finalRIDs.push_back(mrid);
							scanOffset += (1 + 4 + lov + 8);
						}else{
							// DONE. no more to add
							break;
						}
					}

				}else{
					// +infinity
					// add the record
					varcharkeys.push_back(key);
					RID mrid;
					std::memcpy((char*) &mrid, currentPage + scanOffset + 1 + 4 + lov, 8);
					finalRIDs.push_back(mrid);
					scanOffset += (1 + 4 + lov + 8);
				}

			}

		}

		// evaluate whether you have to go beyond this page or not
		if(scanOffset < endOfPage){
			// broke in b/w.
			// if highkey is +infinity, we need to go further, even if the page becomes empty
			if(highKey){
				searchPages = false;
				break;
			}else{
				// + infinity
			}

		}

		int nextPagePointer;
		std::memcpy((char*) &nextPagePointer, currentPage + 1, 4);

		if(nextPagePointer == -1){
			// this is the last leaf page
			//cout << "INSIDE SCAN. This is the last page." << endl;
			searchPages = false;
			break;
		}

		scanOffset = 1 + 4;
		currentPagePointer = nextPagePointer;
	}

	// set the scan iterator object

	if(ix_ScanIterator.initialize(&ixfileHandle, attribute.type) != (RC) 0){
		return (RC) -1;
	}

	if(attribute.type == TypeInt){
		ix_ScanIterator.key_int = intkeys;
		//cout << "Number of valid records int scanned:" << intkeys.size() << endl;
	}else if(attribute.type == TypeReal){
		ix_ScanIterator.key_real = realkeys;
		//cout << "Number of valid records real scanned:" << realkeys.size() << " in table: " << ixfileHandle.lowerFileHandler.fileName << endl;
	}else{
		ix_ScanIterator.key_varchar = varcharkeys;
		//cout << "Number of valid records string scanned:" << varcharkeys.size() << endl;
	}
	ix_ScanIterator.key_rids = finalRIDs;



    return (RC) 0;
}


RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{


	(this->currentIndex) += 1;
	Attribute mTemp;
	mTemp.type = this->keyType; mTemp.name = ""; mTemp.length = 4;



	if(this->keyType == TypeInt){
		if (currentIndex >= key_int.size()){
			return IX_EOF;
		}

		rid = key_rids[currentIndex];
		std::memcpy(key, (char*) &key_int[currentIndex], 4);

	}else if(this->keyType == TypeReal){

		if (currentIndex >= key_real.size()){
			return IX_EOF;
		}

		rid = key_rids[currentIndex];
		std::memcpy(key, (char*) &key_real[currentIndex], 4);

	}else{

		if (currentIndex >= key_varchar.size()){
			return IX_EOF;
		}

		rid = key_rids[currentIndex];
		int lov = (key_varchar[currentIndex]).length();
		std::memcpy(key, (char*) &lov, 4);
		std::memcpy(key + 4, key_varchar[currentIndex].c_str(), lov);

	}

	return (RC) 0;
}




RC IX_ScanIterator::close()
{
	currentIndex = -1;
	key_int.clear();
	key_real.clear();
	key_varchar.clear();

    return (RC) 0;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	if(lowerFileHandler.collectCounterValues(
			ixReadPageCounter, ixWritePageCounter, ixAppendPageCounter) != (RC) 0){
		return (RC) -1;
	}

	readPageCount = ixReadPageCounter;
	writePageCount = ixWritePageCounter;
	appendPageCount = ixAppendPageCounter;

	return (RC) 0;

}

