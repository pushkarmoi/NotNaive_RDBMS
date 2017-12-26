#include "pfm.h"
#include <fstream>
#include <vector>
#include <iostream>
#include <string.h>
#include <errno.h>
using namespace std;

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance(){
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}

RC PagedFileManager::fileExists(const string &fileName){
	// 0 for doesn't exist, 1 for does exist
	if(fileName.empty())
		return 0;

	ifstream mFile;
	mFile.open(fileName.c_str());

	if(mFile.is_open()){
		// exists already
	    //cout << "Inside pfm->fileexists. Error:" << strerror(errno) << endl;
		return 1;
	} else{
		// doesn't exist OR there was some error
		return 0;
	}
}


PagedFileManager::PagedFileManager(){
}


PagedFileManager::~PagedFileManager(){
}


RC PagedFileManager::createFile(const string &fileName){
	if(fileName.empty()){
		return -1;
	}

	if(fileExists(fileName) == 0){
		ofstream zip;
		zip.open(fileName.c_str());
		if(zip.is_open()){
			zip.close();
			return 0;

		}else{
			return -1;
		}
	}else{
		return -1;
	}
}


RC PagedFileManager::destroyFile(const string &fileName){
	if(fileName.empty()){
		return -1;
	}

	if(fileExists(fileName) != 0){
		// file exists
		remove(fileName.c_str());
		string mstring = "freespace_" + fileName;
		remove(mstring.c_str());
		return 0;
	}else{
		// file does not exist
		return -1;
	}
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle){

	if(fileName.empty() || (fileExists(fileName)== (RC) 0) ){
		// file does not exist
		//cout << "File:" << fileName.c_str() << " doesn't ecist" << endl;
		return -1;
	}

	if(!fileHandle.mFile.is_open()){
		// not associated with any file
		fileHandle.mFile.open(fileName.c_str(), ios::in | ios::out | ios::binary);
		if(!fileHandle.mFile.is_open()){
			return -1;
		}
		fileHandle.fileName = fileName;
		//cout << "about to enter stream setter" << endl;
		return fileHandle.stream_setter();
	}else{
		return -1;
	}
}


RC PagedFileManager::closeFile(FileHandle &fileHandle){
	// close the file referred by the fileHandle

	if(fileHandle.mFile.is_open()){
		fileHandle.writeFSpaceToDisk();
		fileHandle.mFile.close();
		return (RC) 0;
	}else{
		//cout << "Error in closing file..." << endl;
		//fileHandle.writeFSpaceToDisk();
		return (RC) -1;
	}

}




FileHandle::FileHandle(){
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    //mData = NULL;

    vector<FreeSpaceNode> v(1);
    FreeSpaceNode fsn(0, -1, -1);
    v[0] = fsn;
    fSpace = v;
}


FileHandle::~FileHandle(){
}


RC FileHandle::readPage(PageNum pageNum, void *data){
	// assume 0th page is for meta-data
	if(mFile.is_open()){
		pageNum++;
		if(pageNum > (unsigned)(getNumberOfPages()) ){
			return (RC) -1;
		}

		mFile.seekg(pageNum*PAGE_SIZE, ios::beg);
		// get characters and write them in the memory pointed by *data
		mFile.read((char *) data, PAGE_SIZE);
		// increment the writepagecounter
		readPageCounter++;
		// update meta-data object
		mData.setCounters(readPageCounter, writePageCounter, appendPageCounter);

		writeMetaDataToDisk();
		return (RC) 0;
	}
    return (RC) -1;
}


RC FileHandle::writePage(PageNum pageNum, const void *data){
	// assume 0th page is for meta-data
	if(mFile.is_open()){
		pageNum++;
		if(pageNum > (unsigned)(getNumberOfPages()) ){
			return -1;
		}
		(mFile).seekp(pageNum*PAGE_SIZE, ios::beg);
		// write the data
		(mFile).write((char *) data, PAGE_SIZE);

		// increment the writepagecounter
		writePageCounter++;
		// update meta-data object
		mData.setCounters(readPageCounter, writePageCounter, appendPageCounter);
		writeMetaDataToDisk();

		// flush the fstream
		mFile.flush();

		return 0;
	}
    return -1;
}


RC FileHandle::appendPage(const void *data){
	// append a block of 4096 BYTES
	if(mFile.is_open()){
		// seek to end and add data
		(mFile).seekp(0, ios::end);
		// write the data
		(mFile).write((char *) data, PAGE_SIZE);


		// increment append counter
		appendPageCounter++;
		// update meta-data object
		mData.setCounters(readPageCounter, writePageCounter, appendPageCounter);
		mData.setPages( (mData.getPages()) + 1 );

		writeMetaDataToDisk();

		// flush the fstream
		mFile.flush();

		return 0;
	}
    return -1;
}


unsigned FileHandle::getNumberOfPages(){
	// return -1, the actual number.
	return ((unsigned) ((mData.getPages()) - 1 ));
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount){

	readPageCount = readPageCounter;
	writePageCount = writePageCounter;
	appendPageCount = appendPageCounter;

    return (RC) 0;
}


RC FileHandle::stream_setter(){
	// mFile is in binary mode.
	if(!mFile.is_open()){
		return -1;
	}
	// called when a file is opened.
	// check if meta-data has been created or not
	mFile.seekg(0, ios::beg);
	size_t start = mFile.tellg();
	mFile.seekg(0, ios::end);
	size_t end = mFile.tellg();


	if(start != end){
		// OLD FILE.
		// does contain some data
		//cout << "This is an old file" << endl;
		mFile.seekg(0, ios::beg);
		mFile.read((char *) &mData, sizeof(MetaData));

		// restore counters
		mData.getCounters(readPageCounter, writePageCounter, appendPageCounter);

		// READ free-space into m/y
		string mString = "freespace_" + (this -> fileName);
		fstream fFile;
		fFile.open(mString.c_str(), ios::in | ios::binary);

			// free space file exists
			if(fFile.is_open()){
				fFile.seekg(0, ios::beg);
				long lengthFS = 0;
				fFile.read((char*) &lengthFS, sizeof(long));

				if(lengthFS != 0){
					fFile.seekg(sizeof(long), ios::beg);
					fSpace.clear();
					fSpace.resize(lengthFS/sizeof(FreeSpaceNode)); // resize fSpace vector;
					fFile.read((char*) &fSpace[0], (streamsize) lengthFS);
					//cout << "ss3" << endl;
					// check for failbit???
					fFile.close();
					//cout << "SS4" << endl;
				}else{
					// 0 length free-space file!
					return -1;
				}

				}else{
					return -1;
				}

	} else{
		// NEW empty file.
		mData.setCounters(0, 0, 0);
		// create the first page.
		mFile.seekp(0, ios::beg);
		for(int i = 0; i < PAGE_SIZE; i++){
			mFile.put('o');
		}
		mData.setPages(1);
		writeMetaDataToDisk();
		writeFSpaceToDisk();

	}

	return 0;
}


RC FileHandle::writeMetaDataToDisk(){
	if(mFile.is_open()){
		mFile.seekp(0, ios::beg);
		mFile.write((char *) &mData, sizeof(MetaData));
		return 0;
	}else{
		return -1;
	}
}


RC FileHandle::writeFSpaceToDisk(){
	// write fspace to the new/old freespace-file
	string mString = "freespace_" + (this -> fileName);
	fstream fFile;

	fFile.open(mString.c_str(), ios::out | ios::binary);

	if(fFile.is_open()){
		fFile.seekp(0, ios::beg);
		long lengthFS = (long) fSpace.size()*sizeof(FreeSpaceNode); // length of characters in the vector
		fFile.write((char*) &lengthFS, sizeof(long));
		fFile.seekp( sizeof(long), ios::beg);
		fFile.write((char*) &fSpace[0], (streamsize) lengthFS);
		//fFile.flush();
		//fFile.close();	// files are closed automatically at end of scope..
		return 0;
	}else{
		//fFile.close();
		return -1;
	}
}


