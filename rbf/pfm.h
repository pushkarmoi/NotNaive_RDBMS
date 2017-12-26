#ifndef _pfm_h_
#define _pfm_h_

typedef unsigned PageNum;
typedef int RC;
typedef char byte;

#define PAGE_SIZE 4096
#include <string>
#include <climits>
#include <fstream>
#include <vector>
using namespace std;

class FileHandle;
class FreeSpaceNode;

class MetaData{
private:
    int pages;
    int version;	// schema version
    unsigned readPageCounter;
    unsigned writePageCounter;
    unsigned appendPageCounter;

public:
    MetaData(){
        pages = 0;
        version = 1;
        setCounters(0, 0, 0);
    }

    ~MetaData(){

    }

    void setPages(int x){
        pages = x;
    }

    int getPages(){
        return pages;
    }

    void setVersion(int x){
    	version = x;
    }

    int getVersion(){
    	return version;
    }

    void setCounters(unsigned rd, unsigned wr, unsigned ap){
        readPageCounter = rd;
        writePageCounter = wr;
        appendPageCounter = ap;
    }

    void getCounters(unsigned &rd, unsigned &wr, unsigned &ap){
        rd = readPageCounter;
        wr = writePageCounter;
        ap = appendPageCounter;
    }

};

class PagedFileManager
{
public:
    static PagedFileManager* instance();                                  // Access to the _pf_manager instance

    RC createFile    (const string &fileName);                            // Create a new file
    RC destroyFile   (const string &fileName);                            // Destroy a file
    RC openFile      (const string &fileName, FileHandle &fileHandle);    // Open a file
    RC closeFile     (FileHandle &fileHandle);                            // Close a file

    RC fileExists(const string &fileName);


protected:
    PagedFileManager();                                                   // Constructor
    ~PagedFileManager();                                                  // Destructor

private:
    static PagedFileManager *_pf_manager;

};


class FileHandle
{

private:
    MetaData mData;
    vector<FreeSpaceNode> fSpace;   // this creates a 0 length vector 

public:
    // variables to keep the counter for each operation
    fstream mFile;
    string fileName;	// also used by RM class
    unsigned readPageCounter;
    unsigned writePageCounter;
    unsigned appendPageCounter;
    
    FileHandle();                                                         // Default constructor
    ~FileHandle();                                                        // Destructor

    RC readPage(PageNum pageNum, void *data);                             // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                      // Write a specific page
    RC appendPage(const void *data);                                      // Append a specific page
    unsigned getNumberOfPages();                                          // Get the number of pages in the file
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);  // Put the current counter values into variables
    
    RC stream_setter();

    RC setMetaData(MetaData md){mData = md; return (RC) 0;}
    MetaData getMetaData(){return mData;}

    RC setFSpace(vector<FreeSpaceNode> v){fSpace = v; return (RC) 0;}
    vector<FreeSpaceNode> getFSpace(){return fSpace;}

    RC writeMetaDataToDisk();                       // writes mData object to the file mFile.
    RC writeFSpaceToDisk();                         // writes FSpace object to the file mFile.

}; 





class FreeSpaceNode{

private:
    int pagenumber;
    int space;
    int tablestart;

public:
    FreeSpaceNode(){
    }   

    FreeSpaceNode(int pn, int sp, int ss){
        setAll(pn, sp, ss);
    }

    ~FreeSpaceNode(){
    }

    void setAll(int a, int b, int c){
       pagenumber = a;
       space = b;
       tablestart = c; 
    }

    void getAll(int &a, int &b, int &c){
        a = pagenumber;
        b = space;
        c = tablestart;
    }

    void setPageNumber(int x){
        pagenumber = x;
    }

    int getPageNumber(){
        return pagenumber;
    }

    void setSpace(int x){
        space = x;
    }

    int getSpace(){
        return space;
    }

    void setTableStart(int x){
        tablestart = x;
    }

    int getTableStart(){
        return tablestart;
    }




};
#endif
