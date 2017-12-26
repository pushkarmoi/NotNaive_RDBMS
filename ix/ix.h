#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan


class IX_ScanIterator;
class IXFileHandle;

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);


        RC getLowestKey(IXFileHandle &ixfileHandle, const Attribute &attribute, int &leafPage);
        RC getPathToLeaf(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, vector<int> &path, int &leafPage);
        RC initializeTree(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);
        RC getSplitPosition(const Attribute &attribute, const void *data, const int dataLength, int &splitPosition);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        RC insertInParent(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *data,
        		vector<int> parents, const int parentID);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;
        void recursivePrint(IXFileHandle &ixfileHandle, const Attribute &attribute,
        		const int root, const int tabs, const bool printComma) const;

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
};



class IXFileHandle {

public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    FileHandle lowerFileHandler;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

};

class IX_ScanIterator {

	private:
		int currentIndex;
		AttrType keyType;
		IXFileHandle* myIxFileHandle;

	public:
		vector<int> key_int;
		vector<float> key_real;
		vector<string> key_varchar;
		vector<RID> key_rids;

		// Constructor
        IX_ScanIterator(){
        	currentIndex = -1;
        }
        // Destructor
        ~IX_ScanIterator(){
        }

        RC initialize(IXFileHandle* myix, AttrType kt){

        	myIxFileHandle = myix;

        	if(!myIxFileHandle->lowerFileHandler.mFile.is_open()){
        		cout << "INitializing iterator with non opened filehandle" << endl;
        		return (RC) -1;
        	}

        	this->keyType = kt;
        	this->currentIndex = -1;
        	// vectors and rids are public

        	return RC (0);
        }

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
};



#endif
