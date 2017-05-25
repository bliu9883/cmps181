#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <stdlib.h>

#include "../rbf/rbfm.h"


# define IX_EOF (-1)  // end of the index scan

typedef uint16_t uMedInt;
typedef uint32_t uLargeInt;
typedef int16_t medInt;
typedef int32_t largeInt;

typedef struct {
    uMedInt numOfItem;
    uMedInt emptySlotStart;
    uLargeInt childPageNum;
}NodeInfo;

typedef struct {
    uMedInt emptySlotStart;
    uMedInt numOfItem;
    uLargeInt leftSibling;
    uLargeInt rightSibling;
}LeafInfo;

typedef struct {
    largeInt offset;
    uLargeInt childPageNum;
}Index;

typedef struct {
    largeInt offset;
    RID rid;
}DataEntry;

typedef struct {
    uLargeInt pageNum;
    void* key;
}TempNode;

class IX_ScanIterator;
class IXFileHandle;

class IndexManager {
    friend class IX_ScanIterator;

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

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

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

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
        PagedFileManager *pfm;

        RC insertUtil(IXFileHandle &ixfileHandle,const Attribute &attribute, const void* key, const RID &rid, TempNode &node, largeInt rootPageNum);
        RC InsertIndex();
        RC InsertLeaf();
        RC splitNode();
        RC splitLeaf();
        largeInt getChildPage(void* page, const Attribute& attribute, const void* key);

};


class IX_ScanIterator {
    public:

		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
};



class IXFileHandle {
    friend class IndexManager;
    public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

    //read write and append pag
    RC readPage(unsigned pagenum, void* data);
    RC writePage(unsigned pagenum, void* data);
    RC appendPage(void* data);
    unsigned getNumberOfPages();   

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    //use the filehandle
private:
    FileHandle handle;

};

#endif