
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

/*
    1) create the file
    2) open it, and write in node type
    3) set up the root node, first page

*/
RC IndexManager::createFile(const string &fileName)
{

    pfm = PagedFileManager::instance();
    IXFileHandle handle;

    if(pfm->createFile(fileName)) return -1;
    if(openFile(fileName,handle)) return -1;
    
    //page for root
    void* page = malloc(PAGE_SIZE);
    memset(page, 1, PAGE_SIZE);

    uLargeInt root = 1;
    memcpy(page, &root, sizeof(uLargeInt));

    //add the root page
    if(handle.appendPage(page)) {
        free(page);
        closeFile(handle);
        return -1;
    }

    //set up next page as a node(root) 0 is normal nodes and 1 is leaf nodes
    char typeOfNode = 0;
    memcpy(page,&typeOfNode, sizeof(char));

    //info about the node, at creation no keys, entire page free, and child is on page 2(root on page 1)
    NodeInfo rootinfo;
    rootinfo.numOfKey = 0;
    rootinfo.freeOffset = PAGE_SIZE;
    rootinfo.childPageNum = 2;
    memcpy((char*)page+1, &rootinfo, sizeof(NodeInfo));
    if(handle.appendPage(page)) {
        free(page);
        closeFile(handle);
        return -1;
    }
    free(page);
    closeFile(handle);
    return 0;
}

RC IndexManager::destroyFile(const string &fileName)
{
    pfm = PagedFileManager::instance();
    if(pfm->destroyFile(fileName)) return -1;
    return 0;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    pfm = PagedFileManager::instance();
    if(pfm->openFile(fileName,ixfileHandle.handle)) return -1;
    return 0;

}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    pfm = PagedFileManager::instance();
    if(pfm->closeFile(ixfileHandle.handle)) return -1;
    return 0;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
    const Attribute &attribute,
    const void      *lowKey,
    const void      *highKey,
    bool			lowKeyInclusive,
    bool        	highKeyInclusive,
    IX_ScanIterator &ix_ScanIterator)
{
    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return -1;
}

RC IX_ScanIterator::close()
{
    return -1;
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
    readPageCount= ixReadPageCounter;
    writePageCount = ixWritePageCounter;
    appendPageCount = ixAppendPageCounter;
    return 0;
}

RC IXFileHandle::readPage(unsigned pagenum, void* data) {
    ixReadPageCounter++;
    return handle.readPage(pagenum,data);
}
RC IXFileHandle::writePage(unsigned pagenum, void* data) {
    ixWritePageCounter++;
    return handle.writePage(pagenum,data);
}
RC IXFileHandle::appendPage(void* data) {
    ixAppendPageCounter++;
    return handle.appendPage(data);
}
unsigned IXFileHandle::getNumberOfPages() {
    return handle.getNumberOfPages();
}
