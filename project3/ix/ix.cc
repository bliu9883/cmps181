
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
    rootinfo.numOfItem = 0;
    rootinfo.emptySlotStart = 1+sizeof(NodeInfo);
    rootinfo.childPageNum = 2;
    memcpy((char*)page+1, &rootinfo, sizeof(NodeInfo));
    if(handle.appendPage(page)) {
        free(page);
        closeFile(handle);
        return -1;
    }

    //make a leaf page too?

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
    //start from the root, descent to leaf node(recursively) and find the spot to insert entry
    void* metaPage = malloc(PAGE_SIZE);
    if(ixfileHandle.handle.readPage(0,metaPage)) return -1;
    uLargeInt rootPageNum;
    memcpy(&rootPageNum,metaPage,sizeof(uLargeInt));
    free(metaPage);
    TempNode tempNode;
    tempNode.key = nullptr;
    tempNode.pageNum = 0;
    if(insertUtil(ixfileHandle,attribute,key,rid,tempNode,rootPageNum) == -1) return -1;
    return 0;
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



RC IndexManager::insertUtil(IXFileHandle &ixfileHandle,const Attribute &attribute, const void* key, const RID &rid, TempNode &node, largeInt rootPageNum) {
    //read in root page 
    void* page = malloc(PAGE_SIZE);
    if(ixfileHandle.readPage(rootPageNum,page)) return -1;

    //find out child page(to descend to leaves)
    char nodeType;
    memcpy(&nodeType,page,sizeof(char));

    //if leaf node, then find a place to insert.

    if(nodeType == 1) {
        //INSERT INTO LEAF
        LeafInfo leaf;
        memcpy(&leaf, page,sizeof(LeafInfo));
        largeInt sizeofkey;
        if(attribute.type == TypeVarChar) {
            largeInt varlen;
            memcpy(&varlen,key,4);
            sizeofkey = sizeof(DataEntry);
            sizeofkey += (4+varlen);
        }else{
            sizeofkey = sizeof(DataEntry);
        }

        //get free space for leaf to see if will fit
        largeInt freeSpace = PAGE_SIZE - leaf.emptySlotStart;
        if(freeSpace < sizeofkey) {
            //got to split the leaf node

        }else{
            int slotNum=0;
            for(int i=0;i<leaf.numOfItem;i++) {
                int comparison = 0;
                if(attribute.type == TypeInt) {
                    Index index;
                    memcpy(&index, (char*)page+1+sizeof(NodeInfo)+(i*sizeof(Index)), sizeof(Index));
                    largeInt _key;
                    memcpy(&_key,key,4);
                    if(_key == index.offset) comparison = 0;
                    if(_key > index.offset) comparison = 1;
                    if(_key < index.offset) comparison = -1;
                }else if(attribute.type == TypeReal) {
                    Index index;
                    memcpy(&index, (char*)page+1+sizeof(NodeInfo)+(i*sizeof(Index)), sizeof(Index));
                    largeInt _key;
                    memcpy(&_key,key,4);
                    if(_key == index.offset) comparison = 0;
                    if(_key > index.offset) comparison = 1;
                    if(_key < index.offset) comparison = -1;
                }else if(attribute.type == TypeVarChar) {
            //use varchar offset to get the value and use that for comparison
                    largeInt varcharsize;
                    memcpy(&varcharsize,key,4);
                    char _key[varcharsize+1];
                    memcpy(&_key,(char*)key+4,varcharsize);
                    _key[varcharsize] = '\0';

                    Index index;
                    memcpy(&index, (char*)page+1+sizeof(NodeInfo)+(i*sizeof(Index)), sizeof(Index));
                    largeInt varcharsize2;
                    memcpy(&varcharsize2,(char*)page+index.offset,4);
                    char val[varcharsize2+1];
                    memcpy(&val,(char*)page+index.offset+4,varcharsize2);
                    val[varcharsize2]='\0';
                    comparison = strcmp(_key,val);
                }
                if(comparison <= 0) {
                    slotNum = i;
                    break;
                }
            }

            unsigned entryBegin = 1+sizeof(LeafInfo) + slotNum*sizeof(DataEntry);
            unsigned entryEnd = 1+sizeof(LeafInfo)+ leaf.numOfItem*sizeof(DataEntry);

            //make space for the new entry
            memmove((char*)page+entryBegin+sizeof(DataEntry), (char*)page+entryBegin, entryEnd-entryBegin);

            DataEntry entry;
            if(attribute.type == TypeVarChar) {
                largeInt keysize;
                memcpy(&keysize,key,4);
                entry.offset = leaf.emptySlotStart;
                memcpy((char*)page+entry.offset, key, keysize+4);
                leaf.emptySlotStart = entry.offset+(keysize+4);
            }else{
                memcpy(&(entry.offset),key,4);
            }
            entry.rid = rid;
            leaf.numOfItem++;
            memcpy((char*)page+1,&leaf,sizeof(LeafInfo));
            unsigned setentryoffset = 1+sizeof(LeafInfo)+slotNum*sizeof(DataEntry);
            memcpy((char*)page+setentryoffset,&entry,sizeof(DataEntry));
            //insert into leaf end

            if(ixfileHandle.writePage(rootPageNum,page)) return -1;
            free(page);
            free(node.key);
            node.key = nullptr;
            node.pageNum = -1;
            return 0;
        }
    }else {
        //if not leaf, get child node and recursively insert
        largeInt childPageNum = getChildPage(page,attribute,key);
        if(insertUtil(ixfileHandle,attribute,key,rid,node,childPageNum) == -1) {
            return -1;
        }
        //inserted entry into correct leaf node
        if(node.pageNum == -1 && node.key == nullptr) {
            return 0;
        }
        //if temp not reset, then it means leaf split, and need to copy up the first index of the second node from split
        void* page = malloc(PAGE_SIZE);
        if(ixfileHandle.readPage(rootPageNum,page)) return -1;
        if(InsertIndex() == -1) {
            if(splitNode() == -1) {
                free(page);
                return -1;
            }
            free(page);
            return 0;
        }else{
            if(ixfileHandle.writePage(rootPageNum,page)) {
                free(node.key);
                node.key = nullptr;
                node.pageNum = -1;
                return -1;
            }
            free(node.key);
            node.key = nullptr;
            node.pageNum = -1;
            return 0;
        }
    }
}


RC IndexManager::InsertIndex(){

}
RC IndexManager::InsertLeaf(){

}
RC IndexManager::splitNode(){

}
RC IndexManager::splitLeaf(){

}


largeInt IndexManager::getChildPage(void* page, const Attribute& attribute, const void* key) {
    NodeInfo info;
    memcpy(&info, (char*)page+1, sizeof(NodeInfo));
    largeInt pageNum;
    int slotNum = 0;
    //find the index on the node that 
    for(int i=0;i<info.numOfItem;i++) {
        int comparison = 0;
        if(attribute.type == TypeInt) {
            Index index;
            memcpy(&index, (char*)page+1+sizeof(NodeInfo)+(i*sizeof(Index)), sizeof(Index));
            largeInt _key;
            memcpy(&_key,key,4);
            if(_key == index.offset) comparison = 0;
            if(_key > index.offset) comparison = 1;
            if(_key < index.offset) comparison = -1;
        }else if(attribute.type == TypeReal) {
            Index index;
            memcpy(&index, (char*)page+1+sizeof(NodeInfo)+(i*sizeof(Index)), sizeof(Index));
            largeInt _key;
            memcpy(&_key,key,4);
            if(_key == index.offset) comparison = 0;
            if(_key > index.offset) comparison = 1;
            if(_key < index.offset) comparison = -1;
        }else if(attribute.type == TypeVarChar) {
            //use varchar offset to get the value and use that for comparison
            largeInt varcharsize;
            memcpy(&varcharsize,key,4);
            char _key[varcharsize+1];
            memcpy(&_key,(char*)key+4,varcharsize);
            _key[varcharsize] = '\0';

            Index index;
            memcpy(&index, (char*)page+1+sizeof(NodeInfo)+(i*sizeof(Index)), sizeof(Index));
            largeInt varcharsize2;
            memcpy(&varcharsize2,(char*)page+index.offset,4);
            char val[varcharsize2+1];
            memcpy(&val,(char*)page+index.offset+4,varcharsize2);
            val[varcharsize2]='\0';
            comparison = strcmp(_key,val);
        }
        if(comparison <= 0) {
            slotNum = i;
            break;
        }
    }

    if(slotNum != 0) {
        Index index;
        memcpy(&index, (char*)page+1+sizeof(NodeInfo)+((slotNum-1)*sizeof(Index)), sizeof(Index));
        pageNum = index.childPageNum;
    }else{
        pageNum = info.childPageNum;
    }
    return  pageNum;

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
