
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
        int result = InsertLeaf(page,attribute,rid,key);
        if(result != 0) {
            LeafInfo leaf;
            memcpy(&leaf,(char*)page+1,sizeof(LeafInfo));
            //SPLIT LEAF START
            void* splitleaf = malloc(PAGE_SIZE);
            unsigned type = 1;
            memcpy(splitleaf,&type,sizeof(char));
            LeafInfo splitleafinfo;
            splitleafinfo.leftSibling = rootPageNum;
            splitleafinfo.rightSibling = leaf.rightSibling;
            splitleafinfo.numOfItem = 0;
            splitleafinfo.emptySlotStart = 1+sizeof(LeafInfo);
            memcpy((char*)splitleaf+1,&splitleafinfo,sizeof(LeafInfo));
            largeInt splitpagenum = ixfileHandle.getNumberOfPages();
            leaf.rightSibling = splitpagenum;
            memcpy((char*)page+1,&leaf,sizeof(LeafInfo));

            int splitsize=0;
            int slotNum=0;
            int oldsize=0;
            for(int i=0;i<leaf.numOfItem;i++) {
                //find the first key of split leaf to copy up to parent
                DataEntry entry;
                memcpy(&entry,(char*)+1+sizeof(LeafInfo)+i*sizeof(DataEntry),sizeof(DataEntry));
                void* middlekey = nullptr;

                if(attribute.type == TypeVarChar) {
                    middlekey = (char*)page+entry.offset;
                }else{
                    middlekey = &(entry.offset);
                }
                //oldsize = getkeylengthleaf STARTS HERE
                if(attribute.type == TypeVarChar) {
                    largeInt tempkeylen;
                    memcpy(&tempkeylen,middlekey,4);
                    oldsize+=4;
                    oldsize+=tempkeylen;
                }else{
                    oldsize = sizeof(DataEntry);
                }
                //getkeylengthleaf ends here. basically key len is size of dataentry except for varchar
                splitsize += oldsize;
                if(splitsize >= PAGE_SIZE/2) {
                    //compleafslot start
                    int comparison = 0;
                    if(attribute.type == TypeInt) {
                        DataEntry entry;
                        memcpy(&entry, (char*)page+1+sizeof(LeafInfo)+((i+1)*sizeof(DataEntry)), sizeof(DataEntry));
                        largeInt _key;
                        memcpy(&_key,middlekey,4);
                        if(_key == entry.offset) comparison = 0;
                        if(_key > entry.offset) comparison = 1;
                        if(_key < entry.offset) comparison = -1;
                    }else if(attribute.type == TypeReal) {
                        DataEntry entry;
                        memcpy(&entry, (char*)page+1+sizeof(LeafInfo)+((i+1)*sizeof(DataEntry)), sizeof(DataEntry));
                        largeInt _key;
                        memcpy(&_key,middlekey,4);
                        if(_key == entry.offset) comparison = 0;
                        if(_key > entry.offset) comparison = 1;
                        if(_key < entry.offset) comparison = -1;
                    }else if(attribute.type == TypeVarChar) {
                        //use varchar offset to get the value and use that for comparison
                        largeInt varcharsize;
                        memcpy(&varcharsize,middlekey,4);
                        char _key[varcharsize+1];
                        memcpy(&_key,(char*)middlekey+4,varcharsize);
                        _key[varcharsize] = '\0';
                        DataEntry entry;
                        memcpy(&entry, (char*)page+1+sizeof(LeafInfo)+((i+1)*sizeof(DataEntry)), sizeof(DataEntry));
                        largeInt varcharsize2;
                        memcpy(&varcharsize2,(char*)page+entry.offset,4);
                        char val[varcharsize2+1];
                        memcpy(&val,(char*)page+entry.offset+4,varcharsize2);
                        val[varcharsize2]='\0';
                        comparison = strcmp(_key,val);
                    }

                    //compleafend, comparison is the return value
                    if(i>=leaf.numOfItem-1 || comparison != 0) {
                        slotNum = i;
                        break;
                    }    
                }
            }

            DataEntry mid;
            memcpy(&mid,(char*)page+1+sizeof(LeafInfo)+slotNum*sizeof(DataEntry),sizeof(DataEntry));
            node.key = malloc(oldsize);
            node.pageNum = splitpagenum;
            int ksize = 4;
            if(attribute.type == TypeVarChar) {
                ksize = oldsize-sizeof(DataEntry);
            }
            if(attribute.type == TypeVarChar) {
                memcpy(node.key,(char*)page+mid.offset,ksize);
            }else{
                memcpy(node.key,&(mid.offset), ksize);
            }
            void* keytomove = malloc(attribute.length+4);
            for(int i=1;i<leaf.numOfItem-slotNum;i++) {
                DataEntry entry;
                memcpy(&entry,(char*)page+1+sizeof(LeafInfo)+(slotNum+1)*sizeof(DataEntry),sizeof(DataEntry));
                RID ridtomove = entry.rid;
                if(attribute.type == TypeVarChar) {
                    largeInt keylen;
                    memcpy(&keylen,(char*)page+entry.offset,4);
                    memcpy(keytomove,&keylen,4);
                    memcpy((char*)keytomove+4,(char*)page+entry.offset+4,keylen);
                }else{
                    memcpy(keytomove,&(entry.offset),4);
                }

                //insert dataentry into newly split leaf
                InsertLeaf(splitleaf,attribute,ridtomove,keytomove);
                //insert ends here 
                //delete old entry from old leaf
                //delete ends here
            }
            free(keytomove);

            int comparison = 0;
            if(attribute.type == TypeInt) {
                DataEntry entry;
                memcpy(&entry, (char*)page+1+sizeof(LeafInfo)+((slotNum+1)*sizeof(DataEntry)), sizeof(DataEntry));
                largeInt _key;
                memcpy(&_key,key,4);
                if(_key == entry.offset) comparison = 0;
                if(_key > entry.offset) comparison = 1;
                if(_key < entry.offset) comparison = -1;
            }else if(attribute.type == TypeReal) {
                DataEntry entry;
                memcpy(&entry, (char*)page+1+sizeof(LeafInfo)+((slotNum+1)*sizeof(DataEntry)), sizeof(DataEntry));
                largeInt _key;
                memcpy(&_key,key,4);
                if(_key == entry.offset) comparison = 0;
                if(_key > entry.offset) comparison = 1;
                if(_key < entry.offset) comparison = -1;
            }else if(attribute.type == TypeVarChar) {
                        //use varchar offset to get the value and use that for comparison
                largeInt varcharsize;
                memcpy(&varcharsize,key,4);
                char _key[varcharsize+1];
                memcpy(&_key,(char*)key+4,varcharsize);
                _key[varcharsize] = '\0';

                DataEntry entry;
                memcpy(&entry, (char*)page+1+sizeof(LeafInfo)+((slotNum+1)*sizeof(DataEntry)), sizeof(DataEntry));
                largeInt varcharsize2;
                memcpy(&varcharsize2,(char*)page+entry.offset,4);
                char val[varcharsize2+1];
                memcpy(&val,(char*)page+entry.offset+4,varcharsize2);
                val[varcharsize2]='\0';
                comparison = strcmp(_key,val);
            }

            if(comparison <= 0) {
                if(InsertLeaf(page,attribute,rid,key)) {
                    free(splitleaf);
                    return -1;
                }
            }else{
                if(InsertLeaf(splitleaf,attribute,rid,key)) {
                    free(splitleaf);
                    return -1;
                }
            }

            //everything went through, just write old leaf page and append new leaf page
            if(ixfileHandle.writePage(rootPageNum,page)) {
                free(splitleaf); 
                return -1;
            }
            if(ixfileHandle.appendPage(splitleaf)) {
                free(splitleaf);
                return -1;
            }
            free(splitleaf);
            free(page);
            return 0;
        }else{
            if(ixfileHandle.writePage(rootPageNum,page)) return -1;
            free(node.key);
            free(page);
            node.key = nullptr;
            node.pageNum = -1;
            return 0;  
        }
    }else{
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
        page = malloc(PAGE_SIZE);
        if(ixfileHandle.readPage(rootPageNum,page)) return -1;
        int result = InsertIndex(page,attribute,node);
        if(result == -1) {
            //split internal
            //split internal uses ixfilehandle, attribute, rootpagenum,page,node
            NodeInfo info;
            memcpy(&info,(char*)page+1,sizeof(NodeInfo));

            largeInt splitpagenum = ixfileHandle.getNumberOfPages();

            int splitsize = 0;
            int slotNum = 0;
            int oldsize = 0;

            for(int i=0;i<info.numOfItem;i++) {
                //find the first key of split leaf to copy up to parent
                Index index;
                memcpy(&index,(char*)+1+sizeof(NodeInfo)+i*sizeof(Index),sizeof(Index));
                void* middlekey = nullptr;

                if(attribute.type == TypeVarChar) {
                    middlekey = (char*)page+index.offset;
                }else{
                    middlekey = &(index.offset);
                }
                //oldsize = getkeylengthleaf STARTS HERE
                if(attribute.type == TypeVarChar) {
                    largeInt tempkeylen;
                    memcpy(&tempkeylen,middlekey,4);
                    oldsize+=4;
                    oldsize+=tempkeylen;
                }else{
                    oldsize = sizeof(Index);
                }
                //getkeylengthleaf ends here. basically key len is size of dataentry except for varchar
                splitsize += oldsize;
                if(splitsize >= PAGE_SIZE/2) {
                    slotNum  = i;
                    break;
                }
            }

            Index middleIndex;
            memcpy(&middleIndex,(char*)page+1+sizeof(NodeInfo)+slotNum*sizeof(Index), sizeof(Index));

            void* splitindexpage = malloc(PAGE_SIZE);
            unsigned type = 0;
            memcpy(splitindexpage,&type,sizeof(char));
            NodeInfo newInfo;
            newInfo.numOfItem = 0;
            newInfo.emptySlotStart = 1+sizeof(NodeInfo);
            newInfo.childPageNum = middleIndex.childPageNum;
            memcpy((char*)splitindexpage+1,&newInfo,sizeof(NodeInfo));

            int ksize = 4;
            if(attribute.type == TypeVarChar) {
                ksize = oldsize-sizeof(Index);
            }
            void* middlekey = malloc(ksize);
            if(attribute.type == TypeVarChar) {
                memcpy(middlekey,(char*)page+middleIndex.offset,ksize);
            }else{
                memcpy(middlekey, &(middleIndex.offset), 4);
            }

            void* keytomove = malloc(attribute.length+4);

            for(int i=1;i<info.numOfItem-slotNum;i++) {
                Index index;
                memcpy(&index,(char*)page+1+sizeof(NodeInfo)+(slotNum+1)*sizeof(Index),sizeof(Index));
                largeInt pagetomove = index.childPageNum;
                if(attribute.type == TypeVarChar) {
                    largeInt keylen;
                    memcpy(&keylen,(char*)page+index.offset,4);
                    memcpy(keytomove,&keylen,4);
                    memcpy((char*)keytomove+4,(char*)page+index.offset+4,keylen);
                }else{
                    memcpy(keytomove,&(index.offset),4);
                }

                TempNode nodeToInsert;
                nodeToInsert.key = keytomove;
                nodeToInsert.pageNum = pagetomove;
                InsertIndex(splitindexpage,attribute,nodeToInsert);
                //delete internal uses attribute, keytomove,page



            }
            free(keytomove);
            //delete internal with attribute,middlekey,page

            //compareslot


        }else{
            if(ixfileHandle.writePage(rootPageNum,page)) {
                free(node.key);
                node.key = nullptr;
                node.pageNum = -1;
                free(page);
                return -1;
            }
            free(node.key);
            node.key = nullptr;
            node.pageNum = -1;
            free(page);
            return 0;
        }
    }
}



RC IndexManager::InsertIndex(void* page, const Attribute& attr, TempNode& nodeToInsert) {

    NodeInfo index;
    memcpy(&index,(char*)page+1,sizeof(NodeInfo));
    largeInt keysize = sizeof(Index);
    if(attr.type == TypeVarChar) {
        largeInt temp_len;
        memcpy(&temp_len,nodeToInsert.key,4);
        keysize += (temp_len+4);
    }

    largeInt freeSpace = PAGE_SIZE - index.emptySlotStart;
    if(freeSpace < keysize) return -1;
    int slotNum = 0;
    for(int i=0;i<index.numOfItem;i++) {
        int comparison = 0;
        if(attr.type == TypeInt) {
            Index index;
            memcpy(&index, (char*)page+1+sizeof(NodeInfo)+i*sizeof(Index), sizeof(Index));
            largeInt _key;
            memcpy(&_key,nodeToInsert.key,4);
            if(_key == index.offset) comparison = 0;
            if(_key > index.offset) comparison = 1;
            if(_key < index.offset) comparison = -1;
        }else if(attr.type == TypeReal) {
            Index index;
            memcpy(&index, (char*)page+1+sizeof(NodeInfo)+i*sizeof(Index), sizeof(Index));
            largeInt _key;
            memcpy(&_key,nodeToInsert.key,4);
            if(_key == index.offset) comparison = 0;
            if(_key > index.offset) comparison = 1;
            if(_key < index.offset) comparison = -1;
        }else if(attr.type == TypeVarChar) {
                        //use varchar offset to get the value and use that for comparison
            largeInt varcharsize;
            memcpy(&varcharsize,nodeToInsert.key,4);
            char _key[varcharsize+1];
            memcpy(&_key,(char*)nodeToInsert.key+4,varcharsize);
            _key[varcharsize] = '\0';

            Index index;
            memcpy(&index, (char*)page+1+sizeof(NodeInfo)+i*sizeof(Index), sizeof(Index));
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

    unsigned entryBegin = 1+sizeof(NodeInfo) + slotNum*sizeof(Index);
    unsigned entryEnd = 1+sizeof(NodeInfo)+ index.numOfItem*sizeof(Index);

    memmove((char*)page+entryBegin+sizeof(Index), (char*)page+entryBegin, entryEnd-entryBegin);

    Index indexToInsert;
    indexToInsert.childPageNum = nodeToInsert.pageNum;
    if(attr.type == TypeVarChar) {
        largeInt temp;
        memcpy(&temp,nodeToInsert.key,4);
        indexToInsert.offset = index.emptySlotStart;
        memcpy((char*)page+indexToInsert.offset, nodeToInsert.key, temp+4);
        index.emptySlotStart = indexToInsert.offset+(keysize+4);
    }else{
        memcpy(&(indexToInsert.offset),nodeToInsert.key,4);
    }
    index.numOfItem++;
    //write in the udpated values for the new index and the directory
    memcpy((char*)page+1,&index,sizeof(NodeInfo));
    memcpy((char*)page+1+sizeof(NodeInfo)+slotNum*sizeof(Index), &indexToInsert, sizeof(Index));
    return 0;
}

RC IndexManager::InsertLeaf(void* page,const Attribute& attr,const RID& rid, const void* key) {
     //INSERT INTO LEAF
    LeafInfo leaf;
    memcpy(&leaf, page,sizeof(LeafInfo));
    largeInt sizeofkey;
    if(attr.type == TypeVarChar) {
        largeInt varlen;
        memcpy(&varlen,key,4);
        sizeofkey = sizeof(DataEntry);
        sizeofkey += (4+varlen);
    }else{
        sizeofkey = sizeof(DataEntry);
    }
        //get free space for leaf to see if will fit
    largeInt freeSpace = PAGE_SIZE - leaf.emptySlotStart;
    if(freeSpace < sizeofkey) return -1;
    int slotNum=0;
    for(int i=0;i<leaf.numOfItem;i++) {
        int comparison = 0;
        if(attr.type == TypeInt) {
            DataEntry entry;
            memcpy(&entry, (char*)page+1+sizeof(NodeInfo)+(i*sizeof(DataEntry)), sizeof(DataEntry));
            largeInt _key;
            memcpy(&_key,key,4);
            if(_key == entry.offset) comparison = 0;
            if(_key > entry.offset) comparison = 1;
            if(_key < entry.offset) comparison = -1;
        }else if(attr.type == TypeReal) {
            DataEntry entry;
            memcpy(&entry, (char*)page+1+sizeof(NodeInfo)+(i*sizeof(DataEntry)), sizeof(DataEntry));
            largeInt _key;
            memcpy(&_key,key,4);
            if(_key == entry.offset) comparison = 0;
            if(_key > entry.offset) comparison = 1;
            if(_key < entry.offset) comparison = -1;
        }else if(attr.type == TypeVarChar) {
                        //use varchar offset to get the value and use that for comparison
            largeInt varcharsize;
            memcpy(&varcharsize,key,4);
            char _key[varcharsize+1];
            memcpy(&_key,(char*)key+4,varcharsize);
            _key[varcharsize] = '\0';

            DataEntry entry;
            memcpy(&entry, (char*)page+1+sizeof(NodeInfo)+(i*sizeof(DataEntry)), sizeof(DataEntry));
            largeInt varcharsize2;
            memcpy(&varcharsize2,(char*)page+entry.offset,4);
            char val[varcharsize2+1];
            memcpy(&val,(char*)page+entry.offset+4,varcharsize2);
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
    if(attr.type == TypeVarChar) {
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
    return 0;
}

RC IndexManager::deleteIndex(void* page, const Attribute& attr, TempNode& nodetoDelete) {
    NodeInfo info;
    memcpy(&info,(char*)page+1,sizeof(NodeInfo));
    largeInt numOfItem = info.numOfItem;

    int slotNum = 0;
    for(int i=0;i<info.numOfItem;i++) {
        int comparison = 0;
        if(attr.type == TypeInt) {
            Index index;
            memcpy(&index, (char*)page+1+sizeof(NodeInfo)+i*sizeof(Index), sizeof(Index));
            largeInt _key;
            memcpy(&_key,nodetoDelete.key,4);
            if(_key == index.offset) comparison = 0;
            if(_key > index.offset) comparison = 1;
            if(_key < index.offset) comparison = -1;
        }else if(attr.type == TypeReal) {
            Index index;
            memcpy(&index, (char*)page+1+sizeof(NodeInfo)+i*sizeof(Index), sizeof(Index));
            largeInt _key;
            memcpy(&_key,nodetoDelete.key,4);
            if(_key == index.offset) comparison = 0;
            if(_key > index.offset) comparison = 1;
            if(_key < index.offset) comparison = -1;
        }else if(attr.type == TypeVarChar) {
                        //use varchar offset to get the value and use that for comparison
            largeInt varcharsize;
            memcpy(&varcharsize,nodetoDelete.key,4);
            char _key[varcharsize+1];
            memcpy(&_key,(char*)nodetoDelete.key+4,varcharsize);
            _key[varcharsize] = '\0';

            Index index;
            memcpy(&index, (char*)page+1+sizeof(NodeInfo)+i*sizeof(Index), sizeof(Index));
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

    if(slotNum == numOfItem) return -1;

    Index index;
    memcpy(&index,(char*)page+1+sizeof(NodeInfo)+slotNum*sizeof(Index),sizeof(Index));

    unsigned entryBegin = 1+sizeof(NodeInfo) + slotNum*sizeof(Index);
    unsigned entryEnd = 1+sizeof(NodeInfo)+ info.numOfItem*sizeof(Index);

    memmove((char*)page+1+sizeof(NodeInfo)+slotNum*sizeof(Index), (char*)page+1+sizeof(NodeInfo)+slotNum*sizeof(Index)+sizeof(Index), (1+sizeof(NodeInfo)+ info.numOfItem*sizeof(Index))-(1+sizeof(NodeInfo) + slotNum*sizeof(Index))-sizeof(Index));
    info.numOfItem--;
    if(attr.type == TypeVarChar) {
        largeInt len;
        largeInt offset = index.offset;
        memcpy(&len,(char*)page+index.offset,4);
        len+=4;
        //freespaceoffset = pagesize-emptyslotstart-1-sizeof(NodeInfo);
        memmove((char*)page+info.emptySlotStart-len,(char*)page+info.emptySlotStart,index.offset-info.emptySlotStart);
        info.emptySlotStart-=len;
        for(int i=0;i<info.numOfItem;i++) {
            Index index;
            memcpy(&index, (char*)page+1+sizeof(NodeInfo)+i*sizeof(Index),sizeof(Index));
            if(index.offset<offset) index.offset -= len;
            memcpy((char*)page+1+sizeof(NodeInfo)+i*sizeof(Index),&index,sizeof(Index));
        }
    }
    memcpy((char*)page+1,&info,sizeof(NodeInfo));
    return 0;
}
RC IndexManager::deleteLeaf(void* page, const Attribute& attr, const RID& rid, const void* key){
    
}


// POSSIBLE NODE STRUCTURE? type, header, entries and then if varchar, its stored from the end of page?


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
