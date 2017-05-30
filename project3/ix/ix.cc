
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
    //memset(page, 1, PAGE_SIZE);

    largeInt root = 1;
    memcpy(page, &root, sizeof(largeInt));

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
    rootinfo.emptySlotStart = PAGE_SIZE;
    rootinfo.childPageNum = 2;
    memcpy((char*)page+1, &rootinfo, sizeof(NodeInfo));
    if(handle.appendPage(page)) {
        free(page);
        closeFile(handle);
        return -1;
    }

    //make a leaf page too?
    void* leafpage = malloc(PAGE_SIZE);
    unsigned type = 1;
    memcpy((char*)leafpage, &type, sizeof(char));
    LeafInfo leafinfo;
    leafinfo.numOfItem = 0;
    leafinfo.emptySlotStart = PAGE_SIZE;
    leafinfo.leftSibling = 0;
    leafinfo.rightSibling = 0;
    memcpy((char*)leafpage+1,&leafinfo,sizeof(LeafInfo));
    if(handle.appendPage(leafpage)) {
        free(leafpage);
        closeFile(handle);
        return -1;
    }

    free(page);
    free(leafpage);
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
    //cout << "root page num is " << rootPageNum << endl;
    free(metaPage);
    TempNode tempNode;
    tempNode.key = nullptr;
    tempNode.pageNum = -1;
    //cout<<"insertutil"<<endl;
    if(insertUtil(ixfileHandle,attribute,key,rid,tempNode,rootPageNum) == -1) return -1;
    return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    //start from root to find the leaf node to delete from
    largeInt rootpagenum;
    void* rootpage = malloc(PAGE_SIZE);
    if(ixfileHandle.readPage(0,rootpage)) {
        free(rootpage);
        cout << "read page fail in deleteentry" << endl;
        return -1;
    }
    memcpy(&rootpagenum,rootpage,sizeof(largeInt));
    //cout << "rootpagenum has value " << rootpagenum << endl;
    largeInt leafPageNum;
    searchTree(ixfileHandle,attribute,key, rootpagenum, leafPageNum);
    //cout << "leaf page num is " << leafPageNum << endl;
    void* leafpage = malloc(PAGE_SIZE);
    if(ixfileHandle.readPage(leafPageNum,leafpage)) {
        cout << "read leaf page failed in deleteentry" << endl;
        return -1;
    }

    LeafInfo temp;
    memcpy(&temp, (char*)leafpage+1,sizeof(LeafInfo));
    //cout << "temp leaf numofitem is " << temp.numOfItem << endl;

    int result = deleteLeaf(leafpage,attribute,rid,key);
    if(result == -1) {
        cout << "deleteleaf failed" << endl;
        free(leafpage);
        return -1;
    }else{
        //write to page
        if(ixfileHandle.writePage(leafPageNum,leafpage)) {
            free(leafpage);
            cout << "write deleted leaf page failed" << endl;
            return -1;
        }else{
            free(leafpage);
        }
    }
    return 0;
}



RC IndexManager::scan(IXFileHandle &ixfileHandle,
    const Attribute &attribute,
    const void      *lowKey,
    const void      *highKey,
    bool			lowKeyInclusive,
    bool        	highKeyInclusive,
    IX_ScanIterator &ix_ScanIterator)
{
        return ix_ScanIterator.setup(ixfileHandle,attribute,lowKey,highKey,lowKeyInclusive,highKeyInclusive);
}

RC IX_ScanIterator::setup(IXFileHandle& _fileHandle, Attribute _attr, const void* _lowKey, const void* _highKey, bool _lowKeyInclusive, bool _highKeyInclusive) {
    fileHandle = &_fileHandle;
    attr = _attr;
    lowKey = _lowKey;
    highKey = _highKey;
    lowKeyInclusive = _lowKeyInclusive;
    highKeyInclusive = _highKeyInclusive;

    page = malloc(PAGE_SIZE);
    if (page == NULL)
        return -1;
    // if(ixfileHandle.readPage(2, ix_ScanIterator.page)) {
    //     free(ix_ScanIterator.page);
    //     return -1;
    // }
    //cout << "past first read" << endl;
    //initialize the starting slot
    slotNum = 0;

    //Find the starting page
    IndexManager *im = IndexManager::instance();
    largeInt startingPage;
    largeInt rootPageNum;

    void* metaPage = malloc(PAGE_SIZE);
    if (metaPage == NULL) return -1;

    //cout << "before readpage 1" << endl;
    if(fileHandle->readPage(0,metaPage)){
        cout << "inside if statement" << endl;
        free(metaPage);
        return -1;
    }

    // cout << "past second read" << endl;

    memcpy(&rootPageNum,(char*)metaPage,sizeof(largeInt));


    // cout << "before rootPageNum assert" << endl;
    cout << "rootpageNum: " <<rootPageNum <<endl;


    //cout << "before searchTree" << endl;
    if(im->searchTree(*fileHandle, attr, lowKey, rootPageNum, startingPage)){
        free(page);
        return -1;
    }
    cout << "starting page is " << startingPage << endl;
    //cout << "after searchTree" << endl;
    //read the page
    if(fileHandle->readPage(startingPage, page)){
        free(page);
        return -1;
    }
    free(metaPage);

    //cout << "after readPage" << endl;
    //find the beginning entry
    LeafInfo info;
    memcpy(&info, (char*)page+1, sizeof(LeafInfo));

    int slotCounter = 0;
    for(int i = 0; i<info.numOfItem; i++){
        int comparison = 0;
        if(lowKey == nullptr) {
            comparison = -1;
        }else{
            if(attr.type == TypeInt) {
                DataEntry entry;
                memcpy(&entry, (char*)page+1+sizeof(LeafInfo)+((i)*sizeof(DataEntry)), sizeof(DataEntry));
                largeInt _key;
                memcpy(&_key, lowKey,4);
                if(_key == entry.offset) comparison = 0;
                if(_key > entry.offset) comparison = 1;
                if(_key < entry.offset) comparison = -1;
            }else if(attr.type == TypeReal) {
                DataEntry entry;
                memcpy(&entry, (char*)page+1+sizeof(LeafInfo)+((i)*sizeof(DataEntry)), sizeof(DataEntry));
                largeInt _key;
                memcpy(&_key,lowKey,4);
                if(_key == entry.offset) comparison = 0;
                if(_key > entry.offset) comparison = 1;
                if(_key < entry.offset) comparison = -1;
            }else if(attr.type == TypeVarChar) {
                        //use varchar offset to get the value and use that for comparison
                largeInt varcharsize;
                memcpy(&varcharsize,lowKey,4);
                char _key[varcharsize+1];
                memcpy(&_key,(char*)lowKey+4,varcharsize);
                _key[varcharsize] = '\0';
                DataEntry entry;
                memcpy(&entry, (char*)page+1+sizeof(LeafInfo)+((i)*sizeof(DataEntry)), sizeof(DataEntry));
                largeInt varcharsize2;
                memcpy(&varcharsize2,(char*)page+entry.offset,4);
                char val[varcharsize2+1];
                memcpy(&val,(char*)page+entry.offset+4,varcharsize2);
                val[varcharsize2]='\0';
                comparison = strcmp(_key,val);
            }

            if(comparison < 0 || ((comparison == 0) && (lowKeyInclusive))) {
                slotCounter = i;
                break;
            }
        }
    }
    //cout << "after for loop" << endl;
    slotNum = slotCounter;
    return 0;
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) 
{
   //  //cout << "inside get next entry" << endl;
   // IndexManager *im = IndexManager::instance();
   //  //cout << "past getting im instance" << endl;
    LeafInfo info;
    memcpy(&info, (char*)page+1, sizeof(LeafInfo));
     //if we are off the page, go to next page
    if(slotNum >= info.numOfItem) {
        //maybe change this later to -1?
        if(info.rightSibling == 0) return -1;
        slotNum = 0;
        largeInt nextPageNum = info.rightSibling;
        if(fileHandle->readPage(nextPageNum,page)) return -1;
        return getNextEntry(rid,key);
    }
     //else, check the range of key in which we should get the entry
    int comparison = 1;
    if(highKey == nullptr) {
    }
    else{
        if(attr.type == TypeInt) {
            DataEntry entry;
            memcpy(&entry, (char*)page+1+sizeof(LeafInfo)+((slotNum)*sizeof(DataEntry)), sizeof(DataEntry));
            largeInt _key;
            memcpy(&_key, highKey,4);
            if(_key == entry.offset) comparison = 0;
            if(_key > entry.offset) comparison = 1;
            if(_key < entry.offset) comparison = -1;
        }else if(attr.type == TypeReal) {
            DataEntry entry;
            memcpy(&entry, (char*)page+1+sizeof(LeafInfo)+((slotNum)*sizeof(DataEntry)), sizeof(DataEntry));
            largeInt _key;
            memcpy(&_key,highKey,4);
            if(_key == entry.offset) comparison = 0;
            if(_key > entry.offset) comparison = 1;
            if(_key < entry.offset) comparison = -1;
        }else if(attr.type == TypeVarChar) {
                            //use varchar offset to get the value and use that for comparison
            largeInt varcharsize;
            memcpy(&varcharsize,highKey,4);
            char _key[varcharsize+1];
            memcpy(&_key,(char*)highKey+4,varcharsize);
            _key[varcharsize] = '\0';
            DataEntry entry;
            memcpy(&entry, (char*)page+1+sizeof(LeafInfo)+((slotNum)*sizeof(DataEntry)), sizeof(DataEntry));
            largeInt varcharsize2;
            memcpy(&varcharsize2,(char*)page+entry.offset,4);
            char val[varcharsize2+1];
            memcpy(&val,(char*)page+entry.offset+4,varcharsize2);
            val[varcharsize2]='\0';
            comparison = strcmp(_key,val);
        }
    }
    if(comparison < 0 || ((comparison == 0) && (!highKeyInclusive))) return -1;

    DataEntry entry;
    memcpy(&entry,(char*)page+1+sizeof(LeafInfo)+slotNum*sizeof(DataEntry), sizeof(DataEntry));
    rid.pageNum = entry.rid.pageNum;
    rid.slotNum = entry.rid.slotNum;

    if(attr.type == TypeVarChar) {
        largeInt tempkeylen;
        memcpy(&tempkeylen, (char*)page+entry.offset, 4);
        memcpy(key, &tempkeylen, 4);
        memcpy((char*)key+4,(char*)page+entry.offset+4,tempkeylen);
    }else{
        memcpy(key, &(entry.offset), 4);
    }

    slotNum+=1;
    return 0;
}


   //  void* newPage = malloc(PAGE_SIZE);
   //  fileHandle->readPage(2,newPage);
   //  NodeInfo header1;
   //  memcpy(&header1, (char*)newPage+1, sizeof(NodeInfo));
   //  //cout << "header1 numofitem is " << header1.numOfItem << endl;

   //  //cout << "past test" << endl;

   //  LeafInfo header;
   //  memcpy(&header, (char*)page+1, sizeof(LeafInfo));
   //  //cout << "header numofitem is " << header.numOfItem << endl;

   //  //LeafInfo header = im->getLeafHeader(page);


   //  //cout << "before if statement1" << endl;
   //  // If we have run off the end of the page, jump to the next one
   //  if (slotNum >= header.numOfItem)
   //  {
   //      // If there is no next page, return EOF
   //      if (header.rightSibling == 0)
   //          return -1;
   //      slotNum = 0;
   //      fileHandle->readPage(header.leftSibling, page);

   //      return getNextEntry(rid, key);
   //  }



   //  //cout << "before comapre statement" << endl;
   //  // If highkey is null, always carry on
   //  // Otherwise, carry on only if highkey is greater than the current key
   //  //int cmp = highKey == NULL ? 1 : im->compareLeaf(attr, highKey, page, slotNum);
   //  if (cmp == 0 && !highKeyInclusive)
   //      return -1;
   //  if (cmp < 0)
   //      return -1;

   //  //cout << "before memcpy statement" << endl;
   //  // Grab the data entry, grab its rid
   //  DataEntry entry;
   //  memcpy(&entry, (char*)page + 1 +sizeof(LeafInfo) + slotNum *sizeof(DataEntry), sizeof(DataEntry));

   //  rid.pageNum = entry.rid.pageNum;
   //  rid.slotNum = entry.rid.slotNum;
   //  // grab its key
   // //cout << "before if statement" << endl;

   //  if (attr.type == TypeInt)
   //      memcpy(key, &(entry.offset), INT_SIZE);
   //  else if (attr.type == TypeReal)
   //      memcpy(key, &(entry.offset), REAL_SIZE);
   //  else
   //  {
   //      int len;
   //      memcpy(&len, (char*)page + entry.offset, VARCHAR_LENGTH_SIZE);
   //      memcpy(key, &len, VARCHAR_LENGTH_SIZE);
   //      memcpy((char*)key + VARCHAR_LENGTH_SIZE, (char*)page + entry.offset + VARCHAR_LENGTH_SIZE, len);
   //  }
   //  //cout << "after else statement" << endl;
   //  // increment slotNum for the next call to getNextEntry
   //  slotNum++;
// return SUCCESS;
// }

// int IndexManager:: getStartPageNum(IXFileHandle &ixfileHandle, int32_t rootPageNum){

//     //allocate space for the meta page
//     void *metaPage = malloc(PAGE_SIZE);
//     if(metaPage == NULL)
//         return -1;

//     //readPage
//     if(ixfileHandle.readPage(0, metaPage)){
//         free(metaPage);
//         return -1;
//     }

//     uint32_t header;
//     memcpy(&header, metaPage, sizeof(leafHeader));
//     free(metaPage);
//     rootPageNum = header;
//     return 0;
// }

int IndexManager::searchTree(IXFileHandle &fh, Attribute attr, const void *key, largeInt currentPageNum, largeInt &finalPageNum){
    //return 2;
    void *pageData = malloc(PAGE_SIZE);

    //read the current page
    if(fh.readPage(currentPageNum, pageData)){
        free (pageData);
        return -1;
    }

    //obtain the type of the node
    char nodeType;
    memcpy(&nodeType, pageData, sizeof(char));

    //if the node is a leaf
    if(nodeType == 1){
        //cout << "we are at the leaf page and leaf page num is " << currentPageNum << endl;
        finalPageNum = currentPageNum;
        free(pageData);
        return 0;
    }

    int32_t nextNode = getChildPage(pageData, attr, key);
    free(pageData);

    return searchTree(fh, attr, key, nextNode, finalPageNum);

}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
    void* pageData = malloc(PAGE_SIZE);
    ixfileHandle.readPage(0,pageData);
    int root;
    memcpy(&root, pageData, sizeof(int32_t));
    // cout<<"root: "<<root<<endl;
    free(pageData);

    cout<<"{";
    recursivePrint(ixfileHandle, root, attribute, " ");
    cout << endl << "}" << endl;
}
// void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
//     void* pageData = malloc(PAGE_SIZE);
//     int root;
//     memcpy(&root, pageData, sizeof(uLargeInt));
//     free(pageData);

//     cout<<"{";
//     recursivePrint(ixfileHandle, root, attribute, " ");
//     cout << endl << "}" << endl;
// }

void IndexManager::recursivePrint(IXFileHandle ixfileHandle, int page, const Attribute &attribute, string space) const {
    void* pageData = malloc(PAGE_SIZE);
    ixfileHandle.readPage(page, pageData);
    // cout<<"pageNum "<<page<<endl;
    //first bit on page, if type==1 --> leaf
    int type;
    // cout<<"segfault memcpy here?"<<endl;
    memcpy(&type, (char*) pageData, 1);
    // cout<<"type"<<type<<endl;
    if (type==1){
        // cout<<"printLeaf"<<endl;
        printLeaf(pageData, attribute);
    }
    else{
        // cout<<"printInternal"<<endl;
        printInternal(ixfileHandle, pageData, attribute, space);
    }
    free(pageData);
}

// void IndexManager::recursivePrint(IXFileHandle ixfileHandle, int page, const Attribute &attribute, string space) const {
//     void* pageData = malloc(PAGE_SIZE);
//     ixfileHandle.readPage(page, pageData);

//     //first bit on page, if type==1 --> leaf
//     int type;
//     memcpy(&type, pageData, 1);
//     cout<<"type "<<type<<endl;
//     if (type==1){
//         cout<<"printleaf"<<endl;
//         printLeaf(pageData, attribute);
//     }
//     else{
//         cout<<"printInternal"<<endl;
//         printInternal(ixfileHandle, pageData, attribute, space);
//     }
//     free(pageData);
// }

void IndexManager::printInternal(IXFileHandle ixfileHandle, const void* pageData, const Attribute &attribute, string space) const {
    NodeInfo info;
    memcpy(&info, (char*)pageData+sizeof(char), sizeof(NodeInfo));
    // cout<<"infoEntriesnum"<<info.numOfItem<<endl;
    cout << "\n" << space << "\"keys\":[";
        for (int i=0; i<info.numOfItem; i++){
        // cout<<"enters the for loop?"<<endl;
            if (i!=0){
                cout<<",";
            }  
            Index entry;
            memcpy(&entry, (char*)pageData + 1 + sizeof(NodeInfo) + i * sizeof(Index), sizeof(Index));
            if (attribute.type==TypeInt || attribute.type==TypeReal){
            // cout<<"int/real"<<endl;
                cout<<""<<entry.offset;
            }
            else{
                int length;
            // cout<<"after memcpy 1"<<endl;
                memcpy(&length, (char*)pageData + entry.offset, VARCHAR_LENGTH_SIZE);
            // cout<<"after memcpy 2"<<endl;
                char varchar[length+1];
                varchar[length] = '\0';
                memcpy(&varchar, (char*)pageData + entry.offset + VARCHAR_LENGTH_SIZE, length);
                cout << varchar;
            }
        }
        cout << "],\n" << space << "\"children\":[\n" << space;

        for (int i=0; i<=info.numOfItem; i++){
            if (i==0){
                cout<<"{";
            // cout<<"segfault here?"<<endl;
                recursivePrint(ixfileHandle, info.childPageNum, attribute, space);
            // cout<<"segfault here5"<<endl;
                cout << "}";
            }
            else{
                cout << ",\n" << space;
                Index entry2;
                memcpy(&entry2, (char*)pageData + 1 + sizeof(NodeInfo) + (i-1) * sizeof(Index), sizeof(Index));
                cout << "{";
            // cout<<"segfault here2?"<<endl;
                recursivePrint(ixfileHandle, entry2.childPageNum, attribute, space);
                cout << "}";
            }
        }
        cout << "\n" << space << "]";
}
// void IndexManager::printInternal(IXFileHandle ixfileHandle, const void* pageData, const Attribute &attribute, string space) const {
//     NodeInfo info;
//     memcpy(&info, (char*)pageData+1, sizeof(NodeInfo));

//     cout << "\n" << space << "\"keys\":[";
//         for (int i=0; i<info.numOfItem; i++){
//             if (i!=0)   cout<<",";
//             printInternal(ixfileHandle, pageData, attribute, space);

//             Index entry;
//             memcpy(&entry, (char*)pageData + 1 + sizeof(NodeInfo) + i * sizeof(Index), sizeof(Index));
//             int length;
//             memcpy(&length, (char*)pageData + entry.offset, VARCHAR_LENGTH_SIZE);
//             char varchar[length+1];
//             varchar[length] = '\0';
//             memcpy(varchar, (char*)pageData + entry.offset + VARCHAR_LENGTH_SIZE, length);
//             cout<<""<<varchar;
//         }
//         cout << "],\n" << space << "\"children\":[\n" << space;

//         for (int i=0; i<info.numOfItem; i++){
//             if (i==0){
//                 cout<<"{";
//                 recursivePrint(ixfileHandle, info.childPageNum, attribute, space);
//                 cout << "}";
//             }
//             else{
//                 cout << ",\n" << space;
//                 Index entry2;
//                 memcpy(&entry2, (char*)pageData + 1 + sizeof(NodeInfo) + (i-1) * sizeof(Index), sizeof(Index));
//                 cout << "{";
//                 recursivePrint(ixfileHandle, entry2.childPageNum, attribute, space);
//                 cout << "}";
//             }
//         }
//         cout << "\n" << space << "]";
// }

void IndexManager::printLeaf(void* pageData, const Attribute &attribute) const {
    LeafInfo header;// = getLeafHeader(pageData);
    // cout<<"leafheader " << header.numOfItem<<endl;
    bool beginning = true;
    vector<RID> key_info;
    int comparison;
    void* key = NULL;
    if (attribute.type != TypeVarChar)
        key = malloc (INT_SIZE);
    // cout<<"beginning header entriesNumber"<<header.entriesNumber<<endl;
    cout<<"\"keys\":[";

        for (int i=0; i<=header.numOfItem; i++){
            DataEntry data;
            memcpy(&data, (char*)pageData+1+sizeof(LeafInfo)+i*sizeof(DataEntry), sizeof(DataEntry));
            if (beginning && i<=header.numOfItem){
            // cout<<"in first if statment";
                key_info.clear();
                beginning = false;
                if (attribute.type==TypeInt || attribute.type==TypeReal){
                    memcpy(key, &data.offset, 4);
                }
                else{
                    int length;
                    memcpy(&length, (char*)pageData+data.offset, VARCHAR_LENGTH_SIZE);
                    free(key);
                    key = malloc(1+length+VARCHAR_LENGTH_SIZE);
                    memcpy(key, &length, VARCHAR_LENGTH_SIZE);
                    memcpy((char*)key+VARCHAR_LENGTH_SIZE, (char*)pageData+data.offset+VARCHAR_LENGTH_SIZE, length);
                    memset((char*)key + VARCHAR_LENGTH_SIZE + length, 0, 1);
                }
            }

            // if (compareLeaf(attribute, key, pageData, i)==0 and i<header.numOfItem){
            //     key_info.push_back(data.rid);
            // }
            if (attribute.type==TypeInt){
            // cout<<"1"<<endl;
                int intKey;
                memcpy(&intKey, key, INT_SIZE);
                if (intKey==data.offset)    comparison=0;
                if (intKey>data.offset)     comparison=1;
                if (intKey<data.offset)     comparison=-1; 
            // cout<<"comparison " << comparison << endl;  
            }
            else if(attribute.type==TypeReal){
            // cout<<"2"<<endl;
                int realKey;
                memcpy(&realKey, key, REAL_SIZE);
                if (realKey==data.offset)   comparison=0;
                if (realKey>data.offset)    comparison=1;
                if (realKey<data.offset)    comparison=-1;
            }
            else{
            // cout<<"3"<<endl;
                int keySize;
                memcpy(&keySize, key, VARCHAR_LENGTH_SIZE);
                char keyText[keySize+1];
                keyText[keySize] = '\0';
                memcpy(keyText, (char*)key + VARCHAR_LENGTH_SIZE, keySize);

                int sizeOfData;
                memcpy(&sizeOfData, (char*)pageData + data.offset, VARCHAR_LENGTH_SIZE);
                char dataText[sizeOfData+1];
                dataText[sizeOfData]='\0';
                memcpy(dataText, (char*)pageData+data.offset, sizeOfData);

                comparison = strcmp(keyText,dataText);
            }
        // cout<<"comparison: "<<comparison<<endl;
        // cout<<"iteration: "<<i<<endl;
        // cout<<"header.entriesNumber "<< header.numOfItem<<endl;
            if (comparison==0 && i<header.numOfItem){
            // cout<<"inside push_back if";
                key_info.push_back(data.rid);
            // cout<<"keyinfo"<<key_info[i].pageNum<<endl;
            }
            else if (i!=0){
                cout<<"\"";
                if (attribute.type==TypeInt or attribute.type==TypeReal){
                // cout<<""<<key;
                    memcpy(key, &data.offset, 4);
                }
                else {
                    cout<<(char*)key+4;
                    int length;
                    memcpy(&length, (char*)pageData+data.offset, VARCHAR_LENGTH_SIZE);
                    free(key);

                    key=malloc(length+5);
                    memcpy(key, &length, VARCHAR_LENGTH_SIZE);
                    memcpy((char*)key+VARCHAR_LENGTH_SIZE, (char*)pageData+data.offset+VARCHAR_LENGTH_SIZE, length);
                    memset((char*)key+VARCHAR_LENGTH_SIZE+length, 0, 1);
                }
                cout <<"\n\tchildren:[";
                    for (int j=0; j<key_info.size(); j++){
                        if (j!=0){
                            cout<<",";
                        }
                        cout<<"("<<key_info[j].pageNum<<","<<key_info[j].slotNum<<")";
                    }
                    cout <<"]\"";
key_info.clear();
key_info.push_back(data.rid);
}
        // cout<<"key_info"<<key_info.size()<<endl;
cout<<"]}";
        // free(key);
}
}
// void IndexManager::printLeaf(void* pageData, const Attribute &attribute) const {
//     LeafInfo header = getLeafHeader(pageData);
//     bool beginning = true;
//     vector<RID> key_info;
//     void* key = NULL;
//     if (attribute.type != TypeVarChar)
//         key = malloc (INT_SIZE);

//     cout<<"\"keys\":[";

//         for (int i=0; i<header.numOfItem; i++){
//             DataEntry data;
//             memcpy(&data, (char*)pageData+1+sizeof(LeafInfo)+i*sizeof(DataEntry), sizeof(DataEntry));
//             if (beginning && i<header.numOfItem){
//                 beginning = false;
//                 int length;
//                 memcpy(&length, (char*)pageData+data.offset, VARCHAR_LENGTH_SIZE);
//                 free(key);
//                 key = malloc(1+length+VARCHAR_LENGTH_SIZE);
//                 memcpy(key, &length, VARCHAR_LENGTH_SIZE);
//                 memcpy((char*)key+VARCHAR_LENGTH_SIZE, (char*)pageData+data.offset+VARCHAR_LENGTH_SIZE, length);
//                 memset((char*)key + VARCHAR_LENGTH_SIZE + length, 0, 1);
//             }

//             if (compareLeaf(attribute, key, pageData, i)==0 and i<header.numOfItem){
//                 key_info.push_back(data.rid);
//             }
//             else if (i!=0){
//                 cout<<"\"";
//                 if (attribute.type==TypeInt or attribute.type==TypeReal){
//                     cout<<""<<key;
//                     memcpy(key, &data.offset, 4);
//                 }
//                 else {
//                     cout<<(char*)key+4;
//                     int length;
//                     memcpy(&length, (char*)pageData+data.offset, VARCHAR_LENGTH_SIZE);
//                     free(key);

//                     key=malloc(length+5);
//                     memcpy(key, &length, VARCHAR_LENGTH_SIZE);
//                     memcpy((char*)key+VARCHAR_LENGTH_SIZE, (char*)pageData+data.offset+VARCHAR_LENGTH_SIZE, length);
//                     memset((char*)key+VARCHAR_LENGTH_SIZE+length, 0, 1);
//                 }
//                 cout <<":[";
//                     for (int j=0; j<key_info.size(); j++){
//                         if (j!=0){
//                             cout<<",";
//                         }
//                         cout<<"("<<key_info[j].pageNum<<","<<key_info[j].slotNum<<")";
//                     }
//                     cout <<"]\"";
// key_info.clear();
// key_info.push_back(data.rid);
// }
// cout<<"]}";
// free(key);
// }
// }


// LeafInfo IndexManager::getLeafHeader(const void* pageData) const{

//     LeafInfo header;
//     int offset = sizeof(char);
//     memcpy(&header, (char*)pageData+offset, sizeof(LeafInfo));
//     return header;
// }

// int IndexManager::compareLeaf(const Attribute attribute, const void *key, const void* pageData, int slotNum) const{
//     DataEntry dataEntry;
//     memcpy(&dataEntry, (char*)pageData+1+sizeof(LeafInfo)+slotNum*sizeof(DataEntry), sizeof(DataEntry));
//     if (attribute.type==TypeInt){
//         int intKey;
//         memcpy(&intKey, key, INT_SIZE);
//         if(intKey == dataEntry.offset) return 0;
//         if(intKey > dataEntry.offset)  return 1;
//         if(intKey < dataEntry.offset)  return -1;
//     }
//     else if(attribute.type==TypeReal){
//         int realKey;
//         memcpy(&realKey, key, REAL_SIZE);
//         if(realKey == dataEntry.offset) return 0;
//         if(realKey > dataEntry.offset)  return 1;
//         if(realKey < dataEntry.offset)  return -1;
//     }
//     else{
//         int sizeOfKey;
//         memcpy(&sizeOfKey, key, VARCHAR_LENGTH_SIZE);
//         char keyText[sizeOfKey+1];
//         keyText[sizeOfKey] = '\0';
//         memcpy(keyText, (char*)key+VARCHAR_LENGTH_SIZE, sizeOfKey);

//         int offset = dataEntry.offset;
//         int sizeOfData;
//         memcpy(&sizeOfData, (char*)pageData+offset, VARCHAR_LENGTH_SIZE);
//         char dataText[sizeOfData+1];
//         dataText[sizeOfData] = '\0';
//         memcpy(dataText, (char*)pageData+offset, sizeOfData);

//         if(keyText == dataText) return 0;
//         if(keyText > dataText)  return 1;
//         if(keyText < dataText)  return -1;
//     }
//     return 0;
// }

RC IndexManager::insertUtil(IXFileHandle &ixfileHandle,const Attribute &attribute, const void* key, const RID &rid, TempNode &node, largeInt rootPageNum) {
    //read in root page 
    void* page = malloc(PAGE_SIZE);
    if(ixfileHandle.readPage(rootPageNum,page)) {
        cout << "read page failed" << endl;
        return -1;
    }
    //find out child page(to descend to leaves)
    char nodeType;
    memcpy(&nodeType,page,sizeof(char));

    //cout << "insert util root page num is " << rootPageNum << endl;

    //if leaf node, then find a place to insert.

    if(nodeType == 1) {
        //cout << "inside leaf segment" << endl;
        //INSERT INTO LEAF
        int result = InsertLeaf(page,attribute,rid,key);
        if(result != 0) {
            LeafInfo leaf;
            memcpy(&leaf,(char*)page+1,sizeof(LeafInfo));
            //SPLIT LEAF START
            //SPLIT LEAF USES rootagenum,page, node
            void* splitleaf = calloc(PAGE_SIZE,1);
            char type = 1;
            memcpy((char*)splitleaf,&type,sizeof(char));
            LeafInfo splitleafinfo;
            splitleafinfo.leftSibling = rootPageNum;
            splitleafinfo.rightSibling = leaf.rightSibling;
            splitleafinfo.numOfItem = 0;
            splitleafinfo.emptySlotStart = PAGE_SIZE;
            memcpy((char*)splitleaf+1,&splitleafinfo,sizeof(LeafInfo));
            largeInt splitpagenum = ixfileHandle.getNumberOfPages();
            leaf.rightSibling = splitpagenum;
            memcpy((char*)page+1,&leaf,sizeof(LeafInfo));

            int splitsize=0;
            int slotNum=0;
            int oldsize=0;
            for(int i=0;i<leaf.numOfItem;i++) {
                //find the first key of split leaf to copy up to parent
                int tempoldsize = sizeof(DataEntry);
                DataEntry entry;
                memcpy(&entry,(char*)page+1+sizeof(LeafInfo)+i*sizeof(DataEntry),sizeof(DataEntry));
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
                    tempoldsize+=4;
                    tempoldsize+=tempkeylen;
                }
                oldsize = tempoldsize;
                //getkeylengthleaf ends here. basically key len is size of dataentry except for varchar
                splitsize += tempoldsize;
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
                deleteLeaf(page,attribute,ridtomove,keytomove);
                //insert ends here 
                //delete old entry from old leaf
                //delete ends here
            }
            free(keytomove);

            int comparison = 0;
            if(attribute.type == TypeInt) {
                DataEntry entry;
                memcpy(&entry, (char*)page+1+sizeof(LeafInfo)+((slotNum)*sizeof(DataEntry)), sizeof(DataEntry));
                largeInt _key;
                memcpy(&_key,key,4);
                if(_key == entry.offset) comparison = 0;
                if(_key > entry.offset) comparison = 1;
                if(_key < entry.offset) comparison = -1;
            }else if(attribute.type == TypeReal) {
                DataEntry entry;
                memcpy(&entry, (char*)page+1+sizeof(LeafInfo)+((slotNum)*sizeof(DataEntry)), sizeof(DataEntry));
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
                memcpy(&entry, (char*)page+1+sizeof(LeafInfo)+((slotNum)*sizeof(DataEntry)), sizeof(DataEntry));
                largeInt varcharsize2;
                memcpy(&varcharsize2,(char*)page+entry.offset,4);
                char val[varcharsize2+1];
                memcpy(&val,(char*)page+entry.offset+4,varcharsize2);
                val[varcharsize2]='\0';
                comparison = strcmp(_key,val);
            }

            if(comparison <= 0) {
                cout<<"1"<<endl;
                if(InsertLeaf(page,attribute,rid,key) == -1) {
                    free(splitleaf);
                    cout << "insert leaf failed" << endl;
                    return -1;
                }
            }else{
                //cout<<"2"<<endl;
                if(InsertLeaf(splitleaf,attribute,rid,key) == -1) {
                    free(splitleaf);
                    cout << "insert leaf split failed" << endl;
                    return -1;
                }
            }

            //everything went through, just write old leaf page and append new leaf page
            //cout<<"3"<<endl;
            if(ixfileHandle.writePage(rootPageNum,page)) {
                free(splitleaf); 
                cout << "write page failed" << endl;
                return -1;
            }
            //cout<<"4"<<endl;
            if(ixfileHandle.appendPage(splitleaf)) {
                free(splitleaf);
                cout << "append splitleaf failed" << endl;
                return -1;
            }
            free(splitleaf);
            free(page);
            return 0;
        }else{
            //cout<<"5"<<endl;
            //cout << "page num to write to is " << rootPageNum << endl;
            if(ixfileHandle.writePage(rootPageNum,page)) {
                cout << "write page failed" << endl;
                return -1;
            }

            //TEST
            void* temppage1 = malloc(PAGE_SIZE);
            ixfileHandle.readPage(2,temppage1);
            LeafInfo tempinfo;
            memcpy(&tempinfo,(char*)temppage1+1,sizeof(LeafInfo));
            //cout << "in insert, page 2 page has " << tempinfo.numOfItem << " items" << endl;
            
            for(int i=0;i<tempinfo.numOfItem;i++) {
                DataEntry entry;
                memcpy(&entry,(char*)temppage1+1+sizeof(LeafInfo)+i*sizeof(DataEntry), sizeof(DataEntry));
                //cout << "entry offset is " << entry.offset << " and rid is " << entry.rid.pageNum << ", " << entry.rid.slotNum << endl;
            }

            free(temppage1);

            //

            LeafInfo temp;
            memcpy(&temp, (char*)page+1, sizeof(LeafInfo));
            //cout << "num of items is " << temp.numOfItem << endl;

            free(node.key);
            free(page);
            node.key = nullptr;
            node.pageNum = -1;
            return 0;  
        }
    }else{
        //cout << "inside index segment" << endl;

        //if not leaf, get child node and recursively insert
        largeInt childPageNum = getChildPage(page,attribute,key);
        //cout << "childpagenum in index is " << childPageNum << endl;

        if(insertUtil(ixfileHandle,attribute,key,rid,node,childPageNum) == -1) {
            cout << "recursive insertutil failed" << endl;
            return -1;
        }
        //inserted entry into correct leaf node
        if(node.pageNum == -1 && node.key == nullptr) {
            return 0;
        }
    //if temp not reset, then it means leaf split, and need to copy up the first index of the second node from split
        page = malloc(PAGE_SIZE);
        if(ixfileHandle.readPage(rootPageNum,page)) {
            cout << "readpage failed" << endl;
            return -1;
        }
        int result = InsertIndex(page,attribute,node);
        if(result == -1) {
            cout << "insertindex failed, in split" << endl;
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
                int tempoldsize = sizeof(Index);
                Index index;
                memcpy(&index,(char*)page+1+sizeof(NodeInfo)+i*sizeof(Index),sizeof(Index));
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
                }
                oldsize = tempoldsize;
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
            char type = 0;
            memcpy((char*)splitindexpage,&type,sizeof(char));
            NodeInfo newInfo;
            newInfo.numOfItem = 0;
            newInfo.emptySlotStart = PAGE_SIZE;
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

                TempNode nodeToModify;
                nodeToModify.key = keytomove;
                nodeToModify.pageNum = pagetomove;
                InsertIndex(splitindexpage,attribute,nodeToModify);
                //delete internal uses attribute, keytomove,page
                deleteIndex(page,attribute,nodeToModify);
            }
            free(keytomove);
            TempNode keytempnode;
            keytempnode.key = middlekey;
            //delete internal with attribute,middlekey,page
            deleteIndex(page,attribute,keytempnode);
            //compareslot

            int comparison = 0;
            if(attribute.type == TypeInt) {
                Index index;
                memcpy(&index, (char*)page+1+sizeof(NodeInfo)+slotNum*sizeof(Index), sizeof(Index));
                largeInt _key;
                memcpy(&_key,node.key,4);
                if(_key == index.offset) comparison = 0;
                if(_key > index.offset) comparison = 1;
                if(_key < index.offset) comparison = -1;
            }else if(attribute.type == TypeReal) {
                Index index;
                memcpy(&index, (char*)page+1+sizeof(NodeInfo)+slotNum*sizeof(Index), sizeof(Index));
                largeInt _key;
                memcpy(&_key,node.key,4);
                if(_key == index.offset) comparison = 0;
                if(_key > index.offset) comparison = 1;
                if(_key < index.offset) comparison = -1;
            }else if(attribute.type == TypeVarChar) {
                        //use varchar offset to get the value and use that for comparison
                largeInt varcharsize;
                memcpy(&varcharsize,node.key,4);
                char _key[varcharsize+1];
                memcpy(&_key,(char*)node.key+4,varcharsize);
                _key[varcharsize] = '\0';

                Index index;
                memcpy(&index, (char*)page+1+sizeof(NodeInfo)+slotNum*sizeof(Index), sizeof(Index));
                largeInt varcharsize2;
                memcpy(&varcharsize2,(char*)page+index.offset,4);
                char val[varcharsize2+1];
                memcpy(&val,(char*)page+index.offset+4,varcharsize2);
                val[varcharsize2]='\0';
                comparison = strcmp(_key,val);
            }
            if(comparison < 0) {
                int result = InsertIndex(page,attribute,node);
                if(result != 0) {
                    cout << "insert index failed" << endl;
                    free(page);
                    free(splitindexpage);
                    return -1;
                }
            }else{
                int result = InsertIndex(splitindexpage,attribute,node);
                if(result != 0) {
                    cout << "insert index failed" << endl;
                    free(splitindexpage);
                    free(page);
                    return -1;
                }
            }

            //write changes to disk
            if(ixfileHandle.appendPage(splitindexpage)) {
                free(splitindexpage);
                free(page);
                cout << "append page failed" << endl;
                return -1;
            }

            if(ixfileHandle.writePage(rootPageNum,page)) {
                free(splitindexpage);
                free(page);
                cout << "write page failed" << endl;
                return -1;
            }

            free(splitindexpage);
            free(node.key);
            node.key = middlekey;
            node.pageNum = splitpagenum;

            largeInt tempPageRootNum;
            void* temprootpage = malloc(PAGE_SIZE);
            if(ixfileHandle.readPage(0,temprootpage)) {
                cout << "read page failed" << endl;
                free(page);
                return -1;
            }
            memcpy(&tempPageRootNum,(char*)temprootpage,sizeof(largeInt));
            free(temprootpage);

            if(tempPageRootNum == rootPageNum) {
                //we just split the root so update rootpage 

                void* splitrootpage = calloc(PAGE_SIZE,1);
                char type = 0;
                memcpy((char*)splitrootpage,&type,sizeof(char));
                NodeInfo splitrootinfo;
                splitrootinfo.numOfItem = 0;
                splitrootinfo.emptySlotStart = PAGE_SIZE;
                splitrootinfo.childPageNum = rootPageNum;
                memcpy((char*)splitrootpage+1,&splitrootinfo,sizeof(NodeInfo));
                if(InsertIndex(splitrootpage,attribute,node) == -1) {
                    cout << "insert index failed in split index" << endl;
                } 
                largeInt splitrootpagenum = ixfileHandle.getNumberOfPages();
                //change root num
                if(ixfileHandle.appendPage(splitrootpage)) {
                    cout << "append splitrootpage failed" << endl;
                    free(page);
                    return -1;
                }
                void* metaPage = malloc(PAGE_SIZE);
                memcpy((char*)metaPage,&splitrootpagenum,sizeof(largeInt));
                if(ixfileHandle.writePage(0,metaPage)) {
                    cout << "write meta page failed" << endl;
                    free(page);
                    return -1;
                }

                free(metaPage);
                free(splitrootpage);
                free(node.key);
                node.key = nullptr;
                node.pageNum = -1;
            }
            free(page);
            page = nullptr;
            return 0;
        }else{
            //insert into internal success
            if(ixfileHandle.writePage(rootPageNum,page)) {
                free(node.key);
                node.key = nullptr;
                node.pageNum = -1;
                free(page);
                cout << "write page failed" << endl;
                return -1;
            }
            free(node.key);
            node.key = nullptr;
            node.pageNum = -1;
            free(page);
            return 0;
        }
    }
    return 0;
}



RC IndexManager::InsertIndex(void* page, const Attribute& attr, TempNode& nodeToInsert) {

    NodeInfo info;
    memcpy(&info,(char*)page+1,sizeof(NodeInfo));
    largeInt keysize = sizeof(info);
    if(attr.type == TypeVarChar) {
        largeInt temp_len;
        memcpy(&temp_len,nodeToInsert.key,4);
        keysize += (temp_len+4);
    }
    largeInt currBytes = 1+sizeof(NodeInfo)+info.numOfItem*sizeof(Index);
    largeInt freeSpace = info.emptySlotStart  - currBytes;

    if(freeSpace < keysize) return -1;
    int slotNum = 0;
    for(int i=0;i<info.numOfItem;i++) {
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
    unsigned entryEnd = 1+sizeof(NodeInfo)+ info.numOfItem*sizeof(Index);

    memmove((char*)page+entryBegin+sizeof(Index), (char*)page+entryBegin, entryEnd-entryBegin);

    Index indexToInsert;
    indexToInsert.childPageNum = nodeToInsert.pageNum;
    if(attr.type == TypeVarChar) {
        largeInt temp;
        memcpy(&temp,nodeToInsert.key,4);
        indexToInsert.offset = info.emptySlotStart-4-temp;
        memcpy((char*)page+indexToInsert.offset, nodeToInsert.key, temp+4);
        info.emptySlotStart = indexToInsert.offset;
    }else{
        memcpy(&(indexToInsert.offset),nodeToInsert.key,4);
    }
    info.numOfItem++;
    //write in the udpated values for the new index and the directory
    memcpy((char*)page+1,&info,sizeof(NodeInfo));
    memcpy((char*)page+1+sizeof(NodeInfo)+slotNum*sizeof(Index), &indexToInsert, sizeof(Index));
    return 0;
}

RC IndexManager::InsertLeaf(void* page,const Attribute& attr,const RID& rid, const void* key) {
     //INSERT INTO LEAF
    //cout << "inserting into leaf" << endl;
    LeafInfo info;
    memcpy(&info, (char*)page+1,sizeof(LeafInfo));
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
    largeInt currBytes = 1+sizeof(LeafInfo)+info.numOfItem*sizeof(DataEntry);
    largeInt freeSpace = info.emptySlotStart - currBytes;
    if(freeSpace < sizeofkey) {
        cout << "not enough space" << endl;
        return -1;
    }
    int slotNum=0;
    for(int i=0;i<info.numOfItem;i++) {
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
    unsigned entryEnd = 1+sizeof(LeafInfo)+ info.numOfItem*sizeof(DataEntry);

            //make space for the new entry
    memmove((char*)page+entryBegin+sizeof(DataEntry), (char*)page+entryBegin, entryEnd-entryBegin);

    DataEntry entry;
    if(attr.type == TypeVarChar) {
        largeInt keysize;
        memcpy(&keysize,key,4);
        entry.offset = info.emptySlotStart - keysize-4;
        memcpy((char*)page+entry.offset, key, keysize+4);
        info.emptySlotStart = entry.offset;
    }else{
        memcpy(&(entry.offset),key,4);
    }
    entry.rid = rid;
    info.numOfItem++;
    memcpy((char*)page+1,&info,sizeof(LeafInfo));
    unsigned setentryoffset = 1+sizeof(LeafInfo)+slotNum*sizeof(DataEntry);
    memcpy((char*)page+setentryoffset,&entry,sizeof(DataEntry));
    //cout << "insertleaf return success" << endl;
    return 0;
}

RC IndexManager::deleteIndex(void* page, const Attribute& attr, TempNode& nodetoDelete) {
    //cout << "delete index" << endl;
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

    //unsigned entryBegin = 1+sizeof(NodeInfo) + slotNum*sizeof(Index);
    //unsigned entryEnd = 1+sizeof(NodeInfo)+ info.numOfItem*sizeof(Index);

    memmove((char*)page+1+sizeof(NodeInfo)+slotNum*sizeof(Index), (char*)page+1+sizeof(NodeInfo)+slotNum*sizeof(Index)+sizeof(Index), (1+sizeof(NodeInfo)+ info.numOfItem*sizeof(Index))-(1+sizeof(NodeInfo) + slotNum*sizeof(Index))-sizeof(Index));
    info.numOfItem--;
    if(attr.type == TypeVarChar) {
        largeInt len;
        largeInt offset = index.offset;
        memcpy(&len,(char*)page+index.offset,4);
        len+=4;
        //freespaceoffset = pagesize-emptyslotstart-1-sizeof(NodeInfo);
        memmove((char*)page+info.emptySlotStart+len,(char*)page+info.emptySlotStart,offset-info.emptySlotStart);
        info.emptySlotStart+=len;
        for(int i=0;i<info.numOfItem;i++) {
            Index index;
            memcpy(&index, (char*)page+1+sizeof(NodeInfo)+i*sizeof(Index),sizeof(Index));
            if(index.offset<offset) index.offset += len;
            memcpy((char*)page+1+sizeof(NodeInfo)+i*sizeof(Index),&index,sizeof(Index));
        }
    }
    memcpy((char*)page+1,&info,sizeof(NodeInfo));
    return 0;
}
RC IndexManager::deleteLeaf(void* page, const Attribute& attr, const RID& rid, const void* key){
    //cout << "deleting leaf" << endl;
    LeafInfo info;
    memcpy(&info,(char*)page+1,sizeof(LeafInfo));
    largeInt numOfItem = info.numOfItem;
    //cout << "num of item beginning is " << numOfItem << endl;

    int slotNum = 0;
    for(int i=0;i<info.numOfItem;i++) {
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
        if(comparison == 0) {
            //we've found a leaf with the same key
            //verify that the rid is the same, if so this is the leaf to delete
            DataEntry entryToDelete;
            memcpy(&entryToDelete,(char*)page+1+sizeof(LeafInfo)+i*sizeof(DataEntry), sizeof(DataEntry));
            if(rid.pageNum == entryToDelete.rid.pageNum && rid.slotNum == entryToDelete.rid.slotNum) {
                slotNum = i;
                break;
            }
        }
    }

    //cout << "deleteleaf slotnum and numofitem is " << slotNum << " " << numOfItem << endl;
    if(slotNum == numOfItem) return -1;


    DataEntry entry;
    memcpy(&entry,(char*)page+1+sizeof(LeafInfo)+slotNum*sizeof(DataEntry),sizeof(DataEntry));

    unsigned entryBegin = 1+sizeof(LeafInfo) + slotNum*sizeof(DataEntry);
    unsigned entryEnd = 1+sizeof(LeafInfo)+ info.numOfItem*sizeof(DataEntry);

    memmove((char*)page+entryBegin,(char*)page+entryBegin+sizeof(DataEntry), entryEnd-entryBegin-sizeof(DataEntry));

    if(attr.type == TypeVarChar) {
        largeInt len;
        largeInt offset = entry.offset;
        memcpy(&len,(char*)page+entry.offset,4);
        len+=4;
        //freespaceoffset = pagesize-emptyslotstart-1-sizeof(NodeInfo);
        memmove((char*)page+info.emptySlotStart+len,(char*)page+info.emptySlotStart, offset-info.emptySlotStart);
        info.emptySlotStart+=len;
        for(int i=0;i<info.numOfItem;i++) {
            DataEntry entry;
            memcpy(&entry, (char*)page+1+sizeof(LeafInfo)+i*sizeof(DataEntry),sizeof(DataEntry));
            if(entry.offset<offset) entry.offset += len;
            memcpy((char*)page+1+sizeof(NodeInfo)+i*sizeof(DataEntry),&entry,sizeof(DataEntry));
        }
        info.numOfItem--;
    }else{
        info.numOfItem--;
    }
    memcpy((char*)page+1,&info,sizeof(LeafInfo));
    return 0;
}


// POSSIBLE NODE STRUCTURE? type, header, entries and then if varchar, its stored from the end of page?


largeInt IndexManager::getChildPage(void* page, const Attribute& attribute, const void* key) {
    NodeInfo info;
    memcpy(&info, (char*)page+1, sizeof(NodeInfo));
    largeInt pageNum;
    int slotNum = 0;
    //find the index on the node that 
    for(int i=0;i<info.numOfItem;i++) {
        if(key == nullptr) break;
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






RC IX_ScanIterator::close()
{
    free(page);
    return SUCCESS;
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
