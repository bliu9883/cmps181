#include <sys/stat.h>
#include "rbfm.h"
#include <math.h>


RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

bool RecordBasedFileManager::fileExists(string fileName){
    struct stat stFileInfo;

    if (stat(fileName.c_str(), &stFileInfo) == 0)
        return true;
    else
        return false;
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    PagedFileManager* pfm=PagedFileManager::instance();

    if (pfm->createFile(fileName))
        return -1;

    
    void * page = calloc(PAGE_SIZE, 0);
    if (page == NULL)
        return -1;
    makeNewPage(page);


    FileHandle handle;
    if (pfm->openFile(fileName.c_str(), handle))
        return -1;
    if (handle.appendPage(page))
        return -1;
    pfm->closeFile(handle);

    free(page);

    return 0;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    PagedFileManager* pfm = PagedFileManager::instance();
    int result = pfm->destroyFile(fileName);
    return result;
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    PagedFileManager* pfm = PagedFileManager::instance();
    int result = pfm->openFile(fileName, fileHandle);
    return result;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    PagedFileManager* pfm = PagedFileManager::instance();
    int result = pfm->closeFile(fileHandle);
    return result;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    // first need to get total size of record data and null flag
    unsigned recordSize = getRecordSize(recordDescriptor);
    unsigned totalSizeNeeded = recordSize + sizeof(SlotRecord);
    bool foundFreePage = false;
    //get the current(last page) to try to insert record
    unsigned pageNum = fileHandle.getNumberOfPages()-1;
    void* page = malloc(PAGE_SIZE);
    fileHandle.readPage(pageNum, page);
    //check to see if lastPage has enough space for the actual record and slot record
    unsigned pageFreeSpace = getFreeSpaceInPage(page);

    //if current page has enough space, leave pageNum alone. Otherwise, find a page
    //that has enough free space
    // cout << "bkpoint1" << endl;
    if(pageFreeSpace < totalSizeNeeded) {    
        unsigned numOfPages = fileHandle.getNumberOfPages();
        //start from first page, read data into page and check to see if size works
        for(unsigned i=0;i<numOfPages;i++) {
            fileHandle.readPage(i, page);
            if(getFreeSpaceInPage(page) >= totalSizeNeeded) {
                //if free page found, set the pageNum, and bool to true
                pageNum = i;
                foundFreePage = true;
                break;
            }
        }
        //if none of the pages have free space, append new one.
    }

    // cout << "bkpoint2" << endl;

    //now we have the right pageNum to insert.
    //set up the slot directory stuff
    //If foundFreePage is false, pageNum+=1 for new page
    if(!foundFreePage) {
        makeNewPage(page);
    }


    //actually put in the record on the page, and update sd and sr
    SlotDir sd = getSlotDir(page);

    rid.pageNum = pageNum;
    rid.slotNum = sd.numOfRecords+1;

    SlotRecord sr = getSlotRecord(page, rid.slotNum);

    putRecordOnPage(page,recordDescriptor,data);

    // cout << "bkpoint3" << endl;

    //update slot record
    sr.len = recordSize;
    sr.recordStartLoc = sd.freeSpaceLoc;
    setSlotRecord(page, sr, rid.slotNum);

    //update slot dir
    sd.freeSpaceLoc+=recordSize;
    sd.numOfRecords+=1;
    setSlotDir(page,sd);

    // write the actual page
    if (foundFreePage)
    {
        cout << "writing" << endl;
        int result = fileHandle.writePage(pageNum, page);
        // if(result != 0) cout << "write failed " << endl;
            
    }else{
        // cout << "appending" << endl;
        int result = fileHandle.appendPage(page);
        // if(result != 0) cout << "append failed" << endl;

    }

    cout<<"# of pages: "<< fileHandle.getNumberOfPages()<<endl;

    free(page);
    return 0;

}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {

    void * page = malloc(PAGE_SIZE);
    if (fileHandle.readPage(1, page) == -1) {
        return -1;
    }

    //see if the page actually exists
    SlotDir sd = getSlotDir(page);
    if(sd.numOfRecords < rid.slotNum) {
        return -1;
    }


    // Gets the slot directory record entry data
    SlotRecord sr = getSlotRecord(page, rid.slotNum);

    // Retrieve the actual entry data
    pullRecordFromPage(page, sr, recordDescriptor, data);

    free(page);
    return 0;

}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
      // Parse the null indicator into an array
    int nullsize = ceil(recordDescriptor.size()/8);
    char nullinfo[nullsize];
    memcpy(&nullinfo, data, sizeof(nullinfo));
    
    // We've read in the null indicator, so we can skip past it now
    unsigned offset = nullsize;
    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        //cout << setw(10) << left << recordDescriptor[i].name << ": ";
        // If the field is null, don't print it
        if(isNullBitOne(nullinfo, i)) {
            cout << recordDescriptor[i].name << ":  " << "NULL" << "  "; continue;
        }

        if(recordDescriptor[i].type == TypeInt || recordDescriptor[i].type == TypeReal) {
            uint32_t val;
            memcpy(&val, (char*)data+offset, 4);
            offset += 4;
            cout << recordDescriptor[i].name << ":  " << val << "  ";
        }

        if(recordDescriptor[i].type == TypeVarChar) {
            uint32_t varsize;
            memcpy(&varsize, (char*)data+offset, 4);
            offset += 4;
            //write to string
            char* str = (char*)malloc(varsize + 1);
            memcpy(str, (char*)data+offset, varsize);
            //terminate c string in null char
            str[varsize] = '\0';
            offset += varsize;
            cout << recordDescriptor[i].name << ":  " << str << "  ";
            free(str);

        }

    }
    cout << endl;
    return 0;

}


unsigned RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor){
    unsigned nullBitSize = ceil(recordDescriptor.size()/8);
    unsigned fieldSize = 0;

    for (int i=0; i<recordDescriptor.size(); i++){

        AttrType type = recordDescriptor[i].type;

        if (type == TypeInt or type == TypeReal){
            fieldSize+=4;
        }
        else{
            int charLength = recordDescriptor[i].name.size();
            fieldSize = fieldSize + 4 + charLength;
        }
    }
    unsigned recordSize = nullBitSize + fieldSize;
    // cout <<"record size " << recordSize << endl;
    return recordSize;
}


void RecordBasedFileManager::makeNewPage(void* page) {
    //set up a slotdir for the page
    SlotDir sd;
    sd.freeSpaceLoc = 0;
    sd.numOfRecords = 0;
    setSlotDir(page, sd);
}

void RecordBasedFileManager::setSlotDir(void* page, const SlotDir& sd) {
    //copy the sd data into the (end of)page
    char* temp = (char*)page;
    temp = temp+PAGE_SIZE-sizeof(sd);
    memcpy(temp, &sd, sizeof(sd));
}
void RecordBasedFileManager::setSlotRecord(void* page, const SlotRecord& sr, unsigned recordNum){
    char* temp = (char*)page;
    temp = temp+PAGE_SIZE-sizeof(SlotDir)-(recordNum*sizeof(sr));
    memcpy(temp, &sr, sizeof(sr));
}
SlotDir RecordBasedFileManager::getSlotDir(void* page){
    SlotDir sd;
    char* temp = (char*)page;
    temp = temp+PAGE_SIZE-sizeof(sd);
    memcpy (&sd, temp, sizeof(sd));
    return sd;
}
SlotRecord RecordBasedFileManager::getSlotRecord(void* page, unsigned recordNum){
    SlotRecord sr;
    return sr;
}

void RecordBasedFileManager::putRecordOnPage(void* page, const vector<Attribute> &recordDescriptor, const void* data){
    SlotDir sd = getSlotDir(page);
    char* recStart = (char*)page + sd.freeSpaceLoc;
    //get null bit info
    unsigned nullSize = ceil(recordDescriptor.size()/8);
    unsigned numOfFields = recordDescriptor.size();
    char nullinfo[nullSize];
    memcpy(&nullinfo, data, sizeof(nullinfo));
    //set nullflag into record
    memcpy(recStart, &nullinfo, sizeof(nullinfo));
    //databegin is where we set actual data
    recStart += sizeof(nullinfo);
    char* fieldStart = recStart + (numOfFields*4);

    char* dataBegin = (char*)data + sizeof(nullinfo);
    unsigned fieldStartAddress = nullSize + (numOfFields*4);

    //iterate thru records, if NOT null, use memcpy
    for(int i=0;i<recordDescriptor.size();i++) {
        //if the field is not null, copy field data
        if(!isNullBitOne(nullinfo, i)) {

            //memcpy the correct values depending on field type
            //if its real type or int type
            if(recordDescriptor[i].type == TypeInt || recordDescriptor[i].type == TypeReal) {
                ///this copies the data to the actual field position
                memcpy(fieldStart,dataBegin, 4);
                dataBegin+=4;
                fieldStart+=4;
                fieldStartAddress+=4;
                memcpy(recStart,&fieldStartAddress,4);
                recStart+=4;
            }
            if(recordDescriptor[i].type == TypeVarChar) {
                unsigned varSize;
                //copy size of varchar
                memcpy(&varSize,dataBegin,4);
                dataBegin+=4;

                //fill data
                memcpy(fieldStart, dataBegin, varSize);
                fieldStart+=varSize;
                fieldStartAddress+=varSize;

                //fill ref.
                memcpy(recStart, &fieldStartAddress, 4);
                recStart+=4;

            }
        }
    }
}

void RecordBasedFileManager::pullRecordFromPage(void* page, const SlotRecord& sr, const vector<Attribute> &recordDescriptor, void* data) {

    char* recStart = (char*)page + sr.recordStartLoc;
    //get null bit info
    unsigned nullSize = ceil(recordDescriptor.size()/8);
    unsigned numOfFields = recordDescriptor.size();
    char nullinfo[nullSize];
    //write nullbit to data
    memcpy(data, recStart, sizeof(nullinfo));
    recStart += sizeof(nullinfo);


    char* dataBegin = (char*)data + sizeof(nullinfo);
    unsigned fieldStartAddress = nullSize + (numOfFields*4);
    //iterate thru the descriptor
    for(int i=0;i<recordDescriptor.size();i++) {
        //if the field is not null, copy field data
        if(!isNullBitOne(nullinfo, i)) {
            unsigned fieldOffset;
            memcpy(&fieldOffset, recStart, 4);
            memcpy(dataBegin, &fieldStartAddress, (fieldStartAddress-fieldOffset));
            dataBegin+=(fieldOffset-fieldStartAddress);
            fieldStartAddress+=(fieldOffset-fieldStartAddress);
            recStart+=4;

        }
    }
}


bool RecordBasedFileManager::isNullBitOne(char* nullflag, int i){
    int bit = i / 8;
    int bitmask  = 1 << (8 - 1 - (i % 8));
    if(nullflag[bit] & bitmask) return true;
    return false;
}


unsigned RecordBasedFileManager::getFreeSpaceInPage(void* page){
    SlotDir sd = getSlotDir(page);

    unsigned startSD = PAGE_SIZE-((sizeof(SlotRecord) * sd.numOfRecords)+sizeof(sd));
    cout << "num of record is " << sd.numOfRecords << endl;
    cout << "size of sd is " << sizeof(sd) << endl;
    cout << "startSD is " << startSD << " and page size is " << PAGE_SIZE << endl;
    cout << "sd.freespaceloc is at " << sd.freeSpaceLoc << endl;
    unsigned freeSpace = startSD - sd.freeSpaceLoc;
    cout<<"size of slot record " << sizeof(SlotRecord) << endl;
    cout<<"start of sd " << startSD << endl;
    cout<<"free space available" << freeSpace << endl;
    return freeSpace;
}

