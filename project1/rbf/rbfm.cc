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
    int result = pfm->createFile(fileName);
    return result;
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

    putRecordOnPage(page, sd, sr, recordDescriptor,data);


    //set up the rid
    rid.pageNum = pageNum;
    rid.slotNum = sd.numOfRecords+1;

    //update slotdir
    //SlotDir sd = getSlotDir(page);
    sd.freeSpaceLoc+=recordSize;
    sd.numOfRecords+=1;
    setSlotDir(page,sd);

    //update slotrecord
    //SlotRecord sr;
    sr.len = recordSize;
    sr.recordStartLoc = sd.freeSpaceLoc;
    setSlotRecord(page, sr, rid.slotNum);








    return 0;
    // // first need to get total size of record data and null flag
    // unsigned recordSize = getRecordSize(&recordDescriptor);
    // //get the current(last page) to try to insert record
    // unsigned lastPage = fileHandle.getNumberOfPages()-1;
    // //check to see if lastPage has enough space for the actual record and slot record
    // unsigned pageFreeSpace = getFreeSpaceInPage();
    return -1;

}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    return -1;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    return -1;
}


unsigned RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor){
    unsigned nullBitSize = ceil(recordDescriptor.size()/8);
    unsigned fieldSize = 0;

    for (int i=0; i<recordDescriptor.size(); i++){
        // cout << "name is " << recordDescriptor[i].name<<endl;
        // cout << "type is " << recordDescriptor[i].type<<endl;
        // cout << "length is " << recordDescriptor[i].length<<endl;

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
    return sd;
}
SlotRecord RecordBasedFileManager::getSlotRecord(void* page, unsigned recordNum){
    SlotRecord sr;
    return sr;
}

void RecordBasedFileManager::putRecordOnPage(void* page, SlotDir& sd, SlotRecord& sr, const vector<Attribute> &recordDescriptor, const void* data){

    char* start = (char*)page + sd.freeSpaceLoc;
    //get null bit info
    unsigned null = ceil(recordDescriptor.size()/8);
    char nullinfo[null];
    memcpy(&nullinfo, data, sizeof(nullinfo));
    //set nullflag into record
    memcpy(start, &nullinfo, sizeof(nullinfo));
    //databegin is where we set actual data
    char* dataBegin = start + sizeof(nullinfo);
    //iterate thru records, if NOT null, use memcpy
    for(int i=0;i<recordDescriptor.size();i++) {
        //if the field is not null, copy field data
        if(!isNullBitOne(nullinfo, i)) {

        }
    }







}

bool RecordBasedFileManager::isNullBitOne(char* nullflag, unsigned i){
    int bit = i / 8;
    int bitmask  = 1 << (8 - 1 - (i % 8));
    if(nullflag[bit] & bitmask) return true;
    return false;
}

unsigned RecordBasedFileManager::getNullBit(const vector<Attribute> &recordDescriptor){

}


unsigned RecordBasedFileManager::getFreeSpaceInPage(void* page){
    SlotDir sd = getSlotDir(page);

    unsigned startSD = PAGE_SIZE-((sizeof(SlotRecord) * sd.numOfRecords)+sizeof(sd));
    unsigned freeSpace = startSD - sd.freeSpaceLoc;
    cout<<"size of slot record " << sizeof(SlotRecord) << endl;
    cout<<"start of sd " << startSD << endl;
    cout<<"free space available" << freeSpace << endl;
    return freeSpace;
}

