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
        bool foundFreePage = false;
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

<<<<<<< HEAD
unsigned RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor){

}
unsigned RecordBasedFileManager::getFreeSpaceInPage(void* pageData) {

}

void RecordBasedFileManager::setSlotDir(void* page, SlotDir slotdir){

}
void RecordBasedFileManager::makeNewPage(void* page){

}

int RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor){
    int nullBitSize = ceil(recordDescriptor.size()/8);
    int fieldSize = 0;

    for (int i=0; i<recordDescriptor.size(); i++){
        cout << "name is " << recordDescriptor[i].name<<endl;
        cout << "type is " << recordDescriptor[i].type<<endl;
        cout << "length is " << recordDescriptor[i].length<<endl;

        AttrType type = recordDescriptor[i].type;

        if (type == TypeInt or type == TypeReal){
            fieldSize+=4;
        }
        else{
            int charLength = recordDescriptor[i].name.size();
            fieldSize = fieldSize + 4 + charLength;
        }
    }
    int recordSize = nullBitSize + fieldSize;
    cout <<"record size " << recordSize << endl;
    return nullBitSize+fieldSize;
}


