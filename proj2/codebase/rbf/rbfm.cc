#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <string>

#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = NULL;
PagedFileManager *RecordBasedFileManager::_pf_manager = NULL;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
    // Initialize the internal PagedFileManager instance
    _pf_manager = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) 
{
    // Creating a new paged file.
    if (_pf_manager->createFile(fileName))
        return RBFM_CREATE_FAILED;

    // Setting up the first page.
    void * firstPageData = calloc(PAGE_SIZE, 1);
    if (firstPageData == NULL)
        return RBFM_MALLOC_FAILED;
    newRecordBasedPage(firstPageData);

    // Adds the first record based page.
    FileHandle handle;
    if (_pf_manager->openFile(fileName.c_str(), handle))
        return RBFM_OPEN_FAILED;
    if (handle.appendPage(firstPageData))
        return RBFM_APPEND_FAILED;
    _pf_manager->closeFile(handle);

    free(firstPageData);

    return SUCCESS;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) 
{
    return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) 
{
    return _pf_manager->openFile(fileName.c_str(), fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) 
{
    return _pf_manager->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) 
{
    // Gets the size of the record.
    unsigned recordSize = getRecordSize(recordDescriptor, data);

    // Cycles through pages looking for enough free space for the new entry.
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;
    bool pageFound = false;
    unsigned i;
    unsigned numPages = fileHandle.getNumberOfPages();
    for (i = 0; i < numPages; i++)
    {
        if (fileHandle.readPage(i, pageData))
            return RBFM_READ_FAILED;

        // When we find a page with enough space (accounting also for the size that will be added to the slot directory), we stop the loop.
        if (getPageFreeSpaceSize(pageData) >= sizeof(SlotDirectoryRecordEntry) + recordSize)
        {
            pageFound = true;
            break;
        }
    }

    // If we can't find a page with enough space, we create a new one
    if(!pageFound)
    {
        newRecordBasedPage(pageData);
    }

    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);

    // Setting the return RID.
    rid.pageNum = i;
    rid.slotNum = slotHeader.recordEntriesNumber;

    // Adding the new record reference in the slot directory.
    SlotDirectoryRecordEntry newRecordEntry;
    newRecordEntry.length = recordSize;
    newRecordEntry.offset = slotHeader.freeSpaceOffset - recordSize;
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, newRecordEntry);

    // Updating the slot directory header.
    slotHeader.freeSpaceOffset = newRecordEntry.offset;
    slotHeader.recordEntriesNumber += 1;
    setSlotDirectoryHeader(pageData, slotHeader);

    // Adding the record data.
    setRecordAtOffset (pageData, newRecordEntry.offset, recordDescriptor, data);

    // Writing the page to disk.
    if (pageFound)
    {
        if (fileHandle.writePage(i, pageData))
            return RBFM_WRITE_FAILED;
    }
    else
    {
        if (fileHandle.appendPage(pageData))
            return RBFM_APPEND_FAILED;
    }

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) 
{
    // Retrieve the specific page
    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);

    // Retrieve the actual entry data
    getRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, data);

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) 
{
    // Parse the null indicator into an array
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, data, nullIndicatorSize);
    
    // We've read in the null indicator, so we can skip past it now
    unsigned offset = nullIndicatorSize;

    cout << "----" << endl;
    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        cout << setw(10) << left << recordDescriptor[i].name << ": ";
        // If the field is null, don't print it
        bool isNull = fieldIsNull(nullIndicator, i);
        if (isNull)
        {
            cout << "NULL" << endl;
            continue;
        }
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
            uint32_t data_integer;
            memcpy(&data_integer, ((char*) data + offset), INT_SIZE);
            offset += INT_SIZE;

            cout << "" << data_integer << endl;
            break;
            case TypeReal:
            float data_real;
            memcpy(&data_real, ((char*) data + offset), REAL_SIZE);
            offset += REAL_SIZE;

            cout << "" << data_real << endl;
            break;
            case TypeVarChar:
                // First VARCHAR_LENGTH_SIZE bytes describe the varchar length
            uint32_t varcharSize;
            memcpy(&varcharSize, ((char*) data + offset), VARCHAR_LENGTH_SIZE);
            offset += VARCHAR_LENGTH_SIZE;

                // Gets the actual string.
            char *data_string = (char*) malloc(varcharSize + 1);
            if (data_string == NULL)
                return RBFM_MALLOC_FAILED;
            memcpy(data_string, ((char*) data + offset), varcharSize);

                // Adds the string terminator.
            data_string[varcharSize] = '\0';
            offset += varcharSize;

            cout << data_string << endl;
            free(data_string);
            break;
        }
    }
    cout << "----" << endl;

    return SUCCESS;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){
    int result = 0;
    //first read in the page
    void *page = malloc(PAGE_SIZE);
    if(fileHandle.readPage(rid.pageNum,page) != 0) return RBFM_READ_FAILED;

    //get the slot directory and validation
    SDH s_header = getSlotDirectoryHeader(page);
    uint32_t num_of_record = s_header.recordEntriesNumber;
    if(num_of_record<rid.slotNum) {
        free(page);
        return RBFM_SLOT_DN_EXIST;
    }

    //get record entry and use offset to see its status
    SlotDirectoryRecordEntry s_entry = getSlotDirectoryRecordEntry(page,rid.slotNum);
    //positive offset = alive, negative offset means moved, NULL means its a dead entry
    if(s_entry.offset<0) {
        //if moved, recursively delete record at the forwarded address using negative forward addr
        RID r2{s_entry.length,(rid.slotNum)*-1};
        result = deleteRecord(fileHandle,recordDescriptor,r2);
    }else if(s_entry.offset>0){
        //if alive, just set record entry to null
        s_entry.offset = 0;
        s_entry.length = 0;
        setSlotDirectoryRecordEntry(page,rid.slotNum,s_entry);
    }else{
        //dead, just return
        result = -1;
    }

    //compact the records on the page
    compact(fileHandle,page,rid.slotNum);
    fileHandle.writePage(rid.pageNum,page);
    //deallocate page
    free(page);

    return result;

}



RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid){
    //get the page
    int result = 0;
    void* page = malloc(PAGE_SIZE);
    if(fileHandle.readPage(rid.pageNum,page) != 0) return RBFM_READ_FAILED;

    //get slot directory and validate
    SDH s_header = getSlotDirectoryHeader(page);
    uint32_t num_of_record = s_header.recordEntriesNumber;
    if(num_of_record < rid.slotNum) {
        free(page);
        return RBFM_SLOT_DN_EXIST;
    }

    //get the slot record entry and check record status
    SlotDirectoryRecordEntry s_entry = getSlotDirectoryRecordEntry(page,rid.slotNum);
    //get record size to use later in checking status of update
    unsigned record_size = getRecordSize(recordDescriptor,data);

    //check to see if record is dead, alive, or moved
    if(s_entry.offset == 0) {
        //dead so nothing to update 
        result = -1;
    }else if(s_entry.offset < 0) {
        free(page);
        //record moved, so update recursively
        // new page_id is the slot recory entry's length, and -1*offset is the new slotnum
        RID r2{s_entry.length,static_cast<uint32_t>((s_entry.offset)*-1)};
        result = updateRecord(fileHandle,recordDescriptor,data,r2);
    }else if(s_entry.offset >0) {
        //entry is alive, but updated record size is too big and cannot fit on page(case 3)
        if(record_size > getPageFreeSpaceSize(page) + s_entry.length) {
            //delete the record and put it on another page.
            deleteRecord(fileHandle,recordDescriptor,rid);
            RID forwardingAddr;
            insertRecord(fileHandle,recordDescriptor,data,forwardingAddr);
            //update current page slot directory with the forwarding address
            s_entry.length = forwardingAddr.pageNum;
            //make forward addr negative
            s_entry.offset = -1*(forwardingAddr.slotNum);
            setSlotDirectoryRecordEntry(page,rid.slotNum,s_entry);
            //write page to file
            fileHandle.writePage(rid.pageNum,page);
        }else{
            //record is alive, and updated record size is either equal or smaller. (case 1 and 2)
            //we can just delete and put it back on the same page.
            //after delete, the records will be compacted
            deleteRecord(fileHandle,recordDescriptor,rid);
            s_header = getSlotDirectoryHeader(page);
            //update slotrecordentry
            s_entry.length = record_size;
            s_entry.offset = s_header.freeSpaceOffset - record_size;
            setSlotDirectoryRecordEntry(page,rid.slotNum,s_entry);
            //update the slotdirectory header
            //s_header.length +=1;
            s_header.freeSpaceOffset = s_entry.offset;
            setSlotDirectoryHeader(page,s_header);
            //put in the updated record
            setRecordAtOffset(page,s_entry.offset,recordDescriptor,data);
            fileHandle.writePage(rid.pageNum,page);

        }
    }
    free(page);
    return result;

}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data)
{
    int result = 0;
    //get the page and validate
    void* page = malloc(PAGE_SIZE);
    if(fileHandle.readPage(rid.pageNum,page) != 0) {
        free(page);
        return RBFM_READ_FAILED;
    }

    //get slot header and entry record
    SDH s_header = getSlotDirectoryHeader(page);
    uint16_t num_of_record = s_header.recordEntriesNumber;
    //if cant find slotnum, return fail
    if(num_of_record < rid.slotNum) return RBFM_SLOT_DN_EXIST;
    SlotDirectoryRecordEntry s_entry = getSlotDirectoryRecordEntry(page,rid.slotNum);

    //check the status of the entry
    if(s_entry.offset == 0) {
        //dead record
        free(page);
        result = -1;
    }else if(s_entry.offset < 0) {
        //recursively call with forward addr
        free(page);
        RID r2{s_entry.length,static_cast<uint32_t>((s_entry.offset)*-1)};
        result = readAttribute(fileHandle,recordDescriptor,r2,attributeName,data);
    }else if(s_entry.offset > 0){
        //record is alive, get the record first
        //iterate thru record descriptor to find the index for the attributename
        unsigned index = -1;
        for(int i=0;i<recordDescriptor.size();i++) {
            if(recordDescriptor[i].name == attributeName) {
                index = i;
                break;
            }
        }
        //couldnt find attribute so it doesnt exist
        if(index == -1) {
            free(page);
            return -1;
        }
        AttrType attr_type = recordDescriptor[index].type;
        getAttribute(page,data,s_entry,index,attr_type);
        free(page);
    }
    return result;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
  const vector<Attribute> &recordDescriptor,
  const string &conditionAttribute,
  const CompOp compOp,                  
  const void *value,                    
  const vector<string> &attributeNames, 
  RBFM_ScanIterator &rbfm_ScanIterator) {

    rbfm_ScanIterator.setup(fileHandle,recordDescriptor,conditionAttribute,compOp,value,attributeNames);
    if(rbfm_ScanIterator.error_setup == true) return -1;
    return 0;


    // //MAKE THESE PUBLIC?
    // rbfm_ScanIterator.recordDescriptor = recordDescriptor;
    // rbfm_ScanIterator.attributeNames = attributeNames;


    // //first make sure the attributes we are looking for actuall exist in recorddescriptor
    // //iterate thru record descriptor and attributenames and check
    // bool attr_exist = false;
    // AttrType cond_attr_type;
    // for(int i=0;i<recordDescriptor.size();i++) {
    //     for(int j=0;j<attributeNames.size();j++) {
    //         if(recordDescriptor[i].name == attributeNames[j].name) attr_exist = true;
    //     }
    //     //cant find the attribute
    //     if(!attr_exist) return -1;
    //     //otherwise, check type of the condition attribute
    // }

    // //go through all the pages, and push rid to list for all records that match the condition
    // //with the rid's in, getnextrecord can just go through all the rid's and readrecord/readattribute from each?

}

void RBFM_ScanIterator::setup(FileHandle &fileHandle,
  const vector<Attribute> &recordDescriptor,
  const string &conditionAttribute,
  const CompOp compOp, 
  const void *value,
  const vector<string> &attributeNames) {


    //set up the first page to read and let getnextrecord handle the rest
    rbfm = RecordBasedFileManager::instance();
    handle = &fileHandle;
    r_descriptor = recordDescriptor;
    condition_attr = conditionAttribute;
    attr_name = attributeNames;
    compare_op = compOp;
    val = value;

    num_of_pages = handle->getNumberOfPages();

    page = malloc(PAGE_SIZE);

    //read in the first page
    if(handle->readPage(0,page)) error_setup = true;
    //get how many slots are on the first page
    SDH s_header = rbfm->getSlotDirectoryHeader(page);
    num_of_slots_on_page = s_header.recordEntriesNumber;
    
    //set up the condition attribute's type and index to use for reading attr
    index = -1;
    for(int i=0;i<r_descriptor.size();i++) {
        if(r_descriptor[i].name == condition_attr) {
            index = i;
            break;
        }
    }
    //couldnt find attribute so it doesnt exist
    if(index == -1) {
        //free(page);
        error_setup = true;
    }
    //set up type
    attr_type = recordDescriptor[index].type;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
    //getnextrecord should automatically keep track of which slot and which page we are on
    //first make sure we are stil have records to pull from
    //int result = 0;
    if(working_slot > num_of_slots_on_page) {
        //get next page and first slot
        working_page +=1;
        if(working_page > num_of_pages) return RBFM_EOF;
        if(handle->readPage(working_page,page)) return RBFM_READ_FAILED;
        //update num of slots for this new page
        SDH s_header = rbfm->getSlotDirectoryHeader(page);
        num_of_slots_on_page = s_header.recordEntriesNumber;
        working_slot = 0;
    }
    //now we should on the correct slot and the correct page
    //make sure record is valid(meets the condition and is live) 
    SlotDirectoryRecordEntry s_entry = rbfm->getSlotDirectoryRecordEntry(page,working_slot);
    while(s_entry.offset <= 0 || !meetCondition()) {
        //get next slot
        working_slot += 1;
        if(working_slot > num_of_slots_on_page) {
        //get next page and first slot
            working_page +=1;
            if(working_page > num_of_pages) return RBFM_EOF;
            if(handle->readPage(working_page,page)) return RBFM_READ_FAILED;
        //update num of slots for this new page
            SDH s_header = rbfm->getSlotDirectoryHeader(page);
            num_of_slots_on_page = s_header.recordEntriesNumber;
            working_slot = 0;
        }
        s_entry = rbfm->getSlotDirectoryRecordEntry(page,working_slot);
    }

    //has working slot, parse and write to data
    void* temp_data = malloc(PAGE_SIZE);
    unsigned data_offset = 0;
    unsigned null_size = rbfm->getNullIndicatorSize(attr_name.size());
    char null_info[null_size];
    memcpy((char*)data,null_info,null_size);
    data_offset += null_size;
    //loop through all the attributes we need 
    for(int i=0;i<attr_name.size();i++) {
        //get index and type
        int index = -1;
        for(int j=0;j<r_descriptor.size();j++) {
            if(attr_name[i] == r_descriptor[j].name) {
                index = j;
                break;
            }
        }
        if(index == -1) {
            free(temp_data);
            return -1;
        }
        AttrType type = r_descriptor[index].type;
        
        //got the right index/type, so write into temp the record
        rbfm->getAttribute(page,temp_data,s_entry,index,type);
        //check to see if attribute has null bit set
        //COULD BE ERROR HERE(USE CHAR INSTEAD)
        char temp;
        memcpy(&temp,temp_data,sizeof(char));
        if(temp) {
            //attribute is null, set null_info 
            int indicatorIndex = i / CHAR_BIT;
            char indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
            null_info[indicatorIndex] |= indicatorMask;
            memcpy((char*)data,null_info,null_size);
        }else if(type == TypeReal) {
            //write real field 
            memcpy((char*)data+data_offset,(char*)temp_data+sizeof(char),4);
            data_offset+=4;
        }else if(type == TypeInt) {
            memcpy((char*)data+data_offset,(char*)temp_data+sizeof(char),4);
            data_offset +=4;
        }else if(type == TypeVarChar) {
            //varchar first write 4 bytes for length of field, then actual field
            //skip 1 byte to pass by the null byte
            int var_size;
            memcpy(&var_size, (char*)temp_data+sizeof(char),4);
            memcpy((char*)data+data_offset,&var_size,4);
            data_offset +=4;
            memcpy((char*)data+data_offset,(char*)temp_data+sizeof(char)+4,var_size);
            data_offset += var_size;
        }

    }
    //free temp data, set the appropriate rid
    free(temp_data);
    rid.pageNum = working_page;
    rid.slotNum = working_slot;
    //next record uses next slot
    working_slot++;
    return 0;
}

bool RBFM_ScanIterator::meetCondition() {
    //if no condition, always meet
    if(compare_op == NO_OP) return true;
    //get the condition attribute
    void* temp_data = malloc(PAGE_SIZE);
    SlotDirectoryRecordEntry s_entry =  rbfm->getSlotDirectoryRecordEntry(page,working_slot);
    rbfm->getAttribute(page,temp_data,s_entry,index,attr_type);

    //do the same thing as get record but check condition by the value instead
    if(attr_type == TypeReal) {
        int64_t val1;
        memcpy(&val1,(char*)temp_data+sizeof(char),4);
        int64_t val2;
        memcpy(&val2,val,4);
        if(compare_op == EQ_OP) 
            free(temp_data);
            return val1 == val2;
        if(compare_op == LT_OP) 
            free(temp_data);
            return val1 < val2;
        if(compare_op == GT_OP) 
            free(temp_data);
            return val1 > val2;
        if(compare_op == LE_OP) 
            free(temp_data);
            return val1 <= val2;
        if(compare_op == GE_OP) 
            free(temp_data);
            return val1 >= val2;
        if(compare_op == NE_OP) 
            free(temp_data);
            return val1 != val2;
        //if(compare_op == NO_OP) 
           // free(temp_data);
           // return true;
    }else if(attr_type == TypeInt) {
        int32_t val1;
        memcpy(&val1,(char*)temp_data+sizeof(char),4);
        int32_t val2;
        memcpy(&val2,val,4);
        if(compare_op == EQ_OP) 
            free(temp_data);
            return val1 == val2;
        if(compare_op == LT_OP) 
            free(temp_data);
            return val1 < val2;
        if(compare_op == GT_OP) 
            free(temp_data);
            return val1 > val2;
        if(compare_op == LE_OP) 
            free(temp_data);
            return val1 <= val2;
        if(compare_op == GE_OP) 
            free(temp_data);
            return val1 >= val2;
        if(compare_op == NE_OP) 
            free(temp_data);
            return val1 != val2;
        //if(compare_op == NO_OP) 
         //   free(temp_data);
         //   return true;
    }else if(attr_type == TypeVarChar) {
        int var_size1;
        memcpy(&var_size1,(char*)temp_data+sizeof(char),4);
        char val1[var_size1*2];
        memcpy(&val1,(char*)temp_data+sizeof(char)+4,var_size1);
        val1[var_size1] = '\0';

        int var_size2;
        memcpy(&var_size2,val,4);
        char val2[var_size2*2];
        memcpy(&val2,(char*)val+4,var_size2);
        val2[var_size2] = '\0';
        if(compare_op == EQ_OP) 
            free(temp_data);
            return (strcmp(val1,val2) == 0);
        if(compare_op == LT_OP) 
            free(temp_data);
            return (strcmp(val1,val2) < 0);
        if(compare_op == GT_OP) 
            free(temp_data);
            return (strcmp(val1,val2) > 0);
        if(compare_op == LE_OP) 
            free(temp_data);
            return (strcmp(val1,val2) > 0);
        if(compare_op == GE_OP) 
            free(temp_data);
            return (strcmp(val1,val2) < 0);
        if(compare_op == NE_OP) 
            free(temp_data);
            return (strcmp(val1,val2) != 0);
        //if(compare_op == NO_OP) 
        //    free(temp_data);
        //   return true;
    }

    return false;

    
}


// RBFM_ScanIterator RBFM_ScanIterator::operator=(RBFM_ScanIterator& scan_itor) {
//   handle = scan_itor.handle;
//   rbfm = scan_itor.rbfm;
//   page = scan_itor.page;
//   val = scan_itor.val;
//   r_descriptor = scan_itor;
//   records = scan_itor.records;
//   attr_name = scan_itor.attr_name;
//   attr_type = scan_itor.attr_type;
//   index = scan_itor.index;
//   condition_attr = scan_itor.condition_attr;
//   compare_op = scan_itor.compare_op;
//   num_of_slots = scan_itor.num_of_slots;
//   num_of_pages = scan_itor.num_of_pages;
// }
// Private helper methods

// Configures a new record based page, and puts it in "page".
void RecordBasedFileManager::newRecordBasedPage(void * page)
{
    memset(page, 0, PAGE_SIZE);
    // Writes the slot directory header.
    SlotDirectoryHeader slotHeader;
    slotHeader.freeSpaceOffset = PAGE_SIZE;
    slotHeader.recordEntriesNumber = 0;
    setSlotDirectoryHeader(page, slotHeader);
}

SlotDirectoryHeader RecordBasedFileManager::getSlotDirectoryHeader(void * page)
{
    // Getting the slot directory header.
    SlotDirectoryHeader slotHeader;
    memcpy (&slotHeader, page, sizeof(SlotDirectoryHeader));
    return slotHeader;
}

void RecordBasedFileManager::setSlotDirectoryHeader(void * page, SlotDirectoryHeader slotHeader)
{
    // Setting the slot directory header.
    memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

SlotDirectoryRecordEntry RecordBasedFileManager::getSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber)
{
    // Getting the slot directory entry data.
    SlotDirectoryRecordEntry recordEntry;
    memcpy  (
        &recordEntry,
        ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
        sizeof(SlotDirectoryRecordEntry)
        );

    return recordEntry;
}

void RecordBasedFileManager::setSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber, SlotDirectoryRecordEntry recordEntry)
{
    // Setting the slot directory entry data.
    memcpy  (
        ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
        &recordEntry,
        sizeof(SlotDirectoryRecordEntry)
        );
}

// Computes the free space of a page (function of the free space pointer and the slot directory size).
unsigned RecordBasedFileManager::getPageFreeSpaceSize(void * page) 
{
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
    return slotHeader.freeSpaceOffset - slotHeader.recordEntriesNumber * sizeof(SlotDirectoryRecordEntry) - sizeof(SlotDirectoryHeader);
}

unsigned RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor, const void *data) 
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Offset into *data. Start just after null indicator
    unsigned offset = nullIndicatorSize;
    // Running count of size. Initialize to size of header
    unsigned size = sizeof (RecordLength) + (recordDescriptor.size()) * sizeof(ColumnOffset) + nullIndicatorSize;

    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        // Skip null fields
        if (fieldIsNull(nullIndicator, i))
            continue;
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
            size += INT_SIZE;
            offset += INT_SIZE;
            break;
            case TypeReal:
            size += REAL_SIZE;
            offset += REAL_SIZE;
            break;
            case TypeVarChar:
            uint32_t varcharSize;
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
            memcpy(&varcharSize, (char*) data + offset, VARCHAR_LENGTH_SIZE);
            size += varcharSize;
            offset += varcharSize + VARCHAR_LENGTH_SIZE;
            break;
        }
    }

    return size;
}

// Calculate actual bytes for nulls-indicator for the given field counts
int RecordBasedFileManager::getNullIndicatorSize(int fieldCount) 
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}

bool RecordBasedFileManager::fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

void RecordBasedFileManager::setRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, const void *data)
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset (nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Points to start of record
    char *start = (char*) page + offset;

    // Offset into *data
    unsigned data_offset = nullIndicatorSize;
    // Offset into page header
    unsigned header_offset = 0;

    RecordLength len = recordDescriptor.size();
    memcpy(start + header_offset, &len, sizeof(len));
    header_offset += sizeof(len);

    memcpy(start + header_offset, nullIndicator, nullIndicatorSize);
    header_offset += nullIndicatorSize;

    // Keeps track of the offset of each record
    // Offset is relative to the start of the record and points to the END of a field
    ColumnOffset rec_offset = header_offset + (recordDescriptor.size()) * sizeof(ColumnOffset);

    unsigned i = 0;
    for (i = 0; i < recordDescriptor.size(); i++)
    {
        if (!fieldIsNull(nullIndicator, i))
        {
            // Points to current position in *data
            char *data_start = (char*) data + data_offset;

            // Read in the data for the next column, point rec_offset to end of newly inserted data
            switch (recordDescriptor[i].type)
            {
                case TypeInt:
                memcpy (start + rec_offset, data_start, INT_SIZE);
                rec_offset += INT_SIZE;
                data_offset += INT_SIZE;
                break;
                case TypeReal:
                memcpy (start + rec_offset, data_start, REAL_SIZE);
                rec_offset += REAL_SIZE;
                data_offset += REAL_SIZE;
                break;
                case TypeVarChar:
                unsigned varcharSize;
                    // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                memcpy(&varcharSize, data_start, VARCHAR_LENGTH_SIZE);
                memcpy(start + rec_offset, data_start + VARCHAR_LENGTH_SIZE, varcharSize);
                    // We also have to account for the overhead given by that integer.
                rec_offset += varcharSize;
                data_offset += VARCHAR_LENGTH_SIZE + varcharSize;
                break;
            }
        }
        // Copy offset into record header
        // Offset is relative to the start of the record and points to END of field
        memcpy(start + header_offset, &rec_offset, sizeof(ColumnOffset));
        header_offset += sizeof(ColumnOffset);
    }
}

// Support header size and null indicator. If size is less than recordDescriptor size, then trailing records are null
// Memset null indicator as 1?
void RecordBasedFileManager::getRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, void *data)
{
    // Pointer to start of record
    char *start = (char*) page + offset;

    // Allocate space for null indicator. The returned null indicator may be larger than
    // the null indicator in the table has had fields added to it
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);

    // Get number of columns and size of the null indicator for this record
    RecordLength len = 0;
    memcpy (&len, start, sizeof(RecordLength));
    int recordNullIndicatorSize = getNullIndicatorSize(len);

    // Read in the existing null indicator
    memcpy (nullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);

    // If this new recordDescriptor has had fields added to it, we set all of the new fields to null
    for (unsigned i = len; i < recordDescriptor.size(); i++)
    {
        int indicatorIndex = (i+1) / CHAR_BIT;
        int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
        nullIndicator[indicatorIndex] |= indicatorMask;
    }
    // Write out null indicator
    memcpy(data, nullIndicator, nullIndicatorSize);

    // Initialize some offsets
    // rec_offset: points to data in the record. We move this forward as we read data from our record
    unsigned rec_offset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
    // data_offset: points to our current place in the output data. We move this forward as we write data to data.
    unsigned data_offset = nullIndicatorSize;
    // directory_base: points to the start of our directory of indices
    char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;
    
    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        if (fieldIsNull(nullIndicator, i))
            continue;
        
        // Grab pointer to end of this column
        ColumnOffset endPointer;
        memcpy(&endPointer, directory_base + i * sizeof(ColumnOffset), sizeof(ColumnOffset));

        // rec_offset keeps track of start of column, so end-start = total size
        uint32_t fieldSize = endPointer - rec_offset;

        // Special case for varchar, we must give data the size of varchar first
        if (recordDescriptor[i].type == TypeVarChar)
        {
            memcpy((char*) data + data_offset, &fieldSize, VARCHAR_LENGTH_SIZE);
            data_offset += VARCHAR_LENGTH_SIZE;
        }
        // Next we copy bytes equal to the size of the field and increase our offsets
        memcpy((char*) data + data_offset, start + rec_offset, fieldSize);
        rec_offset += fieldSize;
        data_offset += fieldSize;
    }
}

void RecordBasedFileManager::compact(FileHandle& fileHandle, void* page, uint32_t pageNum){
    //get directory header so we know how many records there are
    SDH s_header = getSlotDirectoryHeader(page);
    int num_of_record = s_header.recordEntriesNumber;

    void* temp_page = malloc(PAGE_SIZE);
    //unsigned temp_page_offset = 0;
    SlotDirectoryRecordEntry s_entry;
    unsigned compact_record_length = 0;
    //iterate through all the slot records, and for the ones that are alive, just move to end of page
    //need to start with the last record first
    for(int i=num_of_record;i != 0;i--) {
        s_entry = getSlotDirectoryRecordEntry(page,i);
        //if alive, put record at end of page in order
        if(!((s_entry.offset == 0 && s_entry.length == 0) || s_entry.offset < 0)) {
            memcpy((char*)temp_page+compact_record_length,(char*)page+s_entry.offset,s_entry.length);
            compact_record_length += s_entry.length;
        }
    }
    //change the slot record offsets to the compacted location
    int32_t free_offset = PAGE_SIZE;
    //iterate thru the slot records in order, and update offsedt
    for(int i=0;i<num_of_record;i++) {
        s_entry = getSlotDirectoryRecordEntry(page,i);
        if(!((s_entry.offset == 0 && s_entry.length == 0) || s_entry.offset < 0)) {
            free_offset -=s_entry.length;
            s_entry.offset = free_offset;
            setSlotDirectoryRecordEntry(page,i,s_entry);
        }
    }

    //write in the compacted record. slot directory location doesn't change, its just the offsets for entries which we changed above
    memcpy((char*)page+free_offset,temp_page,compact_record_length);
    s_header.freeSpaceOffset = free_offset;
    setSlotDirectoryHeader(page,s_header);
    fileHandle.writePage(pageNum,page);
    free(temp_page);
}
void RecordBasedFileManager::getAttribute(void* page, void* data, SlotDirectoryRecordEntry s_entry,unsigned index,AttrType attr_type){
    //first grab the record and number of fields
    char* record = (char*)page+s_entry.offset;
    ColumnOffset num_of_fields;
    memcpy(&num_of_fields,record,sizeof(num_of_fields));

    //grab the null bits
    unsigned null_size = getNullIndicatorSize(num_of_fields);
    char null_info[null_size];
    memcpy(null_info,record+sizeof(ColumnOffset),null_size);

    //check to see if the attribute we are looking for is null
    char is_null = 0;
    if(fieldIsNull(null_info,index)) {
        //if null, set left most bit to 1 (00000000||10000000)
        //THIS COULD BE WRONG
        is_null = is_null | (128);
    }
    //write the null bits to data, and if its null, just return with the null bit set
    memcpy(data,&is_null,sizeof(is_null));
    if(is_null == 1) return;
    //only 1 byte for null flag(only 1 attribute)
    unsigned data_offset = 1;

    //now find the field with the attribute that we want
    //move offset to in record to field directory
    unsigned field_offset = null_size+2;
    //start of attr offset is the end of the previous index's attr. for first field, its end of field directory
    ColumnOffset start_ptr;
    //first field
    if(index == 0) start_ptr = field_offset+(sizeof(start_ptr)*num_of_fields);
    //not the first field
    else memcpy(&start_ptr,record+field_offset+(index-1)*sizeof(start_ptr),sizeof(start_ptr)); 
    ColumnOffset end_ptr;
    //end ptr is start of record+nullbit+fielddir+#ofthefield
    memcpy(&end_ptr,record+field_offset+index*sizeof(end_ptr),sizeof(end_ptr));

    //write into data the bytes from start_ptr to end_ptr
    if(attr_type != TypeVarChar) {
        memcpy((char*)data+data_offset,record+start_ptr,(end_ptr-start_ptr));
    }else{
        unsigned field_length = end_ptr-start_ptr;
        //if varchar, write how long it is, then the data
        memcpy((char*)data+data_offset,&field_length,4);
        data_offset+=4;
        memcpy((char*)data+data_offset,record+start_ptr,(end_ptr-start_ptr));
    }
}