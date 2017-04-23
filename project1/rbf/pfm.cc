#include <iostream>
#include <fstream>
#include <sys/stat.h>
#include <stdlib.h>
#include <stdio.h>

#include "pfm.h"

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
    if (fileExists(fileName)){
        cout<<"file there already"<<endl;
        return -1;
    }

    FILE * pagedFile = fopen(fileName.c_str(), "wb");

    if (pagedFile == NULL){
        return -1;
    }
    fclose(pagedFile);
    cout<<"created new file"<<endl;
    return 0;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    if (!fileExists(fileName)){
        cout<<"file does not exist"<<endl;
        return -1;
    }
    remove(fileName.c_str());
    return 0;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    if (!fileExists(fileName)){
        cout<<"file does not exit"<<endl;
        return -1;
    }
    FILE *pagedFile = fopen(fileName.c_str(), "rb+");
    fileHandle.handle = pagedFile;
    return 0;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    fflush(fileHandle.handle);
    fclose(fileHandle.handle);
    return 0;
}

bool PagedFileManager::fileExists(string fileName){
    struct stat stFileInfo;

    if (stat(fileName.c_str(), &stFileInfo) == 0)
        return true;
    else
        return false;
}

FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{

    //use handle
    if(handle == NULL) {
        perror ("Error opening file");
        return -1;
    }else{
        if(fseek(handle, PAGE_SIZE*pageNum, SEEK_SET) != 0) {
            return -1;
        }
         //check to see if fseek worked
        size_t result = fread(data, sizeof(char), PAGE_SIZE, handle);
        if(result != PAGE_SIZE) return -1;
    }
    readPageCounter = readPageCounter + 1;
    return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    if(handle == NULL) {
        perror("Error opening file");
        return -1;
    }else{
        if(fseek(handle, PAGE_SIZE*pageNum, SEEK_SET) != 0) {
            return -1;
        }
        size_t result = fwrite(data, sizeof(char),PAGE_SIZE, handle);
        //cout << "result is " << result << endl;
        if(result != PAGE_SIZE) return -1;
        size_t flush = fflush(handle);
        if(flush != 0) return -1;
    }
    writePageCounter = writePageCounter + 1;
    return 0;
}


RC FileHandle::appendPage(const void *data)
{

    if(handle == NULL) {
        perror("Error opening file");
        return -1;
    }else{
        if(fseek(handle,0,SEEK_END) != 0) return -1;
        size_t result = fwrite(data, sizeof(char),PAGE_SIZE,handle);
        if(result != PAGE_SIZE) return -1;
        size_t flush = fflush(handle);
        if(flush != 0) return -1;
    }

    appendPageCounter = appendPageCounter + 1;
    return 0;
}


unsigned FileHandle::getNumberOfPages()
{

    return appendPageCounter;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    return -1;
}
