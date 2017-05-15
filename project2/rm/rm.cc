#include "rm.h"
#include <cmath>


RelationManager* RelationManager::_rm = 0;
RecordBasedFileManager* RelationManager::rbfm = NULL;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
  rbfm = RecordBasedFileManager::instance();
}

RelationManager::~RelationManager()
{
}

RC RelationManager::setTableData(const string &tableName, const void * data, int table_id){

    int offset = int(ceil((double) 2 / CHAR_BIT));

    //id
    memcpy((char*) data + offset, &table_id, INT_SIZE);
    offset += INT_SIZE;

    int32_t length = tableName.length();
    //length of table name
    memcpy ((char*) data + offset, &(length), VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    //table name
    memcpy((char*) data + offset, tableName.c_str(), length);
    offset += length;

    string fileName = tableName+".tbl";
    int32_t file_length = fileName.length();
     
    //file name
    memcpy ((char*) data + offset, &(file_length), VARCHAR_LENGTH_SIZE);    
    offset += VARCHAR_LENGTH_SIZE;

    //table name
    memcpy((char*) data + offset, tableName.c_str(), file_length);
    offset += file_length;
  

    return 0;
}

RC RelationManager::setColumnData(const string &tableName, vector<Attribute> columAttr, const void * data, int id){

    int offset = int(ceil((double)5  / CHAR_BIT));
    int realId = id;

    memcpy ((char*) data + offset, &realId, INT_SIZE);
    offset += INT_SIZE;

    string name = columAttr[id].name.c_str();
    int name_length = name.length();

    memcpy ((char*) data + offset, &name_length, VARCHAR_LENGTH_SIZE); 
    offset += VARCHAR_LENGTH_SIZE;
    memcpy ((char*) data + offset, columAttr[id].name.c_str(), name_length);
    offset += name_length;

    memcpy ((char*) data + offset, &(columAttr[id].type), INT_SIZE);
    offset += INT_SIZE;

    memcpy ((char*) data + offset, &(columAttr[id].length), INT_SIZE);
    offset += INT_SIZE;

    return 0;
}

RC RelationManager::createCatalog()
{
    FileHandle fh;
    void * tabData = malloc(PAGE_SIZE);
    void * columnData = malloc(PAGE_SIZE);

    vector<Attribute> tableAttr = getTableAttr();
    vector<Attribute> columnAttr = getColumnAttr();

    if (rbfm->createFile("Tables.tbl")!=0){
      return -1;
    }
    if (rbfm->openFile("Tables.tbl", fh)!=0){
      return -1;
    }
    void* tableData = malloc(104);

    RID rid, cRid, tRid;

    if (setTableData("Tables", tableData, 1)!=0){
      return -1;
    }
    rbfm->insertRecord(fh, tableAttr, tableData, rid);

    if (setTableData("Columns", tableData, 2)!=0){
      return -1;
    }
    rbfm->insertRecord(fh, tableAttr, tableData, rid);
    rbfm->closeFile(fh);

    if (rbfm->createFile("Columns.clm")!=0){
      return -1;
    }

    if (rbfm->openFile("Columns.clm", fh)!=0){
      return -1;
    }

    for (int i=0; i<columnAttr.size(); i++){
      setColumnData("Columns", columnAttr, columnData, 2);
      rbfm->insertRecord(fh, columnAttr, columnData, cRid);
    }

    for (int i=0; i<tableAttr.size(); i++){
      setColumnData("Tables", tableAttr, tabData, 1);
      rbfm->insertRecord(fh, tableAttr, tabData, tRid);
    }
    rbfm->closeFile(fh);
    return 0;
    // void* tableTable = (void*)malloc(104);
    // void *columnTable = (void*)malloc(66);

    // unsigned table_id = 1;
    // memcpy(&(tableTable), &table_id, 4);
    // cout<<"first memcpy"<<endl;
    // memcpy(&(tableTable)+4, &tableName, 50);
    // memcpy(&(tableTable)+54, &tableName, 50);
    // cout<<"table ememcpy"<<endl;

    // unsigned int column_id = 2;
    // memcpy(&columnTable, &column_id, 4);
    // cout<<"first column memcpy"<<endl;
    // memcpy(&(columnTable)+4, &columnName, 50);
    // memcpy(&(columnTable)+54, &columnName, 50);
    // cout<<"finished column memcpy"<<endl;
    // FileHandle fh;
    // RID rid;

    // //table
    // RC rc;
    // rc = rbfm->createFile(tableName);
    // cout<<"RC: "<< rc <<endl;
    // if (rbfm->createFile("Tables.tbl")){
    //   return -1;
    // }
    // cout<<"after create file"<<endl;
    // if (rbfm->openFile(tableName, fh )!=0){
    //   return -1;
    // }
    // cout<<"before insert"<<endl;
    // rc = rbfm->insertRecord(fh, tableAttr, tableTable, rid);
    // cout<<"after first insert"<<endl;
    // // if (rbfm->insertRecord(fh, tableAttr, &tableTable, rid)!=0){
    // //   return -1;
    // // }
    // if (rbfm->insertRecord(fh, tableAttr, &columnTable, rid)!=0){
    //   return -1;
    // }
    // cout<<"after insert"<<endl;
    // if(rbfm->closeFile(fh)!=0){
    //   return -1;
    // }

    // //columns
    // if (rbfm->createFile(columnName)!=0){
    //   return -1;
    // }
    // if (rbfm->openFile(columnName, fh)!=0){
    //   return -1;
    // }
    // cout<<"before for loop"<<endl;
    // for (int i=0; i<8; i++){
    //   if (rbfm->insertRecord(fh, columnAttr, catalogInfo(i), rid)!=0){
    //     return -1;
    //   }
    // }
    // if (rbfm->closeFile(fh)!=0){
    //   return -1;
    // }
}

RC RelationManager::deleteCatalog()
{
    string tableName = "Tables.tbl";
    string columnName = "Columns.clm";

    struct stat stFileInfo;
    //check table and column file exist
    if (stat(tableName.c_str(), &stFileInfo)!=0){
      return -1;
    }
    if (stat(columnName.c_str(), &stFileInfo)!=0){
      return -1;
    }
    
    rbfm->destroyFile(tableName);
    rbfm->destroyFile(columnName);

    return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    FileHandle fh;
    // cout<<"1"<<endl;
    if (rbfm->createFile(tableName)!=0) return -1;
    if (rbfm->openFile(tableName, fh)!=0) return -1;
    rbfm->closeFile(fh);
    ++t_id;
    // cout<<"t_id " << t_id << endl;
    const vector<Attribute> tableAttr = getTableAttr();
    const vector<Attribute> columnAttr = getColumnAttr();

    void* data = malloc(PAGE_SIZE);
    void* columnData = malloc(PAGE_SIZE);
    // cout<<"Data " << data << endl;
    if (data == NULL) return -1;
    RID rid;
    if (rbfm->openFile("Tables.tbl", fh)!=0) return -1;

    if (setTableData(tableName, data, t_id)!=0){
      return -1;
    }
    rbfm->insertRecord(fh, tableAttr, data, rid);
    rbfm->closeFile(fh);
    if (columnData==NULL) return -1;
    RID rid2;

    if (rbfm->openFile("Columns.clm", fh)!=0) return -1;
    for (unsigned i=1; i<attrs.size(); i++){
      if (setColumnData(tableName, attrs, columnData, i)!=0) return -1;
      rbfm->insertRecord(fh, attrs, columnData, rid2);
    }
    free(data);
    free(columnData);
    rbfm->closeFile(fh);
    

    return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
    return (rbfm->destroyFile(tableName));
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    // FileHandle fh;
    // if (rbfm->openFile(tableName, fh)!=0) return -1;
    // unsigned tableIndex = getTableIndex(tableName);

    return -1;
}

unsigned RelationManager::getTableIndex(const string &tableName){
  // const vector<Attribute> tableAttr = getTableAttr();
  // FileHandle fh;
  // RBFM_ScanIterator scanner;

  // if (rbfm->openFile(tableName, fh)!=0) return -1;
  // vector<string> attrs;
  // attrs.push_back("table-id");
  // attrs.push_back("table-name");
  // attrs.push_back("file-name");

  // void* data = malloc(INT_SIZE);
  // RID rid;

  // if (rbfm->scan(fh, getTableAttr(),"table-name", EQ_OP, tableName.c_str(), attrs, scanner)!=0) return -1;

  // if (scanner.getNextRecord(rid, data)!=0)  return -1;
  
  // int id;
  // memcpy(&id, data, INT_SIZE);
  // scanner.close();

  // rbfm->closeFile(fh);

  return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
  int result = 0;
  rbfm = RecordBasedFileManager::instance();

  //chekc that its not system table
  if(tableName == "Tables.tbl" || tableName == "Columns.clm") {
    return -1;
  }

  vector<Attribute> rs;
  FileHandle handle;

  //open the file
  if(rbfm->openFile(tableName,handle) != 0) return -1;
  getAttributes(tableName,rs);

  result = rbfm->insertRecord(handle,rs,data,rid);
  rbfm->closeFile(handle);
  return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
  int result = 0;
  rbfm = RecordBasedFileManager::instance();

  //chekc that its not system table
  if(tableName == "Tables.tbl" || tableName == "Columns.clm") {
    return -1;
  }

  vector<Attribute> rs;
  FileHandle handle;

  //open the file
  if(rbfm->openFile(tableName,handle) != 0) return -1;
  getAttributes(tableName,rs);

  result = rbfm->deleteRecord(handle,rs,rid);
  rbfm->closeFile(handle);
  return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
  int result = 0;
  rbfm = RecordBasedFileManager::instance();

  //chekc that its not system table
  if(tableName == "Tables.tbl" || tableName == "Columns.clm") {
    return -1;
  }

  vector<Attribute> rs;
  FileHandle handle;

  //open the file
  if(rbfm->openFile(tableName,handle) != 0) return -1;
  getAttributes(tableName,rs);

  result = rbfm->updateRecord(handle,rs,data,rid);
  rbfm->closeFile(handle);
  return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
  int result = 0;
  rbfm = RecordBasedFileManager::instance();

  vector<Attribute> rs;
  FileHandle handle;

  //open the file
  if(rbfm->openFile(tableName,handle) != 0) return -1;
  getAttributes(tableName,rs);

  result = rbfm->readRecord(handle,rs,rid,data);
  rbfm->closeFile(handle);
  return 0;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
  return rbfm->printRecord(attrs,data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
  int result = 0;
  rbfm = RecordBasedFileManager::instance();

  vector<Attribute> rs;
  FileHandle handle;

  //open the file
  if(rbfm->openFile(tableName,handle) != 0) return -1;
  getAttributes(tableName,rs);

  result = rbfm->readAttribute(handle,rs,rid,attributeName,data);
  rbfm->closeFile(handle);
  return 0;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
  int result = 0;
    //use rbfm's scan
    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    if(rbfm->openFile(tableName,rm_ScanIterator.handle)) return -1;
    vector<Attribute> rs;
    if(getAttributes(tableName,rs)) return 1;

    result = rbfm->scan(rm_ScanIterator.handle,rs,conditionAttribute,compOp,value,attributeNames,rm_ScanIterator.rbfm_scanitor);

    return result;

}

vector<Attribute> RelationManager::getTableAttr(){
  vector<Attribute> table;
  Attribute table_id, table_name, file_name;

  table_id.name = "table-id";
  table_id.type = TypeInt;
  table_id.length = 50;

  table_name.name = "table-name";
  table_name.type = TypeVarChar;
  table_name.length = 50;

  file_name.name = "file-name";
  file_name.type = TypeVarChar;
  file_name.length = 50;

  table.push_back(table_id);
  table.push_back(table_name);
  table.push_back(file_name);

  return table;
}

vector <Attribute> RelationManager::getColumnAttr(){
  vector<Attribute> column;
  Attribute table_id, column_name, column_type, column_length, column_position;

  table_id.name = "table-id";
  table_id.type = TypeInt;
  table_id.length = 4;

  column_name.name = "column-name";
  column_name.type = TypeVarChar;
  column_name.length = 50;

  column_type.name = "column-type";
  column_type.type = TypeVarChar;
  column_type.length = 4;

  column_length.name = "column-length";
  column_length.type = TypeInt;
  column_length.length = 4;

  column_position.name = "column-position";
  column_position.type = TypeInt;
  column_position.length = 4;

  column.push_back(table_id);
  column.push_back(column_name);
  column.push_back(column_type);
  column.push_back(column_length);
  column.push_back(column_position);

  return column;
}

void* RelationManager::catalogInfo(unsigned i){
  char* columnInfo = (char*)malloc(66);
  unsigned table_id;
  string column_name;
  unsigned column_type;
  unsigned column_length;
  unsigned column_position;

  switch(i){
    case 0:
        table_id = 1;
        column_name = "table-id";
        column_type = TypeInt;
        column_length = 4;
        column_position = 1;
        break;
    case 1:
        table_id = 1;
        column_name = "table-name";
        column_type = TypeVarChar;
        column_length = 50;
        column_position = 2;
        break;
    case 2:
        table_id = 1;
        column_name = "file-name";
        column_type = TypeVarChar;
        column_length = 50;
        column_position = 3;
        break; 
    case 3:
        table_id = 2;
        column_name = "table-id";
        column_type = TypeInt;
        column_length = 4;
        column_position = 1;
        break;
    case 4:
        table_id = 2;
        column_name = "column-name";
        column_type = TypeVarChar;
        column_length = 50;
        column_position = 2;
        break;   
    case 5:
        table_id = 2;
        column_name = "column-type";
        column_type = TypeInt;
        column_length = 4;
        column_position = 3;
        break; 
    case 6:
        table_id = 2;
        column_name = "column-length";
        column_type = TypeInt;
        column_length = 4;
        column_position = 4;
        break; 
    case 7:
        table_id = 2;
        column_name = "column-position";
        column_type = TypeInt;
        column_length = 4;
        column_position = 5;
        break; 
  }
  memcpy(columnInfo, &table_id, 4);
  memcpy(columnInfo+4, &column_name, 50);
  memcpy(columnInfo+54, &column_type, 4);
  memcpy(columnInfo+58, &column_length, 4);
  memcpy(columnInfo+62, &column_position, 4);
}

 RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
  return rbfm_scanitor.getNextRecord(rid,data);
 }
 RC RM_ScanIterator::close() {
  RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
  rbfm->closeFile(handle);
  rbfm_scanitor.close();
  return 0;
 }


