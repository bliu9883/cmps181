
#include "rm.h"

RelationManager* RelationManager::_rm = 0;
RecordBasedFileManager* RelationManager::rbfm = 0;

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

RC RelationManager::createCatalog()
{
    string tableName = "Tables.tbl";
    string columnName = "Columns.clm";

    vector<Attribute> tableAttr = getTableAttr();
    vector<Attribute> columnAttr = getColumnAttr();

    char* tableTable = (char*)malloc(104);
    char *columnTable = (char*)malloc(66);

    unsigned table_id = 1;
    memcpy(tableTable, &table_id, 4);
    memcpy(tableTable+4, &tableName, 50);
    memcpy(tableTable+54, &tableName, 50);

    unsigned int column_id = 2;
    memcpy(columnTable, &column_id, 4);
    memcpy(columnTable+4, &columnName, 50);
    memcpy(columnTable+54, &columnName, 50);

    FileHandle fh;
    RID rid;

    //table
    if (rbfm->createFile(tableName)!=0){
      return -1;
    }
    if (rbfm->openFile(tableName, fh )!=0){
      return -1;
    }
    if (rbfm->insertRecord(fh, tableAttr, tableTable, rid)!=0){
      return -1;
    }
    if (rbfm->insertRecord(fh, tableAttr, columnTable, rid)!=0){
      return -1;
    }
    if(rbfm->closeFile(fh)!=0){
      return -1;
    }

    //columns
    if (rbfm->createFile(columnName)!=0){
      return -1;
    }
    if (rbfm->openFile(columnName, fh)!=0){
      return -1;
    }

    for (int i=0; i<8; i++){
      if (rbfm->insertRecord(fh, columnAttr, catalogInfo(i), rid)!=0){
        return -1;
      }
    }
    if (rbfm->closeFile(fh)!=0){
      return -1;
    }
    return 0;
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
    
    if (rbfm->createFile(tableName)!=0) return -1;
    if (rbfm->openFile(tableName, fh)!=0) return -1;

    return -1;
}

RC RelationManager::deleteTable(const string &tableName)
{
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    return -1;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    return -1;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    return -1;
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