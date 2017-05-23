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
 RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
 FileHandle handle;
 string tableName = "Tables.tbl";
 string columnName = "Columns.clm";
   //make the files for system tables
 if(rbfm->createFile("Tables.tbl")) return -1;
 if(rbfm->createFile("Columns.clm")) return -1;
 int table_id = 1;
 int column_id = 2;

 if(put_to_table(tableName,table_id,true)) return -1;
 cout << "past first" << endl;
 vector<Attribute> ta = getTableAttr();
 if(put_to_column(table_id,ta)) return -1;
 cout << "past 2nd" << endl;

 vector<Attribute> ca = getColumnAttr();
 if(put_to_table(columnName,column_id,true)) return -1;
 cout << "past 3rd" << endl;
 if(put_to_column(column_id,ca)) return -1;

 return 0;
}

RC RelationManager::deleteCatalog()
{
  string fileName1 = ("Tables.tbl");
  string fileName2 = ("Columns.clm");
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    // Check if "Tables" exists already
  struct stat stFileInfo;
  if (stat(fileName1.c_str(), &stFileInfo) != 0 )
    return -1;
    // Delete "Tables" table
  rbfm->destroyFile(fileName1);
    // Check if "Columns" exists already
  if (stat(fileName2.c_str(), &stFileInfo) != 0)
    return -1;
    // Delete "Columns" table
  rbfm->destroyFile(fileName2);

  return 0;

}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
  RecordBasedFileManager *rbfm =  RecordBasedFileManager::instance();
  int result = 0;
  cout << "before createfile" << endl;
  if(rbfm->createFile(tableName)) return -1;

    //Obtain the table ID
  int32_t tableID;
  vector<Attribute> ta = getTableAttr();

    //rc =  getNextTableID(tableID);  //FUNCTION CALL HERE==================================================================
  FileHandle fh;
  result = next_table(tableID);
    // cout << "before openfile" << endl;
    // if(rbfm->openFile("Tables.tbl",fh)) return -1;

    // //find the table ID
    // vector<string> attrProj;
    // attrProj.push_back("table-id");

    // //use the scanner to get the ID
    // RBFM_ScanIterator scanner;
    // rbfm->scan(fh, ta, "table-id", NO_OP, NULL, attrProj, scanner);

    // RID rid;
    // void * data = malloc (INT_SIZE*2);
    // int32_t maxTableID = 0;
    // while((result = scanner.getNextRecord(rid,data)) == 0)
    // {
    //   cout << "in scanner" << endl;
    //   int32_t tID;

    //   //fromAPI(tableID, data);

    //   char null = 0;

    //   memcpy(&null, data, 1);
    //   if (!null){
    //       int32_t temp;
    //       memcpy(&temp, (char*) data + sizeof(char), 4);
    //       tID = temp;
    //   }

    //   if(tID > maxTableID){
    //     maxTableID = tID;
    //   }
    // }
    // cout << "result value is " << result << endl; 
    // if (result == -1){
    //   result = 0;
    // }

    // free(data);

    // //next table ID is 1 more than the largest table id
    // tableID = maxTableID + 1;
    // scanner.close();
    // rbfm ->closeFile(fh);
    // cout << "before result is != 0" << endl;
  if(result != 0) return -1;

    //==============================FUNCTION CALL GETNEXTTABLE() ENDS HERE =========================================//


    // We have to insert the table into table.tbl 
  cout << "before put to table" << endl;
  if(put_to_table(tableName, tableID, false)) return -1;

  cout << "before put to column" << endl;
  if(put_to_column(tableID, attrs)) return -1;

  return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
  // int result = 0;
  //   // return (rbfm->destroyFile(tableName));
  // RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
  // FileHandle fh;
  // RC rc;
  // RID rid;
  // vector<Attribute> tableAttr = getTableAttr();
  // char null=0;

  // bool inTable;

  // if (rbfm->openFile("Tables.tbl", fh)!=0) return -1;
  // vector<string> projAttr;
  // projAttr.push_back("system");

  // void* temp = malloc(55);
  // int name_length = tableName.length();
  // memcpy(temp, &name_length, INT_SIZE);
  // memcpy((char*)temp + INT_SIZE, tableName.c_str(), name_length);

  // RBFM_ScanIterator scanner;
  // if (rbfm->scan(fh, tableAttr, "table-name", EQ_OP, temp, projAttr, scanner)!=0) return -1;

  // void* data = malloc(5);
  // rc = scanner.getNextRecord(rid, data);
  // if (rc==0){
  //   int value;
  //   memcpy(&null, data, 1);
  //   // if (null) return;
  //   int tmp;
  //   memcpy(&tmp, (char*) data+1, INT_SIZE);
  //   value = tmp;
  //   if (tmp==1){
  //     inTable = true;
  //   }
  //   else{
  //     inTable = false;
  //   }
  // }
  // if (rc==-1){
  //   rc = 0;
  // }
  // free(data);
  // free(temp);
  // rbfm->closeFile(fh);
  // // scanner.close();

  // if(rc==1){
  //   return rc;
  // }
  // if (inTable==1){
  //   return -1;
  // }

  // if (rbfm->destroyFile(tableName)!=0)  return -1;

  // int id;
  // //get index from table, puts into int id
  // if(getTableIdFromName(tableName, id)!=0) return -1;

  // vector<string> projection;
  // void* value = &id;

  // RBFM_ScanIterator scanner2;
  // //look for matching table id in tables
  // rbfm->scan(fh, tableAttr, "table-id", EQ_OP, value, projection, scanner2);

  // if (scanner2.getNextRecord(rid, NULL)!=0)  return -1;

  // //delete rid from table
  // rbfm->deleteRecord(fh, tableAttr, rid);
  // rbfm->closeFile(fh);
  // scanner2.close();

  // if (rbfm->openFile("Columns.clm", fh)!=0) return -1;

  // vector<Attribute> columnAttr = getColumnAttr();
  // //look for matching table ids in column

  // RBFM_ScanIterator scanner3;
  // rbfm->scan(fh, columnAttr, "table-id", EQ_OP, value, projection, scanner3);

  // while ((result = scanner3.getNextRecord(rid, NULL))==0){
  //   //delete rid if matching
  //   if (result = rbfm->deleteRecord(fh, columnAttr, rid)!=0) return -1;
  // }
  // if (result != -1) return result;

  // rbfm->closeFile(fh);
  // scanner3.close();
  // return 0;
  return rbfm->destroyFile(tableName);
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    cout << "does it even go in here?" << endl;
    int rc;

    int32_t tableID;
    if(getTableIdFromName(tableName, tableID)){
      return -1;
    }

    cout << "after get tbale id from name" << endl;

    void *value = &tableID;

    RBFM_ScanIterator scanner;
    vector<string> selectAttr;
    selectAttr.push_back("column-name");
    selectAttr.push_back("column-type");
    selectAttr.push_back("column-length");
    selectAttr.push_back("column-position");

    FileHandle fh;
    if(rbfm->openFile("Columns.clm", fh)) {
      return -1;
    }
    cout << "after file open/ before scan" << endl;

    vector<Attribute> getColumnAttr = RelationManager::getColumnAttr();
    if(rbfm->scan(fh, getColumnAttr, "table-id", EQ_OP, value, selectAttr, scanner)){
      return -1;
    }

    cout << "after scan initialize/before getNextRecord" << endl;

    RID rid;
    void *data = malloc(1 +20 +50);
    vector<std::pair<int,Attribute>> x;
    while ((rc = scanner.getNextRecord(rid, data)) == 0)
    {

      cout << "in the getNextRecord loop" << endl;
      pair<int, Attribute> attribute;
      unsigned offset = 0;

      char null;
      memcpy(&null, data, 1);
      if(null){
          rc = 2;
      }

      offset = 1;
      int32_t name_length;
      memcpy(&name_length, (char*) data +offset, 4);
      offset += 4;

      char name[name_length+1];
      name[name_length] = '\0';
      memcpy(name, (char*) data + offset, name_length);
      offset += name_length;
      attribute.second.name = string(name);

      //read the type
      int32_t type;
      memcpy(&type, (char*) data + offset, 4);
      offset += 4;
      attribute.second.type = (AttrType)type;


      //rea the length
      int32_t length;
      memcpy(&length, (char*) data + offset, 4);
      offset += 4;
      attribute.second.length = length;


      //read the position
      int32_t position;
      memcpy(&position, (char*) data + offset, 4);
      offset += 4;
      attribute.first = position;

      cout << "after the memcpy shit" << endl;
      x.push_back(attribute);
    }

    scanner.close();
    rbfm->closeFile(fh);
    free(data);

    return 0;
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

RC RelationManager::put_to_table(string name,int table_id, bool system){
  FileHandle handle;
  RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
  RID rd;
  const char* c_table_name = name.c_str();
  int t_len = name.length();
  //write into table and column
  if(rbfm->openFile(name,handle)) return -1;
  void* page = malloc(PAGE_SIZE);
   //write table entry null,id,name,file,system flag
  unsigned data_offset = 0;
  char temp = 0;
  memcpy((char*)page+data_offset,&temp,sizeof(char));
  data_offset+=1;
  memcpy((char*)page+data_offset,&table_id,4);
  data_offset+=4;
  memcpy((char*)page+data_offset,&t_len,4);
  data_offset+=4;
  memcpy((char*)page+data_offset,c_table_name,t_len);
  data_offset+=t_len;
  memcpy((char*)page+data_offset,&t_len,4);
  data_offset+=4;
  memcpy((char*)page+data_offset,c_table_name,t_len);
  data_offset+=t_len;
  memcpy((char*)page+data_offset,&system,4);

  vector<Attribute> ta = getTableAttr();
  if(rbfm->insertRecord(handle,ta,page,rd)) return -1;
  free(page);
  return 0;
}


RC RelationManager::put_to_column(int table_id, const vector<Attribute> &rs){
  FileHandle fh;
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
  cout << "before openfile" << endl;
  if(rbfm->openFile("Columns.clm", fh)) return -1;
  cout << "pased open col file " << endl;
  RID rid;
  vector<Attribute> ca = getColumnAttr();
  void *colData = malloc(50);
  for (unsigned i = 0; i< rs.size(); i++)
  {
    int32_t position = i+1;
        //prepareColumnsRecordData(id, position, rs[i], columnData)=================================================FUNCITON PREPARE COLUMNS RECORD DATA =========================
    unsigned offset = 0;
    int name_length = rs[i].name.length();

    char null = 0;

    memcpy((char*) colData +offset, &null, 1);
    offset +=1;

    memcpy((char*) colData+ offset, &table_id, 4);
    offset += 4;

    memcpy((char*) colData + offset, &name_length, 4);
    offset += 4;
    const char* attribute_name = rs[i].name.c_str();
    memcpy((char*)colData+offset,attribute_name,name_length);
    offset += name_length;

    int32_t type = rs[i].type;
    memcpy((char*) colData + offset, &type, 4);
    offset +=4;

    int32_t length = rs[i].length;
    memcpy((char*) colData+ offset, &length, 4);
    offset +=4;

    memcpy((char*) colData+offset, &position, 4);
    offset +=4;

    cout << "before insert" << endl;
        //==============================================this is where the function ends============================
    if(rbfm->insertRecord(fh, ca, colData, rid)) return -1;

    cout << "after insert" << endl;
  }
  rbfm->closeFile(fh);
  free(colData);
  return 0;
}

RC RelationManager::next_table(int &table_id)
{
  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
  FileHandle fh;
  RC rc;

  rc = rbfm->openFile("Tables.tbl", fh);
  if (rc)
    return rc;
    // Grab only the table ID
  vector<string> projection;
  projection.push_back("table-id");

    // Scan through all tables to get largest ID value
  RBFM_ScanIterator scanner;
  rc = rbfm->scan(fh, getTableAttr(), "table-id", NO_OP, NULL, projection, scanner);

  RID rid;
  void *data = malloc (1 + INT_SIZE);
  int max_table_id = 0;
  while ((rc = scanner.getNextRecord(rid, data)) == (SUCCESS))
  {
        // Parse out the table id, compare it with the current max
    int t_id;
        //fromAPI(t_id, data);
    char null = 0;

    memcpy(&null, data, 1);
    if (!null) {
      int32_t tmp;
      memcpy(&tmp, (char*) data + 1, INT_SIZE);
      t_id = tmp;
    }
    if (t_id > max_table_id)
      max_table_id = t_id;
  }
    // If we ended on eof, then we were successful
  if (rc == RM_EOF)
    rc = SUCCESS;

  free(data);
    // Next table ID is 1 more than largest table id
  table_id = max_table_id + 1;
  rbfm->closeFile(fh);
  scanner.close();
  return SUCCESS;
}

unsigned RelationManager::getTableIdFromName(const string &tableName, int& table_id){
  int result = 0;
  const vector<Attribute> tableAttr = getTableAttr();
  RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
  FileHandle fh;
  RBFM_ScanIterator scanner;
  RID rid;

  if (rbfm->openFile("Tables.tbl", fh)!=0) return -1;
  vector<string> attrs;
  attrs.push_back("table-id");

  void* val = malloc(100);
  int name_length = tableName.length();
  memcpy(val, &name_length, 4);
  memcpy((char*)val + 4, tableName.c_str(), name_length);

  rbfm->scan(fh, tableAttr,"table-name", EQ_OP, val, attrs, scanner);


  void* data = malloc(10);
  if ((result = scanner.getNextRecord(rid,data))==0){
    int table_id;
    char null = 0;
    memcpy(&null, data, 1);

    if(!null) {
      int temp;
      memcpy(&temp, (char*)data + 1, INT_SIZE);
      table_id = temp;
    }
  }
  
  free(data);
  free(val);
  rbfm->closeFile(fh);
  scanner.close();
  return 0;
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


