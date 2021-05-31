package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import java.util.ArrayList;
import java.util.List;

class MetaInfo {

  private String tableName;
  private List<Column> columns;
  private String databaseName;

  MetaInfo(String databaseName,String tableName, ArrayList<Column> columns) {
    this.tableName = tableName;
    this.columns = columns;
    this.databaseName = databaseName;
  }

  public int columnFind(String name) {
    for(int i=0;i<columns.size();i++){
      if(columns.get(i).getName().equals(name)){
        return i;
      }
    }
    return -1;
  }

  public String getTableName(){
    return tableName;
  }

  public String getDatabaseName(){
    return databaseName;
  }
}