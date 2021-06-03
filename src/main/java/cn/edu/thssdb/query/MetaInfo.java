package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.type.ColumnType;

import java.util.ArrayList;
import java.util.List;

public class MetaInfo {

  private String tableName;
  private List<Column> columns;
  private String databaseName;

  public MetaInfo(String databaseName,String tableName, ArrayList<Column> columns) {
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

  public ColumnType columnFindType(int idx){
    return columns.get(idx).getType();
  }

  public String getTableName(){
    return tableName;
  }

  public String getDatabaseName(){
    return databaseName;
  }

  public int getColumnSize(){
    return columns.size();
  }
}