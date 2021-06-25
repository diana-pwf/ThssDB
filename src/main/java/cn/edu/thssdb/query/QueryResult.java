package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.IllegalSQLStatement;
import cn.edu.thssdb.exception.QueryColumnCollisionException;
import cn.edu.thssdb.exception.QueryColumnNotFoundException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.type.ComparerType;
import cn.edu.thssdb.utils.Cell;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

public class QueryResult {

  private List<MetaInfo> MetaInfoList;
  private boolean isQueryResult;
  private String message;
  private QueryTable table;
  private ArrayList<Integer> columnSelectIdx;
  public ArrayList<String> columnSelectName;
  private ArrayList<Row> result;
  private boolean selectDistinct;
  private HashSet<String> rowHashSet;

  // TODO:注意 QueryResult在数据为空的时候返回的不是null，而是ArrayList<Row>()

  /**
   *  查询结果构造 支持Distinct指令
   * @param queryTable 需要先构造相应的QueryTable
   * @param selectColumn columnName/tableName.columnName的数组
   * @param distinct 是否需要判 Distinct
   */
  public QueryResult(QueryTable queryTable,String[] selectColumn,boolean distinct) {
    table = queryTable;
    isQueryResult = true;
    columnSelectIdx = new ArrayList<Integer>();
    columnSelectName = new ArrayList<String>();
    result = new ArrayList<Row>();
    MetaInfoList =  new ArrayList<MetaInfo>();
    rowHashSet = new HashSet<String>();
    MetaInfoList.addAll(queryTable.MetaInfoList);
    // distinct
    selectDistinct = distinct;
    // select column
    if(selectColumn!=null){
      for(String column:selectColumn){
        columnSelectIdx.add(getColumnIndex(column));
        columnSelectName.add(column);
      }
    }
    else{
      int seqIndex = 0;
      for(MetaInfo metaInfo:MetaInfoList){
        for(int i=0;i<metaInfo.getColumnSize();i++){
          columnSelectName.add(metaInfo.getColumnName(i));
          columnSelectIdx.add(seqIndex+i);
        }
        seqIndex += metaInfo.getColumnSize();
      }
    }

  }

  // 执行结果
  public QueryResult(String msg){
    isQueryResult = false;
    message = msg;
  }

  // 判断 QueryResult 返回的是否为查询结果，根据结果的不同使用不同的解析方式
  public boolean getIsQueryResult() { return isQueryResult;}
  public String getMessage() { return message;}

  public ArrayList<Row> getResult(){
    return result;
  }

  // 打印结果的metaInfo
  public String printMetaInfo(){
    String metaInfo = "";
    for(int i = 0; i<columnSelectName.size();i++){
       metaInfo += columnSelectName.get(i);
       if(i != columnSelectName.size() -1){
         metaInfo += ",";
       }
    }
    return metaInfo;
  }


  public Integer getColumnIndex(String column){
    // table.column
    if(column.contains(".")){
      String[] seq = column.split("\\.");
      if(seq.length!=2){
        throw new IllegalSQLStatement(column);
      }
      String tableName = seq[0];
      String columnName = seq[1];
      int seqIndex = 0;
      boolean tableExist = false;
      for(MetaInfo metaInfo : MetaInfoList){
        if(metaInfo.getTableName().equals(tableName)){
          tableExist = true;
          int idx = 0;
          idx = metaInfo.columnFind(columnName);
          if(idx == -1){
            throw new QueryColumnNotFoundException(column,metaInfo.getDatabaseName());
          }
          seqIndex += idx;
          break;
        }
        seqIndex += metaInfo.getColumnSize();
      }
      if(!tableExist){
        throw new TableNotExistException(tableName);
      }
      return seqIndex;
    }

    // column
    else{
      int indexFind = 0;
      boolean columnExist = false;
      int seqIndex = 0;
      for(MetaInfo metaInfo:MetaInfoList){
        int idx = metaInfo.columnFind(column);
        if(idx==-1){
          continue;
        }
        if(!columnExist)
        {
          indexFind = seqIndex + idx;
           columnExist = true;
        }
        else{
          throw new QueryColumnCollisionException(column);
        }
        seqIndex += metaInfo.getColumnSize();
      }
      if(!columnExist){
        throw new QueryColumnNotFoundException(column,MetaInfoList.get(0).getDatabaseName());
      }
      return indexFind;
    }
  }

  public void generateQueryRecord() {
    while (table.hasNext()){
      QueryRow row = table.next();
      if(row==null){
        break;
      }
      Entry[] entries = new Entry[columnSelectIdx.size()];
      for(int i=0;i<columnSelectIdx.size();i++){
        entries[i] = row.getEntries().get(columnSelectIdx.get(i));
      }
      Row resultRow = new Row(entries);
      String rowStr = resultRow.toString();

      // 不判distinct或满足distinct要求
      if(!selectDistinct||!rowHashSet.contains(rowStr)){
        result.add(resultRow);
      if(selectDistinct){
        rowHashSet.add(rowStr);
        }
      }
    }
  }
}
