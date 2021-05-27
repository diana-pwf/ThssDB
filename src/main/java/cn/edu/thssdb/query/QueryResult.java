package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.utils.Cell;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class QueryResult {

  private List<MetaInfo> metaInfoInfos;
  private List<Integer> index;
  private List<Cell> attrs;
  private boolean isQueryResult;
  private String message;


  // 查询结果
  public QueryResult(QueryTable[] queryTables) {
    isQueryResult = true;
    // TODO
    this.index = new ArrayList<>();
    this.attrs = new ArrayList<>();
  }

  // 执行结果
  public QueryResult(String msg){
    isQueryResult = false;
    message = msg;
  }

  // 判断 QueryResult 返回的是否为查询结果，根据结果的不同使用不同的解析方式
  public boolean getIsQueryResult() { return isQueryResult;}
  public String getMessage() { return message;}

  public static Row combineRow(LinkedList<Row> rows) {
    // TODO
    return null;
  }

  public Row generateQueryRecord(Row row) {
    // TODO
    return null;
  }
}
