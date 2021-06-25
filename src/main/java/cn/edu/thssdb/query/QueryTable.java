package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.IllegalSQLStatement;
import cn.edu.thssdb.exception.QueryColumnCollisionException;
import cn.edu.thssdb.exception.QueryColumnNotFoundException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.type.ComparerType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

public abstract class QueryTable implements Iterator<Row> {
  LinkedList<QueryRow> queue;
  ArrayList<MetaInfo> MetaInfoList;
  MultipleCondition selectCondition;
  boolean init;

  public abstract void addNext();
  public abstract void createMetaInfo();
  QueryTable() {
    queue = new LinkedList<QueryRow>();
    MetaInfoList = new ArrayList<>();
    init = true;
  }
  public void setSelectCondition(MultipleCondition Condition){
    this.selectCondition = Condition;
  }

  @Override
  public boolean hasNext() {
     return init || !queue.isEmpty();
  }

  @Override
  public QueryRow next() {
    if(queue.isEmpty()){
      addNext();
      if(init){
        init = false;
      }
    }
    if(queue.isEmpty()){
      return null;
    }
    QueryRow row = queue.poll();
    if(queue.isEmpty()){
      addNext();
    }
    return row;
  }

}