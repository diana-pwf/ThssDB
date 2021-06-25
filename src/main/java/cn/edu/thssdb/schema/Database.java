package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.TableAlreadyExistException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.query.Comparer;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.query.MultipleCondition;
import cn.edu.thssdb.type.ColumnType;
import javafx.scene.control.Tab;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database {

  private String name;
  private HashMap<String, Table> tables;
  ReentrantReadWriteLock lock;

  public Database(String name) {
    this.name = name;
    this.tables = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
    try
    {
      this.recover();
    }
    catch (Exception e){
      System.out.println("Error occurs when database recovers data!");
    }
  }

  public void createTableIfNotExists(String tableName, Column[] tableColumns) throws IOException {
    try{
      lock.writeLock().lock();
      // 若哈希表中没有该表名称，则新建并加入
      if (!tables.containsKey(tableName)) {
        Table table = new Table(name, tableName, tableColumns);
        tables.put(tableName, table);
        persist();
      }else{
        throw new TableAlreadyExistException(tableName);
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public Table getTable(String tableName){
    Table table = tables.get(tableName);
    if(table == null){
      throw new TableNotExistException(tableName);
    }
    return table;
  }

  public String getName() { return name;}

  public void dropTable(String tableName,boolean deleteDatabase) throws Exception {
    try{
      lock.writeLock().lock();
      // 判断表是否存在
      if (!tables.containsKey(tableName)) {
        throw new TableNotExistException(tableName);
      }

      Table table = tables.get(tableName);
      if(!deleteDatabase){
        tables.remove(tableName);
      }


      try{
        table.dropSelf();
      }catch (Exception e){
        e.printStackTrace();
        throw e;
      }
      finally {
        String path = "DATA/"+"meta_"+name+"_"+tableName+".data";
        File file = new File(path);
        if(file.exists()&&file.isFile()){
          if(file.delete()){
            System.out.println("successfully drop table "+tableName+"meta file!");
          }
          else{
            throw new Exception("Error occurs when drop table "+tableName+"meta file!");
          }
        }
      }
      table = null;

    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    } finally {
      lock.writeLock().unlock();
    }

  }

  public QueryResult select(String[] columnNames, QueryTable queryTable, MultipleCondition conditions, Boolean distinct) {
    try {
      lock.readLock().lock();
      queryTable.setSelectCondition(conditions);
      QueryResult queryResult = new QueryResult(queryTable, columnNames, distinct);
      queryResult.generateQueryRecord();
      return queryResult;
    }
    finally {
      lock.readLock().unlock();
    }


  }



  public void insert(String tableName, ArrayList<String> columnsName, String[] values){
    try {
      Table table = getTable(tableName);
      table.insert(columnsName, values);
    } catch (Exception e){
      throw e;
    }
  }

  public String delete(String tableName, MultipleCondition condition){
    try {
      Table table = getTable(tableName);
      return table.delete(condition);
    } catch (Exception e){
      throw e;
    }
  }

  public String update(String tableName, String columnName, Comparer comparer, MultipleCondition conditions) {
    try{
      Table table = getTable(tableName);
      return table.update(columnName, comparer, conditions);
    } catch (Exception e){
      throw e;
    }
  }

  public void persist() throws IOException {
    for(Table table:tables.values()){
      // store format: "DATA/meta_DatabaseName_tableName.data"
      File file = new File("DATA/"+"meta"+"_"+this.name+'_'+table.tableName+".data");
      FileOutputStream fop = new FileOutputStream(file);
      OutputStreamWriter writer = new OutputStreamWriter(fop);
      for(Column col : table.columns){
        writer.write(col.toString()+"\n");
      }
      writer.close();
      fop.close();
    }
  }

  private void recover() throws IOException {
    File dir = new File("DATA/");
    File[] fileList = dir.listFiles();
    if(fileList == null){
      System.out.println("Directory contains no file.");
      return;
    }
    for(File file : fileList){
      if(!file.isFile()){
        continue;
      }
      String[] index = file.getName().split("\\.")[0].split("_");
      if(index[0].equals("meta") && index[1].equals(this.name)){
        String tableName = index[2];
        ArrayList<Column> columns = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String str = reader.readLine();
//        if(str == null){
//          throw new TableNotExistException(tableName);
//        }
        while (str!=null) {
          String[] colAttrs = str.split(",");
          String name = colAttrs[0];
          ColumnType type = ColumnType.valueOf(colAttrs[1]);
          int primary = Integer.parseInt(colAttrs[2]);
          boolean notNull = Boolean.parseBoolean(colAttrs[3]);
          int maxLength = Integer.parseInt(colAttrs[4]);
          columns.add(new Column(name,type,primary,notNull,maxLength));
          str = reader.readLine();
        }
        createTableIfNotExists(tableName,columns.toArray(new Column[0]));
        reader.close();
      }
    }
  }

  public void quit() throws IOException {
    try {
      lock.writeLock().lock();
      for (Table table : tables.values()) {
        table.persist();
      }
      persist();
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void dropSelf() throws Exception {
    try{
      lock.writeLock().lock();

      for (Table table: tables.values()) {
        dropTable(table.tableName,true);
      }

      // tables.clear();
      tables = null;

    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public String showTableMeta(String tableName) {
    Table table = getTable(tableName);
    if(table == null){
      throw new TableNotExistException(tableName);
    }
    return table.showMeta();
  }

  public String showAllTables() {
    StringBuilder result = new StringBuilder("databaseName: " + this.name + "\n" + "\n");
    for(Table table : tables.values()) {
      if(table != null) {
        result.append(table.showMeta()).append("\n");
      }
    }
    return result.toString();
  }
}
