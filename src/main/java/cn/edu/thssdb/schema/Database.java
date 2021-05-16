package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
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
    recover();
  }

  private void persist() {
    // TODO
  }

  public void createTableIfNotExists(String tableName, Column[] tableColumns) {
    try{
      lock.writeLock().lock();
      // 若哈希表中没有该表名称，则新建并加入
      if (!tables.containsKey(tableName)) {
        Table table = new Table(name, tableName, tableColumns);
        tables.put(tableName, table);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void dropTable(String tableName) {
    try{
      lock.writeLock().lock();
      // 判断表是否存在
      if (!tables.containsKey(tableName)) {
        throw new TableNotExistException();
      }

      Table table = tables.get(tableName);

      // TODO: 取决于Table类中的drop函数实现
      // TODO: 可以再检查是否已删除表对应的记录文件
      table.drop();

      table = null;
      tables.remove(tableName);

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      lock.writeLock().unlock();
    }

  }

  public String select(QueryTable[] queryTables) {
    // TODO
    QueryResult queryResult = new QueryResult(queryTables);
    return null;
  }

  private void recover() {
    // TODO
  }

  public void quit() {
    // TODO
  }

  public void dropSelf() {
    try{
      lock.writeLock().lock();

      // TODO: 删除 database对应的文件

      for (Table table: tables.values()) {
        dropTable(table.tableName);

      }
      // tables.clear();
      tables = null;

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      lock.writeLock().unlock();
    }
  }
}
