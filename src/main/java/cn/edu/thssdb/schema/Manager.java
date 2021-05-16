package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.server.ThssDB;

import javax.xml.crypto.Data;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
  private HashMap<String, Database> databases;
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private Database currentDatabase;

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() {
    databases = new HashMap<String, Database>();
    currentDatabase = null;
  }

  private void createDatabaseIfNotExists(String databaseName) {
    try{
      lock.writeLock().lock();
      // 若哈希表中没有该数据库名称，则新建并加入
      if (!databases.containsKey(databaseName)) {
        Database database = new Database(databaseName);
        databases.put(databaseName, database);

        if (currentDatabase == null) {
          currentDatabase = database;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void dropDatabase(String databaseName) {
    try{
      lock.writeLock().lock();
      // 判断数据库是否存在
      if (!databases.containsKey(databaseName)) {
        throw new DatabaseNotExistException();
      }

      Database database = databases.get(databaseName);
      database.dropSelf();
      database = null;
      databases.remove(databaseName);

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      lock.writeLock().unlock();
    }

  }

  private void switchDatabase(String databaseName) {
    try{
      lock.readLock().lock();

      // 判断数据库是否存在
      if (!databases.containsKey(databaseName)) {
        throw new DatabaseNotExistException();
      }

      currentDatabase = databases.get(databaseName);

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      lock.readLock().unlock();
    }
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();
    private ManagerHolder() {

    }
  }

  public Database getCurrentDatabase() {
    return currentDatabase;
  }

}

// TODO: persist recover quit