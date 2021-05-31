package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.server.ThssDB;
// import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;

import javax.xml.crypto.Data;
import java.io.*;
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
    try
    {
      this.recover();
    }
    catch (Exception e){
      System.out.println("Error occurs when manager recovers data!");
    }
  }

  public void createDatabaseIfNotExists(String databaseName) {
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

  public void dropDatabase(String databaseName) {
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

  public void switchDatabase(String databaseName) {
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

  public void persist() throws IOException {
    File file = new File("manager.data");
    FileOutputStream fop = new FileOutputStream(file);
    OutputStreamWriter writer = new OutputStreamWriter(fop);
    // 将数据库名称存储到manager.data中
    for(String databaseName : databases.keySet()){
      writer.write(databaseName+'\n');
    }
    writer.close();
    fop.close();
  }

  public void recover() throws IOException {
    File file = new File("manager.data");
    BufferedReader reader = new BufferedReader(new FileReader(file));
    while (reader.readLine()!=null) {
       String databaseName = reader.readLine();
       createDatabaseIfNotExists(databaseName);
    }
    reader.close();
  }

  public void  quit(){
    lock.writeLock().lock();
    try{for(Database database : databases.values()){
      database.quit();
    }
      persist();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public String ShowAllDatabases() {
    StringBuilder result = new StringBuilder("Current databases:" + "\n");
    for (Database database: databases.values()) {
      result.append(database.showAllTables()).append("\n");
    }
    return result.toString();
  }

}