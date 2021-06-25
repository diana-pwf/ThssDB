package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DatabaseAlreadyExistException;
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
      Database database = null;
      // 若哈希表中没有该数据库名称，则新建并加入
      if (!databases.containsKey(databaseName)) {
        database = new Database(databaseName);
        databases.put(databaseName, database);
      }
      else{
        database = databases.get(databaseName);
        throw new DatabaseAlreadyExistException(databaseName);
      }
      currentDatabase = database;
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void dropDatabase(String databaseName) throws Exception {
    try{
      lock.writeLock().lock();
      // 判断数据库是否存在
      if (!databases.containsKey(databaseName)) {
        throw new DatabaseNotExistException();
      }
      if(currentDatabase!=null){
        if(currentDatabase.getName().equals(databaseName)){
          currentDatabase = null;
        }
      }
      Database database = databases.get(databaseName);
      database.dropSelf();
      database = null;
      databases.remove(databaseName);
      persist();

    } catch (Exception e) {
      e.printStackTrace();
      throw e;
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
      throw e;
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


  /**
   * 将数据库名称存储到 manager.data中
   * @throws IOException
   */
  public void persist() throws IOException {
    File file = new File("DATA/manager.data");
    FileOutputStream fop = new FileOutputStream(file);
    OutputStreamWriter writer = new OutputStreamWriter(fop);
    // 将数据库名称存储到manager.data中
    for(String databaseName : databases.keySet()){
      writer.write(databaseName+'\n');
    }
    writer.close();
    fop.close();
  }

  /**
   * 读取manger.data下的数据，并创建database
   * @throws IOException
   */
  public void recover() throws IOException {

    File file = new File("DATA/manager.data");

    if (!file.exists()) {
      try {
        file.createNewFile();

      } catch (IOException e) {
        e.printStackTrace();

      }
    }

    BufferedReader reader = new BufferedReader(new FileReader(file));
    String str;
    while ((str=reader.readLine())!=null) {
       String databaseName = str;
       createDatabaseIfNotExists(databaseName);
    }
    currentDatabase = null;
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