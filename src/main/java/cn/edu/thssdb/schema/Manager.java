package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.StatementErrorListener;
import cn.edu.thssdb.parser.StatementVisitor;
import cn.edu.thssdb.server.ThssDB;
import cn.edu.thssdb.service.IServiceHandler;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
// import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
  private HashMap<String, Database> databases;
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private Database currentDatabase;

  public ArrayList<Long> transactionSessions;
  public HashMap<Long, ArrayList<String>> sLockDict;
  public HashMap<Long, ArrayList<String>> xLockDict;
  public ArrayList<Long> blockedSessions;

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() {
    databases = new HashMap<String, Database>();
    currentDatabase = null;
    transactionSessions = new ArrayList<>();
    blockedSessions = new ArrayList<>();
    xLockDict = new HashMap<>();
    sLockDict = new HashMap<>();

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

  public void persistDatabase(String databaseName)
  {
    try {
      lock.writeLock().lock();
      Database db = databases.get(databaseName);
      // 对数据库中每张表的数据做持久化
      db.quit();
      // 将每个数据库的名字记录进manager.data
      persist();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      lock.writeLock().unlock();
    }
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

    try {
      BufferedReader reader = new BufferedReader(new FileReader(file));
      String str;
      while ((str=reader.readLine())!=null) {
        String databaseName = str;
        createDatabaseIfNotExists(databaseName);
        readLog(databaseName);
      }
      currentDatabase = null;
      reader.close();
    }
    catch (IOException e) {
      e.printStackTrace();
    }

  }

  public void quit(){
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

  public void writeLog(String statement) {
    // 找到对应的数据库并将操作语句写入文件
    Database db = getCurrentDatabase();
    File file = new File("DATA/" + db.getName() + ".data");

    try {
      FileOutputStream fop = new FileOutputStream(file);
      OutputStreamWriter writer = new OutputStreamWriter(fop);
      writer.write(statement +'\n');
      writer.close();
      fop.close();
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void readLog(String databaseName) {
    String fileName = "DATA/" + databaseName + ".data";
    File file = new File(fileName);
    if (file.exists() && file.isFile()) {
      System.out.println("Database log file size: " + file.length() + " Byte");
      System.out.println("Read WAL log to recover database.");
      handleCommand("use" + databaseName, this);

      try {
        InputStreamReader reader = new InputStreamReader(new FileInputStream(file));
        BufferedReader bufferedReader = new BufferedReader(reader);

        String line;
        ArrayList<String> lines = new ArrayList<>();
        ArrayList<Integer> transcations = new ArrayList<>();
        ArrayList<Integer> commits = new ArrayList<>();

        // 遍历读到的每一行 并记录begin transaction与 commit 所在行号
        int index = 0;
        while ((line = bufferedReader.readLine()) != null) {
          if (line.equals("begin transaction")) {
            transcations.add(index);
          } else if (line.equals("commit")) {
            commits.add(index);
          }
          lines.add(line);
          index++;
        }

        // 从开头命令重新执行到最后一次commit

        int lastCommit = 0;
        if (transcations.size() == commits.size()) {
          lastCommit = lines.size() - 1;
        } else {
          lastCommit = commits.get(commits.size() - 1);
        }
        for (int i = 0; i <= lastCommit; i++) {
          handleCommand(lines.get(i), this);
        }
        System.out.println("read " + (lastCommit + 1) + " lines");
        reader.close();
        bufferedReader.close();

        // 如果有部分未commit的语句被执行，将清空log并重写实际执行部分
        if (transcations.size() != commits.size()) {
          FileWriter writer_1 = new FileWriter(fileName);
          writer_1.write("");
          writer_1.close();
          FileWriter writer_2 = new FileWriter(fileName, true);
          for (int i = 0; i <= lastCommit; i++) {
            writer_2.write(lines.get(i) + "\n");
          }
          writer_2.close();
        }

      } catch (IOException e) {
        e.printStackTrace();
      }
    }

  }

  // 在系统恢复 执行指令时调用
  public String handleCommand(String command, Manager manager) {
    String cmd = command.split(" ")[0].toLowerCase();

    //词法分析
    SQLLexer lexer = new SQLLexer(CharStreams.fromString(command));
    lexer.removeErrorListeners();
    lexer.addErrorListener(StatementErrorListener.instance);
    CommonTokenStream tokens = new CommonTokenStream(lexer);

    //句法分析
    SQLParser parser = new SQLParser(tokens);
    parser.removeErrorListeners();
    parser.addErrorListener(StatementErrorListener.instance);

    //语义分析
    try {
      StatementVisitor visitor = new StatementVisitor(manager, -1);
      return String.valueOf(visitor.visitParse(parser.parse()));
    } catch (Exception e) {
      return ("Exception: illegal SQL statement! Error message: " + e.getMessage());
    }

  }

}