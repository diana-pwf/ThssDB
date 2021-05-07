package cn.edu.thssdb.schema;

import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.utils.Pair;
import cn.edu.thssdb.exception.OperateTableWithNullException;
import cn.edu.thssdb.exception.SchemaLengthMismatchException;

import javax.lang.model.type.ArrayType;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table implements Iterable<Row> {
  ReentrantReadWriteLock lock;
  private String databaseName;
  public String tableName;
  public ArrayList<Column> columns;
  public BPlusTree<Entry, Row> index;
  private int primaryIndex;
  public ArrayList<Entry> entries;

  public Table(String databaseName, String tableName, Column[] columns) {
    // TODO
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.columns = new ArrayList<>(Arrays.asList(columns));
    this.index = new BPlusTree<>();
    this.lock = new ReentrantReadWriteLock();
    for (int i = 0; i < this.columns.size(); i++)
    {
      if (this.columns.get(i).isPrimary())
        primaryIndex = i;
    }
    this.lock = new ReentrantReadWriteLock();
  }

  private void recover() {
    File file = new File("Data/table_init.txt");
    ArrayList<Row>rows = deserialize(file);
    for(Row row : rows){
      index.put(row.getEntries().get(this.primaryIndex),row);
      entries.add(row.getEntries().get(this.primaryIndex));
    }
  }

  private void checkNull(ArrayList<Column> columns, ArrayList<Entry> entries){
    if(columns == null){
      throw new OperateTableWithNullException("columns");
    }else if(entries == null){
      throw new OperateTableWithNullException("entries");
    }
  }

  private void checkLen(ArrayList<Column> columns, ArrayList<Entry> entries){
    int schemaLen = this.columns.size();
    int columnsLen = columns.size();
    int entriesLen = entries.size();
    if(columnsLen != schemaLen){
      throw new SchemaLengthMismatchException(schemaLen, columnsLen, "columns");
    }else if(entriesLen != schemaLen){
      throw new SchemaLengthMismatchException(schemaLen, entriesLen, "entries");
    }
  }



  /**
   *  功能：传入欲查询记录主 entry，返回对应的一行记录
   *  参数：entry为待查询记录的主 entry
   */
  public Row get(Entry entry){
    Row row;
    try{
      row = index.get(entry);
    }catch (Exception e){
      throw e;
    }
    return row;
  }

  /**
   *  功能：插入记录
   *  参数：columns 为 schema, entries 为记录键
   */
  public void insert(ArrayList<Column> columns, ArrayList<Entry> entries) {
    // TODO
    // check whether there is null columns or entries
    // check whether the length of columns and entries is preferable
    try{
      checkNull(columns, entries);
      checkLen(columns, entries);
    }catch (Exception e){
      throw e;
    }

    // TODO: check whether the inserted entries is valid
    Row row = new Row(entries.toArray(new Entry[0]));
    try{
      lock.writeLock().lock();
      index.put(entries.get(primaryIndex), row);
    }catch (Exception e){
      throw e;
    }
    finally{
      lock.writeLock().unlock();
    }
  }

  /**
   *  功能：提供待删除记录的主 entry，将对应记录自 index 中删除
   *  参数：entry为待删除记录的 entry
   */
  public void delete(Entry entry) {
    // TODO
    try{
      lock.writeLock().lock();
      index.remove(entry);
    }catch (Exception e){
      throw e;
    }finally{
      lock.writeLock().unlock();
    }
  }

  public void update() {
    // TODO
  }

  public void persist(){
    ArrayList<Row> rows = new ArrayList<Row>();
    for(Entry entry : entries){
      rows.add(index.get(entry));
    }
    serialize(rows,"Data/table_int.txt");
  }

  private void serialize(ArrayList<Row> rows,String fileName){
    try{
      ObjectOutputStream oo = new ObjectOutputStream(new FileOutputStream(
              new File(fileName)));
      oo.writeObject(rows);
      System.out.println("successfully serialize file!");
      oo.close();}
    catch(Exception e){
      System.out.println("error occurs when serializing file");
    }
  }

  private ArrayList<Row> deserialize(File file){
    ArrayList<Row> rows;
    try{
      ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
      rows = (ArrayList<Row>) ois.readObject();
      System.out.println("successfully deserialize file! ");
    } catch (Exception e) {
      rows = null;
      System.out.println("error occurs when deserializing file");
    }
    return rows;
  }

  private class TableIterator implements Iterator<Row> {
    private Iterator<Pair<Entry, Row>> iterator;

    TableIterator(Table table) {
      this.iterator = table.index.iterator();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Row next() {
      return iterator.next().right;
    }
  }

  @Override
  public Iterator<Row> iterator() {
    return new TableIterator(this);
  }
}
