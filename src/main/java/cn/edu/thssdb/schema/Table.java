package cn.edu.thssdb.schema;

import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.query.Comparer;
import cn.edu.thssdb.query.Condition;
import cn.edu.thssdb.query.multipleCondition;
import cn.edu.thssdb.utils.Pair;

import cn.edu.thssdb.helper.*;
import cn.edu.thssdb.exception.*;

import javax.lang.model.type.ArrayType;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *  参数说明：
 *  primaryIndex: 记录本张表的主键位置作为主属性
 *  entries: 记录本表所有主键（FIXME: 根据参考架构，可以改到页式）
 *  index<主键，记录>: 记录索引，使用 row = index.get(primaryEntry) 可以拿到对应主键的记录
 */


public class Table implements Iterable<Row> {
  ReentrantReadWriteLock lock;
  private String databaseName;
  public String tableName;
  public ArrayList<Column> columns;
  public int schemaLength;
  public BPlusTree<Entry, Row> index;
  private int primaryIndex;
  public ArrayList<Entry> entries;

  public Table(String databaseName, String tableName, Column[] columns) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.columns = new ArrayList<>(Arrays.asList(columns));
    this.schemaLength = this.columns.size();
    this.index = new BPlusTree<>();
    this.lock = new ReentrantReadWriteLock();
    // 记录 primary key 的位置 primary index
    for (int i = 0; i < this.columns.size(); i++)
    {
      if (this.columns.get(i).isPrimary()){
        primaryIndex = i;
        break;
      }
    }
    this.lock = new ReentrantReadWriteLock();
  }

  private void checkNull(ArrayList<Column> columns, ArrayList<Entry> entries){
    if(columns == null){
      throw new OperateTableWithNullException("columns");
    }else if(entries == null){
      throw new OperateTableWithNullException("entries");
    }
  }

  /**
   * FIXME: redundant
   * @param columns
   * @param entries
   * @param equal
   */
  private void checkLen(ArrayList<Column> columns, ArrayList<Entry> entries, boolean equal){
    int columnsLen = columns.size();
    int entriesLen = entries.size();
    if(equal){
      if(columnsLen != schemaLength){
        throw new SchemaLengthMismatchException(schemaLength, columnsLen, "columns");
      }else if(entriesLen != schemaLength){
        throw new SchemaLengthMismatchException(schemaLength, entriesLen, "entries");
      }
    }else{
      if(columnsLen > schemaLength){
        throw new ExceedSchemaLengthException(schemaLength, columnsLen, "columns");
      }else if(entriesLen > schemaLength) {
        throw new ExceedSchemaLengthException(schemaLength, entriesLen, "entries");
      }else if(columnsLen != entriesLen){
        throw new SchemaLengthMismatchException(columnsLen, entriesLen, "columns");
      }
    }
  }


  /**
   *  功能：传入欲查询记录主 entry，返回对应的一行记录
   *  参数：entry为待查询记录的主 entry
   */
  public Row getRow(Entry entry){
    Row row;
    try{
      row = index.get(entry);
    }catch (Exception e){
      throw e;
    }
    return row;
  }

  /**
   * 功能：给定一个 ArrayList<Entry>，实际将数据插入表中
   * @param entry_list
   * TODO: 页式存储修改
   */
  public void insert(ArrayList<Entry> entry_list) {
    try{
      lock.writeLock().lock();
      index.put(entry_list.get(primaryIndex), new Row(entry_list.toArray(new Entry[0])));
      entries.add(entry_list.get(primaryIndex));
    }catch (Exception e){
      throw e;
    }finally{
      lock.writeLock().unlock();
    }
  }

  /**
   * 功能：给定一个 String 列表，构建一个 ArrayList<Entry> 之后传给 insert(ArrayList<Entry> entry_list) 真正进行插入
   * @param values: 遍历语法树后得到的 String 列表
   */

  public void insert(String[] values){
    if(values == null){
      throw new OperateTableWithNullException("value");
    }

    if(values.length > schemaLength){
      throw new SchemaLengthMismatchException(schemaLength, values.length, "value");
    }

    ArrayList<Entry> rowEntries = new ArrayList<Entry>();
    Column col;
    Comparable value;
    ValueParser vp = new ValueParser();

    for(int i = 0 ; i < schemaLength; i++){
      col = columns.get(i);
      // 如果 column 数多于 value 数，表示后面的 value 都是 null
      value = i < values.length ? vp.getValue(col, values[i]) : null;
      try{
        vp.checkValid(col, value);
        rowEntries.add(new Entry(value));
      }catch (Exception e){
        throw e;
      }
    }

    insert(rowEntries);
  }

  /**
   * 功能：给定一个 Column 列表和 String 列表，依 columns 和 values 的对应位置构建一个 ArrayList<Entry>
   *     之后传给 insert(ArrayList<Entry> entry_list) 真正进行插入
   * @param columns: specifying 要插入的列
   * @param values: 要插入的值（以 String[] 传入）
   */

  public void insert(ArrayList<Column> columns, String[] values){
    if(values == null){
      throw new OperateTableWithNullException("value");
    } else if(columns == null){
      throw new OperateTableWithNullException("column");
    }

    int columnsLen = columns.size();
    int valuesLen = values.length;
    if(columnsLen > schemaLength){
      throw new ExceedSchemaLengthException(schemaLength, columnsLen, "columns");
    }else if(valuesLen > schemaLength) {
      throw new ExceedSchemaLengthException(schemaLength, valuesLen, "values");
    }else if(columnsLen != valuesLen){
      throw new SchemaLengthMismatchException(columnsLen, valuesLen, "columns");
    }

    // 检查是否有未定义 column 或有重复 column
    for(Column column: columns){
      if(this.columns.indexOf(column) < 0){
        throw new ColumnNotExistException(databaseName, tableName, column.getName());
      }
      if(columns.indexOf(column) != columns.lastIndexOf(column)){
        throw new DuplicateColumnException(column.getName());
      }
    }

    ArrayList<Entry> rowEntries = new ArrayList<Entry>();
    int i = 0;
    Comparable value;
    ValueParser vp = new ValueParser();

    for(Column col : this.columns){
      value = null;
      for(; i < columnsLen; i++){
        if(col.compareTo(columns.get(i)) != 0) continue;
        value = vp.getValue(col, values[i]);
        break;
      }
      try{
        vp.checkValid(col, value);
        rowEntries.add(new Entry(value));
      }catch (Exception e){
        throw e;
      }
    }

    insert(rowEntries);
  }


  /**
   *  功能：提供待删除记录的主 entry，将对应记录自 index 中删除
   *  参数：entry为待删除记录的主 entry
   */
  public void delete(Entry entry) {
    try{
      lock.writeLock().lock();
      index.remove(entry);
      entries.remove(entry);
    }catch (Exception e){
      throw e;
    }finally{
      lock.writeLock().unlock();
    }
  }


  /**
   * 功能：遍历表中的 Row，针对每一行数据进行逻辑判断（是否符合删除的条件）
   * @param condition: 删除条件
   */

  //FIXME: use multipleCondition to delete
  public void delete(Condition condition){
    for(Row row : this){
//      if(condition != null && condition.JudgeCondition(row) == false ) continue;
     delete(row.getEntries().get(primaryIndex));

    }
  }

  public void drop(){
    
  }

  /**
   *  FIXME:
   *  功能：提供待修改记录的主 entry，将根据传入参数修改 row
   *  参数：entry为待修改记录的主 entry，columns 和 entries 是要修改的对应属性和值
   */
  public void update(Entry primaryEntry, ArrayList<Column> columns, ArrayList<Entry> entries) {
    // check whether there is null columns or entries
    // check whether the length of columns and entries is preferable
    /*
    try{
      checkNull(columns, entries);
      checkLen(columns, entries, false);
    }catch (Exception e){
      throw e;
    }
    */

    Row row = this.getRow(primaryEntry);
    int columnsLen = columns.size();
    int schemaIndex = 0, columnIndex = 0;
    for(; columnIndex < columnsLen; columnIndex++){
      while(columns.get(columnIndex).compareTo(this.columns.get(schemaIndex)) != 0){
        ++schemaIndex;
      }
      row.entries.get(schemaIndex).value = entries.get(schemaIndex).value;
    }
    try {
      lock.writeLock().lock();
      index.update(row.getEntries().get(primaryIndex), row);
      entries.set(primaryIndex, row.getEntries().get(primaryIndex));
    }catch(Exception e){
      throw e;
    }finally{
      lock.writeLock().unlock();
    }
  }


  /**
   *  TODO:
   *  参数：columnName为要更新的那一列的属性名称，comparer为待更新的值， conditions为where后所接的条件表达式
   *  功能：将满足条件表达式的行的相应属性更新为相应的值
   *  返回值：向客户端说明执行情况
   */
  public String update(String columnName, Comparer comparer, multipleCondition conditions) {
    return "";
  }


  private void recover() {
    File file = new File("DATA/"+this.databaseName+'_'+this.tableName+".data");
    if(!file.exists()){
      System.out.println("Table file doesn't exist!");
      return;
    }
    ArrayList<Row>rows = deserialize(file);
    for(Row row : rows){
      if(row.entries.size()!=this.columns.size()){
        System.out.println("ERROR: File column data doesn't match with schema!");
        return;
      }
      index.put(row.getEntries().get(this.primaryIndex),row);
      entries.add(row.getEntries().get(this.primaryIndex));
    }
  }

  public void persist(){
    ArrayList<Row> rows = new ArrayList<Row>();
    for(Entry entry : entries){
      rows.add(index.get(entry));
    }
    // store format:"databaseName_tableName.data"
    serialize(rows,"DATA/"+this.databaseName+'_'+this.tableName+".data");
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
