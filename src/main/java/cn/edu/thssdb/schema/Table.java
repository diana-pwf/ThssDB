package cn.edu.thssdb.schema;

import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.query.*;
import cn.edu.thssdb.type.ResultType;
import cn.edu.thssdb.utils.Pair;

import cn.edu.thssdb.helper.*;
import cn.edu.thssdb.exception.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *  参数说明：
 *  primaryIndex: 记录本张表的主键位置作为主属性
 *  entries: 记录本表所有主键
 *  index<主键，记录>: 记录索引，使用 row = index.get(primaryEntry) 可以拿到对应主键的记录
 */


public class Table implements Iterable<Row> {
  ReentrantReadWriteLock lock;
  public String databaseName;
  public String tableName;
  public ArrayList<Column> columns;
  // 列名列表
  private ArrayList<String> columnsName;
  public int schemaLength;
  public BPlusTree<Entry, Row> index;
  private int primaryIndex;
  public ArrayList<Entry> entries;

  public Table(String databaseName, String tableName, Column[] columns) {
    this.lock = new ReentrantReadWriteLock();
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.columns = new ArrayList<>(Arrays.asList(columns));
    this.columnsName = new ArrayList<>();
    for(Column column: this.columns){
      this.columnsName.add(column.getName());
    }
    this.schemaLength = this.columns.size();
    this.index = new BPlusTree<>();
    // 记录 primary key 的位置 primary index
    for (int i = 0; i < this.columns.size(); i++)
    {
      if (this.columns.get(i).isPrimary()){
        primaryIndex = i;
        break;
      }
    }
    this.entries = new ArrayList<>();
    recover();
  }

  /**
   *  功能：传入欲查询记录主 entry，返回对应的一行记录
   *  参数：entry为待查询记录的主 entry
   */
  public Row getRow(Entry entry){
    try{
      return index.get(entry);
    }catch (Exception e){
      throw e;
    }
  }

  /**
   * 功能：给定一个 ArrayList<Entry>，实际将数据插入表中
   * @param entryList
   */

  public void insertEntries(ArrayList<Entry> entryList) {
    try{
      // put 里面自己有检测 duplicated key
      index.put(entryList.get(primaryIndex), new Row(entryList.toArray(new Entry[0])));
      entries.add(entryList.get(primaryIndex));
    }catch (Exception e){
      throw e;
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

    ArrayList<Entry> rowEntries = new ArrayList<>();
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
    try{
      lock.writeLock().lock();
      insertEntries(rowEntries);
    }catch (Exception e){
      throw e;
    }finally{
      lock.writeLock().unlock();
    }

  }

  /**
   * 功能：给定一个 Column 列表和 String 列表，依 columns 和 values 的对应位置构建一个 ArrayList<Entry>
   *     之后传给 insert(ArrayList<Entry> entry_list) 真正进行插入
   * @param columnsName: specifying 要插入的列
   * @param values: 要插入的值（以 String[] 传入）
   */
  public void insert(ArrayList<String> columnsName, String[] values){
    if(columnsName.size() == 0){
      insert(values);
      return;
    }

    if(values == null){
      throw new OperateTableWithNullException("value");
    } else if(columns == null){
      throw new OperateTableWithNullException("column");
    }

    int columnsLen = columnsName.size();
    int valuesLen = values.length;
    if(columnsLen > schemaLength){
      throw new ExceedSchemaLengthException(schemaLength, columnsLen, "columns");
    }else if(valuesLen > schemaLength) {
      throw new ExceedSchemaLengthException(schemaLength, valuesLen, "values");
    }else if(columnsLen != valuesLen){
      throw new SchemaLengthMismatchException(columnsLen, valuesLen, "columns");
    }

    // 检查是否有未定义 column 或重复 column
    for(String name: columnsName){
      int index = this.columnsName.indexOf(name);
      if(index < 0){
        throw new ColumnNotExistException(databaseName, tableName, name);
      }
      if(index != this.columnsName.lastIndexOf(name)){
        throw new DuplicateColumnException("insert", name);
      }
    }

    ArrayList<Entry> rowEntries = new ArrayList<>();
    Comparable value;
    ValueParser vp = new ValueParser();

    // 按表的列序构造 entry
    for(Column col : this.columns){
      int index = columnsName.indexOf(col.getName());
      value = null;
      // 当前列被指定
      if(index > -1){
        try {
          value = vp.getValue(col, values[index]);
        } catch (Exception e){
          throw e;
        }
      }
      try{
        vp.checkValid(col, value);
        rowEntries.add(new Entry(value));
      }catch (Exception e){
        throw e;
      }
    }
    try{
      lock.writeLock().lock();
      insertEntries(rowEntries);
    }catch (Exception e){
      throw e;
    }finally{
      lock.writeLock().unlock();
    }
  }


  /**
   *  功能：提供待删除记录的主 entry，将对应记录自 index 中删除
   *  参数：entry为待删除记录的主 entry
   */
  public void deleteEntry(Entry entry) {
    try{
      index.remove(entry);
      entries.remove(entry);
    }catch (Exception e){
      throw e;
    }
  }


  /**
   * 功能：遍历表中的 Row，针对每一行数据进行逻辑判断（是否符合删除的条件）
   * @param conditions: 删除条件
   */

  public String delete(MultipleCondition conditions){
    Integer count = 0;
    MultipleCondition condition = conditions;
    for(Row row : this){
      MetaInfo info = new MetaInfo(databaseName, tableName, columns);
      QueryRow queryRow = new QueryRow(info, row);
      if(condition == null || condition.JudgeMultipleCondition(queryRow) == ResultType.TRUE ) {
        try{
          lock.writeLock().lock();
          deleteEntry(row.getEntries().get(primaryIndex));
        }catch (Exception e){
          throw e;
        }finally{
          lock.writeLock().unlock();
        }
        ++count;
      }
    }
    if(count == 0) return "Deleted no data in " + tableName + ". There might be something wrong while specifying columns or conditions.";
    return "Successfully deleted " + count.toString() + " data from the table: " + tableName;
  }

  public void dropSelf() throws Exception {
    try {
      lock.writeLock().lock();
      columns.clear();
      index = null;
      entries.clear();
      columns.clear();
      String filename = "DATA/" + databaseName + "_" + tableName + ".data";
      File file = new File(filename);
      if (file.isFile() && file.exists()) {
        if (file.delete()) {
          System.out.println("successfully drop table " + tableName);
        } else {
          throw new Exception("Error occurs when drop table" + tableName);
        }
      }
    }catch (Exception e){
      throw  e;
    }finally {
      lock.writeLock().unlock();
    }
  }

  /**
   *  参数：columnName为要更新的那一列的属性名称，comparer为待更新的值， conditions为where后所接的条件表达式
   *  功能：将满足条件表达式的行的相应属性更新为相应的值
   *  返回值：向客户端说明执行情况
   */
  public String update(String columnName, Comparer comparer, MultipleCondition conditions) {
    Integer count = 0;

    // 取得修改的位置
    int index = this.columnsName.indexOf(columnName);
    // 没有找到对应列
    if(index < 0){
      throw new ColumnNotExistException(databaseName, tableName, columnName);
    }
    // 设置修改列
    Column column = this.columns.get(index);

    // 检查 type
    ValueParser vp = new ValueParser();
    Comparable newValue;
    try {
      newValue = vp.compararToComparable(column, comparer);
      vp.checkValid(column, newValue);
    } catch (Exception e){
      throw e;
    }

    // 记录逻辑判断条件
    MultipleCondition condition = new MultipleCondition(conditions);
    for(Row row : this){
      MetaInfo info = new MetaInfo(databaseName, tableName, columns);
      QueryRow queryRow = new QueryRow(info, row);
      if(condition == null || condition.JudgeMultipleCondition(queryRow) == ResultType.TRUE ) {
        // 取得旧主键
        Entry entry = queryRow.getEntries().get(primaryIndex);

        // 取得旧行并构建新行
        Row oldRow = getRow(entry);
        Comparable oldValue = oldRow.getEntries().get(index).value;
        Row newRow = new Row(oldRow);
        newRow.getEntries().get(index).value = newValue;

        // 分为是否更新主键有不同操作

        try {
          lock.writeLock().lock();
          if(column.isPrimary() && oldValue != newValue) {
            insertEntries(newRow.getEntries());
            deleteEntry(entry);
          }else {
            this.index.update(entry, newRow);
          }
        } catch (Exception e){
          throw e;
        } finally {
          lock.writeLock().unlock();
        }
        ++count;
      }
    }
    if(count == 0) return "No data in " + tableName + " matched conditions. Updated no data in " + tableName + ".";
    return "Successfully updated " + count.toString() + " data from the table: " + tableName;
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
      oo.close();
    } catch(Exception e){
      System.out.println("error occurs when serializing file");
    }
  }

  private ArrayList<Row> deserialize(File file){
    ArrayList<Row> rows;
    try{
      ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
      rows = (ArrayList<Row>) ois.readObject();
      System.out.println("successfully deserialize file! ");
      ois.close();
    } catch (Exception e) {
      rows = null;
      System.out.println("error occurs when deserializing file");
    }
    return rows;
  }

  public String showMeta() {
    String schema = "columnName, columnType, primaryKey, isNull, maxLength";
    StringBuilder result = new StringBuilder("tableName: " + tableName + "\n" + schema + "\n");
    for(Column column : columns) {
      if(column != null) {
        result.append(column.toString()).append("\n");
      }
    }
    return result.toString();
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
