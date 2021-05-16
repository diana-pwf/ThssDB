package cn.edu.thssdb.exception;

/**
 * 非空 column 被插入空值时报错
 * 成员变量：列名称
 */

public class NullValueException extends RuntimeException{
  private String columnName;
  public NullValueException(String columnName){
    super();
    this.columnName = columnName;
  }
  @Override
  public String getMessage() { return "Exception: " + columnName + " must not be null."; }
}
