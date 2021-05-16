package cn.edu.thssdb.exception;

/**
 * 非空 column 被插入空值时报错
 * 成员变量：列名称
 */

public class ValueExceedMaxLengthException extends RuntimeException{
  private String columnName;
  private int maxLength;
  private int valLength;
  public ValueExceedMaxLengthException(String columnName, int maxLength, int valLength){
    super();
    this.columnName = columnName;
    this.maxLength = maxLength;
    this.valLength = valLength;
  }
  @Override
  public String getMessage() {
    return "Exception: max length of " + columnName + " is " + maxLength +
            ", however got value with length: " + valLength + "."; }
}
