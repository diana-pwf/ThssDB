package cn.edu.thssdb.exception;

/**
 * 插入元素长度超过长度限制时的异常
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
