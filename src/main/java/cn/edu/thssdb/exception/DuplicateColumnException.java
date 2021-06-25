package cn.edu.thssdb.exception;

public class DuplicateColumnException extends RuntimeException{
  private String opType;
  private String columnName;

  public DuplicateColumnException(String opType, String columnName){
    this.opType = opType;
    this.columnName = columnName;
  }

  @Override
  public String getMessage() {
    return "Exception: " + opType + " with duplicate columns: " + columnName + ".";
  }
}
