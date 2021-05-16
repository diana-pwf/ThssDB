package cn.edu.thssdb.exception;

public class DuplicateColumnException extends RuntimeException{
  private String columnName;

  public DuplicateColumnException(String columnName){
    this.columnName = columnName;
  }

  @Override
  public String getMessage() {
    return "Exception: insert with duplicate columns: " + columnName + ".";
  }
}
