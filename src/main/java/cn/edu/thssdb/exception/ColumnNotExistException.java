package cn.edu.thssdb.exception;

public class ColumnNotExistException extends RuntimeException{
  private String databaseName;
  private String tableName;
  private String columnName;
  public ColumnNotExistException(String databaseName, String tableName, String columnName){
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.columnName = columnName;
  }

  @Override
  public String getMessage() {
    return "Exception: column name " + columnName + " doesn't exist in database: " +
            databaseName + " ,table: " + tableName + ".";
  }
}
