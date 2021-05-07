package cn.edu.thssdb.exception;

public class OperateTableWithNullException extends RuntimeException{
  private String nullObject;
  public OperateTableWithNullException(String nullObject){
    super();
    this.nullObject = nullObject;
  }
  @Override
  public String getMessage() {
    return "Exception: trying to operate on table with null " + this.nullObject;
  }
}
