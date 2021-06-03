package cn.edu.thssdb.exception;

public class DuplicatePrimaryKeyException extends RuntimeException{
  @Override
  public String getMessage() {
    return "Exception: table can only has one primary key!";
  }
}
