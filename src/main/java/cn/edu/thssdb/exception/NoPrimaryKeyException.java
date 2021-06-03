package cn.edu.thssdb.exception;

public class NoPrimaryKeyException extends RuntimeException{
  @Override
  public String getMessage() {
    return "Exception: table need to has one primary key!";
  }
}
