package cn.edu.thssdb.exception;

import cn.edu.thssdb.schema.Column;

public class ValueTypeMismatchException extends RuntimeException{
  private Column column;

  public ValueTypeMismatchException(Column column){
    this.column = column;
  }

  @Override
  public String getMessage() {
    return "Exception: Value type mismatch! Expected " +
            column.getType() + " value for " + column.getName() + ".";
  }
}
