package cn.edu.thssdb.exception;

public class SchemaLengthMismatchException extends RuntimeException{
  private int schemaLen;
  private int obtainLen;
  private String mismatchTarget;
  public SchemaLengthMismatchException(int schemaLen, int obtainLen, String mismatchTarget){
    super();
    this.schemaLen = schemaLen;
    this.obtainLen = obtainLen;
    this.mismatchTarget = mismatchTarget;
  }
  @Override
  public String getMessage() {

    return "Exception: the schema expected " + this.schemaLen + " " + this.mismatchTarget + ", " +
            "but got " + this.obtainLen + " " + this.mismatchTarget + ".";
  }
}
