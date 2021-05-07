package cn.edu.thssdb.exception;

public class ExceedSchemaLengthException extends RuntimeException{
  private int schemaLen;
  private int obtainLen;
  private String mismatchTarget;
  public ExceedSchemaLengthException(int schemaLen, int obtainLen, String mismatchTarget){
    super();
    this.schemaLen = schemaLen;
    this.obtainLen = obtainLen;
    this.mismatchTarget = mismatchTarget;
  }
  @Override
  public String getMessage() {

    return "Exception: got " + this.obtainLen + " " + this.mismatchTarget + ", " +
            "which exceeded schema length: " + this.schemaLen + ".";
  }
}
