package cn.edu.thssdb.exception;

public class QueryColumnCollisionException extends RuntimeException{
    String column;
    public QueryColumnCollisionException(String column){
        this.column = column;
    }
    @Override
    public String getMessage(){
        return "Exception: column " + column + " exists in more than one table! ";
    }
}
