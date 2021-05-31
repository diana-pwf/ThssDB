package cn.edu.thssdb.exception;

public class IllegalSQLStatement extends RuntimeException{
    String statement;
    public IllegalSQLStatement(String stmt){
        statement = stmt;
    }
    @Override
    public String getMessage(){
        return "Exception: illegal SQL statement "+statement;
    }
}
