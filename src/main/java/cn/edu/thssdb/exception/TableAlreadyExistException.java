package cn.edu.thssdb.exception;

public class TableAlreadyExistException extends RuntimeException{

    private String tableName;

    public TableAlreadyExistException(String tableName){
        this.tableName = tableName;
    }

    @Override
    public String getMessage() {
        return "Exception: table " + tableName + " already existed!";
    }
}
