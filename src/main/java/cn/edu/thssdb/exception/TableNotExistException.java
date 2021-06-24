package cn.edu.thssdb.exception;

public class TableNotExistException extends RuntimeException{
    private String tableName;
    public TableNotExistException(String tableName){
        this.tableName = tableName;
    }

    @Override
    public String getMessage() {
        return "Exception: targeted table " + tableName +" doesn't exist!";
    }
}

