package cn.edu.thssdb.exception;

public class QueryColumnNotFoundException extends RuntimeException{
    String column;
    String databaseName;
    public QueryColumnNotFoundException(String column, String databaseName){
        this.column = column;
        this.databaseName = databaseName;
    }
    @Override
    public String getMessage(){
        return "Exception: column " + column + " doesn't exist in database: " +
                databaseName;
    }
}
