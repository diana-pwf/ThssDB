package cn.edu.thssdb.exception;

public class DatabaseAlreadyExistException extends RuntimeException{

    private String databaseName;

    public DatabaseAlreadyExistException(String databaseName){
        this.databaseName = databaseName;
    }

    @Override
    public String getMessage() {
        return "Exception: database " + databaseName + " already existed!";
    }
}
