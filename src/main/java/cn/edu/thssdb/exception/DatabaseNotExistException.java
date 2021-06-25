package cn.edu.thssdb.exception;

public class DatabaseNotExistException extends RuntimeException{
    @Override
    public String getMessage() {
        return "Exception: database does not exist or have not specified current database!";
    }
}
