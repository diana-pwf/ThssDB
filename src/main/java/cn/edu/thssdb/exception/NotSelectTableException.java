package cn.edu.thssdb.exception;

public class NotSelectTableException extends RuntimeException{
    @Override
    public String getMessage() {
        return "Exception: not select any table!";
    }
}
