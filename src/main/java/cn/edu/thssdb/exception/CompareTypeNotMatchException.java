package cn.edu.thssdb.exception;

import cn.edu.thssdb.type.ComparatorType;
import cn.edu.thssdb.type.ComparerType;

public class CompareTypeNotMatchException extends RuntimeException{
    private ComparerType left_type;
    private ComparerType right_type;
    public CompareTypeNotMatchException(ComparerType left, ComparerType right){
        left_type = left;
        right_type = right;
    }

    private String comparerType2String(ComparerType type){
        switch (type){
            case COLUMN:
                return "Column";
            case NUMERIC:
                return "Numeric";
            case STRING:
                return "String";
            default:
                return "Null";
        }
    }

    @Override
    public String getMessage() {
        String left_string = comparerType2String(left_type);
        String right_string = comparerType2String(right_type);
        return "Exception: comparer type " + left_string+","+right_string+"doesn't match!";
    }
}
