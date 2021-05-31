package cn.edu.thssdb.query;

/*
comparer :
    column_full_name
    | literal_value ;

literal_value :
    NUMERIC_LITERAL
    | STRING_LITERAL
    | K_NULL ;

column_full_name:
    ( table_name '.' )? column_name ;

 */

import cn.edu.thssdb.type.ComparerType;

public class Comparer {
    ComparerType mType;
    Comparable mValue;
    public Comparer(ComparerType type, String value){
        mType = type;
        switch (type){
            case COLUMN:
            case STRING:
                mValue = value;
                break;
            case NUMERIC:
                mValue = Double.parseDouble(value);
                break;
            default:
                mValue = null;
        }
    }
}
