package cn.edu.thssdb.helper;

import cn.edu.thssdb.schema.Column;

import cn.edu.thssdb.exception.NullValueException;
import cn.edu.thssdb.exception.ValueExceedMaxLengthException;
import cn.edu.thssdb.type.ColumnType;

public class ValueParser {
    // private Column column;
    // private String string;
    // private Comparable value;

    public ValueParser(){};

    public Comparable getValue(Column column, String string){
        switch (column.getType()) {
            case INT:
                return Integer.parseInt(string);
            case LONG:
                return Long.parseLong(string);
            case FLOAT:
                return Float.parseFloat(string);
            case DOUBLE:
                return Double.parseDouble(string);
            case STRING:
                // FIXME:
                return string;
            default:
                return null;
        }
    }

    public void checkValid(Column column, Comparable value){
        // 检查非 null 限制
        if(column.isNotNull() && value == null){
            throw new NullValueException(column.getName());
        }
        // 检查最大长度限制
        if(column.getType() == ColumnType.STRING){
            int maxLength = column.getMaxLength();
            int valLength = ((String)value).length();
            if(value != null && maxLength > -1 && valLength > maxLength){
                throw new ValueExceedMaxLengthException(column.getName(), maxLength, valLength);
            }
        }
    }
}
