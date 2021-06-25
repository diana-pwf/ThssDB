package cn.edu.thssdb.helper;

import cn.edu.thssdb.exception.ValueTypeMismatchException;
import cn.edu.thssdb.query.Comparer;
import cn.edu.thssdb.schema.Column;

import cn.edu.thssdb.exception.NullValueException;
import cn.edu.thssdb.exception.ValueExceedMaxLengthException;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.type.ComparerType;

public class ValueParser {

    public ValueParser(){};

    public Comparable getValue(Column column, String string){
        try {
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
                    return string;
                default:
                    return null;
            }
        } catch (Exception e){
            throw new ValueTypeMismatchException(column);
        }
    }

    public void checkValid(Column column, Comparable value){
        // 检查非 null 限制
        if(column.isNotNull() && value == null){
            throw new NullValueException(column.getName());
        }
        // 检查最大长度限制
        if(value != null && column.getType() == ColumnType.STRING ){
            int maxLength = column.getMaxLength();
            int valLength = ((String)value).length();
            if(maxLength > -1 && valLength > maxLength){
                throw new ValueExceedMaxLengthException(column.getName(), maxLength, valLength);
            }
        }
    }

    public Comparable compararToComparable(Column column, Comparer comparer) throws ValueTypeMismatchException{
        // null
        if(comparer == null || comparer.getValue() == null || comparer.getType() == null){
            return null;
        } else if (comparer.getType() == ComparerType.NUMERIC){
            // num
            String valueString = comparer.getValue().toString();
            switch (column.getType()){
                case INT:
                    return Integer.parseInt(valueString.substring(0, valueString.indexOf('.')));
                case LONG:
                    return Long.parseLong(valueString.substring(0, valueString.indexOf('.')));
                case FLOAT:
                    return Float.parseFloat(valueString);
                case DOUBLE:
                    return Double.parseDouble(valueString);
                default:
            }
        }else if (comparer.getType() == ComparerType.STRING && column.getType() == ColumnType.STRING){
            // string
            return comparer.getValue().toString();
        }

        // 如果不属于上面任何一种情况，表示类型有错误
        // FIXME: 请新增一种错误
        throw new ValueTypeMismatchException(column);
    }
}
