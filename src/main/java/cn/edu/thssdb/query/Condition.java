package cn.edu.thssdb.query;

/*
according to SQL.g4
__________________________________________
condition :
        expression comparator expression;
expression :
        comparer
        | expression ( MUL | DIV ) expression
        | expression ( ADD | SUB ) expression
        | '(' expression ')';
 comparator :
        EQ | NE | LE | GE | LT | GT ;
__________________________________________

we simplify expression as comparer:
__________________________________________
condition :
        comparer comparator comparer
__________________________________________

when parsing statement,should convert the parsed part to the corresponding class


*/

import cn.edu.thssdb.exception.CompareTypeNotMatchException;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.ComparatorType;
import cn.edu.thssdb.type.ComparerType;
import cn.edu.thssdb.type.ResultType;

import java.sql.PreparedStatement;

public class Condition {
    Comparer mLeft;
    Comparer mRight;
    ComparatorType mType;
    public Condition(Comparer left, Comparer right, ComparatorType type){
        mLeft = left;
        mRight = right;
        mType = type;
    }

    // called when compared with column
    ResultType JudgeCondition(QueryRow row){
        Comparer left = new Comparer(mLeft.mType,String.valueOf(mLeft.mValue));
        Comparer right = new Comparer(mRight.mType,String.valueOf(mRight.mValue));
        if(mLeft.mType== ComparerType.COLUMN){
            left = row.calColumnComparer((String) mLeft.mValue);
        }
        if(mRight.mType==ComparerType.COLUMN){
            right = row.calColumnComparer((String)mRight.mValue);
        }
        return JudgeCondition(new Condition(left,right,mType));
    }

    // called when two constants are compared
    ResultType JudgeCondition(Condition condition){
        if(condition.mRight.mType==ComparerType.NULL||condition.mLeft.mType==ComparerType.NULL){
            return ResultType.UNKNOWN;
        }
        if(condition.mLeft.mType!=condition.mRight.mType){
            throw new CompareTypeNotMatchException(condition.mLeft.mType,condition.mRight.mType);
        }
        boolean result = false;
        switch (mType){
            case EQ:
                result = condition.mLeft.mValue.compareTo(condition.mRight.mValue) == 0;
                break;
            case NE:
                result = condition.mLeft.mValue.compareTo(condition.mRight.mValue) != 0;
                break;
            case GE:
                result = condition.mLeft.mValue.compareTo(condition.mRight.mValue) >= 0;
                break;
            case LE:
                result = condition.mLeft.mValue.compareTo(condition.mRight.mValue) <= 0;
                break;
            case GT:
                result = condition.mLeft.mValue.compareTo(condition.mRight.mValue) > 0;
                break;
            case LT:
                result = condition.mLeft.mValue.compareTo(condition.mRight.mValue) < 0;
                break;
        }
        if(!result){
            return ResultType.FALSE;
        }
        return ResultType.TRUE;
    }
}
