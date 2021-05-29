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
TODO:
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
    Condition(Comparer left,Comparer right,ComparatorType type){
        mLeft = left;
        mRight = right;
        mType = type;
    }

    // called when compared with column
    ResultType JudgeCondition(QueryRow row){
        if(mLeft.mType== ComparerType.COLUMN){
            mLeft = row.calColumnComparer((String) mLeft.mValue);
        }
        if(mRight.mType==ComparerType.COLUMN){
            mRight = row.calColumnComparer((String)mRight.mValue);
        }
        return JudgeCondition();
    }

    // called when two constants are compared
    ResultType JudgeCondition(){
        if(mRight.mType==ComparerType.NULL||mLeft.mType==ComparerType.NULL){
            return ResultType.UNKNOWN;
        }
        if(mLeft.mType!=mRight.mType){
            throw new CompareTypeNotMatchException(mLeft.mType,mRight.mType);
        }
        boolean result = false;
        switch (mType){
            case EQ:
                result = mLeft.mValue.compareTo(mRight.mValue) == 0;
                break;
            case NE:
                result = mLeft.mValue.compareTo(mRight.mValue) != 0;
                break;
            case GE:
                result = mLeft.mValue.compareTo(mRight.mValue) >= 0;
                break;
            case LE:
                result = mLeft.mValue.compareTo(mRight.mValue) <= 0;
                break;
            case GT:
                result = mLeft.mValue.compareTo(mRight.mValue) > 0;
                break;
            case LT:
                result = mLeft.mValue.compareTo(mRight.mValue) < 0;
                break;
        }
        if(!result){
            return ResultType.FALSE;
        }
        return ResultType.TRUE;
    }
}
