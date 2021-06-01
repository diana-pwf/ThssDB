package cn.edu.thssdb.query;

/*
according to SQL.g4
------------------------------------------------
multiple_condition :
    condition
    | multiple_condition AND multiple_condition
    | multiple_condition OR multiple_condition ;
------------------------------------------------

 */


import cn.edu.thssdb.type.ConditionType;
import cn.edu.thssdb.type.ResultType;

public class MultipleCondition {
    MultipleCondition mLeft;
    MultipleCondition mRight;
    ConditionType mType;

    boolean mSingle;
    Condition mSingleCondition;

    // construct with multiple condition
    public MultipleCondition(MultipleCondition Left, MultipleCondition Right, ConditionType type){
        mLeft = Left;
        mRight = Right;
        mType = type;
        mSingle = false;
    }

    // construct with single condition
    public MultipleCondition(Condition condition){
        mSingleCondition = condition;
        mSingle = true;
    }

    public ResultType JudgeMultipleCondition(QueryRow row){
        // Todo:需要保证multipleCondition != null
        // only one condition
        if(mSingle){
            if(mSingleCondition == null){
                return ResultType.TRUE;
            }
            else{
                return mSingleCondition.JudgeCondition(row);
            }
        }
        //TODO: the visitor function should assure that mLeft and mRight exists
        assert mLeft!=null && mRight!=null;
        ResultType mLeftType = mLeft.JudgeMultipleCondition(row);
        ResultType mRightType = mRight.JudgeMultipleCondition(row);
        if(mType==ConditionType.AND){
            // Preferentially,when there contains false,then return false
            // false AND unknown,return false
            if(mLeftType==ResultType.FALSE||mRightType==ResultType.FALSE){
                return ResultType.FALSE;
            }
            // then when there contains unknown,return unknown
            if(mLeftType==ResultType.UNKNOWN||mRightType==ResultType.UNKNOWN){
                return ResultType.UNKNOWN;
            }
            // leaves true AND true
            return ResultType.TRUE;
        }
        // OR
        else{
            // Preferentially,when there contains true,then return true
            // true AND unknown,return true
            if(mLeftType==ResultType.TRUE||mRightType==ResultType.TRUE){
                return ResultType.TRUE;
            }
            // then when there contains unknown,return unknown
            if(mLeftType==ResultType.UNKNOWN||mRightType==ResultType.UNKNOWN){
                return ResultType.UNKNOWN;
            }
            // leaves false or false
            return ResultType.FALSE;
        }
    }


}
