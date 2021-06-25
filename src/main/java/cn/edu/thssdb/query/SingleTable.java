package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.ComparatorType;
import cn.edu.thssdb.type.ComparerType;
import cn.edu.thssdb.type.ResultType;

import java.util.Iterator;

public class SingleTable extends QueryTable{
    private Table table;
    private Iterator<Row> iterators;

    public SingleTable(Table table){
        this.table = table;
        this.iterators = table.iterator();
        createMetaInfo();
    }


    // 通过对逻辑的判断来进行部分优化
    // 如果无逻辑判断/判断为null/常值比较则直接查找下一个
    // 如果有逻辑判断，但是可以通过get(const value)实现可以通过直接get方法拿到

    @Override
    public void addNext() {
        if(this.selectCondition==null){
            addNextDirect();
            return;
        }
        if(this.selectCondition.mSingle){
            if(this.selectCondition.mSingleCondition==null){
                addNextDirect();
                return;
            }
            Condition cond = this.selectCondition.mSingleCondition;
            if(cond.mLeft.mType!= ComparerType.COLUMN && cond.mRight.mType!=ComparerType.COLUMN){
                if(cond.JudgeCondition(cond)== ResultType.TRUE){
                    addNextDirect();
                    return;
                }
                return;
            }
//            if(cond.mType== ComparatorType.EQ&&init){
//                Comparable constValue;
//                if(cond.mLeft.mType!=cond.mRight.mType){
//                    if(cond.mLeft.mType==ComparerType.COLUMN&&table.cond.mLeft.mValue){
//                        constValue = cond.mRight.mValue;
//                    }
//                    else{
//                        constValue = cond.mLeft.mValue;
//                    }
//                    addNextByCache(constValue)
//                }
//            }
        }
        addNextByCondition();

    }

    private void addNextByCondition() {
        while(iterators.hasNext()){
             QueryRow row = new QueryRow(MetaInfoList.get(0),iterators.next());
             if(selectCondition.JudgeMultipleCondition(row)==ResultType.TRUE){
                 queue.add(row);
                 break;
             }
        }
    }

    @Override
    public void createMetaInfo() {
        MetaInfo info = new MetaInfo(table.databaseName,table.tableName,table.columns);
        MetaInfoList.add(info);
    }

    private void addNextDirect() {
        if(iterators.hasNext()){
            queue.add(new QueryRow(MetaInfoList.get(0),iterators.next()));
        }
    }

}
