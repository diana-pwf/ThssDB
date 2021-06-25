package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.ResultType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

public class JointTable extends QueryTable{

    private ArrayList<Table> tables;
    private ArrayList<Iterator<Row>> iterators;
    private MultipleCondition JoinCondition;
    private LinkedList<Row> rowsJoin;

    public JointTable(ArrayList<Table> tables,MultipleCondition condition){
        this.tables = tables;
        JoinCondition = condition;
        iterators = new ArrayList<Iterator<Row>>();
        rowsJoin = new LinkedList<Row>();
        for(Table table : tables){
            iterators.add(table.iterator());
        }
        createMetaInfo();
    }

    @Override
    public void addNext() {
        while(true){
            QueryRow jointRow= JoinRowTogether();
            if(jointRow==null){
                return;
            }
            if(JoinCondition==null||JoinCondition.JudgeMultipleCondition(jointRow)== ResultType.TRUE){
                if(selectCondition==null||selectCondition.JudgeMultipleCondition(jointRow)==ResultType.TRUE){
                    queue.add(jointRow);
                    return;
                }
            }
        }
    }

    @Override
    public void createMetaInfo() {
        for(Table table:tables){
            MetaInfo info = new MetaInfo(table.databaseName,table.tableName,table.columns);
            MetaInfoList.add(info);
        }
    }

    /**
     * 注意！TODO：在调用next前一定要先判断hasNext，否则可能出错
     * @return 返回所有表进行笛卡尔积的一种结果
     */
    // 多表进行循环遍历笛卡尔积的方法非常类似加法的进位
    // 000 -> 001 -> 010 -> 011 -> 100 ->...
    private QueryRow JoinRowTogether(){

        if(rowsJoin.isEmpty()){
            for(Iterator<Row> iter:iterators){
                if(!iter.hasNext()){
                    return null;
                }
                rowsJoin.add(iter.next());
            }
            return new QueryRow(MetaInfoList,rowsJoin);
        }
        int idx;
        //从后往前寻找仍有后继的表
        for(idx = iterators.size()-1;idx>=0;idx--){
            rowsJoin.removeLast();

            if(!iterators.get(idx).hasNext()){
                // 如果没有后继则恢复到最开始重新遍历
                iterators.set(idx,tables.get(idx).iterator());
            }
            else{
                break;
            }
        }
        // 所有的table都已经遍历结束
        if (idx<0){
            return null;
        }
        // 此时从后继的表开始更新到后继，其他的表重新开始向下寻找后继
        for(int i = idx;i<iterators.size();i++){
            if(!iterators.get(i).hasNext()){
                return null;
            }
            rowsJoin.add(iterators.get(i).next());
        }
        return new QueryRow(MetaInfoList,rowsJoin);
    }
}
