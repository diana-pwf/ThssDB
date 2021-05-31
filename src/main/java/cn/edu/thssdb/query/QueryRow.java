package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.ColumnNotExistException;
import cn.edu.thssdb.exception.IllegalSQLStatement;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import sun.awt.AWTAccessor;

import java.util.ArrayList;
import java.util.LinkedList;

// Notice:
// 为什么要扩展QueryRow？
// JOIN可能带来column和table对应关系——用MetaInfoList来存储这部分
// 同时增加calColumnComparer来计算table.column的值->同样需要column和table的对应关系
public class QueryRow extends Row {
    private ArrayList<MetaInfo> MetaInfoList;
    QueryRow(MetaInfo metaInfo, Row row){
        super();
        MetaInfoList = new ArrayList<>();
        MetaInfoList.add(metaInfo);
        entries.addAll(row.getEntries());
    }
    // 此处采用LinkedList是因为在QueryTable中使用LinkedList更易于实现队列，笛卡尔积
    // tables和rows应该是对应的关系
    /**
     *  构造QueryRow
     * @param metaInfo 与row对应的table的信息
     */
    QueryRow(ArrayList<MetaInfo> metaInfo , LinkedList<Row> rows){
        super();
        MetaInfoList = new ArrayList<>();
        MetaInfoList.addAll(metaInfo);
        for(Row row:rows){
            entries.addAll(row.getEntries());
        }
    }
// TODO: implement calColumnComparer
    public Comparer calColumnComparer(String column){
        // table.column
        if(column.contains(".")){
            String[] seq = column.split(".");
            if(seq.length!=2){
                throw new IllegalSQLStatement(column);
            }
            String table_name = seq[0];
            String column_name = seq[1];
            for(MetaInfo metaInfo : MetaInfoList){
                if(metaInfo.getTableName().equals(table_name)){
                    if(metaInfo.columnFind(column_name)==-1){
                        throw new ColumnNotExistException(metaInfo.getDatabaseName(),metaInfo.getTableName(),column_name);
                    }


                }
            }

        }
        return null;
    }
}
