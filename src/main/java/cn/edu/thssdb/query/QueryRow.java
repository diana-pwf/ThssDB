package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.type.ComparerType;
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

    /**
     *  构造QueryRow
     * @param metaInfo 与row对应的table的信息
     * @param rows 与metaInfo的table对应，采用LinkedList是因为在QueryTable中使用LinkedList更易于实现队列，笛卡尔积
     */
    QueryRow(ArrayList<MetaInfo> metaInfo , LinkedList<Row> rows){
        super();
        MetaInfoList = new ArrayList<>();
        MetaInfoList.addAll(metaInfo);
        for(Row row:rows){
            entries.addAll(row.getEntries());
        }
    }

    ComparerType columnType2ComparerType(ColumnType columnType){
        switch (columnType){
            case INT:
            case FLOAT:
            case DOUBLE:
            case LONG:
                return ComparerType.NUMERIC;
            case STRING:
                return ComparerType.STRING;
        }
        return ComparerType.NULL;
    }

    /**
     *  将column转换为Comparer
     * @param column SQL语句中table.column/column
     * 将两种形式的column都转换为对应的Comparer
     * 注意当SQL语句中为column时，需要遍历整个metaInfoList,来确认column可以指代唯一的table.column
     */
    public Comparer calColumnComparer(String column){

        ComparerType type = ComparerType.NULL;

        // table.column
        if(column.contains(".")){
            String[] seq = column.split(".");
            if(seq.length!=2){
                throw new IllegalSQLStatement(column);
            }
            String tableName = seq[0];
            String columnName = seq[1];
            int seqIndex = 0;
            boolean tableExist = false;
            for(MetaInfo metaInfo : MetaInfoList){
                if(metaInfo.getTableName().equals(tableName)){
                    tableExist = true;
                    int idx = 0;
                    idx = metaInfo.columnFind(columnName);
                    if(idx == -1){
                        throw new QueryColumnNotFoundException(column,metaInfo.getDatabaseName());
                    }
                    ColumnType columnType = metaInfo.columnFindType(idx);
                    type = columnType2ComparerType(columnType);
                    seqIndex += idx;
                    break;
                }
                seqIndex += metaInfo.getColumnSize();
            }
            if(!tableExist){
                throw new TableNotExistException();
            }
            return new Comparer(type, (String) entries.get(seqIndex).value);
        }

        // column
        else{
           Comparer comparer = null;
           boolean columnExist = false;
           int seqIndex = 0;
           for(MetaInfo metaInfo:MetaInfoList){
               int idx = metaInfo.columnFind(column);
               if(idx==-1){
                   continue;
               }
               type = columnType2ComparerType(metaInfo.columnFindType(idx));
               if(!columnExist)
               {
                   comparer = new Comparer(type,(String) entries.get(seqIndex+idx).value);
                   columnExist = true;
               }
               else{
                    throw new QueryColumnCollisionException(column);
               }
               seqIndex += metaInfo.getColumnSize();
           }
           if(!columnExist){
               throw new QueryColumnNotFoundException(column,MetaInfoList.get(0).getDatabaseName());
           }
           return comparer;
        }
    }
}
