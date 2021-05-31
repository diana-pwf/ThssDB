package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;

import java.util.ArrayList;
import java.util.LinkedList;

// Notice:
// 为什么要扩展QueryRow？
// JOIN可能带来column和table对应关系——用MetaInfo来存储这部分
// 同时增加calColumnComparer来计算table.column的值->同样需要column和table的对应关系
public class QueryRow extends Row {
    private ArrayList<Table> tableList;
    QueryRow(Table table,Row row){
        super();
        tableList = new ArrayList<>();
        tableList.add(table);
        entries.addAll(row.getEntries());
    }
    // 此处采用LinkedList是因为在QueryTable中使用LinkedList更易于实现队列，笛卡尔积
    // tables和rows应该是对应的关系
    QueryRow(ArrayList<Table> tables, LinkedList<Row> rows){
        super();
        tableList = new ArrayList<>();
        tableList.addAll(tables);
        for(Row row:rows){
            entries.addAll(row.getEntries());
        }
    }
// TODO: implement calColumnComparer
    public Comparer calColumnComparer(String column){
        return null;
    }
}
