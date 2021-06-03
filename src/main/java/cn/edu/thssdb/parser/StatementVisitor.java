package cn.edu.thssdb.parser;

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.exception.DuplicatePrimaryKeyException;
import cn.edu.thssdb.exception.NoPrimaryKeyException;
import cn.edu.thssdb.query.*;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Pair;
import cn.edu.thssdb.type.ComparatorType;
import cn.edu.thssdb.type.ComparerType;
import cn.edu.thssdb.type.ConditionType;

import javax.xml.crypto.Data;
import java.awt.image.AreaAveragingScaleFilter;
import java.util.ArrayList;
import java.util.StringJoiner;

public class StatementVisitor extends SQLBaseVisitor{
    private Manager manager;
    private long session;

    public StatementVisitor(Manager manager, long session) {
        // super();
        this.manager = manager;
        this.session = session;
    }

    @Override
    public ArrayList<QueryResult> visitParse(SQLParser.ParseContext ctx) {
        return visitSql_stmt_list(ctx.sql_stmt_list());
    }

    @Override
    public ArrayList<QueryResult> visitSql_stmt_list(SQLParser.Sql_stmt_listContext ctx) {
        // StringJoiner sj = new StringJoiner("\n\n");
        ArrayList<QueryResult> result = new ArrayList<QueryResult>();
        for (SQLParser.Sql_stmtContext subCtx : ctx.sql_stmt())
            result.add(visitSql_stmt(subCtx));
        return result;
    }

    @Override
    public QueryResult visitSql_stmt(SQLParser.Sql_stmtContext ctx) {
        // 处理各种语句和情况
        // create db
        if(ctx.create_db_stmt() != null) {
            return visitCreate_db_stmt(ctx.create_db_stmt());
        }

        // drop db
        if(ctx.drop_db_stmt() != null){ return visitDrop_db_stmt(ctx.drop_db_stmt());}

        // use db
        if(ctx.use_db_stmt() != null){ return visitUse_db_stmt(ctx.use_db_stmt());}

        // create table
        if(ctx.create_table_stmt() != null){ return visitCreate_table_stmt(ctx.create_table_stmt());}

        // drop table
        if(ctx.drop_table_stmt() != null){ return visitDrop_table_stmt(ctx.drop_table_stmt());}

        // TODO: USER 好像不是必须？
        /*
        // create user
        if(ctx.create_user_stmt() != null){

        }

        // drop user
        if(ctx.drop_user_stmt() != null){

        }

         */

        // insert
        if(ctx.insert_stmt() != null){ return visitInsert_stmt(ctx.insert_stmt());}

        // delete
        if(ctx.delete_stmt() != null){

        }

        // select
        if(ctx.select_stmt() != null){
            return visitSelect_stmt(ctx.select_stmt());
        }

        // update
        if(ctx.update_stmt() != null){
            return visitUpdate_stmt(ctx.update_stmt());
        }

        //
        if(ctx.show_db_stmt() != null){
            return visitShow_db_stmt(ctx.show_db_stmt());
        }

        //
        if(ctx.show_table_stmt() != null){
            return visitShow_table_stmt(ctx.show_table_stmt());
        }

        //
        if(ctx.show_meta_stmt() != null){
            return visitShow_meta_stmt(ctx.show_meta_stmt());
        }

        // quit
        if(ctx.quit_stmt() != null){ return visitQuit_stmt(ctx.quit_stmt());}



        return null;
    }

    /**
     * 执行创建数据库的指令
     * @param ctx
     * @return 返回创建成功的信息或创建失败的原因
     */
    @Override
    public QueryResult visitCreate_db_stmt(SQLParser.Create_db_stmtContext ctx){
        String dbname = visitDatabase_name(ctx.database_name());
        String msg = "Successfully created database: " + dbname;
        try {
            manager.createDatabaseIfNotExists(dbname);
            // TODO: 考虑是否将 recover 放进 createDatabaseIfNotExists 的一环（即一创建就存储数据库名）
            manager.recover();
        } catch (Exception e){
            msg = e.getMessage();
        }
        return new QueryResult(msg);
    }

    /**
     * 执行删除数据库的指令
     * @param ctx
     * @return 返回删除成功的信息或删除失败的原因
     */
    @Override
    public QueryResult visitDrop_db_stmt(SQLParser.Drop_db_stmtContext ctx){
        String dbname = visitDatabase_name(ctx.database_name());
        String msg = "Successfully dropped database: " + dbname;
        try {
            manager.dropDatabase(dbname);
        } catch (Exception e){
            msg = e.getMessage();
        }
        return new QueryResult(msg);
    }

    /**
     * 执行切换数据库的指令
     * @param ctx
     * @return 返回切换成功的信息或切换失败的原因
     */
    @Override
    public QueryResult visitUse_db_stmt(SQLParser.Use_db_stmtContext ctx){
        String dbname = visitDatabase_name(ctx.database_name());
        String msg = "Successfully switch to database: " + dbname;
        try{
            manager.switchDatabase(dbname);
        } catch (Exception e){
            msg = e.getMessage();
        }
        return new QueryResult(msg);
    }

    /**
     * 处理数据库名称格式
     * @param ctx
     * @return 返回转换成小写的数据库名称
     */
    @Override
    public String visitDatabase_name(SQLParser.Database_nameContext ctx){
        return ctx.getText().toLowerCase();
    }

    /**
     * 执行在当前数据库创建表的指令
     * @param ctx
     * @return 返回创建成功的信息或创建失败的原因
     */
    @Override
    public QueryResult visitCreate_table_stmt(SQLParser.Create_table_stmtContext ctx){
        String tableName = visitTable_name(ctx.table_name());
        Database db = manager.getCurrentDatabase();
        String msg = "Successfully created table: " + tableName + " in database: " + db.getName();

        // 建立列
        int primaryCount = 0;
        ArrayList<Column> columnList = new ArrayList<>();
        for(SQLParser.Column_defContext columnDefCtx: ctx.column_def()){
            Column column = visitColumn_def(columnDefCtx);
            if(column.isPrimary()){
                ++primaryCount;
                if(primaryCount > 1) break;
            }
            columnList.add(column);
        }
        if(primaryCount == 0){
            msg = new NoPrimaryKeyException().getMessage();
        } else if(primaryCount > 1){
            msg = new DuplicatePrimaryKeyException().getMessage();
        } else {
            Column[] columns = columnList.toArray(new Column[0]);
            try{
                db.createTableIfNotExists(tableName, columns);
            } catch (Exception e){
                msg = e.getMessage();
            }
        }
        return new QueryResult(msg);
     }

    /**
     * 读取列定义中的“名字、类型和最大长度、是否为主键和非空的限制”
     * @param ctx
     * @return 返回重建的 Column
     */
    @Override
    public Column visitColumn_def(SQLParser.Column_defContext ctx){
        // 名字
        String columnName = visitColumn_name(ctx.column_name());

        // 类型和最大长度
        Pair<ColumnType, Integer> columnType = visitType_name(ctx.type_name());

        // 限制
        Pair<Integer, Boolean> columnConstraint = new Pair<>(0, false);
        for(SQLParser.Column_constraintContext constraintCtx: ctx.column_constraint()){
            Pair<Integer, Boolean> constraint = visitColumn_constraint(constraintCtx);
            if(constraint.left == 1) columnConstraint.left = 1;
            if(constraint.right == true) columnConstraint.right = true;
        }

        return new Column(columnName, columnType.left, columnConstraint.left, columnConstraint.right, columnType.right);
    }

    /**
     * 读取列名字并且改成 lowercase
     * @param ctx
     * @return
     */
    @Override
    public String visitColumn_name(SQLParser.Column_nameContext ctx){
        return ctx.getText().toLowerCase();
    }

    /**
     * 处理列定义中关于类型和限制长度的信息（只有在 ColumnType 为 String 时才需要设定，其他时候都默认为-1）
     * @param ctx
     * @return
     */
    @Override
    public Pair<ColumnType, Integer> visitType_name(SQLParser.Type_nameContext ctx){
        if(ctx.T_INT() != null) return new Pair<>(ColumnType.INT, -1);
        if(ctx.T_LONG() != null) return new Pair<>(ColumnType.LONG, -1);
        if(ctx.T_FLOAT() != null) return new Pair<>(ColumnType.FLOAT, -1);
        if(ctx.T_DOUBLE() != null) return new Pair<>(ColumnType.DOUBLE, -1);
        if(ctx.T_STRING() != null){
            try{
                return new Pair<>(ColumnType.STRING, Integer.parseInt(ctx.NUMERIC_LITERAL().getText()));
            } catch (Exception e){
                // TODO: 创建一种新的 Exception
            }
        }
        return null;
     }

    /**
     * 根据 K_PRIMARY 和 K_NULL ，优先级：主键约束大于非空约束（因为主键必要求非空）
     * @param ctx
     * @return 返回是否列约束
     */
    @Override
    public Pair<Integer, Boolean> visitColumn_constraint(SQLParser.Column_constraintContext ctx){
        if(ctx.K_PRIMARY() != null && ctx.K_KEY() != null) return new Pair<>(1, true);
        if(ctx.K_NOT() != null && ctx.K_NULL() != null) return new Pair<>(0, true);
        return new Pair<>(0, false);
    }

    // FIXME: 搞清楚 visitTable_constraint 在干嘛
    // @Override
    // public visitTable_constraint


    /**
     * 执行在当前数据库删除表的指令
     * @param ctx
     * @return 返回删除成功的信息或删除失败的原因
     */
    @Override
    public QueryResult visitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx){
        String tableName = visitTable_name(ctx.table_name());
        Database db = manager.getCurrentDatabase();
        String msg = "Successfully dropped table: " + tableName + " in database: " + db.getName();
        try{
            db.dropTable(tableName);
        }catch (Exception e){
            msg = e.getMessage();
        }
        return new QueryResult(msg);
    }

    /**
     * 处理表名称格式
     * @param ctx
     * @return 返回转换成小写的表名称
     */
    @Override
    public String visitTable_name(SQLParser.Table_nameContext ctx){
        return ctx.getText().toLowerCase();
    }

    /**
     * 执行数据插入指令
     * @param ctx
     * @return 返回插入执行成功信息或错误原因
     */
    @Override
    public QueryResult visitInsert_stmt(SQLParser.Insert_stmtContext ctx){
        Database db = manager.getCurrentDatabase();
        // TODO: find out difference between toString() and getText()
        //       toString = '[' + getText() + ']' （貌似）
        String tableName = visitTable_name(ctx.table_name());
        // Table table = db.getTable(tableName);
        String msg = "Successfully inserted data into the table: " + tableName + " in database: " + db.getName();

        // FIXME: naive insert without dealing with transaction and lock
        // 处理插入值
        ArrayList<String[]> valueList = new ArrayList<>();
        for(SQLParser.Value_entryContext valueEntryContext: ctx.value_entry()){
            valueList.add(visitValue_entry(valueEntryContext));
        }

        // 根据是否指定插入列进行插入操作
//        if(ctx.column_name() != null)
        if (ctx.column_name().size() != 0)
        {
            ArrayList<String> columnsName = new ArrayList<>();
            for(SQLParser.Column_nameContext columnNameContext: ctx.column_name()){
                columnsName.add(visitColumn_name(columnNameContext));
            }
            for(String[] values: valueList){
                try {
                    // FIXME: 考虑在 Database 中增加 insert 接口？
                    db.insert(tableName, columnsName, values);
                } catch (Exception e){
                    msg = e.getMessage();
                }
            }
        }else{
            for(String[] values: valueList){
                try {
                    db.insert(tableName, values);
                } catch (Exception e){
                    msg = e.getMessage();
                }
            }
        }

        return new QueryResult(msg);
    }

    /**
     * 执行数据删除指令
     * @param ctx
     * @return 返回删除执行成功信息或错误原因
     */
    @Override
    public QueryResult visitDelete_stmt(SQLParser.Delete_stmtContext ctx){
        Database db = manager.getCurrentDatabase();
        String tableName = visitTable_name(ctx.table_name());
        // Table table = db.getTable(tableName);
        String msg = "";

        // FIXME: naive delete without dealing with transaction and lock
        MultipleCondition conditions = null;
        if (ctx.K_WHERE() == null) {
            try{
                // FIXME: 考虑在 Database 中增加 delete 接口？
                msg.concat("Successfully deleted " + db.delete(tableName,null) + " data from the table: " + tableName);
            } catch (Exception e){
                msg.concat(e.getMessage());
            }
        }else{
            conditions = visitMultiple_condition(ctx.multiple_condition());
            try{
                msg.concat("Successfully deleted " + db.delete(tableName, conditions) + " data from the table: " + tableName);
            } catch (Exception e){
                msg.concat(e.getMessage());
            }
        }
        return new QueryResult(msg);
    }


    /**
     * 将输入的 entry value 变为一字符串列表
     * @param ctx
     * @return 返回 entry value 的字符串列表
     */
    @Override
    public String[] visitValue_entry(SQLParser.Value_entryContext ctx){
        int entryNumber = ctx.literal_value().size();
        String[] values = new String[entryNumber];
        for(int i = 0; i < entryNumber; i++){
            values[i] = ctx.literal_value(i).getText();
        }
        return values;
    }

    /**
     * 执行 quit 指令
     * @param ctx
     * @return 返回是否成功推出的信息
     */
    @Override
    public QueryResult visitQuit_stmt(SQLParser.Quit_stmtContext ctx){
        String msg = "Successfully quited.";
        try{
            manager.quit();
        }catch (Exception e){
            msg = e.getMessage();
        }
        return new QueryResult(msg);
    }

    /** 执行select指令 **/
    @Override
    public QueryResult visitSelect_stmt(SQLParser.Select_stmtContext ctx){

        Database database = manager.getCurrentDatabase();
        if(database == null) {
            throw new DatabaseNotExistException();
        }

        boolean distinct = (ctx.K_DISTINCT() != null);

        MultipleCondition conditions = null;
        if (ctx.K_WHERE() != null) {
            conditions = visitMultiple_condition(ctx.multiple_condition());
        }

        ArrayList<String> columnNames = new ArrayList<String>();
        int columnNum = ctx.result_column().size();
        for (int i = 0; i < columnNum; i++) {
            String columnName = visitResult_column(ctx.result_column(i));
            if (columnName.equals("*")) {
                columnNames.clear();
                break;
            }
            columnNames.add(columnName);
        }

        // TODO: 读取QueryTable变量

        QueryTable queryTable = null;

        int tableNum = ctx.table_query().size();
        if (tableNum == 0) {
            // TODO: 抛异常
        }
        for (int i = 0; i < tableNum; i++) {

        }

        // TODO: 考虑事务
        try {
            // TODO
            // database.select(columnNames, queryTable, conditions, distinct);
            return new QueryResult("");
        } catch (Exception e) {
            return new QueryResult(e.toString());

        }

        // return null;
    }

    @Override
    public String visitResult_column(SQLParser.Result_columnContext ctx){
        return ctx.getText().toLowerCase();
    }

    /** 执行update指令 **/
    @Override
    public QueryResult visitUpdate_stmt(SQLParser.Update_stmtContext ctx){
        Database database = manager.getCurrentDatabase();

        String table_name = visitTable_name(ctx.table_name());

        String column_name = visitColumn_name(ctx.column_name());

        Comparer comparer = visitExpression(ctx.expression());

        MultipleCondition conditions = null;
        if (ctx.K_WHERE() != null) {
            conditions = visitMultiple_condition(ctx.multiple_condition());
        }

        // TODO: 考虑事务
        try{
            return new QueryResult(database.update(table_name, column_name, comparer, conditions));
        }
        catch (Exception e) {
            return new QueryResult(e.getMessage());
        }

    }

    /** 执行show db指令 **/
    @Override
    public QueryResult visitShow_db_stmt(SQLParser.Show_db_stmtContext ctx) {
        String msg;
        try {
            msg = manager.ShowAllDatabases();
        } catch (Exception e) {
            msg = e.getMessage();
        }
        return new QueryResult(msg);
    }

    /** 执行show table指令 **/
    @Override
    public QueryResult visitShow_table_stmt(SQLParser.Show_table_stmtContext ctx) {
        Database database = manager.getCurrentDatabase();
        String msg;
        try {
            msg = database.showAllTables();
        } catch (Exception e) {
            msg = e.getMessage();
        }
        return new QueryResult(msg);
    }

    /** 执行show meta指令 **/
    @Override
    public QueryResult visitShow_meta_stmt(SQLParser.Show_meta_stmtContext ctx) {
        if (ctx.table_name() != null){
            String msg;
            String tableName = visitTable_name(ctx.table_name());
            Database database = manager.getCurrentDatabase();
            try {
                msg = database.showTableMeta(tableName);
            } catch (Exception e) {
                msg = e.getMessage();
            }
            return new QueryResult(msg);
        }
        return null;
    }

    /** 处理复合逻辑 **/
    @Override
    public MultipleCondition visitMultiple_condition(SQLParser.Multiple_conditionContext ctx) {
        // 单一条件
        if (ctx.condition() != null) {
            return new MultipleCondition(visitCondition(ctx.condition()));
        }

        // 复合逻辑
        ConditionType type = null;
        if (ctx.AND() != null) {
            type = ConditionType.AND;
        }
        else if (ctx.OR() != null) {
            type = ConditionType.OR;
        }
        return new MultipleCondition(visitMultiple_condition(ctx.multiple_condition(0)),
                visitMultiple_condition(ctx.multiple_condition(1)), type);
    }

    /** 获取整个逻辑表达式 **/
    @Override
    public Condition visitCondition(SQLParser.ConditionContext ctx) {
        Comparer left = visitExpression(ctx.expression(0));
        Comparer right = visitExpression(ctx.expression(0));
        ComparatorType type = visitComparator(ctx.comparator());
        return new Condition(left, right, type);
    }

    /** 获取单个条件表达式 **/
    @Override
    public Comparer visitExpression(SQLParser.ExpressionContext ctx) {
        if (ctx.comparer() != null) {
            return visitComparer(ctx.comparer());
        }
        return null;
    }

    /** 获取单个条件表达式 **/
    @Override
    public Comparer visitComparer(SQLParser.ComparerContext ctx) {
        if (ctx.column_full_name() != null) {
            return new Comparer(ComparerType.COLUMN, ctx.column_full_name().getText());
        }

        ComparerType literalType = visitLiteralType(ctx.literal_value());
        String literalValue = ctx.literal_value().getText();
        return new Comparer(literalType, literalValue);
    }


    public ComparerType visitLiteralType(SQLParser.Literal_valueContext ctx) {
        if (ctx.NUMERIC_LITERAL() != null) {
            return ComparerType.NUMERIC;
        }
        else if (ctx.STRING_LITERAL() != null) {
            return ComparerType.STRING;
        }
        else if (ctx.K_NULL() != null)
        {
            return ComparerType.NULL;
        }
        return null;
    }

    @Override
    public ComparatorType visitComparator(SQLParser.ComparatorContext ctx) {
        if (ctx.EQ() != null) {
            return ComparatorType.EQ;
        }
        else if (ctx.GE() != null) {
            return ComparatorType.GE;
        }
        else if (ctx.GT() != null) {
            return ComparatorType.GT;
        }
        else if (ctx.LE() != null) {
            return ComparatorType.LE;
        }
        else if (ctx.LT() != null) {
            return ComparatorType.LT;
        }
        else if (ctx.NE() != null) {
            return ComparatorType.NE;
        }
        return null;
    }




    public QueryTable visitQueryTable() {
        // TODO: 单一表

        // TODO: 处理复合逻辑

        return null;
    }
}
