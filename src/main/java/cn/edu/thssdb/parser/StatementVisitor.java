package cn.edu.thssdb.parser;

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.query.*;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.ComparatorType;
import cn.edu.thssdb.type.ComparerType;
import cn.edu.thssdb.type.ConditionType;

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
        StringJoiner sj = new StringJoiner("\n\n");
        ArrayList<QueryResult> result = new ArrayList<QueryResult>();
        for (SQLParser.Sql_stmtContext subCtx : ctx.sql_stmt())
            result.add(visitSql_stmt(subCtx));
        return result;
    }

    @Override
    public QueryResult visitSql_stmt(SQLParser.Sql_stmtContext ctx) {
        // 处理各种语句和情况
        // create db
        if(ctx.create_db_stmt() != null){ return visitCreate_db_stmt(ctx.create_db_stmt());}

        // drop db
        if(ctx.drop_db_stmt() != null){ return visitDrop_db_stmt(ctx.drop_db_stmt());}

        // use db
        if(ctx.use_db_stmt() != null){ return visitUse_db_stmt(ctx.use_db_stmt());}

        // TODO: create table (because need to parse column)
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

        // delete
        if(ctx.delete_stmt() != null){

        }

        // insert
        if(ctx.insert_stmt() != null){
            visitInsert_stmt(ctx.insert_stmt());
            // return new QueryResult(msg);
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

        }

        //
        if(ctx.show_table_stmt() != null){

        }

        //
        if(ctx.show_meta_stmt() != null){

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
        String dbname = ctx.database_name().getText().toLowerCase();
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
        String dbname = ctx.database_name().getText().toLowerCase();
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
        String dbname = ctx.database_name().getText().toLowerCase();
        String msg = "Successfully switch to database: " + dbname;
        try{
            manager.switchDatabase(dbname);
        } catch (Exception e){
            msg = e.getMessage();
        }
        return new QueryResult(msg);
    }

    /**
     * 执行在当前数据库创建表的指令
     * @param ctx
     * @return 返回创建成功的信息或创建失败的原因
     */
    @Override
    public QueryResult visitCreate_table_stmt(SQLParser.Create_table_stmtContext ctx){
        String tableName = ctx.table_name().getText().toLowerCase();
        Database db = manager.getCurrentDatabase();
        String msg = "Successfully created table: " + tableName + " in database: " + db.getName();

        /*
        try{
            db.createTableIfNotExists();
        } catch (Exception e){
            msg = e.getMessage();
        }

         */
        return new QueryResult(msg);
     }

    /**
     * 执行在当前数据库删除表的指令
     * @param ctx
     * @return 返回删除成功的信息或删除失败的原因
     */
    @Override
    public QueryResult visitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx){
        String tableName = ctx.table_name().getText().toLowerCase();
        Database db = manager.getCurrentDatabase();
        String msg = "Successfully dropped table: " + tableName + " in database: " + db.getName();
        try{
            db.dropTable(tableName);
        }catch (Exception e){
            msg = e.getMessage();
        }
        return new QueryResult(msg);
    }

    @Override
    public String visitInsert_stmt(SQLParser.Insert_stmtContext ctx){
        //
        Database db = manager.getCurrentDatabase();
        // TODO: find out difference between toString() and getText()
        //       toString = '[' + getText() + ']' （貌似）
        Table table = db.getTable(ctx.table_name().getText().toLowerCase());



        // FIXME: naive insert without dealing with transaction and lock

        if(ctx.column_name() != null){
            ArrayList<Column> columns = new ArrayList<Column>();

        }else{

        }
        return "insert";
    }

    @Override
    public Column visitColumn_def(SQLParser.Column_defContext ctx){

        String columnName = ctx.column_name().getText().toLowerCase();

        // TODO:
        return null;
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

        multipleCondition logic = null;
        if (ctx.K_WHERE() != null) {
            logic = visitMultiple_condition(ctx.multiple_condition());
        }

        ArrayList<String> columnNames = new ArrayList<String>();

        int columnNum = ctx.result_column().size();
        for (int i = 0; i < columnNum; i++) {
            String columnName = ctx.result_column(i).getText().toLowerCase();
            if (columnName.equals("*")) {
                columnNames.clear();
                break;
            }
            columnNames.add(columnName);
        }

        // TODO: 读取QueryTable变量

        int tableNum = ctx.table_query().size();
        if (tableNum == 0) {
            // TODO: 抛异常
        }
        for (int i = 0; i < tableNum; i++) {

        }

        // TODO: 考虑事务
        try {
            return database.select(columnNames, the_query_table, logic, distinct);
        } catch (Exception e) {
            QueryResult error_result = new QueryResult(e.toString());
            return error_result;
        }
    }

    /** 执行update指令 **/
    @Override
    public QueryResult visitUpdate_stmt(SQLParser.Update_stmtContext ctx){
        Database database = manager.getCurrentDatabase();

        String table_name = ctx.table_name().getText().toLowerCase();

        String column_name = ctx.column_name().getText().toLowerCase();

        Comparer comparer = visitExpression(ctx.expression());

        multipleCondition conditions = null;
        if (ctx.K_WHERE() != null) {
            conditions = visitMultiple_condition(ctx.multiple_condition());
        }

        // TODO: 读取Comparer变量

        // TODO: 考虑事务

        return database.update(table_name, column_name, comparer, conditions);
    }


    /** 处理复合逻辑 **/
    @Override
    public multipleCondition visitMultiple_condition(SQLParser.Multiple_conditionContext ctx) {
        // 单一条件
        if (ctx.condition() != null) {
            return new multipleCondition(visitCondition(ctx.condition()));
        }

        // 复合逻辑
        ConditionType type = null;
        if (ctx.AND() != null) {
            type = ConditionType.AND;
        }
        else if (ctx.OR() != null) {
            type = ConditionType.OR;
        }
        return new multipleCondition(visitMultiple_condition(ctx.multiple_condition(0)),
                visitMultiple_condition(ctx.multiple_condition(1)), type);
    }

    @Override
    public Condition visitCondition(SQLParser.ConditionContext ctx) {
        Comparer left = visitExpression(ctx.expression(0));
        Comparer right = visitExpression(ctx.expression(0));
        ComparatorType type = visitComparator(ctx.comparator());
        return new Condition(left, right, type);
    }

    @Override
    public Comparer visitExpression(SQLParser.ExpressionContext ctx) {
        if (ctx.comparer() != null) {
            return visitComparer(ctx.comparer());
        }
        return null;
    }

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
    }
}
