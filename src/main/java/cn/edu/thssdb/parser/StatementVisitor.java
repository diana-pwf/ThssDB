package cn.edu.thssdb.parser;

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.exception.DuplicateColumnException;
import cn.edu.thssdb.exception.DuplicatePrimaryKeyException;
import cn.edu.thssdb.exception.NoPrimaryKeyException;
import cn.edu.thssdb.exception.NotSelectTableException;
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

import javax.management.Query;
import javax.xml.crypto.Data;
import java.awt.image.AreaAveragingScaleFilter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Locale;
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
        ArrayList<QueryResult> result = new ArrayList<QueryResult>();
        for (SQLParser.Sql_stmtContext subCtx : ctx.sql_stmt())
            result.add(visitSql_stmt(subCtx));
        return result;
    }

    @Override
    public QueryResult visitSql_stmt(SQLParser.Sql_stmtContext ctx) {
        // 处理各种语句和情况
        // create db
        if(ctx.create_db_stmt() != null) { return visitCreate_db_stmt(ctx.create_db_stmt()); }

        // drop db
        if(ctx.drop_db_stmt() != null){ return visitDrop_db_stmt(ctx.drop_db_stmt());}

        // use db
        if(ctx.use_db_stmt() != null){ return visitUse_db_stmt(ctx.use_db_stmt());}

        // create table
        if(ctx.create_table_stmt() != null){ return visitCreate_table_stmt(ctx.create_table_stmt());}

        // drop table
        if(ctx.drop_table_stmt() != null){ return visitDrop_table_stmt(ctx.drop_table_stmt());}

        // insert
        if(ctx.insert_stmt() != null){ return visitInsert_stmt(ctx.insert_stmt());}

        // delete
        if(ctx.delete_stmt() != null){ return visitDelete_stmt(ctx.delete_stmt()); }

        // select
        if(ctx.select_stmt() != null){ return visitSelect_stmt(ctx.select_stmt()); }

        // update
        if(ctx.update_stmt() != null){ return visitUpdate_stmt(ctx.update_stmt()); }

        //
        if(ctx.show_db_stmt() != null){ return visitShow_db_stmt(ctx.show_db_stmt()); }

        //
        if(ctx.show_table_stmt() != null){ return visitShow_table_stmt(ctx.show_table_stmt()); }

        //
        if(ctx.show_meta_stmt() != null){ return visitShow_meta_stmt(ctx.show_meta_stmt()); }

        // quit
        if(ctx.quit_stmt() != null){ return visitQuit_stmt(ctx.quit_stmt());}

        if (ctx.begin_transaction_stmt() != null) {
            return new QueryResult(visitBegin_transaction_stmt(ctx.begin_transaction_stmt()));
        }

        if (ctx.commit_stmt() != null) {
            return new QueryResult(visitCommit_stmt(ctx.commit_stmt()));
        }

        if (ctx.auto_begin_transaction_stmt() != null) {
            return new QueryResult(visitAuto_begin_transaction_stmt(ctx.auto_begin_transaction_stmt()));
        }

        if (ctx.auto_commit_stmt() != null) {
            return new QueryResult(visitAuto_commit_stmt(ctx.auto_commit_stmt()));
        }

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
            manager.persist();
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
        Database db = null;
        try{
            db = manager.getCurrentDatabase();
        }
        catch (Exception e){
            return new QueryResult(e.getMessage());
        }

        String tableName = visitTable_name(ctx.table_name());
        String msg = "Successfully created table: " + tableName + " in database: " + db.getName();

        // 检查是否指定主键
        Boolean hasPrimary = false;
        ArrayList<Column> columnList = new ArrayList<>();
        ArrayList<String> columnNames = new ArrayList<>();

        if(ctx.table_constraint() != null){
            String primaryKeyName = visitTable_constraint(ctx.table_constraint());
            for(SQLParser.Column_defContext columnDefCtx: ctx.column_def()){
                Column column = visitColumn_def(columnDefCtx);
                columnNames.add(column.getName());
                if(column.getName().equals(primaryKeyName)){
                    column.setPrimary();
                    hasPrimary = true;
                }
                columnList.add(column);
            }
        }

        // 建立列
        if(hasPrimary == false){
            msg = new NoPrimaryKeyException().getMessage();
            return new QueryResult(msg);
        }
        // 检查是否有重复列名
        for(String columnName : columnNames){
            if(columnNames.indexOf(columnName) != columnNames.lastIndexOf(columnName)){
                msg = new DuplicateColumnException("create table", columnName).getMessage();
                return new QueryResult(msg);
            }
        }
        // 建立表
        Column[] columns = columnList.toArray(new Column[0]);
        try{
            db.createTableIfNotExists(tableName, columns);
        } catch (Exception e){
            msg = e.getMessage();
        }

        return new QueryResult(msg);
     }


    /**
     * 返回主键列名
     * @param ctx
     * @return 返回主键列名
     */
    public String visitTable_constraint(SQLParser.Table_constraintContext ctx) {
        String keyConstraint = "";
        int size = ctx.column_name().size();
        for(int i = 0; i < size; ++i){
            keyConstraint += ctx.column_name(i).getText().toLowerCase();
        }
        return keyConstraint;
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
        Pair<ColumnType, Integer> columnType;
        try {
            // 类型和最大长度
            columnType = visitType_name(ctx.type_name());
        } catch (Exception e){
            throw e;
        }
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
                throw e;
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


    /**
     * 执行在当前数据库删除表的指令
     * @param ctx
     * @return 返回删除成功的信息或删除失败的原因
     */
    @Override
    public QueryResult visitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx){
        Database db = null;
        try{
            db = manager.getCurrentDatabase();
        }
        catch (Exception e){
            return new QueryResult(e.getMessage());
        }
        String tableName = visitTable_name(ctx.table_name());
        String msg = "Successfully dropped table: " + tableName + " in database: " + db.getName();
        try{
            db.dropTable(tableName,false);
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
        Database db = null;
        try{
            db = manager.getCurrentDatabase();
        }
        catch (Exception e){
            return new QueryResult(e.getMessage());
        }
        String tableName = visitTable_name(ctx.table_name());
        String msg = "Successfully inserted data into the table: " + tableName + " in database: " + db.getName();

        // 处理插入值
        ArrayList<String[]> valueList = new ArrayList<>();
        for(SQLParser.Value_entryContext valueEntryContext: ctx.value_entry()){
            valueList.add(visitValue_entry(valueEntryContext));
        }

        // 根据是否指定插入列进行插入操作
        ArrayList<String> columnsName = new ArrayList<>();
        for(SQLParser.Column_nameContext columnNameContext: ctx.column_name()){
            columnsName.add(visitColumn_name(columnNameContext));
        }

        if (!manager.transactionSessions.contains(session)) {
            for(String[] values: valueList){
                try {
                    db.insert(tableName, columnsName, values);
                } catch (Exception e){
                    msg = e.getMessage();
                }
            }
            return new QueryResult(msg);
        }

        // session在事务中
        Table table = db.getTable(tableName);
        while (true) {
            if (!manager.blockedSessions.contains(session)) {
                // 尚未被阻塞，看看能不能加x锁

                // 能加锁/成功，跳出循环正常执行
                int result = table.getXLock(session);
                if (result != -1) {
                    if (result == 1) {
                        // 能加锁
                        ArrayList<String> tmp = manager.xLockDict.get(session);
                        tmp.add(tableName);
                        manager.xLockDict.put(session,tmp);
                    }
                    // 移出等待队列
                    manager.blockedSessions.remove(session);
                    break;
                }
                // 不能则加入等待队列
                manager.blockedSessions.add(session);
            }
            else if (manager.blockedSessions.get(0).equals(session)) {
                int result = table.getXLock(session);
                if (result != -1)
                {
                    if (result == 1)
                    {
                        ArrayList<String> tmp = manager.xLockDict.get(session);
                        tmp.add(tableName);
                        manager.xLockDict.put(session,tmp);
                    }
                    manager.blockedSessions.remove(0);
                    break;
                }
            }
            // 因为阻塞，所以休眠一会  -- 繁忙等待
            try {
                Thread.sleep(300);
            }
            catch(Exception e) {
                System.out.println(e.getMessage());
            }
        }

        for(String[] values: valueList){
            try {
                db.insert(tableName, columnsName, values);
            } catch (Exception e){
                msg = e.getMessage();
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
        Database db = null;
        try{
            db = manager.getCurrentDatabase();
        }
        catch (Exception e){
            return new QueryResult(e.getMessage());
        }
        String tableName = visitTable_name(ctx.table_name());
        String msg = "";

        MultipleCondition conditions = (ctx.K_WHERE() == null ? null : visitMultiple_condition(ctx.multiple_condition()));

        // 不在事务中正常执行
        if (!manager.transactionSessions.contains(session)) {
            try{
                msg = "Successfully deleted " + db.delete(tableName, conditions) + " data from the table: " + tableName;
            } catch (Exception e){
                msg = e.getMessage();
            }
            return new QueryResult(msg);
        }

        Table table = db.getTable(tableName);

        while (true) {
            if (!manager.blockedSessions.contains(session)) {
                // 尚未被阻塞，看看能不能加x锁

                // 能加锁/成功，跳出循环正常执行
                int result = table.getXLock(session);
                if (result != -1) {
                    if (result == 1) {
                        // 能加锁
                        ArrayList<String> tmp = manager.xLockDict.get(session);
                        tmp.add(tableName);
                        manager.xLockDict.put(session,tmp);
                    }
                    // 移出等待队列
                    manager.blockedSessions.remove(session);
                    break;
                }
                // 不能则加入等待队列
                manager.blockedSessions.add(session);
            }
            else if (manager.blockedSessions.get(0).equals(session)) {
                // 已经被阻塞，看看本次能否轮到

                int result = table.getXLock(session);
                if (result != -1)
                {
                    if (result == 1)
                    {
                        ArrayList<String> tmp = manager.xLockDict.get(session);
                        tmp.add(tableName);
                        manager.xLockDict.put(session,tmp);
                    }
                    manager.blockedSessions.remove(0);
                    break;
                }
            }
            // 因为阻塞，所以休眠一会  -- 繁忙等待
            try {
                Thread.sleep(300);
            }
            catch(Exception e) {
                System.out.println(e.getMessage());
            }
        }

        try{
            msg = db.delete(tableName, conditions);
        } catch (Exception e){
            msg = e.getMessage();
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
    public QueryResult visitSelect_stmt(SQLParser.Select_stmtContext ctx) {

        Database database = null;
        try{
            database = manager.getCurrentDatabase();
        }
        catch (Exception e){
            return new QueryResult(e.getMessage());
        }

        // distict 处理
        boolean distinct = (ctx.K_DISTINCT() != null);

        // condition 处理
        MultipleCondition conditions = null;
        if (ctx.K_WHERE() != null) {
            conditions = visitMultiple_condition(ctx.multiple_condition());
        }

        // column 处理
        int columnNum = ctx.result_column().size();
        String[] columnNames = new String[columnNum];
        for (int i = 0; i < columnNum; i++) {
            String columnName = visitResult_column(ctx.result_column(i));
            if (columnName.equals("*")) {
                columnNames = null;
                break;
            }
            columnNames[i] = columnName;
        }

        QueryTable queryTable = null;
        try{
            queryTable = visitTable_query_stmt(ctx.table_query(0));
        }catch (Exception e){
            return new QueryResult(e.getMessage());
        }


        // 若session不在事务中，直接执行
        if (!manager.transactionSessions.contains(session)) {
            try {
                return database.select(columnNames, queryTable, conditions, distinct);
            } catch (Exception e) {
                return new QueryResult(e.getMessage());
            }
        }

        ArrayList<String> tableNames = new ArrayList<String>();
        for (SQLParser.Table_nameContext subctx : ctx.table_query(0).table_name()) {
            tableNames.add(subctx.getText().toLowerCase());
        }

        while (true) {
            if (!manager.blockedSessions.contains(session)) {
                // 尚未被阻塞，看看能不能加s锁
                int index;
                boolean blocked = false;
                for (index = 0; index < tableNames.size(); index++) {
                    Table table = database.getTable(tableNames.get(index));
                    if (table.getSLock(session) == -1) {
                        blocked = true;
                        break;
                    }
                }
                // 不能加锁，加入等待队列
                if (blocked) {
                    for (int i = 0; i < index; i++) {
                        Table table = database.getTable(tableNames.get(i));
                        table.freeSLock(session);
                    }
                    manager.blockedSessions.add(session);
                }
                // 能加锁，跳出循环正常执行
                else {
                    break;
                }
            }
            else if (manager.blockedSessions.get(0).equals(session)) {
                // 已经被阻塞，看看本次能否轮到
                int index;
                boolean blocked = false;
                for (index = 0; index < tableNames.size(); index++) {
                    Table table = database.getTable(tableNames.get(index));
                    if (table.getSLock(session) == -1) {
                        blocked = true;
                        break;
                    }
                }
                if (!blocked) {
                    manager.blockedSessions.remove(session);
                    break;
                }
            }
            // 因为阻塞，所以休眠一会  -- 繁忙等待
            try {
                Thread.sleep(300);
            }
            catch(Exception e) {
                System.out.println(e.getMessage());
            }
        }

        // 在事务中正常执行
        try {
            for (String table_name : tableNames) {
                Table table = database.getTable(table_name);
                table.freeSLock(session);
            }
            return database.select(columnNames, queryTable, conditions, distinct);
        } catch (Exception e) {
            return new QueryResult(e.getMessage());
        }


    }

    @Override
    public String visitResult_column(SQLParser.Result_columnContext ctx){
        return ctx.getText().toLowerCase();
    }

    /** 执行update指令 **/
    @Override
    public QueryResult visitUpdate_stmt(SQLParser.Update_stmtContext ctx){
        Database database = null;
        try{
            database = manager.getCurrentDatabase();
        }
        catch (Exception e){
            return new QueryResult(e.getMessage());
        }

        String tableName = visitTable_name(ctx.table_name());

        String columnName = visitColumn_name(ctx.column_name());

        Comparer comparer = visitExpression(ctx.expression());

        MultipleCondition conditions = null;
        if (ctx.K_WHERE() != null) {
            conditions = visitMultiple_condition(ctx.multiple_condition());
        }

        Table table = database.getTable(tableName);

        // 不在事务中正常执行
        if (!manager.transactionSessions.contains(session)) {
            try {
                // table.freeXLock(session);
                return new QueryResult(database.update(tableName, columnName, comparer, conditions));
            } catch (Exception e) {
                return new QueryResult(e.getMessage());
            }
        }

        while (true) {
            if (!manager.blockedSessions.contains(session)) {
                // 尚未被阻塞，看看能不能加x锁

                // 能加锁/成功，跳出循环正常执行
                int result = table.getXLock(session);
                if (result != -1) {
                    if (result == 1) {
                    // 能加锁
                        ArrayList<String> tmp = manager.xLockDict.get(session);
                        tmp.add(tableName);
                        manager.xLockDict.put(session,tmp);
                    }
                    // 移出等待队列
                    manager.blockedSessions.remove(session);
                    break;
                }
                // 不能则加入等待队列
                manager.blockedSessions.add(session);
            }
            else if (manager.blockedSessions.get(0).equals(session)) {
                // 已经被阻塞，看看本次能否轮到

                int result = table.getXLock(session);
                if (result != -1)
                {
                    if (result == 1)
                    {
                        ArrayList<String> tmp = manager.xLockDict.get(session);
                        tmp.add(tableName);
                        manager.xLockDict.put(session,tmp);
                    }
                    manager.blockedSessions.remove(0);
                    break;
                }
            }
            // 因为阻塞，所以休眠一会  -- 繁忙等待
            try {
                Thread.sleep(300);
            }
            catch(Exception e) {
                System.out.println(e.getMessage());
            }
        }

        String msg = "";
        try{
            msg = database.update(tableName, columnName, comparer, conditions);
            return new QueryResult(msg);
        }
        catch (Exception e) {
            msg = e.getMessage();
            return new QueryResult(msg);
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
        Database database = null;
        try{
            database = manager.getCurrentDatabase();
        }
        catch (Exception e){
            return new QueryResult(e.getMessage());
        }
        String databaseName = visitDatabase_name(ctx.database_name());
        String msg;
        try {
            msg = manager.getDatabaseByName(databaseName).showAllTables();
            //msg = database.showAllTables();
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
            Database database = null;
            try{
                database = manager.getCurrentDatabase();
            }
            catch (Exception e){
                return new QueryResult(e.getMessage());
            }
            String tableName = visitTable_name(ctx.table_name());
            try {
                msg = database.showTableMeta(tableName);
            } catch (Exception e) {
                msg = e.getMessage();
            }
            return new QueryResult(msg);
        }
        return null;
    }

    /**
     * 执行开始事务的指令
     * @param ctx
     * @return 返回开始事务的信息或创建失败的原因
     */
    @Override
    public String visitBegin_transaction_stmt(SQLParser.Begin_transaction_stmtContext ctx){
        try {
            if (manager.transactionSessions.contains(session)) {
                return "already in transaction";
            }
            manager.transactionSessions.add(session);
            manager.sLockDict.put(session, new ArrayList<String>());
            manager.xLockDict.put(session, new ArrayList<String>());
            return "successfully begin transaction";
        }
        catch (Exception e) {
            return e.getMessage();
        }

    }

    /**
     * 执行提交事务的指令
     * @param ctx
     * @return 返回提交事务的信息或创建失败的原因
     */
    @Override
    public String visitCommit_stmt(SQLParser.Commit_stmtContext ctx){
        try {
            if (!manager.transactionSessions.contains(session)) {
                return "not in transaction";
            }
            manager.transactionSessions.remove(session);
            // 不直接移除,要保留 key 和空的 value
            Database database = manager.getCurrentDatabase();
            for (String tableName: manager.xLockDict.get(session))
            {
                Table table = database.getTable(tableName);
                table.freeXLock(session);
            }
            ArrayList<String> tablesLock = new ArrayList<>();
            manager.sLockDict.replace(session, tablesLock);
            ArrayList<String> tablexLock = new ArrayList<>();
            manager.xLockDict.replace(session, tablexLock);
            // sLock 在语句执行完后释放

            // 某数据库记录文件超过一定大小的时候，进行log清空日志操作，并对该数据库中的每张表做持久化
            String fileName = "DATA/" + "database_" + database.getName() + ".data";
            File file = new File(fileName);
            if(file.exists() && file.isFile() && file.length()>50000)
            {
                System.out.println("Clear database log");
                try
                {
                    FileWriter writer=new FileWriter(fileName);
                    writer.write( "");
                    writer.close();
                } catch (IOException e)
                {
                    e.printStackTrace();
                }
                manager.persistDatabase(database.getName());
            }

            return "Successfully commit";
        }
        catch (Exception e) {
            return e.getMessage();
        }
    }

    /**
     * 执行自动开始事务的指令
     * @param ctx
     * @return 返回自动开始事务的信息或创建失败的原因
     */
    @Override
    public String visitAuto_begin_transaction_stmt(SQLParser.Auto_begin_transaction_stmtContext ctx){
        try {
            if (manager.transactionSessions.contains(session)) {
                return "already in transaction";
            }
            manager.transactionSessions.add(session);
            manager.sLockDict.put(session, new ArrayList<String>());
            manager.xLockDict.put(session, new ArrayList<String>());
            return "successfully auto begin transaction";
        }
        catch (Exception e) {
            return e.getMessage();
        }

    }

    /**
     * 执行自动提交事务的指令
     * @param ctx
     * @return 返回自动提交事务的信息或创建失败的原因
     */
    @Override
    public String visitAuto_commit_stmt(SQLParser.Auto_commit_stmtContext ctx){
        try {
            if (!manager.transactionSessions.contains(session)) {
                return "not in transaction";
            }
            manager.transactionSessions.remove(session);
            // 不直接移除,要保留 key 和空的 value
            Database database = manager.getCurrentDatabase();
            for (String tableName: manager.xLockDict.get(session))
            {
                Table table = database.getTable(tableName);
                table.freeXLock(session);
            }
            ArrayList<String> tablesLock = new ArrayList<>();
            manager.sLockDict.replace(session, tablesLock);
            ArrayList<String> tablexLock = new ArrayList<>();
            manager.xLockDict.replace(session, tablexLock);

            return "Successfully auto commit";
        }
        catch (Exception e) {
            return e.getMessage();
        }
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
        if (ctx.K_AND() != null) {
            type = ConditionType.AND;
        }
        else if (ctx.K_OR() != null) {
            type = ConditionType.OR;
        }
        return new MultipleCondition(visitMultiple_condition(ctx.multiple_condition(0)),
                visitMultiple_condition(ctx.multiple_condition(1)), type);
    }

    /** 获取整个逻辑表达式 **/
    @Override
    public Condition visitCondition(SQLParser.ConditionContext ctx) {
        Comparer left = visitExpression(ctx.expression(0));
        Comparer right = visitExpression(ctx.expression(1));
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

    // @Override
    public QueryTable visitTable_query_stmt (SQLParser.Table_queryContext ctx) {
        Database database = null;
        try {
            database = manager.getCurrentDatabase();
        }catch (Exception e){
            throw e;
        }
        // 处理单一逻辑
        if (ctx.K_JOIN().size() == 0) {
            Table table = database.getTable(ctx.table_name(0).getText());
            return new SingleTable(table);
        }
    // 处理复合逻辑
        MultipleCondition multipleCondition = visitMultiple_condition(ctx.multiple_condition());
        ArrayList<Table> tables = new ArrayList<Table>();
        for (SQLParser.Table_nameContext subctx : ctx.table_name()) {
            tables.add(database.getTable(subctx.getText().toLowerCase()));
        }
        return new JointTable(tables, multipleCondition);
    }
}
