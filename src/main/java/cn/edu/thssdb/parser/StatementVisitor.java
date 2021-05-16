package cn.edu.thssdb.parser;

import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;

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

    public ArrayList<QueryResult> visitParse(SQLParser.ParseContext ctx) {
        return visitSql_stmt_list(ctx.sql_stmt_list());
    }

    public ArrayList<QueryResult> visitSql_stmt_list(SQLParser.Sql_stmt_listContext ctx) {
        StringJoiner sj = new StringJoiner("\n\n");
        ArrayList<QueryResult> result = new ArrayList<QueryResult>();
        for (SQLParser.Sql_stmtContext subCtx : ctx.sql_stmt())
            result.add(visitSql_stmt(subCtx));
        return result;
    }

    public QueryResult visitSql_stmt(SQLParser.Sql_stmtContext ctx) {
        // 处理各种语句和情况
//        if (ctx.delete_stmt() != null) {
//            String message = visitDelete_stmt(ctx.delete_stmt());
//            return new QueryResult(message);
//        }
        return null;
    }


}
