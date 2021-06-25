package cn.edu.thssdb.service;

import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.StatementErrorListener;
import cn.edu.thssdb.parser.StatementVisitor;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.rpc.thrift.ConnectReq;
import cn.edu.thssdb.rpc.thrift.ConnectResp;
import cn.edu.thssdb.rpc.thrift.DisconnetReq;
import cn.edu.thssdb.rpc.thrift.DisconnetResp;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementReq;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.rpc.thrift.GetTimeReq;
import cn.edu.thssdb.rpc.thrift.GetTimeResp;
import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.utils.Global;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Date;

public class IServiceHandler implements IService.Iface {
  private static long sessionNum = 0;


  public Manager manager = Manager.getInstance();

  @Override
  public GetTimeResp getTime(GetTimeReq req) throws TException {
    GetTimeResp resp = new GetTimeResp();
    resp.setTime(new Date().toString());
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public ConnectResp connect(ConnectReq req) throws TException {
    // 暂时不检查用户名和密码
    ConnectResp resp = new ConnectResp();
    sessionNum += 1;

    // 分配session成功
    if (sessionNum > 0) {
      resp.setStatus(new Status(Global.SUCCESS_CODE));
      resp.setSessionId(sessionNum);
      return resp;
    }

    // 分配session失败
    Status respStatus = new Status(Global.FAILURE_CODE);
    respStatus.setMsg("Session num exceeds!");
    resp.setStatus(respStatus);
    resp.setSessionId(-1);
    return resp;
  }

  @Override
  public DisconnetResp disconnect(DisconnetReq req) throws TException {
    DisconnetResp resp = new DisconnetResp();
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
    ExecuteStatementResp resp = new ExecuteStatementResp();

    // 检查客户端的session是否处于连接状态
    long clientSession = req.getSessionId();
    if (clientSession <= 0 || clientSession > sessionNum){
      Status respStatus = new Status(Global.FAILURE_CODE);
      respStatus.setMsg("Client disconnected!");
      resp.setStatus(respStatus);
      resp.setIsAbort(false);
      resp.setHasResult(false);
      return resp;
    }

    String[] statements = req.getStatement().split(";");
    ArrayList<QueryResult> queryResults = new ArrayList<QueryResult>();
    for (String statement : statements) {
      // 对空白符有做一些处理

      statement = statement.trim();

      // 无内容则跳过
      if (statement.length() == 0) {
        continue;
      }

      String command = statement.split(" ")[0].toLowerCase();
      if (!manager.transactionSessions.contains(req.sessionId) && (command.equals("insert") || command.equals("update") || command.equals("delete") || command.equals("select"))) {
        handleCommand("auto begin transaction", req.sessionId, manager);
        ArrayList<QueryResult> result = handleCommand(statement, req.sessionId, manager);
        queryResults.addAll(result);
        handleCommand("auto commit", req.sessionId, manager);
      }
      else {
        ArrayList<QueryResult> result = handleCommand(statement, req.sessionId, manager);
        queryResults.addAll(result);
      }

    }

    // 将queryResults中的结果加入resp

    // 情况1：无任何查询结果
    if (queryResults.size() == 0) {
      resp.addToColumnsList("null");
    }
    // 情况2：只有一个查询 且 返回结果无异常
    else if (queryResults.size() == 1 && queryResults.get(0) != null && queryResults.get(0).getIsQueryResult()) {
      for (Row row : queryResults.get(0).getResult()) {
        resp.addToRowList(row.toStringList());
      }
      // 查询结果为空 初始化响应的rowList为一个空数组
      if (!resp.isSetRowList()) {
        resp.rowList = new ArrayList<>();
      }
      for (String columnName: queryResults.get(0).columnSelectName) {
        resp.addToColumnsList(columnName);
      }
    }
    // 情况3：存在异常或只需要返回信息
    else {
      for (QueryResult resultItem : queryResults) {
        if (resultItem == null) {
          resp.addToColumnsList("null");
        }
        else {
          resp.addToColumnsList(resultItem.getMessage());
        }
      }
    }
    // 思考：有多个正确的查询结果怎么破？

    resp.setStatus(new Status(Global.SUCCESS_CODE));
    resp.setIsAbort(false);
    resp.setHasResult(true);
    return resp;
  }


  public ArrayList<QueryResult> handleCommand(String command, long sessionId, Manager manager) {
    String cmd = command.split(" ")[0].toLowerCase();

    if (sessionId == 0 && (cmd.equals("insert") || cmd.equals("update") || cmd.equals("delete") || cmd.equals("select"))) {
      manager.writeLog(command);
    }


    //词法分析
    SQLLexer lexer = new SQLLexer(CharStreams.fromString(command));
    lexer.removeErrorListeners();
    lexer.addErrorListener(StatementErrorListener.instance);
    CommonTokenStream tokens = new CommonTokenStream(lexer);

    //句法分析
    SQLParser parser = new SQLParser(tokens);
    parser.removeErrorListeners();
    parser.addErrorListener(StatementErrorListener.instance);

    //语义分析
    try {
      StatementVisitor visitor = new StatementVisitor(manager, sessionId);
      return visitor.visitParse(parser.parse());
    } catch (Exception e) {
      ArrayList<QueryResult> queryResult = new ArrayList<QueryResult>();
      queryResult.add(new QueryResult("Exception: illegal SQL statement! Error message: " + e.getMessage()));
      return queryResult;
    }

  }

}



