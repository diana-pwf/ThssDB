package cn.edu.thssdb.service;

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
import cn.edu.thssdb.utils.Global;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Date;

public class IServiceHandler implements IService.Iface {
  private static long sessionNum = 0;

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
    for (String statement : statements) {
      statement = statement.trim();

      // 无内容则跳过
      if (statement.length() == 0) {
        continue;
      }

      String command = statement.split(" ")[0];
      // 根据command类型 分类执行
    }

    resp.setStatus(new Status(Global.SUCCESS_CODE));
    resp.setIsAbort(false);
    resp.setHasResult(true);
    return resp;
  }
}
