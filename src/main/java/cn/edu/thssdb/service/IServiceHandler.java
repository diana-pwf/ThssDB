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

import java.util.Date;

public class IServiceHandler implements IService.Iface {

  @Override
  public GetTimeResp getTime(GetTimeReq req) throws TException {
    GetTimeResp resp = new GetTimeResp();
    resp.setTime(new Date().toString());
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public ConnectResp connect(ConnectReq req) throws TException {
    // TODO
    // pwf - 暂时不检查用户名和密码
    ConnectResp resp = new ConnectResp();
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    // sessionId暂时一律设置为 1
    resp.setSessionId(1);
    return resp;
    // pwf
  }

  @Override
  public DisconnetResp disconnect(DisconnetReq req) throws TException {
    // TODO
    // pwf
    DisconnetResp resp = new DisconnetResp();
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
    // pwf
  }

  @Override
  public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
    // TODO
    return null;
  }
}
