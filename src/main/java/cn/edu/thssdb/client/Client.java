package cn.edu.thssdb.client;

import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.utils.Global;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.List;
import java.util.Scanner;

import static cn.edu.thssdb.utils.Global.SUCCESS_CODE;

public class Client {

  private static final Logger logger = LoggerFactory.getLogger(Client.class);

  static final String HOST_ARGS = "h";
  static final String HOST_NAME = "host";

  static final String HELP_ARGS = "help";
  static final String HELP_NAME = "help";

  static final String PORT_ARGS = "p";
  static final String PORT_NAME = "port";

  private static final PrintStream SCREEN_PRINTER = new PrintStream(System.out);
  private static final Scanner SCANNER = new Scanner(System.in);

  private static TTransport transport;
  private static TProtocol protocol;
  private static IService.Client client;
  private static CommandLine commandLine;

  private static String username;
  private static String password;
  private static long sessionId = -1;
  private static String transaction = "";

  public static void main(String[] args) {
    commandLine = parseCmd(args);
    if (commandLine.hasOption(HELP_ARGS)) {
      showHelp();
      return;
    }
    try {
      echoStarting();
      String host = commandLine.getOptionValue(HOST_ARGS, Global.DEFAULT_SERVER_HOST);
      int port = Integer.parseInt(commandLine.getOptionValue(PORT_ARGS, String.valueOf(Global.DEFAULT_SERVER_PORT)));
      transport = new TSocket(host, port);
      transport.open();
      protocol = new TBinaryProtocol(transport);
      client = new IService.Client(protocol);
      boolean open = true;
      while (true) {
        print(Global.CLI_PREFIX + transaction);
        String msg = SCANNER.nextLine();
        long startTime = System.currentTimeMillis();
        switch (msg.trim()) {
          case Global.SHOW_TIME:
            getTime();
            break;
          case Global.CONNECT:
            connect();
            break;
          case Global.DISCONNECT:
            disconnect();
            break;
          case Global.QUIT:
          case Global.QUIT_FAKE:
            open = false;
          default:
            executeStatement(msg.trim());
            break;
        }
        long endTime = System.currentTimeMillis();
        println("It costs " + (endTime - startTime) + " ms.");
        if (!open) {
          break;
        }
      }
      transport.close();
    } catch (TTransportException e) {
      logger.error(e.getMessage());
    }
  }

  private static void connect() {
    if (sessionId > 0) {
      println("Already connected!");
      return;
    }

    println("please input username:");
    username = SCANNER.nextLine().trim();
    println("please input password:");
    password = SCANNER.nextLine().trim();

    ConnectReq req = new ConnectReq(username, password);
    try {
      ConnectResp connectResp = client.connect(req);
      if (connectResp.status.code == SUCCESS_CODE) {
        sessionId = connectResp.sessionId;
        println("connect success!");
        println("sessionId is " + String.valueOf(sessionId));
      }
      else {
        println("connect fail!" + connectResp.status.getMsg());
      }
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  private static void disconnect() {
    if (sessionId == -1){
      println("Already disconnected!");
      return;
    }
    DisconnetReq req = new DisconnetReq(sessionId);
    try {
      DisconnetResp disconnectResp = client.disconnect(req);
      if (disconnectResp.status.code == SUCCESS_CODE) {
        sessionId = -1;
        println("disconnect success!");
      }
      else
      {
        println("disconnect fail!");
      }
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  private static void executeStatement(String executeStatement) {
    if (sessionId == -1){
      println("Connect first!");
      return;
    }

    // ???????????? executeStatementRequest
    ExecuteStatementReq req = new ExecuteStatementReq(sessionId, executeStatement);
    try {
      ExecuteStatementResp executeStatementResp = client.executeStatement(req);
      if (executeStatementResp.status.code == SUCCESS_CODE) {
        // ????????????????????????????????????
        if (executeStatementResp.isSetRowList()) {
          for (String columnName : executeStatementResp.columnsList) {
            print(columnName + " ");
          }
          println();
          for (List<String> row : executeStatementResp.rowList) {
            for (String rowItem : row) {
              print(rowItem + " ");
            }
            println();
          }
        }
        // ????????????????????????????????????????????????
        else {
          for (String item: executeStatementResp.columnsList) {
              item = item.trim();
              if(item.equals("successfully begin transaction")) {
                transaction = "(in transaction)";
              }
              else if(item.equals("Successfully commit"))
              {
                transaction = "";
              }
              println(item);
          }
        }

      }
      else {
        println("fail!");
        println(executeStatementResp.status.getMsg());
      }
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }


  private static void getTime() {
    GetTimeReq req = new GetTimeReq();
    try {
      println(client.getTime(req).getTime());
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  static Options createOptions() {
    Options options = new Options();
    options.addOption(Option.builder(HELP_ARGS)
        .argName(HELP_NAME)
        .desc("Display help information(optional)")
        .hasArg(false)
        .required(false)
        .build()
    );
    options.addOption(Option.builder(HOST_ARGS)
        .argName(HOST_NAME)
        .desc("Host (optional, default 127.0.0.1)")
        .hasArg(false)
        .required(false)
        .build()
    );
    options.addOption(Option.builder(PORT_ARGS)
        .argName(PORT_NAME)
        .desc("Port (optional, default 6667)")
        .hasArg(false)
        .required(false)
        .build()
    );
    return options;
  }

  static CommandLine parseCmd(String[] args) {
    Options options = createOptions();
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      logger.error(e.getMessage());
      println("Invalid command line argument!");
      System.exit(-1);
    }
    return cmd;
  }

  static void showHelp() {
    println("DO IT YOURSELF");
  }

  static void echoStarting() {
    println("----------------------");
    println("Starting ThssDB Client");
    println("----------------------");
  }

  static void print(String msg) {
    SCREEN_PRINTER.print(msg);
  }

  static void println() {
    SCREEN_PRINTER.println();
  }

  static void println(String msg) {
    SCREEN_PRINTER.println(msg);
  }
}
