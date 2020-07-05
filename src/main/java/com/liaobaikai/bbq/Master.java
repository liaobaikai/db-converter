package com.liaobaikai.bbq;

import com.liaobaikai.bbq.db.Database;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.sql.SQLException;

public class Master {

    public static void main(String[] args) {

        // https://argparse4j.github.io/examples.html



//        ArgumentParser parser = ArgumentParsers.newFor("databaseConverter").addHelp(false).build().defaultHelp(false).description("数据库转换器");
//        parser.addArgument("-m", "--module").choices("SQLServer2MySQLConverter", "MySQL2SQLServerConverter", "Oracle2MySQLConverter");
//        parser.addArgument("-h", "--source-host").setDefault("127.0.0.1").help("源数据库主机");
//        parser.addArgument("-p", "--source-port").setDefault("#").help("源数据库端口");
//        parser.addArgument("-d", "--source-database").setDefault("#").help("源数据库数名称");
//        parser.addArgument("-u", "--source-username").setDefault("#").help("源数据库用户名");
//        parser.addArgument("-a", "--source-password").setDefault("#").help("源数据库密码");
//
//        parser.addArgument("-H", "--target-host").setDefault("127.0.0.1").help("目标数据库主机");
//        parser.addArgument("-P", "--target-port").setDefault("#").help("目标数据库端口");
//        parser.addArgument("-D", "--target-database").setDefault("#").help("目标数据库数名称");
//        parser.addArgument("-U", "--target-username").setDefault("#").help("目标数据库用户名");
//        parser.addArgument("-A", "--target-password").setDefault("#").help("目标数据库密码");

//
//        Namespace ns = null;
//        try {
//            ns = parser.parseArgs(args);
//        } catch (ArgumentParserException e) {
//            parser.handleError(e);
//            System.exit(1);
//        }

        Database d1 = new Database("a.baikai.top", "14330", "sa", "baikai#1234", "db_web382788");
        Database d2 = new Database("127.0.0.1", "3306", "baikai", "baikai#1234", "db_web382788");
        d2.setCharset("utf8mb4");
        d2.setEngine("innodb");

        SQLServer2MySQLConverter converter = new SQLServer2MySQLConverter(d1, d2);
        try {
            converter.convertMetadata(null, null, false, "replace");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            converter.convertData(new String[]{"bb", "cc"}, null, null, false);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
