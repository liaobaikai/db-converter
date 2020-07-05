package com.liaobaikai.argparse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Argument2 {

    /**
     * 源主机
     * --source-host
     * -h
     */
    private String sourceHost;

    /**
     * 目标主机
     * --target-host
     * -H
     */
    private String targetHost;

    /**
     * --source-port
     * -p
     */
    private String sourcePort;

    /**
     * --target-port
     * -P
     */
    private String targetPort;

    /**
     * --source-database
     * -d
     */
    private String sourceDataBase;

    /**
     * --target-database
     * -D
     */
    private String targetDataBase;

    /**
     * --source-username
     * -u
     */
    private String sourceUserName;

    /**
     * --target-username
     * -U
     */
    private String targetUserName;

    /**
     * --source-password
     * -a
     */
    private String sourcePassword;

    /**
     * --target-password
     * -A
     */
    private String targetPassword;

    /**
     * 导出表名
     * --table
     * -t
     */
    private String[] table;

    /**
     * 执行的语句
     * --execute
     * -e
     */
    private String execute;

    /**
     * 已存在的操作
     * --exists
     * replace/ignore
     */
    private String exists;

    /**
     * 已存在的直接替换
     */
    public static final String EXISTS_REPLACE = "replace";

    /**
     * 已存在的直接忽略
     */
    public static final String EXISTS_IGNORE = "ignore";


    public Argument2(String[] args){

        for(int i = 0, len = args.length; i < len; i++){
            String arg = args[i];
            if(arg != null && arg.length() > 0){
                if(arg.startsWith("--")){
                    // 长名字
                    // 例如：--source-database
                    if(i != len - 1){
                        i++;
                    }
                    this.parseLongName(arg, args[i]);
                } else if(arg.startsWith("-")){
                    // 短名字
                    // 例如：-d
                    // 例如：-ddizhiju
                    if(arg.length() == 2){
                        // 需要下一个值
                        if(i != len - 1){
                            i++;
                            arg = arg + args[i];
                        }
                    }
                    this.parseShortName(arg);
                } else if(arg.trim().length() > 0){
                    // 不指定
                    this.parseName(arg);
                }
            }
        }

    }

    /**
     * 解析参数
     * @param arg
     */
    private void parseName(String arg) {
        System.out.println("parseName,arg:" + arg);
        if(arg.equalsIgnoreCase(EXISTS_REPLACE)){
            this.exists = EXISTS_REPLACE;
        } else if(arg.equalsIgnoreCase(EXISTS_IGNORE)){
            this.exists = EXISTS_IGNORE;
        }
    }

    /**
     * 解析短名字参数
     * @param arg
     */
    private void parseShortName(String arg) {
        System.out.println("parseShortName,arg:" + arg);
        if(arg.length() <= 2){
            return;
        }
        String name = arg.substring(1, 2);
        String value = arg.substring(2);

        if("h".equals(name)){
            this.sourceHost = value;
        } else if("H".equals(name)){
            this.targetHost = value;
        } else if("p".equals(name)){
            this.sourcePort = value;
        } else if("P".equals(name)){
            this.targetPort = value;
        } else if("d".equals(name)){
            this.sourceDataBase = value;
        } else if("D".equals(name)){
            this.targetDataBase = value;
        } else if("u".equals(name)){
            this.sourceUserName = value;
        } else if("U".equals(name)){
            this.targetUserName = value;
        } else if("a".equals(name)){
            this.sourcePassword = value;
        } else if("A".equals(name)){
            this.targetPassword = value;
        } else if("t".equals(name)){
            this.parseTableValue(value);
        } else if("e".equals(name)){
            this.execute = value;
        }

    }


    /**
     * 解析长名字参数
     * @param arg
     */
    private void parseLongName(String arg, String value){

        arg = arg.substring(2);

        // // --source-host ''
        //        // --target-host ''
        //        // --source-port
        //        // --target-port
        //        // --source-database
        //        // --target-database
        //        // --source-username
        //        // --target-username
        //        // --source-password
        //        // --target-password

        if("source-host".equals(arg)){
            this.sourceHost = value;
        } else if("target-host".equals(arg)){
            this.targetHost = value;
        } else if("source-port".equals(arg)){
            this.sourcePort = value;
        } else if("target-port".equals(arg)){
            this.targetPort = value;
        } else if("source-database".equals(arg)){
            this.sourceDataBase = value;
        } else if("target-database".equals(arg)){
            this.targetDataBase = value;
        } else if("source-username".equals(arg)){
            this.sourceUserName = value;
        } else if("target-username".equals(arg)){
            this.targetUserName = value;
        } else if("source-password".equals(arg)){
            this.sourcePassword = value;
        } else if("target-password".equals(arg)){
            this.targetPassword = value;
        } else if("table".equals(arg)){
            this.parseTableValue(value);
        } else if("execute".equals(arg)){
            this.execute = value;
        }

    }


    @Override
    public String toString() {
        return "Argument{" +
                "sourceHost='" + sourceHost + '\'' +
                ", targetHost='" + targetHost + '\'' +
                ", sourcePort='" + sourcePort + '\'' +
                ", targetPort='" + targetPort + '\'' +
                ", sourceDataBase='" + sourceDataBase + '\'' +
                ", targetDataBase='" + targetDataBase + '\'' +
                ", sourceUserName='" + sourceUserName + '\'' +
                ", targetUserName='" + targetUserName + '\'' +
                ", sourcePassword='" + sourcePassword + '\'' +
                ", targetPassword='" + targetPassword + '\'' +
                ", table=" + Arrays.toString(table) +
                ", execute='" + execute + '\'' +
                ", exists='" + exists + '\'' +
                '}';
    }

//    public static void main(String[] args) {
//
//        // "192.168.0.110:1433" "dizhiju" "sa" "1" "127.0.0.1:3306" "test" "baikai" "baikai#1234"
//        String str = "database --source-port 1433 --source-username sa --source-password 1 -H127.0.0.1 -P3306 replace -h192.168.0.110  -d    dizhiju -t a,b,vd";
//        Argument2 argument = new Argument2(str.split("\\s+"));
//
//        System.out.println(argument);
//    }

    private void parseTableValue(String value){
        String[] str = value.split(",");
        List<String> tables = new ArrayList<String>();
        for(String t : str){
            t = t.trim();
            if(t.length() == 0){
                continue;
            }
            tables.add(t);
        }
        this.table = new String[tables.size()];
        tables.toArray(this.table);
    }

}
