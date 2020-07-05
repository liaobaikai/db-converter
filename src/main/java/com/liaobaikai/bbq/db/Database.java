package com.liaobaikai.bbq.db;

public class Database {

    private String hostname;
    private String port;
    private String username;
    private String password;
    private String dbName;
    // 连接数据库的字符集
    private String encoding = "utf8";
    // 生成表的字符集
    private String charset = "utf8";
    // 生成表的存储引擎，MySQL的innodb, MyISAM
    private String engine = "";

    public Database(String hostname, String port, String username, String password, String dbName) {
        this.hostname = hostname;
        this.port = port;
        this.username = username;
        this.password = password;
        this.dbName = dbName;
    }

    public Database(String hostname, String port, String username, String password, String dbName, String encoding) {
        this.hostname = hostname;
        this.port = port;
        this.username = username;
        this.password = password;
        this.dbName = dbName;
        this.encoding = encoding;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public String getEngine() {
        return engine;
    }

    public void setEngine(String engine) {
        this.engine = engine;
    }
}
