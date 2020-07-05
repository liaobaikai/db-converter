//package com.liaobaikai.bbq;
//
//import com.alibaba.druid.pool.DruidDataSource;
//import org.springframework.jdbc.core.JdbcTemplate;
//import org.springframework.jdbc.core.RowMapper;
//
//import java.sql.*;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//public class Mssql2Mysql {
//
//    private static final String MSSQL = "Microsoft SQL Server";
//    private static final String MSSQL_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
//    private static final String MSSQL_URL = "jdbc:sqlserver://%s;databaseName=%s";
//
//    private static final String MYSQL = "MySQL";
//    private static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
//    private static final String MYSQL_URL = "jdbc:mysql://%s/%s?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&useSSL=true&serverTimezone=GMT%%2B8";
//
//    private static final Map<String, String> MSSQL2MYSQL = new HashMap<String, String>();
//    static {
//        MSSQL2MYSQL.put("bigint", "bigint");
//        MSSQL2MYSQL.put("binary", "binary");
//        MSSQL2MYSQL.put("bit", "tinyint");
//        MSSQL2MYSQL.put("char", "char");
//        MSSQL2MYSQL.put("date", "date");
//        MSSQL2MYSQL.put("datetime", "datetime");
//        MSSQL2MYSQL.put("datetime2", "datetime");
//        MSSQL2MYSQL.put("datetimeoffset", "datetime");
//        MSSQL2MYSQL.put("decimal", "decimal");
//        MSSQL2MYSQL.put("float", "float");
//        MSSQL2MYSQL.put("int", "int");
//        MSSQL2MYSQL.put("money", "float");
//        MSSQL2MYSQL.put("nchar", "char");
//        MSSQL2MYSQL.put("ntext", "text");
//        MSSQL2MYSQL.put("numeric", "decimal");
//        MSSQL2MYSQL.put("nvarchar", "varchar");
//        MSSQL2MYSQL.put("real", "float");
//        MSSQL2MYSQL.put("smalldatetime", "datetime");
//        MSSQL2MYSQL.put("smallint", "smallint");
//        MSSQL2MYSQL.put("smallmoney", "float");
//        MSSQL2MYSQL.put("text", "text");
//        MSSQL2MYSQL.put("time", "time");
//        MSSQL2MYSQL.put("timestamp", "timestamp");
//        MSSQL2MYSQL.put("tinyint", "tinyint");
//        MSSQL2MYSQL.put("uniqueidentifier", "varchar(40)");
//        MSSQL2MYSQL.put("varbinary", "varbinary");
//        MSSQL2MYSQL.put("varchar", "varchar");
//        MSSQL2MYSQL.put("xml", "text");
//    }
//
//
//    public static void main(String[] args) throws SQLException {
//
//        // --source-host ''
//        // --target-host ''
//        // --source-port
//        // --target-port
//        // --source-database
//        // --target-database
//        // --source-username
//        // --target-username
//        // --source-password
//        // --target-password
//
//
//
//        // -t sys.tables -c name -e "select name from sys.tables" -f "id = 1"
//        String sourceHost = args[0];
//        String sourceDBName = args[1];
//        String sourceUser = args[2];
//        String sourcePwd = args[3];
//
//        String targetHost = args[4];
//        final String targetDBName = args[5];
//        String targetUser = args[6];
//        String targetPwd = args[7];
//
//        String tableName = null;
//        if(args.length >= 9){
//            tableName = args[8];
//        }
//
//        String execute = null;
//        if(args.length >= 10){
//            execute = args[9];
//        }
//
//        String exists = "ignore";
//        if(args.length >= 11){
//            exists = args[10];
//        }
//
////        args[0] = "192.168.3.20:1433";
////        args[1] = "dizhiju";
////        args[2] = "sa";
////        args[3] = "1";
////
////        args[4] = "182.61.55.239:8033";
////        args[5] = "test_20200624";
////        args[6] = "baikai";
////        args[7] = "baikai#1234";
////
////
////        args[8] = "articleTemplate";    // 需要生成的表名
////        args[9] = "select a.*, b.menuName\n" +
////                "from T_TempNews a\n" +
////                "left join T_SysMenuVar b\n" +
////                "on a.menuIndexVar = b.menuIndexVar";       // 执行的语句
//
//        final DruidDataSource sourceDS = new DruidDataSource();
//        sourceDS.setDriverClassName(MSSQL_DRIVER);
//        sourceDS.setUrl(String.format(MSSQL_URL, sourceHost, sourceDBName));
//        sourceDS.setUsername(sourceUser);
//        sourceDS.setPassword(sourcePwd);
//
//        JdbcTemplate sourceTemplate = new JdbcTemplate();
//        sourceTemplate.setDataSource(sourceDS);
//        final String sourceDB = sourceDS.getConnection().getMetaData().getDatabaseProductName();
//
//        final DruidDataSource targetDS = new DruidDataSource();
//        targetDS.setDriverClassName(MYSQL_DRIVER);
//        targetDS.setUrl(String.format(MYSQL_URL, targetHost, targetDBName));
//        targetDS.setUsername(targetUser);
//        targetDS.setPassword(targetPwd);
//
//        final JdbcTemplate targetTemplate = new JdbcTemplate();
//        targetTemplate.setDataSource(targetDS);
//        final String targetDB = targetDS.getConnection().getMetaData().getDatabaseProductName();
//
//        // 往目标数据库创建一个表并插入数据
//        //
//
//
////        String tableName = "articleTemplate";
////        String columns = "name";
////        String execute = "select a.*, b.menuName\n" +
////                "from T_TempNews a\n" +
////                "left join T_SysMenuVar b\n" +
////                "on a.menuIndexVar = b.menuIndexVar";
//
//        // 如果args[8] 和 args[9]为空的的话，就是迁移整个数据库
//        if(args.length == 8){
//            final String catalog = sourceDS.getConnection().getCatalog();
//            System.out.println("catalog：" + catalog);
//            final DatabaseMetaData dbMetaData = sourceDS.getConnection().getMetaData();
//            System.out.println("即将为您导出源数据库[" + sourceDBName + "]全部表...");
//            ResultSet tableResultSet = dbMetaData.getTables(catalog,
//                    null, null, new String[]{"TABLE"});
//            int count = 0;
//            while(tableResultSet.next()){
//                String name = tableResultSet.getString("TABLE_NAME");
//
//                // 查询主键
//                ResultSet primaryKeyResultSet = dbMetaData.getPrimaryKeys(catalog,null, name);
//
////                List<String> primaryKeys = new ArrayList<String>();
//                StringBuilder primaryKeys = new StringBuilder();
//                while(primaryKeyResultSet.next()){
//                    String primaryKeyColumnName = primaryKeyResultSet.getString("COLUMN_NAME");
//                    primaryKeys.append(primaryKeyColumnName).append(",");
//                }
//
//
//                // 获取表的所有字段
//                // 获取到所有表。。。
//                ResultSet columnResultSet = dbMetaData.getColumns(catalog, "%", name, "%");
//                StringBuilder tableDDL = new StringBuilder();
//                while (columnResultSet.next()){
//
//                    // TABLE_CAT
//                    // TABLE_SCHEM
//                    // TABLE_NAME
//                    // COLUMN_NAME
//                    // DATA_TYPE
//                    // TYPE_NAME
//                    // COLUMN_SIZE          // 列大小
//                    // BUFFER_LENGTH        // 未使用
//                    // DECIMAL_DIGITS       // 小数部分的位数
//                    // NUM_PREC_RADIX       // 基数（通常为 10 或 2）
//                    // NULLABLE
//                    // REMARKS              // 描述列的注释
//                    // COLUMN_DEF
//                    // SQL_DATA_TYPE
//                    // SQL_DATETIME_SUB
//                    // CHAR_OCTET_LENGTH
//                    // ORDINAL_POSITION
//                    // IS_NULLABLE          YES/NO
//                    // SCOPE_CATALOG
//                    // SCOPE_SCHEMA
//                    // SOURCE_DATA_TYPE
//                    // IS_AUTOINCREMENT     YES/NO
//                    // IS_GENERATEDCOLUMN
//                    // SS_IS_SPARSE
//                    // SS_IS_COLUMN_SET
//                    // SS_UDT_CATALOG_NAME
//                    // SS_UDT_SCHEMA_NAME
//                    // SS_UDT_ASSEMBLY_TYPE_NAME
//                    // SS_XML_SCHEMACOLLECTION_CATALOG_NAME
//                    // SS_XML_SCHEMACOLLECTION_SCHEMA_NAME
//                    // SS_XML_SCHEMACOLLECTION_NAME
//
//                    String columnName = columnResultSet.getString("COLUMN_NAME");
//                    String columnType = columnResultSet.getString("TYPE_NAME");
//
//                    int digits = columnResultSet.getInt("DECIMAL_DIGITS");
//                    System.out.println("DECIMAL_DIGITS:" + digits);
//                    System.out.println("COLUMN_SIZE:" + columnResultSet.getInt("COLUMN_SIZE"));
//
//                    int sqlDataType = columnResultSet.getInt("SQL_DATA_TYPE");  // java.sql.Types
//                    int dataType = columnResultSet.getInt("DATA_TYPE");  // java.sql.Types
//                    String remark = columnResultSet.getString("REMARKS");
//
////                    boolean nullable = columnResultSet.getInt("NULLABLE") == 1;
//                    String autoIncrement = columnResultSet.getString("IS_AUTOINCREMENT");
//                    String nullable = columnResultSet.getString("IS_NULLABLE");
//
//                    System.out.println("IS_NULLABLE:" + columnResultSet.getString("IS_NULLABLE"));
//                    System.out.println("TABLE_CAT:" + columnResultSet.getString("TABLE_CAT"));
//                    System.out.println("TABLE_SCHEM:" + columnResultSet.getString("TABLE_SCHEM"));
//                    System.out.println("IS_AUTOINCREMENT:" + columnResultSet.getString("IS_AUTOINCREMENT"));
//                    System.out.println("SQL_DATA_TYPE:" + sqlDataType);
//                    System.out.println("DATA_TYPE:" + dataType);
//                    System.out.println("COLUMN_NAME:" + columnName);
//                    System.out.println("columnType:" + columnType);
//                    System.out.println("REMARKS:" + remark);
//
//
//                    tableDDL.append("  ").append(columnName).append(" ");
//
//                    if(columnType.contains(" "))
//                        columnType = columnType.substring(0, columnType.indexOf(" "));
//
//                    String type = "";
//                    if(sourceDB.equalsIgnoreCase(MSSQL) && targetDB.equalsIgnoreCase(MYSQL)){
//                        type = MSSQL2MYSQL.get(columnType);
//                    } else {
//                        type = columnType;
//                    }
//
//                    if(!(type.equalsIgnoreCase("datetime")
//                            || type.equalsIgnoreCase("date")
//                            || type.equalsIgnoreCase("time")
//                            || type.equalsIgnoreCase("timestamp")
//                            || type.equalsIgnoreCase("text"))){
//                        // 如果不是以上的数据类型，都是需要定义长度
//                        if(type.contains("(") && type.contains(")")){
//                            // uniqueidentifier -> varchar(40)
//                        } else {
//                            // 需要定义长度
//                            int dataSize = columnResultSet.getInt("COLUMN_SIZE");
//                            type = String.format("%s(%s)", type, dataSize);
//                        }
//                    }
//
//                    tableDDL.append(type).append(" ");
//
//                    if(nullable.equalsIgnoreCase("YES")){
//                        nullable = "DEFAULT NULL";
//                    } else {
//                        nullable = "NOT NULL";
//                    }
//
//                    tableDDL.append(nullable);
//
//                    if(autoIncrement.equalsIgnoreCase("YES")){
//                        autoIncrement = " AUTO_INCREMENT";
//                    } else {
//                        autoIncrement = "";
//                    }
//
//                    tableDDL.append(autoIncrement);
//
//                    if(remark != null){
//                        remark = String.format(" comment '%s'", remark);
//                    } else {
//                        remark = "";
//                    }
//
//                    tableDDL.append(remark).append(", \n");
//
//                }
//
//                tableDDL.delete(tableDDL.length() - 3, tableDDL.length());
//
//                // 主键
//                if(primaryKeys.length() > 0){
//                    primaryKeys.delete(primaryKeys.length() - 1, primaryKeys.length());
//                    tableDDL.append(", \n");
//                    tableDDL.append(String.format("PRIMARY KEY (%s)", primaryKeys));
//                }
//
//                System.out.println(String.format("CREATE TABLE IF NOT EXISTS %s\n(\n" +
//                        "%s" +
//                        "\n) ENGINE=innodb DEFAULT CHARSET=utf8", name, tableDDL.toString()));
//
//                count++;
//            }
//
//            System.out.println("<<< 查询到" + count + "个表。");
//
//
////            List<Map<String, Object>> result = sourceTemplate.queryForList("select name from sys.tables");
////            System.out.println("源数据库[" + sourceDBName + "]所有表数量为" + result.size());
////            if(result.size() > 0){
////                System.out.println("即将导出的表名如下：");
////                if(tableName == null){
////                    tableName = "";
////                }
////                for(Map<String, Object> item : result){
////                    tableName += item.get("name").toString() + ",";
////                    System.out.println(item.get("name").toString());
////                }
////
////                tableName = tableName.substring(0, tableName.length() - 1);
////
////            }
//        }
//
//        System.exit(0);
//
//        boolean exportDB = false;
//        if(execute == null){
//            exportDB = true;
//        }
//
//        final List<String> errorTables = new ArrayList<String>();
//
//        for(final String table : tableName.split(",")){
//
//            if(exportDB){
//                execute = String.format("select * from %s", table);
//            }
//
//            System.out.print(String.format("正在分析源数据库[%s]表[%s]...", sourceDBName, table));
//            try{
//                System.out.println(sourceTemplate.queryForList(execute).size() + "行。");
//            }catch (Exception e){
//                System.out.println(e.getMessage());
//                continue;
//            }
//
//            System.out.println(String.format("正在导入目标数据库[%s]表[%s]...", targetDBName, table));
//            final int[] insertCount = {0};
//            sourceTemplate.query(execute, new RowMapper<Object>() {
//                StringBuilder sql;
//                public Object mapRow(ResultSet resultSet, int rowNum) throws SQLException {
//
//                    ResultSetMetaData rsmd = resultSet.getMetaData();
//                    int count = rsmd.getColumnCount();
//                    if(rowNum == 0) {
//
//                        String template = "create table %s (\n%s \n)";
//
//                        StringBuilder tableDDL = new StringBuilder();
//                        for (int i = 0; i < count; i++) {
//
//                            String type;
//                            if(sourceDB.equalsIgnoreCase(MSSQL) && targetDB.equalsIgnoreCase(MYSQL)){
//                                type = MSSQL2MYSQL.get(rsmd.getColumnTypeName(i + 1));
//                            } else {
//                                type = rsmd.getColumnTypeName(i + 1);
//                            }
//
//                            tableDDL.append(" ")
//                                    .append(rsmd.getColumnName(i + 1))
//                                    .append(" ")
//                                    .append(type);
//
//                            // 需要设置长度
//                            if(type.equalsIgnoreCase("varchar")
//                                    || type.equalsIgnoreCase("char")){
//                                tableDDL.append("(")
//                                        .append(rsmd.getColumnDisplaySize(i + 1))
//                                        .append(")");
//                            }
//
//                            if(i != count - 1){
//                                tableDDL.append(", \n");
//                            }
//                        }
//
//                        try{
//                            System.out.print(String.format("目标数据库[%s]创建表[%s]...", targetDBName, table));
//                            targetTemplate.execute(String.format(template, table, tableDDL.toString()));
//                            System.out.println("Ok.");
//                        }catch (Exception e){
//                            errorTables.add(table);
//                            System.out.println(e.getMessage());
//                        }
//                    }
//
//                    boolean flag = false;
//                    StringBuilder columnNames = null;
//                    StringBuilder values = null;
//                    Object[] data = new Object[count];
//                    if(sql == null){
//                        sql = new StringBuilder();
//                        columnNames = new StringBuilder();
//                        values = new StringBuilder();
//                        flag = true;
//                    }
//                    for(int i = 0; i < count; i++){
//                        if(flag){
//                            columnNames.append(rsmd.getColumnName(i + 1));
//                            values.append("?");
//                        }
//
//                        if(i != count - 1){
//                            if(flag){
//                                values.append(", ");
//                                columnNames.append(", ");
//                            }
//                        }
//                        data[i] = resultSet.getObject(i + 1);
//                    }
//
//                    if(flag){
//                        sql.append(String.format("insert into %s (%s) values ( %s )", table, columnNames.toString(), values.toString()));
//                    }
//
//                    targetTemplate.update(sql.toString(), data);
//                    insertCount[0] += 1;
//                    return null;
//                }
//            });
//
//            System.out.println(">>> " + insertCount[0] + "行已导入。");
//
//        }
//
//        System.out.println("错误的表：");
//        System.out.println(errorTables);
//
//    }
//}
