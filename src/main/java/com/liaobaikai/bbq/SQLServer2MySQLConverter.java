package com.liaobaikai.bbq;

import com.alibaba.druid.pool.DruidDataSource;
import com.liaobaikai.bbq.db.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class SQLServer2MySQLConverter implements DatabaseConverter {

    private DruidDataSource sDataSource;
    private DruidDataSource tDataSource;
    private JdbcTemplate sJdbcTemplate;
    private JdbcTemplate tJdbcTemplate;

    private Connection sConnection;
    private Connection tConnection;

    private Database sDatabase;
    private Database tDatabase;
    private Logger logger = LoggerFactory.getLogger(SQLServer2MySQLConverter.class);

    private static final Map<String, String> MSSQL2MYSQL = new HashMap<String, String>();
    static {
        MSSQL2MYSQL.put("bigint", "bigint");
        MSSQL2MYSQL.put("binary", "binary");
        MSSQL2MYSQL.put("bit", "tinyint");
        MSSQL2MYSQL.put("char", "char");
        MSSQL2MYSQL.put("date", "date");
        MSSQL2MYSQL.put("datetime", "datetime");
        MSSQL2MYSQL.put("datetime2", "datetime");
        MSSQL2MYSQL.put("datetimeoffset", "datetime");
        MSSQL2MYSQL.put("decimal", "decimal");
        MSSQL2MYSQL.put("float", "float");
        MSSQL2MYSQL.put("int", "int");
        MSSQL2MYSQL.put("money", "float");
        MSSQL2MYSQL.put("nchar", "char");
        MSSQL2MYSQL.put("ntext", "text");
        MSSQL2MYSQL.put("numeric", "decimal");
        MSSQL2MYSQL.put("nvarchar", "varchar");
        MSSQL2MYSQL.put("real", "float");
        MSSQL2MYSQL.put("smalldatetime", "datetime");
        MSSQL2MYSQL.put("smallint", "smallint");
        MSSQL2MYSQL.put("smallmoney", "float");
        MSSQL2MYSQL.put("text", "text");
        MSSQL2MYSQL.put("time", "time");
        MSSQL2MYSQL.put("timestamp", "timestamp");
        MSSQL2MYSQL.put("tinyint", "tinyint");
        MSSQL2MYSQL.put("uniqueidentifier", "varchar(40)");
        MSSQL2MYSQL.put("varbinary", "varbinary");
        MSSQL2MYSQL.put("varchar", "varchar");
        MSSQL2MYSQL.put("xml", "text");
        MSSQL2MYSQL.put("image", "blob");

    }

    private static final List<String> IGNORE_LENGTH_DATA_TYPES = new ArrayList<String>();
    static {
        IGNORE_LENGTH_DATA_TYPES.add("int");
        IGNORE_LENGTH_DATA_TYPES.add("smallint");
    }

    public SQLServer2MySQLConverter(Database source, Database target) {
        sDataSource = new DruidDataSource();
        sDataSource.setDriverClassName(this.getSourceDriverClassName());
        sDataSource.setUrl(String.format(this.getSourceUrl(), source.getHostname(), source.getPort(), source.getDbName()));
        sDataSource.setUsername(source.getUsername());
        sDataSource.setPassword(source.getPassword());

        this.sDatabase = source;
        this.tDatabase = target;

        tDataSource = new DruidDataSource();
        tDataSource.setDriverClassName(this.getTargetDriverClassName());
        tDataSource.setUrl(String.format(this.getTargetUrl(), target.getHostname(), target.getPort(), target.getDbName(), target.getEncoding()));
        tDataSource.setUsername(target.getUsername());
        tDataSource.setPassword(target.getPassword());

        sJdbcTemplate = new JdbcTemplate();
        sJdbcTemplate.setDataSource(sDataSource);

        tJdbcTemplate = new JdbcTemplate();
        tJdbcTemplate.setDataSource(tDataSource);

    }

    @Override
    public String getSourceDatabaseProductName() {
        return "Microsoft SQL Server";
    }

    @Override
    public String getTargetDatabaseProductName() {
        return "MySQL";
    }

    @Override
    public String getSourceDriverClassName() {
        return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    }

    @Override
    public String getTargetDriverClassName() {
        return "com.mysql.cj.jdbc.Driver";
    }

    @Override
    public String getSourceUrl() {
        return "jdbc:sqlserver://%s:%s;databaseName=%s";
    }

    @Override
    public String getTargetUrl() {
        return "jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=%s&rewriteBatchedStatements=true&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true&useSSL=true&serverTimezone=GMT%%2B8";
    }

    @Override
    public List<String> getAllTableNames() throws SQLException {
        List<String> allTableNames = new ArrayList<String>();
        // 获取所有的表
        ResultSet tableResultSet = sConnection.getMetaData()
                .getTables(sConnection.getCatalog(),null, null, new String[]{"TABLE"});
        while (tableResultSet.next()){
            allTableNames.add(tableResultSet.getString("TABLE_NAME"));
        }
        return allTableNames;
    }

    @Override
    public List<String> convertMetadata(String[] tables, String[] ignoreTables, boolean includeIndex, String exists) throws SQLException {

        if(sConnection == null){
            sConnection = this.sDataSource.getConnection();
        }

        final String sCatalog = sConnection.getCatalog();
        final DatabaseMetaData sDBMetaData = sConnection.getMetaData();

        // 数据库所有表
        List<String> allTableNames = this.getAllTableNames();
        // 当前需要操作的表
        List<String> tableNames = new ArrayList<String>();

        HashMap<String, String> createTableDDL = new HashMap<String, String>();

        if(tables == null){
            // 通过查询数据库获取所有表
            logger.info("即将导出源数据库[{}]全部表...", sCatalog);
            tableNames = allTableNames;
        } else {
            for(String tableName : tables){
                if(tableName.trim().length() == 0) continue;
                tableNames.add(tableName);
            }
            logger.info("即将导出源数据库[{}]表：{}。", sCatalog, tableNames.toString());
        }

        String columnName, columnType, remark, autoIncrement, nullable;
        int digits, dataType;
        long columnSize;

        final String tableTemplate = "CREATE TABLE IF NOT EXISTS `%s`\n(\n%s\n) ENGINE=%s DEFAULT CHARSET=%s";

        // 成功创建的表
        List<String> successTables = new ArrayList<String>();
        List<String> failTables = new ArrayList<String>();
        StringBuilder sqlBuilder = new StringBuilder();
        StringBuilder primaryKeys = new StringBuilder();

        //
        int convertTableCount = 0;
        // 所有表
        for(String tableName : tableNames){

            if(ignoreTables != null && ignoreTables.length > 0){
                // 判断需要忽略的表
                boolean isIgnore = false;
                for (String ignoreTable : ignoreTables) {
                    if (tableName.equalsIgnoreCase(ignoreTable)) {
                        isIgnore = true;
                        break;
                    }
                }
                if(isIgnore) continue;
            }

            if(!allTableNames.contains(tableName)){
                logger.error("源数据库[{}]不存在表[{}]!!!", sCatalog, tableName);
                continue;
            }

            convertTableCount++;

            // 获取表的所有列
            ResultSet tableColumnResultSet = sDBMetaData.getColumns(sCatalog, "%", tableName, "%");

            // 所有varchar的列加起来不能大于65532(字符集为latin1)
            // 每一列需要2个字节记录该列的长度
            // 因此，实际上一个行的所有varchar列加起来不能大于 65532(字符集为latin1) - n * 2，n代表varchar列数

            // 实验环境MariaDB 10.4.11的数据：
            //      字符集   行长度(所有varchar列)
            //      latin1  65532(1个字节一个字符)
            //      gbk     65532/2 => 32766(2个字节一个中文，最大只能存在32766个中文)
            //      utf8    65532/3 => 21844(3个字节一个中文，最大只能存在21844个中文)
            //      utf8mb4 65532/4 => 16383(4个字节一个中文，最大只能存在16383个中文)
            // 注：SQLServer不存在该方式控制。

            // 占用字节说明：
            // not null = 1
            // null = 2

            //      字符集   行长度(所有char列), innodb_page_size = 16k  -- > 8126
            // https://dev.mysql.com/doc/refman/8.0/en/column-count-limit.html

            // 源数据库(SQLServer)，所有的char列加起来的长度不能大于8050（SQLServer不用考虑not null和null的问题），否则
            // 提示如下错误：
            // CREATE TABLE t2  (
            //   c1 CHAR(255),c2 CHAR(255),c3 CHAR(255),
            //   c4 CHAR(255),c5 CHAR(255),c6 CHAR(255),
            //   c7 CHAR(255),c8 CHAR(255),c9 CHAR(255),
            //   c10 CHAR(255),c11 CHAR(255),c12 CHAR(255),
            //   c13 CHAR(255),c14 CHAR(255),c15 CHAR(255),
            //   c16 CHAR(255),c17 CHAR(255),c18 CHAR(255),
            //   c19 CHAR(255),c20 CHAR(255),c21 CHAR(255),
            //   c22 CHAR(255),c23 CHAR(255),c24 CHAR(255),
            //   c25 CHAR(255),c26 CHAR(255),c27 CHAR(255),
            //   c28 CHAR(255),c29 CHAR(255),c30 CHAR(255),
            //   c31 CHAR(255),c32 CHAR(255)
            //   )
            // 创建或更改表 't2' 失败，因为最小行大小是 8170，包括 10 字节的内部开销。而此值超出了允许的 8060 字节的最大表行大小。
            // 255 * 32 + 10 = 8170,
            // 8060 - 255 * 31 - 10 = 145, 因此c32最大的长度只能为145。
            // 这个计算方式和MySQL有所区别。


            // 创建表
            while (tableColumnResultSet.next()){
                // TABLE_CAT
                // TABLE_SCHEM
                // TABLE_NAME
                // COLUMN_NAME
                // DATA_TYPE
                // TYPE_NAME
                // COLUMN_SIZE          // 列大小
                // BUFFER_LENGTH        // 未使用
                // DECIMAL_DIGITS       // 小数部分的位数
                // NUM_PREC_RADIX       // 基数（通常为 10 或 2）
                // NULLABLE
                // REMARKS              // 描述列的注释
                // COLUMN_DEF
                // SQL_DATA_TYPE
                // SQL_DATETIME_SUB
                // CHAR_OCTET_LENGTH
                // ORDINAL_POSITION
                // IS_NULLABLE          YES/NO
                // SCOPE_CATALOG
                // SCOPE_SCHEMA
                // SOURCE_DATA_TYPE
                // IS_AUTOINCREMENT     YES/NO
                // IS_GENERATEDCOLUMN
                // SS_IS_SPARSE
                // SS_IS_COLUMN_SET
                // SS_UDT_CATALOG_NAME
                // SS_UDT_SCHEMA_NAME
                // SS_UDT_ASSEMBLY_TYPE_NAME
                // SS_XML_SCHEMACOLLECTION_CATALOG_NAME
                // SS_XML_SCHEMACOLLECTION_SCHEMA_NAME
                // SS_XML_SCHEMACOLLECTION_NAME

                columnName = tableColumnResultSet.getString("COLUMN_NAME");
                columnType = tableColumnResultSet.getString("TYPE_NAME");
                columnSize = tableColumnResultSet.getLong("COLUMN_SIZE");
                digits = tableColumnResultSet.getInt("DECIMAL_DIGITS"); // 精度
                dataType = tableColumnResultSet.getInt("DATA_TYPE");  // java.sql.Types
                remark = tableColumnResultSet.getString("REMARKS");
                autoIncrement = tableColumnResultSet.getString("IS_AUTOINCREMENT");
                nullable = tableColumnResultSet.getString("IS_NULLABLE");

                sqlBuilder.append("  `").append(columnName).append("` ");
//                Map<Integer, String> types = new HashMap<Integer, String>();
//                types.put(Types.ARRAY, "");
//                types.put(Types.BIGINT, "BIGINT");
//                types.put(Types.BINARY, "BINARY");
//                types.put(Types.BIT, "TINYINT");
//                types.put(Types.BLOB, "BLOB");
//                types.put(Types.BOOLEAN, "TINYINT(1)");
//                types.put(Types.CHAR, "CHAR(1)");
//                types.put(Types.CLOB, "TEXT");
//                types.put(Types.DATALINK, "");
//                types.put(Types.DATE, "DATE");
//                types.put(Types.DECIMAL, "DECIMAL");
//                types.put(Types.DISTINCT, "");
//                types.put(Types.DOUBLE, "DOUBLE");
//                types.put(Types.FLOAT, "FLOAT");
//                types.put(Types.INTEGER, "INT");
//                types.put(Types.JAVA_OBJECT, "");
//                types.put(Types.LONGNVARCHAR, "TEXT");
//                types.put(Types.LONGVARCHAR, "TEXT");
//                types.put(Types.NCHAR, "CHAR");
//                types.put(Types.NCLOB, "CLOB");
//                types.put(Types.NUMERIC, "DECIMAL");
//                types.put(Types.NVARCHAR, "VARCHAR");
//                types.put(Types.REAL, "FLOAT");
//                types.put(Types.SMALLINT, "SMALLINT");
//                types.put(Types.SQLXML, "");
//                types.put(Types.STRUCT, "");
//                types.put(Types.TIME, "TIME");
//                types.put(Types.TIMESTAMP, "TIMESTAMP");
//                types.put(Types.TINYINT, "TINYINT");
//                types.put(Types.VARBINARY, "VARBINARY");
//                types.put(Types.VARCHAR, "VARCHAR");
//
//                sqlBuilder.append(types.get(dataType));
                if(columnType.contains(" ")){
                    columnType = columnType.substring(0, columnType.indexOf(" "));
                }

                // SQLServer返回的列类型有可能是"numeric()", 这是一个BUG? UNKNOWN.
                if(columnType.contains("()")){
                    columnType = columnType.replace("()", "");
                }

                String mysqlType = MSSQL2MYSQL.get(columnType.toLowerCase()).toUpperCase();
                boolean skipSize = false;

                if(columnSize > 256 && columnSize <= 65535){
                    // 不能使用TINYTEXT
                    // 类型转换
                    if(mysqlType.equalsIgnoreCase("TINYTEXT")){
                        mysqlType = "TEXT";
                        skipSize = true;
                    } else if(mysqlType.equalsIgnoreCase("VARCHAR") && columnSize == 8000){
                        // varchar(max)最大为8000
                        mysqlType = "TEXT";
                        skipSize = true;
                    }
                } else if(columnSize > 65535 && columnSize <= 16777215){
                    // 不能使用TEXT
                    if(mysqlType.equalsIgnoreCase("TINYTEXT")
                            || mysqlType.equalsIgnoreCase("TEXT")
                            || mysqlType.equalsIgnoreCase("VARCHAR")
                            || mysqlType.equalsIgnoreCase("CHAR")){
                        mysqlType = "MEDIUMTEXT";
                        skipSize = true;
                    } else if(mysqlType.equalsIgnoreCase("BLOB")){
                        mysqlType = "MEDIUMBLOB";
                        skipSize = true;
                    }
                } else if(columnSize > 16777215){
                    // 不能使用MEDIUMTEXT
                    if(mysqlType.equalsIgnoreCase("TINYTEXT")
                            || mysqlType.equalsIgnoreCase("TEXT")
                            || mysqlType.equalsIgnoreCase("MEDIUMTEXT")
                            || mysqlType.equalsIgnoreCase("VARCHAR")
                            || mysqlType.equalsIgnoreCase("CHAR")){
                        mysqlType = "LONGTEXT";
                        skipSize = true;
                    } else if(mysqlType.equalsIgnoreCase("BLOB")
                            || mysqlType.equalsIgnoreCase("MEDIUMBLOB")){
                        mysqlType = "LONGBLOB";
                        skipSize = true;
                    }
                }

                // 自动增长的主键应该使用整型
                // https://blog.csdn.net/finish_dream/article/details/61193106
                if("YES".equalsIgnoreCase(autoIncrement)){
                    // 自动增长。
                    // 有符号 - 无符号
                    if((-128 <= columnSize && columnSize <= 127) || (columnSize >= 0 && columnSize <= 255)){
                        mysqlType = "tinyint";
                    } else if((-32768 <= columnSize && columnSize <= 32767) || (columnSize >= 0 && columnSize <= 65535)){
                        mysqlType = "smallint";
                    } else if((-8388608 <= columnSize && columnSize <= 8388607) || (columnSize >= 0 && columnSize <= 16777215)){
                        mysqlType = "mediumint";
                    } else if((-2147483648L <= columnSize && columnSize <= 2147483647) || (columnSize >= 0 && columnSize <= 4294967295L)){
                        mysqlType = "int";
                    } else {
                        mysqlType = "bigint";
                    }
                }

                sqlBuilder.append(mysqlType);

                if(!skipSize && IGNORE_LENGTH_DATA_TYPES.contains(mysqlType)){
                    skipSize = true;
                }

                if(!(columnType.contains("(") && columnType.contains(")")) && !skipSize){

                    if(digits > 0 && dataType != Types.TIMESTAMP){
                        // 存在精度问题
                        sqlBuilder.append("(").append(columnSize).append(", ").append(digits).append(")");
                    } else {

                        // 处理长度问题
                        switch (dataType){
                            case Types.DECIMAL:
                            case Types.DOUBLE:
                            case Types.FLOAT:
                            case Types.INTEGER:
                            case Types.NCHAR:
                            case Types.NUMERIC:
                            case Types.NVARCHAR:
                            case Types.REAL:
                            case Types.SMALLINT:
                            case Types.TINYINT:
                            case Types.VARCHAR:
                                sqlBuilder.append("(").append(columnSize).append(")");
                                break;
                        }
                    }
                }

                // 是否为空
                if("YES".equals(nullable)){
                    nullable = " DEFAULT NULL";
                } else {
                    nullable = " NOT NULL";
                }
                sqlBuilder.append(nullable);

                // 自动增长
                if("YES".equals(autoIncrement)){
                    autoIncrement = " AUTO_INCREMENT";
                    primaryKeys.append(columnName).append(",");
                } else {
                    autoIncrement = "";
                }
                sqlBuilder.append(autoIncrement);

                // 注释
                if(remark != null && remark.length() > 0){
                    remark = String.format(" COMMENT '%s'", remark);
                } else {
                    remark = "";
                }
                sqlBuilder.append(remark).append(",\n");

            }

            // 主键
            ResultSet primaryKeyResultSet = sDBMetaData.getPrimaryKeys(sCatalog, null, tableName);
            while(primaryKeyResultSet.next()){
                String primaryKeyColumnName = primaryKeyResultSet.getString("COLUMN_NAME");
                if(primaryKeys.indexOf(primaryKeyColumnName) == -1){
                    primaryKeys.append(primaryKeyColumnName).append(",");
                }
            }
            if(primaryKeys.length() > 0){
                primaryKeys.delete(primaryKeys.length() - 1, primaryKeys.length());
                sqlBuilder.append("  ").append("PRIMARY KEY (").append(primaryKeys).append(")");
            } else {
                // 没有主键
                sqlBuilder.delete(sqlBuilder.length() - 2, sqlBuilder.length());
            }

            // 获取该表的所有索引
            if(includeIndex){
                ResultSet indexResultSet = sDBMetaData.getIndexInfo(sCatalog, null, tableName, false, false);
            }

            try{
                // 如果需要的话，删除表
                if("replace".equals(exists)){
                    final String sql = "DROP TABLE IF EXISTS " + tableName;
                    logger.info("即将执行删除表语句: \n{}; ", sql);
                    tJdbcTemplate.execute(sql);
                    logger.info("表[{}]已删除。", tableName);
                }
            }catch (Exception e){
                // 表创建失败
                logger.error("表[{}]删除失败!", tableName);
                logger.error("原因:", e);
            }

            try{
                final String sql = String.format(tableTemplate, tableName, sqlBuilder.toString(),
                        this.tDatabase.getEngine(), this.tDatabase.getCharset());
                createTableDDL.put(tableName, sql);
                logger.info("即将执行创建表语句: \n{}; ", sql);
                tJdbcTemplate.execute(sql);
                successTables.add(tableName);
                logger.info("表[{}]已创建。", tableName);
            } catch (Exception e){
                // 表创建失败
                failTables.add(tableName);
                logger.error("表[{}]创建失败。", tableName);
                logger.error("原因：", e);
            } finally {
                sqlBuilder.delete(0, sqlBuilder.length());
                primaryKeys.delete(0, primaryKeys.length());
            }
        }

        logger.info("|============> 转换成功率: {}% <============|", Math.round((float)successTables.size() / (float)convertTableCount * 100.0));
        if(failTables.size() > 0){
            logger.error("转换失败的表：");
            logger.error(failTables.toString());
        }

        return successTables;

    }

    @Override
    public void convertData(String[] tables, String[] ignoreTables, String execute, boolean ignoreNonEmpty) throws SQLException {

        if(sConnection == null){
            sConnection = this.sDataSource.getConnection();
        }

        Date startTime = new Date();
        final String sCatalog = sConnection.getCatalog();
        final String tCatalog = this.tDataSource.getConnection().getCatalog();
        final DatabaseMetaData sDBMetaData = sConnection.getMetaData();
        // 数据库所有表
        List<String> allTableNames = this.getAllTableNames();
        // 当前需要操作的表
        List<String> tableNames = new ArrayList<String>();

        if(tables == null){
            // 通过查询数据库获取所有表
            //
            tableNames = allTableNames;
        } else {
            for(String tableName : tables){
                if(tableName.trim().length() == 0) continue;
                tableNames.add(tableName);
            }
            logger.info("即将导出源数据库[{}]表：{}。", sCatalog, tableNames.toString());
        }

        // 转换计数
        HashMap<String, Long> convertCounter = new HashMap<String, Long>();

        // 非空的表
        List<String> nonEmptyTables = new ArrayList<String>();

        // 行数不一样的表
        List<String> invalidTables = new ArrayList<String>();

        // 导入失败的表
        List<String> failTables = new ArrayList<String>();

        /// 数据转换开始
        // 所有表
        for(String tableName : tableNames) {

            if (ignoreTables != null && ignoreTables.length > 0) {
                // 判断需要忽略的表
                boolean isIgnore = false;
                for (String ignoreTable : ignoreTables) {
                    if (tableName.equalsIgnoreCase(ignoreTable)) {
                        isIgnore = true;
                        break;
                    }
                }
                if (isIgnore) continue;
            }

            if (!allTableNames.contains(tableName)) {
                logger.error("源数据库[{}]不存在表[{}]!!!", sCatalog, tableName);
                continue;
            }

            // 查询目标数据库表的数据
            // 查看该表是否为空表，如果不是空表的话，忽略。
            final long targetRowCount = this.getTableRowCount(this.tJdbcTemplate, tableName);
            if(ignoreNonEmpty && targetRowCount > 0){
                //
                nonEmptyTables.add(tableName);
                logger.error("!!目标数据库[{}]表{}为非空表，存在{}行数据, 已忽略导入。", tCatalog, tableName, targetRowCount);
                continue;
            }

            logger.info("表[{}] 正在进行...", tableName);

            // 获取源数据表的数据条数
            long rowCount = this.getTableRowCount(this.sJdbcTemplate, tableName);
            convertCounter.put(tableName, rowCount);

            // 如果为0 的话，就不用下一步了
            if(rowCount == 0) continue;

            // 每一行的数据
            final List<String> columnNames = new ArrayList<String>();
            List<Object[]> rows = this.sJdbcTemplate.query("select * from " + tableName, new RowMapper<Object[]>() {
                @Override
                public Object[] mapRow(ResultSet resultSet, int rowNum) throws SQLException {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    if(rowNum == 0){
                        // 第一行的时候，获取该表的所有列名
                        for(int i = 0; i < columnCount; i++){
                            columnNames.add(metaData.getColumnName(i + 1));
                        }
                    }

                    Object[] row = new Object[columnCount];
                    for(int j = 0; j < columnCount; j++){
                        row[j] = resultSet.getObject(j + 1);
                    }

                    return row;
                }
            });

            // 每一行的数据
            final String sqlTemplate = String.format("insert into %s ( %s ) values ( %s )", tableName,
                    columnNames.toString().replace("[", "").replace("]", ""),
                    this.getSQLPlaceHolder(columnNames));

            final Date beginTime = new Date();

            try{
                tJdbcTemplate.batchUpdate(sqlTemplate, rows);
            } catch (Exception e){
                logger.error("导入失败: {}", e.getMessage());
                failTables.add(tableName);
                continue;
            }

            final long interval = new Date().getTime() - beginTime.getTime();
            SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss:SSS");
            formatter.setTimeZone(TimeZone.getTimeZone("GMT+00:00"));
            logger.info("已导入表[{}]数据, 耗时{}", tableName, formatter.format(interval));
            logger.info("即将校验表[{}]数据...", tableName);

            // 再次查询数据库表
            // 查询目标数据库表的数据
            final long targetRowCount2 = this.getTableRowCount(this.tJdbcTemplate, tableName);
            if(rowCount != targetRowCount2){
                //
                invalidTables.add(tableName);
                logger.error("数据校验不通过，源数据库[{}]表[{}]: 行数 {} 条, 目标数据库[{}]表[{}]: 行数 {} 条", sCatalog, tableName, rowCount, tCatalog, tableName, targetRowCount2);
            } else {
                logger.info("数据校验成功，行数一致。");
            }

            logger.info("表[{}]导入记录数: {}条", tableName, targetRowCount2 - targetRowCount);

        }

        logger.info("总结：");
        final long interval = new Date().getTime() - startTime.getTime();
        SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss:SSS");
        formatter.setTimeZone(TimeZone.getTimeZone("GMT+00:00"));
        logger.info("总耗时: {}", formatter.format(interval));
        if(nonEmptyTables.size() > 0){
            logger.info("非空表的表：{}", nonEmptyTables);
        }

        if(invalidTables.size() > 0){
            logger.info("行数校验失败的表：{}", invalidTables);
        }

        if(failTables.size() > 0){
            logger.info("导入失败的表：{}", failTables);
        }

    }

    /**
     * 获取占位符
     * @param columns
     * @return
     */
    private String getSQLPlaceHolder(List<String> columns){
        StringBuilder sb = new StringBuilder();
        for(int i = 0, len = columns.size(); i < len; i++){
            sb.append("?");
            if(i != len - 1){
                sb.append(", ");
            }
        }
        return sb.toString();
    }


    /**
     * 获取行数
     * @param jdbcTemplate
     * @param tableName
     * @return
     */
    private long getTableRowCount(JdbcTemplate jdbcTemplate, String tableName){

        List<Long> rowCount = jdbcTemplate.query(String.format("select COUNT(0) from %s", tableName), new RowMapper<Long>(){

            @Override
            public Long mapRow(ResultSet resultSet, int i) throws SQLException {
                return resultSet.getLong(1);
            }
        });

        return rowCount.get(0);
    }



}
