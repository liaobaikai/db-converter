package com.liaobaikai.bbq;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.SQLException;
import java.util.List;


public interface DatabaseConverter {

    String getSourceDatabaseProductName();
    String getTargetDatabaseProductName();

    String getSourceDriverClassName();
    String getTargetDriverClassName();

    String getSourceUrl();
    String getTargetUrl();

    /**
     * 转换表
     * @param tables 传入需要处理的表、传 null 默认为全库的表
     * @param ignoreTables 忽略不处理的表
     * @param includeIndex  是否包含索引
     * @param exists 已存在的表怎么操作，replace, ignore
     * @return 成功的表名
     * @throws SQLException
     */
    List<String> convertMetadata(String[] tables, String[] ignoreTables, boolean includeIndex, String exists) throws SQLException;

    /**
     * 导出数据库
     * @param tables 传入需要处理的表、传 null 默认为全库的表
     * @param ignoreTables 忽略不处理的表
     * @param execute 执行的语句
     * @param ignoreNonEmpty 忽略非空表
     * @throws SQLException
     */
    void convertData(String[] tables, String[] ignoreTables, String execute, boolean ignoreNonEmpty) throws SQLException;

    /**
     * 获取所有的表名
     * @return
     */
    List<String> getAllTableNames() throws SQLException;
}
