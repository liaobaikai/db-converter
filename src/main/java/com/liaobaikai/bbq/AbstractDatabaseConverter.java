package com.liaobaikai.bbq;

import com.alibaba.druid.pool.DruidDataSource;
import com.liaobaikai.bbq.db.DataBase;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public abstract class AbstractDatabaseConverter implements DatabaseConverter {

    protected DruidDataSource sDataSource;
    protected DruidDataSource tDataSource;
    protected JdbcTemplate sJdbcTemplate;
    protected JdbcTemplate tJdbcTemplate;

    protected Connection sConnection;
    protected Connection tConnection;

    protected DataBase sDataBase;
    protected DataBase tDataBase;

    public AbstractDatabaseConverter(DataBase source, DataBase target){
        sDataSource = new DruidDataSource();
        sDataSource.setDriverClassName(this.getSourceDriverClassName());
        sDataSource.setUrl(String.format(this.getSourceUrl(), source.getHostname(),
                source.getPort(), source.getDbName()));
        sDataSource.setUsername(source.getUsername());
        sDataSource.setPassword(source.getPassword());

        this.sDataBase = source;
        this.tDataBase = target;

        tDataSource = new DruidDataSource();
        tDataSource.setDriverClassName(this.getTargetDriverClassName());
        tDataSource.setUrl(String.format(this.getTargetUrl(), target.getHostname(),
                target.getPort(), target.getDbName(), target.getEncoding()));
        tDataSource.setUsername(target.getUsername());
        tDataSource.setPassword(target.getPassword());

        sJdbcTemplate = new JdbcTemplate();
        sJdbcTemplate.setDataSource(sDataSource);

        tJdbcTemplate = new JdbcTemplate();
        tJdbcTemplate.setDataSource(tDataSource);
    }

    @Override
    public List<String> convertMetadata(String[] tables, String[] ignoreTables, boolean includeIndex, String exists) throws SQLException {

        return null;
    }

    @Override
    public void convertData(String[] tables, String[] ignoreTables, String execute, boolean ignoreNonEmpty) throws SQLException {

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


}
