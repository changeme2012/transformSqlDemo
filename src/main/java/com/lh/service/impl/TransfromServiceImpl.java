package com.lh.service.impl;

import com.lh.service.TransformService;
import com.lh.util.ColumnTypeUtil;
import com.lh.util.DataBaseUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class TransfromServiceImpl implements TransformService {
    private static final List<String>  TABLE_NAMES = new ArrayList<>();

    private static final String QUERY_TABLENAMES_SQL = "SELECT  DISTINCT(TABLE_NAME)\n" +
            " FROM information_schema.COLUMNS\n" +
            " WHERE TABLE_SCHEMA= 'gmall';";

    private static String SCHEMA_QUERY = "SELECT table_name,column_name,data_type,column_comment \n" +
            "FROM information_schema.COLUMNS \n" +
            "WHERE TABLE_SCHEMA = ? and table_name = ?";

    private static final String TABLE_NAME = "TABLE_NAME";


    private static final String COLUMN_NAME = "COLUMN_NAME";

    private static final String COLUMN_TYPE = "DATA_TYPE";

    private static final String COLUMN_COMMENT = "COLUMN_COMMENT";


    private static Connection connection;
    private static PreparedStatement preparedStatement;
    private static ResultSet resultSet;

    private ArrayList<String> tableFilterList = new ArrayList<>();

    static {
        try {
            connection = DataBaseUtil.getConnection();
            preparedStatement = connection.prepareStatement(QUERY_TABLENAMES_SQL);
            preparedStatement.execute();
            resultSet = preparedStatement.getResultSet();
            while (resultSet.next()){
                TABLE_NAMES.add(resultSet.getString("TABLE_NAME"));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> transform(String schema, List<String> tableList) {
        List<String> resultSqls = null;
        try {
            //返回sql结果集
            resultSqls = new ArrayList<>();
            List<String> sqlList = new ArrayList<>();
            StringBuilder sqlString = new StringBuilder();

            //
            if (tableList == null || tableList.size() == 0){
                tableList = new ArrayList<>(TABLE_NAMES);
            }
            for (String transfromTable : tableList) {
                resultSet = readSchema(schema, transfromTable);
//                String tableNameFlag = null;

                while (resultSet.next()) {
                    String tableName = resultSet.getString(TABLE_NAME);

                    String oldType = resultSet.getString(COLUMN_TYPE);
                    String newType = transfromDataType(oldType);

                    String columnName = resultSet.getString(COLUMN_NAME);
                    String columnComment = resultSet.getString(COLUMN_COMMENT);

                    String sql = "`" + columnName + "` " + newType + " COMMENT '" + columnComment + "',\n";
                    sqlList.add(sql);

                    //需要转换，则进入执行体
//                if ((tableFilterList != null && tableFilterList.size() != 0 && tableFilterList.contains(tableName)) || tableFilterList == null) {

                    // 用表标记位判断当前表是否结束，结束就导出当前表的sql
                    if (resultSet.isLast()) {

                        sqlList.forEach(sqlOne -> sqlString.append(sqlOne));
                        String newSql = generateSql(tableName, sqlString.substring(0, sqlString.lastIndexOf(",")));

                        resultSqls.add(newSql);
                        //清空list信息，避免重复数据

                        System.out.println(newSql);
                        System.out.println();
                        sqlList.clear();
                        sqlString.setLength(0);
                    }
//                    tableNameFlag = tableName;

                }
            }

//            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
        return resultSqls;
    }

    //执行SQL返回结果集
    @Override
    public ResultSet readSchema(String schema, String tableName) throws SQLException {
        preparedStatement = connection.prepareStatement(SCHEMA_QUERY);
        preparedStatement.setString(1, schema);
        preparedStatement.setString(2, tableName);
        preparedStatement.execute();
        return preparedStatement.getResultSet();
    }


    //类型转换
    @Override
    public String transfromDataType(String columnType) {
        return ColumnTypeUtil.getColumeType(columnType);
    }


    //sql拼接
    @Override
    public String generateSql(String tableName, String sql) throws IOException {
        Properties properties = new Properties();
        Path path = Paths.get("properties/sql.properties");
        properties.load(Files.newBufferedReader(path));
        String prefixes = properties.getProperty("Prefixes");
        String suffixes = properties.getProperty("suffixes");
        String partitionword = properties.getProperty("partitionword");
        String delimited = properties.getProperty("delimited");
        Object nullformat = properties.get("nullformat");
        String location = properties.getProperty("location");

        String sqlHeader = "DROP TABLE IF EXISTS " + prefixes + tableName + suffixes + ";\n" +
                "CREATE EXTERNAL TABLE " + prefixes + tableName + suffixes + "(\n";
        String sqlTableName = prefixes + tableName + suffixes;

        String sqlTail = "\n)" +
                "PARTITIONED BY ('" + partitionword + "' STRING)\n" +
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY '" + delimited + "'\n" +
                "NULL DEFINED AS '" + nullformat + "'\n" +
                "LOCATION '" + location + sqlTableName + "/';";
        return sqlHeader + sql + sqlTail;

    }

    @Override
    public void getFilterTable() throws IOException {
        Properties properties = new Properties();
        properties.load(TransfromServiceImpl.class.getClassLoader().getResourceAsStream("properties/sql.properties"));
        String tablefilter = properties.getProperty("tablefilter");

        //获取过滤表信息 放入集合
        if (tablefilter != null && !tablefilter.equals("")) {
            String[] filter = tablefilter.split(",");
            for (String table : filter) {
                tableFilterList.add(table);
            }
        }
    }
}
