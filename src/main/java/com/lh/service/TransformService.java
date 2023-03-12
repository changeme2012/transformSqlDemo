package com.lh.service;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public interface TransformService {
    // sql转换
    List<String> transform(String schema,List<String> tableList);

    // 读元数据表信息
    ResultSet readSchema(String schema,String tableName) throws SQLException;

    // 数据类型转换
    String transfromDataType(String columnType);

    // 封装新sql
    String generateSql(String tableName,String sql) throws IOException;

    //读取过滤表
     void getFilterTable()throws IOException;
}
