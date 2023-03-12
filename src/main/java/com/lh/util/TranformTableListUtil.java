package com.lh.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * ClassName:TranformTableListUtil
 * Package:com.lh.util
 * Description:
 *
 * @Author: 龙成
 * @Create: 2023/3/12-17:35
 */
public class TranformTableListUtil {


    public static List<String> getFilterTable() throws IOException {
        Properties properties = new Properties();
        List<String> tableList = new ArrayList<>();
        Path path = Paths.get("properties/sql.properties");
        properties.load(Files.newBufferedReader(path));
        String tablefilter = properties.getProperty("tablefilter");

        //获取过滤表信息 放入集合
        if (tablefilter != null && !tablefilter.equals("")) {
            String[] filter = tablefilter.split(",");
            for (String table : filter) {
                tableList.add(table);
            }
        }
        return tableList;
    }
}
