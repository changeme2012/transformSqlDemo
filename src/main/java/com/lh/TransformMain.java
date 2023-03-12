package com.lh;

import com.lh.service.impl.TransfromServiceImpl;
import com.lh.util.TranformTableListUtil;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

public class TransformMain {
    public static void main(String[] args) {
        TransfromServiceImpl transfromService = new TransfromServiceImpl();
        Properties properties = new Properties();
        FileOutputStream outputStream = null;
        try {
            Path path = Paths.get("properties/sql.properties");
            properties.load(Files.newBufferedReader(path));
//            properties.load(TransformMain.class.getClassLoader().getResourceAsStream("properties/sql.properties"));
            List<String> transforms = transfromService.transform(properties.getProperty("schema"), TranformTableListUtil.getFilterTable());

            // 输出为sql文件
            File file = new File(properties.getProperty("path"));
            for (String transform : transforms) {
                byte[] bytes = transform.getBytes(StandardCharsets.UTF_8);
                outputStream = new FileOutputStream(file, true);
                outputStream.write(bytes);
                outputStream.write("\n\n".getBytes(StandardCharsets.UTF_8));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
