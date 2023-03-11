package com.lh;

import com.lh.service.impl.TransfromServiceImpl;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

public class TransformMain {
    public static void main(String[] args) {
        TransfromServiceImpl transfromService = new TransfromServiceImpl();
        Properties properties = new Properties();
        FileOutputStream outputStream = null;
        try {
            properties.load(TransformMain.class.getClassLoader().getResourceAsStream("properties/sql.properties"));
            List<String> transforms = transfromService.transform(properties.getProperty("schema"));

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
