package com.lh.util;

import java.util.Locale;

public class ColumnTypeUtil {
    public static final String BIGINT="BIGINT";
    public static final String STRING="STRING";
    public static final String DOUBLE="DOUBLE";

    public static String getColumeType(String columntype){

        String type = columntype.toLowerCase(Locale.ROOT);

        //columntype类型转换
        switch (type){
            case "decimal":
            case  "double":
            case   "float":
                type=DOUBLE;
                break;
            case "bigint":
            case "int":
            case "smallint":
            case "tinyint":
                type=BIGINT;
                break;
            default:
                type=STRING;
                break;
        }

        return type;
    }
}
