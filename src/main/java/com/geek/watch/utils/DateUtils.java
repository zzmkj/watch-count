package com.geek.watch.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Classname: DateUtils
 * @Author: Ming
 * @Date: 2020/1/27 8:58 下午
 * @Version: 1.0
 * @Description: 日期工具类
 **/
public class DateUtils {

    private static SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd");
    private static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");

    /**
     * 转换日期格式
     * 从yyyyMMdd转换成yyyy-MM-dd
     * @param dateTime
     * @return
     */
    public static String transDateFormat(String dateTime) {
        String res = "1970-01-01";
        try {
            Date date = sdf1.parse(dateTime);
            res = sdf2.format(date);
        } catch (Exception e) {
            System.out.println("日期转换失败：" + dateTime);
        }
        return res;
    }

}
