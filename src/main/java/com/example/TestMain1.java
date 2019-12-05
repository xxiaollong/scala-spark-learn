package com.example;


import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 */
public class TestMain1 {

    public static void main(String[] args) {

        Long times = 1503158415993L;    // 日志中的时间戳
        Date date = new Date(times);
        String dateStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);  // 转换后的时间
        System.out.println(dateStr);


//        String str = "aa";
//        boolean aa = str.contains("aa");
//
//        System.out.println(aa +" "+ str);
    }

}


