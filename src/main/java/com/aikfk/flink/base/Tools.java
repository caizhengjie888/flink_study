package com.aikfk.flink.base;

import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/16 9:10 下午
 */
public class Tools {

    public  static  String getMsToDate(long times) {

        String date_str = null ;
        try {
            Date  date = new Date(times);
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            date_str = format.format(date);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return date_str ;
    }

}
