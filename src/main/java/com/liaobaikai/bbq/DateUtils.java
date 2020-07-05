package com.liaobaikai.bbq;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public final class DateUtils {

    /**
     * 通过毫秒值格式化正常显示的时间
     * @param interval 毫秒值
     * @return
     */
    public static String formatTime(long interval){
        SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss:SSS");
        formatter.setTimeZone(TimeZone.getTimeZone("GMT+00:00"));
        return formatter.format(interval);
    }

    /**
     * 通过毫秒值格式化正常显示的时间
     * @param begin 开始时间
     * @param end 结束时间
     * @return
     */
    public static String formatTime(Date begin, Date end){
        long interval = end.getTime() - begin.getTime();
        return formatTime(interval);
    }

}
