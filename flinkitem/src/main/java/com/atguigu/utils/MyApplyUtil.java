package com.atguigu.utils;

import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.lang.reflect.Field;

/**
 * ClassName: MyApplyUtil
 * Package: com.atguigu.utils
 * Description:
 *
 * @Author fajun-mei
 * @Create 2023/5/8 8:48
 * @Version 1.2
 */

public class MyApplyUtil {




    public static class MyApply<T> implements AllWindowFunction<T, T, TimeWindow> {
        @Override
        public void apply(TimeWindow window, Iterable<T> values, Collector<T> out) throws Exception {
            T next = values.iterator().next();

            Field ts = next.getClass().getDeclaredField("ts");
            ts.setAccessible(true);
            ts.set(next, System.currentTimeMillis());

            Field stt = next.getClass().getDeclaredField("stt");
            stt.setAccessible(true);
            stt.set(next, DateFormatUtil.toYmdHms(window.getStart()));

            Field edt = next.getClass().getDeclaredField("edt");
            edt.setAccessible(true);
            edt.set(next, DateFormatUtil.toYmdHms(window.getEnd()));

            out.collect(next);
        }
    }


    public static class  MyWindowUtil<T,KEY> implements WindowFunction<T,T,KEY,TimeWindow> {

        @Override
        public void apply(KEY key, TimeWindow window, Iterable<T> input, Collector<T> out) throws Exception {
            T next = input.iterator().next();

            Field ts = next.getClass().getDeclaredField("ts");
            ts.setAccessible(true);
            ts.set(next, System.currentTimeMillis());

            Field stt = next.getClass().getDeclaredField("stt");
            stt.setAccessible(true);
            stt.set(next, DateFormatUtil.toYmdHms(window.getStart()));

            Field edt = next.getClass().getDeclaredField("edt");
            edt.setAccessible(true);
            edt.set(next, DateFormatUtil.toYmdHms(window.getEnd()));


            out.collect(next);

        }
    }


}