package com.aikfk.flink.datastream.event;

import com.aikfk.flink.base.Tools;
import org.apache.flink.streaming.api.windowing.triggers.*;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author ：caizhengjie
 * @description：TODO
 * @date ：2021/3/16 9:12 下午
 */

public class MyEventTimeTrigger extends Trigger<Object, TimeWindow> {
    private static final long serialVersionUID = 1L;

    private MyEventTimeTrigger() {
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
//            System.out.println("============================"+Thread.currentThread().getId()+"================================");
//            System.out.println("onElement >>>>进入........... 初始水印为："+Tools.getMsToDate(ctx.getCurrentWatermark()) );

        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            System.out.println("onElement >>>>触发 | " +
                    element.toString() + "\n"+
                    "\n | 窗口开始 = "+Tools.getMsToDate(window.getStart()) +
                    "\n | 窗口结束 = "+Tools.getMsToDate(window.getEnd())) ;
//                        "\n | 窗口结束-1 = "+Tools.getMsToDate(window.maxTimestamp()) );
            System.out.println("============================================================");
            return TriggerResult.FIRE;
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp());
            System.out.println("" +
                    "onElement >>>>继续>>>进入.... 上一次水印为："+Tools.getMsToDate(ctx.getCurrentWatermark())+" | 事件数据 【" +
                    Tools.getMsToDate(timestamp) + "】"+
                    "\n | 加入【 "+Tools.getMsToDate(window.getStart()) +
                    " ——  "+Tools.getMsToDate(window.getEnd())+"】 ");
            System.out.println("\n");
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
        TriggerResult re_bool = null ;
        if (time == window.maxTimestamp() ){

            System.out.println("onEventTime ****** 触发计算 "+
                    "\n | 新水印 = "+Tools.getMsToDate(ctx.getCurrentWatermark()) + ""+
                    "\n | 触发计算 ----->  窗口 【 "+Tools.getMsToDate(window.getStart()) +
                    " ——  "+Tools.getMsToDate(window.getEnd())+"】 ");
//                        "\n | MaxTime = "+Tools.getMsToDate(window.maxTimestamp()) +

            System.out.println("**************************************************");
            re_bool = TriggerResult.FIRE ;
        }else{
            System.out.println("onEventTime >>>>继续 | " +
                    "\n startTime = "+Tools.getMsToDate(window.getStart()) +
                    "\n | endTime = "+Tools.getMsToDate(window.getEnd()) +
                    "\n | MaxTime = "+Tools.getMsToDate(window.maxTimestamp()) +
                    "\n | 水印="+Tools.getMsToDate(ctx.getCurrentWatermark()));
            System.out.println("============================================================");
            re_bool = TriggerResult.CONTINUE ;
        }
        return re_bool;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        System.out.println("onProcessingTime: "+ctx.getCurrentWatermark());
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
        System.out.println("清除........");

    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) {
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
            ctx.registerEventTimeTimer(windowMaxTimestamp);
        }

        System.out.println("merge: "+ctx.getCurrentWatermark());
    }

    @Override
    public String toString() {
        return "EventTimeTrigger()";
    }

    public static MyEventTimeTrigger create() {
        return new MyEventTimeTrigger();
    }

}
