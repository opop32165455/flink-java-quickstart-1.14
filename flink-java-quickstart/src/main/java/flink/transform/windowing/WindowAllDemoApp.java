package flink.transform.windowing;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.map.MapUtil;
import flink.model.FlinkStreamModel;
import flink.sink.GenericAbstractBranchSink;
import flink.source.TestDataGeneratorSource;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author zhangxuecheng
 * @package flink.transform.windowing
 * @className WindowDemoApp
 * @description window demo
 * @date 2023/6/26 9:40
 */
@Slf4j
public class WindowAllDemoApp extends FlinkStreamModel {

    public static void main(String[] args) throws Exception {
        initEnv(args);

        val testSourceDs = env.addSource(new TestDataGeneratorSource(2, 1000));

        testSourceDs.print();

        testSourceDs
                .assignTimestampsAndWatermarks(
                        //watermark允许十秒延迟
                        WatermarkStrategy.<Tuple3<String, Integer, Date>>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner(
                                        //指定f2为事件时间
                                        (SerializableTimestampAssigner<Tuple3<String, Integer, Date>>) (element, recordTimestamp) -> element.f2.getTime()
                                )
                )
                //todo sliding滑动  Tumbling滚动 根据业务情况 使用Event Process time
                //.window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(2)))
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5), Time.seconds(2)))
                .trigger(EventTimeTrigger.create())
                .process(new ProcessAllWindowFunction<Tuple3<String, Integer, Date>, Object, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<Tuple3<String, Integer, Date>, Object, TimeWindow>.Context context, Iterable<Tuple3<String, Integer, Date>> elements, Collector<Object> out) throws Exception {
                        for (Tuple3<String, Integer, Date> element : elements) {
                            out.collect(element);
                        }
                    }
                })
                .addSink(new GenericAbstractBranchSink<Object>(1) {
                    @Override
                    public void flush(List<Object> elements) {
                        log.warn(">>>>>> print log :{}", elements);
                    }
                });


        env.execute();

    }


}