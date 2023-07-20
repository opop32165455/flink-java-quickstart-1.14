package flink.launch.stream.windowing;

import flink.model.FlinkStreamModel;
import flink.source.TestDataGeneratorSource;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

/**
 * @author zhangxuecheng
 * @package flink.launch.stream.windowing
 * @className WindowDemoApp
 * @description window demo
 * @date 2023/6/26 9:40
 */
@Slf4j
public class WindowJoinDemoApp extends FlinkStreamModel {

    public static void main(String[] args) throws Exception {
        initEnv(args);

        val testSourceDs1 = env.addSource(new TestDataGeneratorSource(2, 1000));
        val testSourceDs2 = env.addSource(new TestDataGeneratorSource(2, 1000));

        var stream1 = testSourceDs1
                .assignTimestampsAndWatermarks(
                        //watermark允许十秒延迟
                        WatermarkStrategy.<Tuple3<String, Integer, Date>>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner(
                                        //指定f2为事件时间
                                        (SerializableTimestampAssigner<Tuple3<String, Integer, Date>>) (element, recordTimestamp) -> element.f2.getTime()
                                )
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .trigger(EventTimeTrigger.create())
                .process(new ProcessAllWindowFunction<Tuple3<String, Integer, Date>, Tuple3<String, Integer, Date>, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<Tuple3<String, Integer, Date>, Tuple3<String, Integer, Date>, TimeWindow>.Context context, Iterable<Tuple3<String, Integer, Date>> elements, Collector<Tuple3<String, Integer, Date>> out) throws Exception {
                        for (Tuple3<String, Integer, Date> element : elements) {
                            out.collect(element);
                        }
                    }
                });

        testSourceDs2.keyBy(t -> t.f1)
                .intervalJoin(stream1.keyBy(t -> t.f1))
                .between(Time.seconds(-10), Time.seconds(0))
                .process(new ProcessJoinFunction<Tuple3<String, Integer, Date>, Tuple3<String, Integer, Date>, Object>() {
                    @Override
                    public void processElement(Tuple3<String, Integer, Date> left, Tuple3<String, Integer, Date> right, ProcessJoinFunction<Tuple3<String, Integer, Date>, Tuple3<String, Integer, Date>, Object>.Context ctx, Collector<Object> out) throws Exception {

                    }
                })
                .print();


        env.execute();

    }


}