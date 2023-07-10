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
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
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
public class WindowDemoApp extends FlinkStreamModel {

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
                .keyBy(tuple -> tuple.f1)
                //todo sliding滑动  Tumbling滚动 根据业务情况 使用Event Process time
                //.window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(2)))
                .window(TumblingEventTimeWindows.of(Time.seconds(5), Time.seconds(2)))
                .trigger(EventTimeTrigger.create())
                .process(new ProcessWindowFunction<Tuple3<String, Integer, Date>, Map<String, List<String>>, Integer, TimeWindow>() {

                    private transient ValueState<Long> longState;


                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        //上一次触发时间
                        ValueStateDescriptor<Long> lastTriggerTimeDescriptor = new ValueStateDescriptor<>("lastTriggerTime", Long.class);

                        longState = getRuntimeContext().getState(lastTriggerTimeDescriptor);
                    }

                    @Override
                    public void process(Integer key, ProcessWindowFunction<Tuple3<String, Integer, Date>, Map<String, List<String>>, Integer, TimeWindow>.Context context, Iterable<Tuple3<String, Integer, Date>> elements, Collector<Map<String, List<String>>> out) throws Exception {
                        List<String> nameList = CollUtil.newArrayList(elements)
                                .stream()
                                .map(tuple -> tuple.f0 + "-" + DateUtil.date(tuple.f2))
                                .map(String::valueOf)
                                .collect(Collectors.toList());
                        if (longState.value() == null) {
                            longState.update(1L);
                            log.info("key:{} >>>>>> change long 0----->1", key);
                        } else {
                            Long value = longState.value();
                            long update = value + nameList.size();
                            longState.update(update);
                            log.info("key:{} >>>>>> change long {}----->{}", key, value, update);
                        }

                        out.collect(MapUtil.of(String.valueOf(key), nameList));
                    }
                })

                .addSink(new GenericAbstractBranchSink<Map<String, List<String>>>(1) {
                    @Override
                    public void flush(List<Map<String, List<String>>> elements) {
                        log.warn(">>>>>> print log :{}", elements);
                    }
                });


        env.execute();

    }


}