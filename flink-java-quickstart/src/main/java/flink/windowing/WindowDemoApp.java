package flink.windowing;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.json.JSONObject;
import flink.model.FlinkStreamModel;
import flink.sink.GenericAbstractSink;
import flink.source.TestDataGeneratorSource;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author zhangxuecheng
 * @package flink.windowing
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

        testSourceDs.map(tuple -> new JSONObject().putOnce("name", tuple.f0).putOnce("number", tuple.f1))
                .keyBy(json -> String.valueOf(json.get("number")))
                //todo sliding滑动  Tumbling滚动 根据业务情况 使用Event Process time
                .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(2)))
                .process(new ProcessWindowFunction<JSONObject, Map<String, List<String>>, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<JSONObject, Map<String, List<String>>, String, TimeWindow>.Context context, Iterable<JSONObject> elements, Collector<Map<String, List<String>>> out) throws Exception {

                        List<String> nameList = CollUtil.newArrayList(elements)
                                .stream()
                                .map(json -> json.get("name"))
                                .map(String::valueOf)
                                .collect(Collectors.toList());

                        out.collect(MapUtil.of(key, nameList));
                    }
                })
                .addSink(new GenericAbstractSink<Map<String, List<String>>>(1) {
                    @Override
                    public void flush(List<Map<String, List<String>>> elements) {
                        log.warn(">>>>>> print log :{}", elements);
                    }
                });


        env.execute();

    }


}