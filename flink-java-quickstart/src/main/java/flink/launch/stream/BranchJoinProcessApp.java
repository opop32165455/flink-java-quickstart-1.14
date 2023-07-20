package flink.launch.stream;

import com.alibaba.fastjson2.JSONObject;
import flink.model.FlinkStreamModel;
import flink.pojo.TestBean;
import flink.sink.Log4jPrintSink;
import flink.source.DateGenUtil;
import flink.transform.AbstractProcessingTimeBranchFunc;
import flink.transform.BranchSimpleJoinDorisFunc;
import flink.utils.DimJoinUtil;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * 一堆数据
 * <p>
 * 用例: 读取数据找到正确的json数据打印 不正确的输出
 * todo IDEA run main方法时 need [add IDEA provided选项 ] AND [add -local local to program argument]
 *
 * @author zhangxuecheng4441
 * @date 2022/3/13/013 15:21
 */
@Slf4j
public class BranchJoinProcessApp extends FlinkStreamModel {


    public static void main(String[] args) throws Exception {
        initEnv(args);

        val generatorDsV1 = env.addSource(DateGenUtil.getDataSource(1, 1000, (DateGenUtil.TSupplier<TestBean>) () -> TestBean.builder()
                                .id(DateGenUtil.randomDataGenerator.nextHexString(5))
                                .number(DateGenUtil.randomDataGenerator.nextInt(1, 4))
                                .build()
                        )
                )
                .returns(TypeInformation.of(TestBean.class))
                .setParallelism(1);

        generatorDsV1.print();

        generatorDsV1.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new ProcessAllWindowFunction<TestBean, TestBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<TestBean, TestBean, TimeWindow>.Context context, Iterable<TestBean> elements, Collector<TestBean> out) throws Exception {
                        DimJoinUtil.simpleLargeJoin(elements,
                                TestBean::getNumber,
                                "name",
                                "select * from table ",
                                (element,json)->{
                                    element.setDesc(json.getString("name"));
                                },
                                out::collect
                                );
                    }
                })
                .addSink(new Log4jPrintSink<>());

        env.execute("BranchJoinProcessApp");
    }
}
